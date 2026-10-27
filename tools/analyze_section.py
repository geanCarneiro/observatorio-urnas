#!/usr/bin/env python3
"""Analyze a section log with an allowlist that drops identifying log fields."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import zipfile
import io
import unicodedata
import asn1tools
from rdv import decode_rdv
from rdv import diagnose_rdv_failure
from diagnostics import diagnostic, file_state, sha256_file, summarize_diagnostics
from election_turns import turn_for_pleito
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

EVENTS = {
    "Eleitor foi habilitado": "habilitacao",
    "O voto do eleitor foi computado": "fim_sessao_votacao",
    "Voto confirmado para [Governador]": "voto_confirmado_governador",
    "Voto confirmado para [Presidente]": "voto_confirmado_presidente",
    "Inicio do Encerramento": "inicio_encerramento",
    "Operador selecionou: Encerrar votação": "comando_encerramento",
    "Procedimento de encerramento confirmado": "conclusao_encerramento",
    "Voto confirmado para [<cargo>]": "voto_confirmado_<cargo>",
}
EVENT_KEYWORDS = {
    "deslig": "desligamento",
    "reinici": "reinicializacao",
    "reinicial": "reinicializacao",
    "falha": "falha",
    "erro": "falha",
    "inicializ": "inicializacao",
    "suspens": "suspensao",
    "interrup": "interrupcao",
    "cancel": "interrupcao",
}
POTENTIALLY_RELEVANT_KEYWORDS = (
    "vot", "habilit", "confirm", "comput", "suspens", "interrup",
    "falha", "erro", "cancel", "deslig", "reinici", "reinicial", "encerr",
)
VOTING_EVIDENCE_TYPES = {
    "habilitacao", "voto_confirmado_governador", "voto_confirmado_presidente",
}
SESSION_END_EVENT_TYPES = {"fim_sessao_votacao"}
OPERATIONAL_LIFECYCLE_TYPES = {
    "suspensao", "interrupcao", "falha", "desligamento", "reinicializacao", "inicializacao",
}
ADMINISTRATIVE_EVENT_TYPES = {
    "comando_encerramento", "inicio_encerramento", "conclusao_encerramento",
}
INTERRUPTED_SESSION_STATUSES = {
    "suspensa_sem_voto", "suspensa_com_voto_confirmado", "suspensa_com_voto_atribuido",
    "interrompida_por_falha", "interrompida_por_desligamento",
    "interrompida_por_inicializacao",
}
DEFAULT_CUTS = ("17:00", "17:15", "17:30", "17:45", "18:00", "18:30", "19:00")
BU_SCHEMA_SOURCE = "https://www.tse.jus.br/eleicoes/eleicoes-2026-content/arquivos/formato-arquivos-de-bu-rdv-e-assinatura-digital"
BU_SCHEMA_BUNDLE_SHA256 = "b5ac413e81121d53de2bb1657af611f84a45fdc84388e9d9821872994bbfbafd"
BU_SCHEMA_SHA256 = "ef64bf723f774403b423ed0b617f5a83a3f50c0023d0003e18cd29170404ab81"
RDV_SCHEMA_SHA256 = "282dcde0c7ba1f1bb128fe27443ad8d18f7490416c9546d033910a7ec557a465"


class BUProcessingError(ValueError):
    def __init__(self, code: str, stage: str, error_type: str, cause: str, original: BaseException,
                 context: dict | None = None, recoverability: str = "permanent"):
        super().__init__(cause)
        self.code, self.stage, self.error_type = code, stage, error_type
        self.original, self.context, self.recoverability = original, context or {}, recoverability


def parse_cuts(values: list[str]) -> list[str]:
    for value in values:
        if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value):
            raise ValueError(f"invalid clock cut {value!r}; expected HH:MM")
    return sorted(set(values))


def closure_sequence_matches_observed_order(event_types: list[str]) -> bool:
    """Match the closure sequence observed in official logs.

    The operator may select closure more than once. The log then records one
    or more commands, the confirmation, and finally "Inicio do Encerramento".
    """
    command = "comando_encerramento"
    expected_suffix = ["conclusao_encerramento", "inicio_encerramento"]
    return (len(event_types) >= 3
            and event_types[-2:] == expected_suffix
            and all(event_type == command for event_type in event_types[:-2]))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _event_type(message: str) -> str | None:
    normalized = unicodedata.normalize("NFKD", message.casefold())
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    exact = next((event_type for known_message, event_type in EVENTS.items()
                  if known_message != "Voto confirmado para [<cargo>]"
                  and _normalize_event_message(known_message) == normalized), None)
    if exact:
        return exact
    confirmation = re.fullmatch(r"voto confirmado para \[([^\[\]]+)\]", normalized)
    if confirmation:
        cargo = _cargo_slug(confirmation.group(1))
        if cargo:
            return f"voto_confirmado_{cargo}"
    suspension_null_vote = re.fullmatch(
        r"atribuido voto nulo por suspensao \[([^\[\]]+)\]", normalized.strip())
    if suspension_null_vote:
        cargo = _cargo_slug(suspension_null_vote.group(1))
        if cargo:
            return f"voto_nulo_atribuido_apos_suspensao_{cargo}"
    for keyword, event_type in EVENT_KEYWORDS.items():
        if keyword in normalized:
            return event_type
    if any(keyword in normalized for keyword in POTENTIALLY_RELEVANT_KEYWORDS):
        return "evento_desconhecido_relevante"
    return None


def _normalize_event_message(message: str) -> str:
    normalized = unicodedata.normalize("NFKD", message.casefold())
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _is_cargo_confirmation(event_type: str | None) -> bool:
    return isinstance(event_type, str) and event_type.startswith("voto_confirmado_")


def _is_suspension_assigned_null_vote(event_type: str | None) -> bool:
    return isinstance(event_type, str) and event_type.startswith("voto_nulo_atribuido_apos_suspensao_")


def _is_cargo_vote_event(event_type: str | None) -> bool:
    return _is_cargo_confirmation(event_type) or _is_suspension_assigned_null_vote(event_type)


def _cargo_from_vote_event_type(event_type: str) -> str:
    if _is_cargo_confirmation(event_type):
        return event_type.removeprefix("voto_confirmado_")
    if _is_suspension_assigned_null_vote(event_type):
        return event_type.removeprefix("voto_nulo_atribuido_apos_suspensao_")
    return ""


def _cargo_slug(label: str) -> str:
    normalized = _normalize_event_message(label)
    slug = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")
    aliases = {
        "presidente_da_republica": "presidente",
        "governador_do_estado": "governador",
        "primeiro_suplente_de_senador": "primeiro_suplente_senador",
        "segundo_suplente_de_senador": "segundo_suplente_senador",
    }
    return aliases.get(slug, slug)


def _cargo_confirmation_label(message: str) -> str | None:
    confirmation = re.fullmatch(r"\s*voto confirmado para \[([^\[\]]+)\]\s*", message, re.IGNORECASE)
    return confirmation.group(1).strip() if confirmation else None


def _suspension_assigned_null_vote_label(message: str) -> str | None:
    match = re.fullmatch(
        r"\s*atribu[ií]do voto nulo por suspens[aã]o \[([^\[\]]+)\]\s*", message, re.IGNORECASE)
    return match.group(1).strip() if match else None


BU_CARGO_LABELS = {
    1: "presidente", 2: "vice_presidente", 3: "governador", 4: "vice_governador",
    5: "senador", 6: "deputado_federal", 7: "deputado_estadual", 8: "deputado_distrital",
    9: "primeiro_suplente_senador", 10: "segundo_suplente_senador", 11: "prefeito",
    12: "vice_prefeito", 13: "vereador",
}


def classify_unassigned_event(item: dict) -> dict:
    """Classify why a raw event is unassigned and whether it blocks vote reconciliation."""
    event_type = item.get("event", {}).get("event_type")
    if event_type in OPERATIONAL_LIFECYCLE_TYPES:
        return {"classification": "operational_lifecycle", "reconciliation_relevant": False,
                "reconciliation_impact": "informational",
                "classification_reason": "Lifecycle/operational event without an open voting session; preserved but not itself a vote."}
    if event_type in ADMINISTRATIVE_EVENT_TYPES:
        return {"classification": "administrative", "reconciliation_relevant": False,
                "reconciliation_impact": "informational",
                "classification_reason": "Administrative election/closure event is retained separately from voter-session evidence."}
    if event_type in SESSION_END_EVENT_TYPES:
        return {"classification": "session_termination_without_session", "reconciliation_relevant": True,
                "reconciliation_impact": "blocking",
                "classification_reason": "Session-ending event has no open habilitation to associate with its voting session."}
    if event_type in VOTING_EVIDENCE_TYPES or _is_cargo_vote_event(event_type):
        return {"classification": "voting_evidence_without_session", "reconciliation_relevant": True,
                "reconciliation_impact": "blocking",
                "classification_reason": "Voting evidence without an associated habilitation can affect session and vote interpretation."}
    return {"classification": "potentially_relevant_unknown", "reconciliation_relevant": True,
            "reconciliation_impact": "blocking",
            "classification_reason": "Unknown event semantics are preserved and treated as potentially relevant until classified."}


def _interruption_classification(session: dict) -> tuple[str, dict | None]:
    state_events = session.get("state_events", [])
    if not state_events:
        return "incompleta_indeterminada", None
    classification = {
        "suspensao": ("suspensa_com_voto_confirmado" if session.get("confirmation_events") else
                      "suspensa_com_voto_atribuido" if any(
                          _is_suspension_assigned_null_vote(item.get("event_type"))
                          for item in session.get("computed_vote_events", [])) else "suspensa_sem_voto"),
        "falha": "interrompida_por_falha",
        "desligamento": "interrompida_por_desligamento",
        "reinicializacao": "interrompida_por_desligamento",
        "inicializacao": "interrompida_por_inicializacao",
    }
    for event in state_events:
        status = classification.get(event.get("event_type"))
        if status:
            return status, event
    return "incompleta_indeterminada", state_events[0]


def reconstruct_voting_sessions(events: list[dict]) -> dict:
    """Group cargo vote confirmations into sessions and treat the final marker as session termination."""
    sessions = []
    unassigned = []
    active_by_equipment: dict[str, dict] = {}
    session_numbers = Counter()
    associated_event_count = 0

    def finish(equipment: str, status: str, end_event: dict | None = None,
               closure_trigger_event: dict | None = None) -> dict | None:
        current = active_by_equipment.pop(equipment, None)
        if current is None:
            return None
        current["status"] = status
        current["end_event"] = end_event
        current["closure_trigger_event"] = closure_trigger_event or end_event
        current["completed"] = status == "completed"
        current["session_end_event"] = end_event if status == "completed" else None
        current["completion_after_lifecycle_event"] = bool(current["completed"] and current["state_events"])
        lifecycle_type = current["state_events"][-1].get("event_type") if current["state_events"] else None
        current["lifecycle_before_outcome"] = ({
            "classification": lifecycle_type,
            "event": current["state_events"][-1],
            "followed_by_session_end": bool(current["completed"]),
            "prior_confirmation_count": len(current["confirmation_events"]),
            "prior_confirmation_cargos": sorted({item.get("cargo") for item in current["confirmation_events"]}),
            "prior_computed_vote_count": len(current["computed_vote_events"]),
            "prior_computed_vote_cargos": sorted({item.get("cargo") for item in current["computed_vote_events"]}),
        } if lifecycle_type else None)
        current["outcome"] = ("completed_after_lifecycle_event" if current["completed"] and lifecycle_type
                               else "completed" if current["completed"]
                               else "interrupted" if lifecycle_type
                               else "incomplete")
        current["events_in_log_order"] = sorted(
            [current["habilitation_event"], *current["computed_vote_events"], *current["state_events"]]
            + ([end_event] if end_event else []),
            key=lambda item: (item.get("line_number") is None, item.get("line_number") or 0))
        if status in INTERRUPTED_SESSION_STATUSES or status == "incompleta_indeterminada":
            current["interruption_classification"] = status
        cargos = {"governador", "presidente"}
        cargos.update(event["cargo"] for event in current["confirmation_events"])
        cargos.update(event["cargo"] for event in current["computed_vote_events"])
        current["cargo_confirmations"] = {
            cargo: [event for event in current["confirmation_events"] if event["cargo"] == cargo]
            for cargo in sorted(cargos)
        }
        current["cargo_computed_votes"] = {
            cargo: [event for event in current["computed_vote_events"] if event["cargo"] == cargo]
            for cargo in sorted(cargos)
        }
        sessions.append(current)
        return current

    for event in events:
        equipment = str(event.get("equipment_identifier") or event.get("source_file_sha256") or "unknown")
        kind = event["event_type"]
        if kind == "habilitacao":
            previous = None
            if equipment in active_by_equipment:
                active = active_by_equipment[equipment]
                status, terminal_event = _interruption_classification(active)
                if status == "incompleta_indeterminada" and not active["state_events"]:
                    status = "incompleta_indeterminada"
                    terminal_event = None
                previous = finish(equipment, status, terminal_event, event)
            session_numbers[equipment] += 1
            current = {
                "equipment_identifier": equipment,
                "session_number": session_numbers[equipment],
                "habilitation_event": event,
                "confirmation_events": [],
                "computed_vote_events": [],
                "state_events": [],
                "session_end_event": None,
            }
            if (previous and previous.get("state_events")
                    and previous.get("habilitation_event", {}).get("event_date") == event.get("event_date")
                    and previous.get("computed_vote_events")):
                current["possible_resumption"] = {
                    "classification": "retomada_indeterminada",
                    "possibility": "possible",
                    "identity_status": "indeterminate",
                    "prior_session_number": previous["session_number"],
                    "evidence": {
                        "same_log_stream": True,
                        "prior_interruption_status": previous["status"],
                        "prior_habilitation_line": previous["habilitation_event"].get("line_number"),
                        "prior_cargo_confirmation_lines": [item.get("line_number")
                            for item in previous["confirmation_events"]],
                        "prior_computed_vote_lines": [item.get("line_number")
                            for item in previous["computed_vote_events"]],
                        "interruption_line": (previous.get("end_event") or {}).get("line_number"),
                        "new_habilitation_line": event.get("line_number"),
                    },
                    "identity_link_established": False,
                    "reason": "A new habilitation follows an explicitly interrupted session in the same log stream, but logs contain no elector/session identifier to prove this is the same elector; sessions are not merged.",
                }
            active_by_equipment[equipment] = current
            associated_event_count += 1
        elif _is_cargo_confirmation(kind):
            current = active_by_equipment.get(equipment)
            if current is None:
                unassigned.append({"reason": "confirmation_without_open_habilitation", "event": event})
            else:
                cargo = event.get("cargo") or kind.removeprefix("voto_confirmado_")
                cargo_event = {**event, "cargo": cargo, "recorded_vote_type": "explicit_confirmation"}
                current["confirmation_events"].append(cargo_event)
                current["computed_vote_events"].append(cargo_event)
                associated_event_count += 1
        elif _is_suspension_assigned_null_vote(kind):
            current = active_by_equipment.get(equipment)
            if current is None:
                unassigned.append({"reason": "suspension_vote_assignment_without_open_habilitation", "event": event})
            else:
                cargo = event.get("cargo") or _cargo_from_vote_event_type(kind)
                current["computed_vote_events"].append({**event, "cargo": cargo,
                    "recorded_vote_type": "null_after_suspension"})
                associated_event_count += 1
        elif kind in SESSION_END_EVENT_TYPES:
            current = active_by_equipment.get(equipment)
            if current is None:
                unassigned.append({"reason": "session_termination_without_open_habilitation", "event": event})
            else:
                current["session_end_event"] = event
                finish(equipment, "completed", event)
                associated_event_count += 1
        elif kind in {"suspensao", "interrupcao", "falha", "inicializacao"}:
            current = active_by_equipment.get(equipment)
            if current is None:
                unassigned.append({"reason": "state_event_without_open_habilitation", "event": event})
            else:
                current["state_events"].append(event)
                associated_event_count += 1
        elif kind in {"desligamento", "reinicializacao"}:
            current = active_by_equipment.get(equipment)
            if current is None:
                unassigned.append({"reason": "lifecycle_event_without_open_habilitation", "event": event})
            else:
                current["state_events"].append(event)
                associated_event_count += 1
        else:
            unassigned.append({"reason": "unrecognized_event_without_session_semantics", "event": event})

    for equipment in list(active_by_equipment):
        current = active_by_equipment[equipment]
        status, terminal_event = _interruption_classification(current)
        finish(equipment, status, terminal_event)
    for item in unassigned:
        item.update(classify_unassigned_event(item))
    sessions.sort(key=lambda item: (item["equipment_identifier"], item["session_number"]))
    unassigned_counts = Counter(item["classification"] for item in unassigned)
    relevant_unassigned_count = sum(item["reconciliation_relevant"] for item in unassigned)
    normalized_event_count = len(events)
    unassigned_event_count = len(unassigned)
    return {"sessions": sessions, "unassigned_events": unassigned,
            "unassigned_event_count": unassigned_event_count,
            "unassigned_event_classification_counts": dict(sorted(unassigned_counts.items())),
            "reconciliation_relevant_unassigned_event_count": relevant_unassigned_count,
            "event_accounting": {
                "normalized_event_count": normalized_event_count,
                "associated_event_count": associated_event_count,
                "unassigned_event_count": unassigned_event_count,
                "blocking_unassigned_event_count": relevant_unassigned_count,
                "nonblocking_unassigned_event_count": unassigned_event_count - relevant_unassigned_count,
                "partition_matches_normalized_events": (associated_event_count + unassigned_event_count
                    == normalized_event_count),
                "counting_unit": "deduplicated normalized event records passed to session reconstruction",
            },
            "semantics": "Each explicit cargo confirmation and each cargo-specific null-vote assignment after suspension is preserved as a separate computed-vote event for the indicated cargo. 'O voto do eleitor foi computado' marks session completion and is not an additional cargo vote. Suspension assignments are not reclassified as ordinary confirmations. An unassigned follow-up habilitation is recorded as a possible, indeterminate resumption without merging sessions."}


def summarize_events_by_date(events: list[dict]) -> dict:
    grouped = defaultdict(list)
    for event in events:
        grouped[event["event_date"]].append(event)
    summary = {}
    for event_date, day in sorted(grouped.items()):
        closure = [event for event in day if event["event_type"] in {
            "comando_encerramento", "inicio_encerramento", "conclusao_encerramento"}]
        closure_types = [event["event_type"] for event in closure]
        counts = Counter(event["event_type"] for event in day)
        cuts = Counter()
        for event in day:
            for cut in event.get("at_or_after_clock_cuts", []):
                cuts[f"{event['event_type']}_a_partir_de_{cut.replace(':', 'h')}"] += 1

        def last_of(kind: str):
            return max((event["event_timestamp_raw"] for event in day if event["event_type"] == kind), default=None)

        summary[event_date] = {
            "event_counts": dict(sorted(counts.items())),
            "last_habilitacao_raw": last_of("habilitacao"),
            "last_fim_sessao_votacao_raw": last_of("fim_sessao_votacao"),
            "last_comando_encerramento_raw": last_of("comando_encerramento"),
            "last_inicio_encerramento_raw": last_of("inicio_encerramento"),
            "last_conclusao_encerramento_raw": last_of("conclusao_encerramento"),
            "closure_events_in_log_order": [{"line_number": event["line_number"], "event_type": event["event_type"],
                                              "event_timestamp_raw": event["event_timestamp_raw"]} for event in closure],
            "closure_clock_order": "non_monotonic" if any(a["event_timestamp_raw"] > b["event_timestamp_raw"]
                for a, b in zip(closure, closure[1:])) else "monotonic",
            "closure_semantic_order": "matches_expected" if closure_sequence_matches_observed_order(closure_types)
                else "differs_from_expected",
            "activity_after_clock_cuts": dict(cuts), "relevant_events": day,
        }
    return summary


def presidential_only_session_metrics(sessions: list[dict], event_date: str,
                                     unassigned_events: list[dict] | None = None,
                                     event_date_present: bool = True,
                                     habilitation_event_count: int | None = None) -> dict:
    election_day_sessions = [session for session in sessions
                             if session.get("habilitation_event", {}).get("event_date") == event_date]
    incomplete_session_count = sum(session.get("status") == "incompleta_indeterminada"
                                   for session in election_day_sessions)
    incomplete_without_confirmation_count = sum(
        session.get("status") == "incompleta_indeterminada" and not session.get("confirmation_events")
        for session in election_day_sessions)
    presidential_only_session_count = 0
    presidential_only_confirmation_count = 0
    ambiguous = 0
    for session in election_day_sessions:
        confirmations = [event for event in session.get("confirmation_events", [])
                         if event.get("event_date") == event_date]
        cargos = {event.get("cargo") for event in confirmations}
        presidential_confirmations = [event for event in confirmations if event.get("cargo") == "presidente"]
        if presidential_confirmations and cargos == {"presidente"}:
            presidential_only_session_count += 1
            presidential_only_confirmation_count += len(presidential_confirmations)
            if session.get("possible_resumption"):
                ambiguous += 1
    completed_sessions = [session for session in sessions
        if (session.get("session_end_event") or {}).get("event_date") == event_date]
    unassigned_for_date = [item for item in (unassigned_events or [])
                           if item.get("event", {}).get("event_date") == event_date]
    classified_unassigned = [item if "reconciliation_relevant" in item else
                             {**item, **classify_unassigned_event(item)} for item in unassigned_for_date]
    relevant_unassigned = [item for item in classified_unassigned if item.get("reconciliation_relevant")]
    unassigned_classification_counts = Counter(item.get("classification", "unknown") for item in classified_unassigned)
    interrupted_counts = Counter(session.get("status") for session in election_day_sessions
        if session.get("status") in INTERRUPTED_SESSION_STATUSES)
    session_coverage_matches = (habilitation_event_count is None
                                or habilitation_event_count == len(election_day_sessions))
    return {"count": presidential_only_confirmation_count,
            "presidential_only_confirmation_count": presidential_only_confirmation_count,
            "presidential_only_session_count": presidential_only_session_count,
            "ambiguous_session_count": ambiguous,
            "completed_session_count": len(completed_sessions), "incomplete_session_count": incomplete_session_count,
            "incomplete_without_confirmation_session_count": incomplete_without_confirmation_count,
            "unassigned_event_count": len(unassigned_for_date),
            "unassigned_operational_event_count": len(classified_unassigned) - len(relevant_unassigned),
            "reconciliation_relevant_unassigned_event_count": len(relevant_unassigned),
            "unassigned_event_classification_counts": dict(sorted(unassigned_classification_counts.items())),
            "habilitation_event_count": habilitation_event_count,
            "reconstructed_session_count": len(election_day_sessions),
            "session_coverage_matches_habilitations": session_coverage_matches,
            "interrupted_session_counts": dict(sorted(interrupted_counts.items())),
            "possible_resumption_session_count": sum(bool(session.get("possible_resumption"))
                for session in election_day_sessions),
            "event_date_present": event_date_present,
            "status": "complete" if event_date_present and session_coverage_matches
                and not relevant_unassigned and not incomplete_without_confirmation_count else "incomplete",
            "semantics": "Each presidential confirmation is counted as a computed presidential vote. The count is restricted to reconstructed sessions with presidential confirmations and no confirmation for another cargo; session-end markers, interruptions, and possible-resumption annotations are reported separately and do not erase confirmed votes. No elector identity or TTE-record link is inferred."}


def reconcile_log_metrics(events: list[dict], sessions: list[dict], bu: dict,
                          rdvs: list[dict], bu_date: str,
                          unassigned_events: list[dict] | None = None) -> dict:
    day_events = [event for event in events if event.get("event_date") == bu_date]
    date_present = any(event.get("event_date") == bu_date for event in events)
    event_counts = Counter(event["event_type"] for event in day_events)
    completed_sessions = [session for session in sessions
                         if (session.get("session_end_event") or {}).get("event_date") == bu_date]
    exclusive_presidential = presidential_only_session_metrics(sessions, bu_date, unassigned_events, date_present,
        event_counts.get("habilitacao", 0))
    session_end_count = event_counts.get("fim_sessao_votacao", 0)
    attendance = bu.get("qtd_eleitores_compareceram")
    session_end_status = ("inconclusive" if not date_present or session_end_count != len(completed_sessions) else
                          "matched" if session_end_count == attendance else "divergence")

    rdv_by_cargo = {}
    rdv_bu_by_cargo = {}
    for rdv in rdvs:
        for comparison in rdv.get("reconciliation_with_bu", {}).get("by_cargo", []):
            key = (str(comparison.get("election_id")), comparison.get("cargo_kind"), str(comparison.get("cargo_code")))
            rdv_bu_by_cargo[key] = comparison
        for election in rdv.get("elections", []):
            for cargo in election.get("cargos", []):
                key = (str(election.get("election_id")), cargo.get("cargo_kind"), str(cargo.get("cargo_code")))
                rdv_by_cargo[key] = cargo.get("record_count")

    cargo_rows = []
    confirmations_by_kind = {}
    suspension_null_votes_by_kind = {}
    computed_votes_by_kind = {}
    for kind, count in event_counts.items():
        if not _is_cargo_vote_event(kind):
            continue
        cargo_name = _cargo_from_vote_event_type(kind)
        computed_votes_by_kind[cargo_name] = computed_votes_by_kind.get(cargo_name, 0) + count
        if _is_cargo_confirmation(kind):
            confirmations_by_kind[cargo_name] = count
        else:
            suspension_null_votes_by_kind[cargo_name] = count
    unknown_blockers = sum(item.get("classification") == "potentially_relevant_unknown"
        and item.get("event", {}).get("event_date") == bu_date for item in (unassigned_events or []))
    for election in bu.get("elections", []):
        for result in election.get("results", []):
            for cargo in result.get("cargos", []):
                code = cargo.get("cargo")
                if isinstance(code, int):
                    cargo_name = BU_CARGO_LABELS.get(code)
                    cargo_kind = "cargoConstitucional"
                else:
                    cargo_name = _cargo_slug(str(code))
                    cargo_kind = cargo.get("cargo_kind")
                confirmation_count = event_counts.get(f"voto_confirmado_{cargo_name}", 0) if cargo_name else None
                suspension_null_vote_count = (event_counts.get(
                    f"voto_nulo_atribuido_apos_suspensao_{cargo_name}", 0) if cargo_name else None)
                computed_vote_count = (confirmation_count + suspension_null_vote_count
                    if confirmation_count is not None and suspension_null_vote_count is not None else None)
                rdv_count = rdv_by_cargo.get((str(election.get("id_eleicao")), cargo_kind, str(code)))
                bu_total = cargo.get("qtd_votos_bu")
                rdv_bu = rdv_bu_by_cargo.get((str(election.get("id_eleicao")), cargo_kind, str(code)))
                confirmations_by_kind[cargo_name or str(code)] = confirmation_count
                computed_votes_by_kind[cargo_name or str(code)] = computed_vote_count
                suspension_null_votes_by_kind[cargo_name or str(code)] = suspension_null_vote_count
                available_totals = [value for value in (bu_total, rdv_count) if value is not None]
                cargo_status = ("not_available" if computed_vote_count is None or not available_totals else
                    "inconclusive" if unknown_blockers else
                    "divergence" if any(computed_vote_count != value for value in available_totals) else "matched")
                lifecycle_context = []
                if cargo_name:
                    for session in sessions:
                        if session.get("habilitation_event", {}).get("event_date") != bu_date:
                            continue
                        computed_events = [item for item in session.get("computed_vote_events",
                            session.get("confirmation_events", []))
                            if item.get("cargo") == cargo_name and item.get("event_date") == bu_date]
                        if not computed_events or not session.get("state_events"):
                            continue
                        cargo_events = [item for item in session.get("confirmation_events", [])
                                        if item.get("cargo") == cargo_name and item.get("event_date") == bu_date]
                        assigned_null_votes = [item for item in computed_events
                            if item.get("recorded_vote_type") == "null_after_suspension"]
                        lifecycle_context.append({
                            "session_number": session.get("session_number"),
                            "equipment_identifier": session.get("equipment_identifier"),
                            "session_outcome": session.get("outcome", session.get("status")),
                            "computed_vote_count": len(computed_events),
                            "explicit_confirmation_count": len(cargo_events),
                            "explicit_confirmation_lines": [item.get("line_number") for item in cargo_events],
                            "suspension_assigned_null_vote_count": len(assigned_null_votes),
                            "suspension_assigned_null_vote_lines": [item.get("line_number")
                                for item in assigned_null_votes],
                            "lifecycle_events": [{"event_type": item.get("event_type"),
                                "line_number": item.get("line_number"),
                                "event_timestamp_raw": item.get("event_timestamp_raw")}
                                for item in session.get("state_events", [])],
                            "session_end_line": (session.get("session_end_event") or {}).get("line_number"),
                        })
                lifecycle_confirmation_count = sum(item["explicit_confirmation_count"] for item in lifecycle_context)
                lifecycle_computed_vote_count = sum(item["computed_vote_count"] for item in lifecycle_context)
                cargo_rows.append({
                    "election_id": election.get("id_eleicao"), "cargo": cargo_name or code,
                    "explicit_confirmation_count": confirmation_count,
                    "log_computed_vote_confirmations": confirmation_count,
                    "suspension_assigned_null_vote_count": suspension_null_vote_count,
                    "log_computed_vote_count": computed_vote_count,
                    "confirmations_in_sessions_with_lifecycle_events": lifecycle_confirmation_count,
                    "computed_votes_in_sessions_with_lifecycle_events": lifecycle_computed_vote_count,
                    "lifecycle_context": lifecycle_context,
                    "bu_vote_total": bu_total, "rdv_record_count": rdv_count,
                    "confirmation_count_minus_bu": confirmation_count - bu_total if confirmation_count is not None and bu_total is not None else None,
                    "confirmation_count_minus_rdv": confirmation_count - rdv_count if confirmation_count is not None and rdv_count is not None else None,
                    "computed_vote_count_minus_bu": computed_vote_count - bu_total if computed_vote_count is not None and bu_total is not None else None,
                    "computed_vote_count_minus_rdv": computed_vote_count - rdv_count if computed_vote_count is not None and rdv_count is not None else None,
                    "rdv_bu_reconciliation_status": rdv_bu.get("status") if rdv_bu else "not_available",
                    "rdv_minus_bu": rdv_bu.get("divergence_rdv_minus_bu") if rdv_bu else None,
                    "comparison_status": cargo_status,
                    "reason": ("Unclassified potentially relevant events prevent a complete cargo comparison." if cargo_status == "inconclusive" else
                        "No comparable BU/RDV cargo total is available." if cargo_status == "not_available" else
                        "Computed log votes by cargo combine explicit confirmations and cargo-specific null assignments after suspension. The two event categories remain separate; session-end markers do not add votes."),
                })
    rdv_statuses = [comparison.get("status") for comparison in rdv_bu_by_cargo.values()]
    cargo_statuses = [row["comparison_status"] for row in cargo_rows]
    overall_status = ("divergence" if session_end_status == "divergence" or "divergence" in rdv_statuses
                          or "divergence" in cargo_statuses else
                      "inconclusive" if session_end_status == "inconclusive" or "inconclusive" in cargo_statuses
                          or exclusive_presidential.get("status") != "complete" else
                      "partial" if ("partial" in rdv_statuses or "no_matching_bu_cargo" in rdv_statuses
                                     or (cargo_rows and not rdv_statuses)) else "matched")
    return {
        "status": overall_status,
        "session_end_vs_bu_attendance_status": session_end_status,
        "event_date": bu_date,
        "event_date_present": date_present,
        "by_metric": {
            "habilitacoes": {"log_event_count": event_counts.get("habilitacao", 0),
                             "bu_attendance": attendance, "comparison_status": "not_compared",
                             "reason": "Habilitation is not equivalent to a computed vote or attendance."},
            "fim_de_sessao_vs_bu_comparecimento": {"log_session_end_count": session_end_count,
                "reconstructed_completed_session_count": len(completed_sessions), "bu_attendance": attendance,
                "difference_log_minus_bu": session_end_count - attendance if date_present else None,
                "status": session_end_status, "event_date_present": date_present,
                "reason": ("No timestamped log events were found on the BU election date." if not date_present else
                           "Some session-end events could not be assigned to reconstructed habilitation sessions." if session_end_count != len(completed_sessions) else None)},
            "votos_computados_confirmados_por_cargo": confirmations_by_kind,
            "votos_nulos_atribuidos_apos_suspensao_por_cargo": suspension_null_votes_by_kind,
            "votos_computados_por_cargo": computed_votes_by_kind,
            "votos_confirmados_exclusivamente_presidente": exclusive_presidential,
            "suspensoes": event_counts.get("suspensao", 0),
            "interrupcoes": event_counts.get("interrupcao", 0),
            "falhas": event_counts.get("falha", 0),
        },
        "by_cargo": cargo_rows,
        "rdv_bu_by_cargo": [comparison for rdv in rdvs
                             for comparison in rdv.get("reconciliation_with_bu", {}).get("by_cargo", [])],
        "limitations": "Aggregate log/BU/RDV counts do not link individual vote records to electors or sessions.",
    }


def decode_bu(bu_path: Path, schema_path: Path) -> dict:
    raw = bu_path.read_bytes()
    try:
        spec = asn1tools.compile_files(str(schema_path), codec="ber")
    except Exception as error:
        raise BUProcessingError("BU_SCHEMA_COMPILE_FAILED", "schema_load", "internal",
            "The configured BU ASN.1 schema could not be loaded.", error,
            {"schema_path": str(schema_path), "schema_sha256": sha256_file(schema_path) if schema_path.exists() else None},
            "indeterminate") from error
    try:
        envelope = spec.decode("EntidadeEnvelopeGenerico", raw)
    except Exception as error:
        raise BUProcessingError("BU_ENVELOPE_DECODE_FAILED", "asn1_envelope_decode", "decoding",
            "The file could not be decoded as the official generic ASN.1 envelope.", error,
            {"bytes_read": len(raw), "first_tag_hex": raw[:1].hex() if raw else None}) from error
    if envelope.get("tipoEnvelope") != "envelopeBoletimUrna":
        error = ValueError(f"expected envelopeBoletimUrna, received {envelope.get('tipoEnvelope')}")
        raise BUProcessingError("BU_ENVELOPE_TYPE_UNSUPPORTED", "format_validation", "format_incompatibility",
            str(error), error, {"detected_envelope_type": envelope.get("tipoEnvelope")})
    if "seguranca" in envelope:
        error = ValueError("BU envelope includes security/encryption metadata but no decryption key is configured")
        raise BUProcessingError("BU_ENCRYPTED_UNSUPPORTED", "decryption", "decryption", str(error), error,
            {"envelope_metadata_decoded": True, "encrypted_content_bytes": len(envelope.get("conteudo", b""))}, "indeterminate")
    try:
        bu = spec.decode("EntidadeBoletimUrna", envelope["conteudo"])
    except Exception as error:
        raise BUProcessingError("BU_CONTENT_DECODE_FAILED", "asn1_content_decode", "decoding",
            "The envelope was decoded, but its content did not match EntidadeBoletimUrna.", error,
            {"envelope_type": envelope["tipoEnvelope"], "content_bytes": len(envelope.get("conteudo", b"")),
             "envelope_partial_interpretation": {key: value for key, value in envelope.items() if key != "conteudo"}}) from error
    try:
        election_date = datetime.strptime(bu["dataHoraEmissao"], "%Y%m%dT%H%M%S").strftime("%Y-%m-%d")
    except (ValueError, TypeError) as error:
        raise BUProcessingError("BU_EMISSION_TIMESTAMP_INVALID", "semantic_validation", "semantic_validation",
            "BU dataHoraEmissao does not match the official YYYYMMDDThhmmss form.", error,
            {"dataHoraEmissao": bu.get("dataHoraEmissao"), "bu_content_decoded": True}) from error
    section_data = bu["dadosSecaoSA"]
    if section_data[0] != "dadosSecao":
        section_times = None
    else:
        section_times = section_data[1]

    elections = []
    for election in bu["resultadosVotacaoPorEleicao"]:
        results = []
        for result in election["resultadosVotacao"]:
            cargos = []
            for cargo in result["totaisVotosCargo"]:
                code_kind, code_value = cargo["codigoCargo"]
                vote_total = 0
                vote_types = Counter()
                nominal_by_number = Counter()
                legend_by_number = Counter()
                for vote in cargo["votosVotaveis"]:
                    count = vote["quantidadeVotos"]
                    vote_total += count
                    vote_type = vote["tipoVoto"]
                    vote_types[vote_type] += count
                    identification = vote.get("identificacaoVotavel")
                    if identification and vote_type == "nominal":
                        nominal_by_number[str(identification["codigo"])] += count
                    elif identification and vote_type == "legenda":
                        legend_by_number[str(identification["partido"])] += count
                cargos.append({"cargo_kind": code_kind, "cargo": code_value,
                               "qtd_votos_bu": vote_total,
                               "vote_type_counts": dict(vote_types),
                               "nominal_by_number": dict(nominal_by_number),
                               "legend_by_number": dict(legend_by_number)})
            results.append({"qtd_comparecimento": result["qtdComparecimento"], "cargos": cargos})
        elections.append({"id_eleicao": election["idEleicao"], "qtd_eleitores_aptos": election["qtdEleitoresAptos"], "results": results})

    attendance_detail = bu.get("detalhamentoComparecimento")
    if attendance_detail:
        detail = {key: value for key, value in attendance_detail.items()}
        detail_sum = sum(detail.values())
    else:
        detail, detail_sum = None, None
    urna_type = bu["urna"]["tipoUrna"]
    load_codes = bu.get("historicoCodigosCarga", [])
    electoral_identifier = bu.get("cabecalho", {}).get("idEleitoral")
    electoral_identifier_kind, electoral_identifier_value = electoral_identifier if electoral_identifier else (None, None)
    pleito_id = electoral_identifier_value if electoral_identifier_kind == "idPleito" else None
    data = {
        "phase": envelope["fase"], "envelope_type": envelope["tipoEnvelope"],
        "electoral_identifier": {"kind": electoral_identifier_kind, "value": electoral_identifier_value},
        "pleito_id": pleito_id, "turno": turn_for_pleito(pleito_id),
        "section": bu["identificacaoSecao"], "emitted_at_raw": bu["dataHoraEmissao"],
        "election_date": election_date, "urn_type": urna_type,
        "qtd_eleitores_compareceram": bu["qtdEleitoresCompareceram"],
        "attendance_detail": detail, "attendance_detail_sum": detail_sum,
        "elections": elections,
        "load_history_entries": len(load_codes),
        "unique_load_codes": len(set(load_codes)),
        "section_times": section_times,
    }
    return data


def analyze(log_path: Path, bu_path: Path, collection_path: Path, cuts: list[str], schema_path: Path,
            rdv_paths: list[Path] | None = None, rdv_schema_path: Path = Path("schemas/rdv.asn1")) -> dict:
    collection = json.loads(collection_path.read_text(encoding="utf-8"))
    metadata = next((f for f in collection["files"] if f["type"] == "log" and f["name"] == log_path.name), None)
    bu_metadata = next((f for f in collection["files"] if f["type"] == "bu" and f["name"] == bu_path.name), None)
    if not metadata or not bu_metadata:
        raise ValueError("input files are not listed in the acquisition manifest")

    parsed_log = process_log_file(log_path, metadata, collection["section_key"], cuts)
    events = parsed_log["events"]
    counts_by_date = parsed_log.get("event_counts_by_date", {})
    by_date = parsed_log.get("events_by_date", {})
    substitution_keyword_matches = parsed_log.get("substitution_keyword_matches", 0)

    bu = decode_bu(bu_path, schema_path)
    rdvs = [decode_rdv(path, collection_path, rdv_schema_path, bu) for path in (rdv_paths or [])]
    bu_section = bu["section"]
    municipio_zona = bu_section["municipioZona"]
    bu_identity = {"phase": bu["phase"], "envelope_type": bu["envelope_type"],
                   "municipio": municipio_zona["municipio"], "zona": municipio_zona["zona"],
                   "secao": bu_section["secao"]}
    expected = collection["section_key"].split("/")
    identity_matches = (str(bu_identity["municipio"]) == expected[1]
                        and f'{bu_identity["zona"]:04d}' == expected[2]
                        and f'{bu_identity["secao"]:04d}' == expected[3])
    event_counts = Counter(counts_by_date.get(bu["election_date"], {}))
    event_date_present = bu["election_date"] in counts_by_date
    session_reconstruction = reconstruct_voting_sessions(events)
    reconciliation = reconcile_log_metrics(events, session_reconstruction["sessions"], bu, rdvs,
        bu["election_date"], session_reconstruction["unassigned_events"])
    detail_sum_matches = (bu["attendance_detail_sum"] is None or
                          bu["attendance_detail_sum"] == bu["qtd_eleitores_compareceram"])
    section_times = bu["section_times"] or {}
    closing_raw = section_times.get("dataHoraEncerramento")
    closing_event_raw = (datetime.strptime(closing_raw, "%Y%m%dT%H%M%S").strftime("%d/%m/%Y %H:%M:%S")
                         if closing_raw else None)
    completion_matches = any(e["event_type"] == "conclusao_encerramento" and
                             e["event_timestamp_raw"] == closing_event_raw for e in events)
    bu_time_validation = {
        "status": "clock_agreement" if completion_matches else "clock_disagreement",
        "opening_raw": section_times.get("dataHoraAbertura"),
        "closing_raw": closing_raw,
        "log_closure_matches_bu_closing": completion_matches,
        "timezone": "timezone/UTC offset remains unvalidated; same-device clock agreement does not identify a timezone",
    }
    substitution_signal = (bu["urn_type"] in {"contingencia", "reservaSecao", "reservaEncerrandoSecao"}
                          or bu["unique_load_codes"] > 1 or substitution_keyword_matches > 0)
    substitution_validation = {
        "status": "review_signal" if substitution_signal else "no_signal_detected_in_available_fields",
        "urn_type": bu["urn_type"],
        "manifest_hash_entries": len(collection.get("manifest_payload_unverified", {}).get("hashes", [])),
        "load_history_entries": bu["load_history_entries"],
        "unique_load_codes": bu["unique_load_codes"],
        "log_keyword_signal_count": substitution_keyword_matches,
        "scope_note": "absence of a signal is not proof that no substitution occurred",
    }
    manifest_path = collection_path.parent / "manifest.aux.jws"
    manifest_matches = (manifest_path.exists() and collection.get("manifest_sha256") and
                        sha256(manifest_path) == collection["manifest_sha256"])
    log_matches = sha256(log_path) == metadata.get("sha256")
    bu_matches = sha256(bu_path) == bu_metadata.get("sha256")
    if not detail_sum_matches and reconciliation["status"] == "matched":
        reconciliation["status"] = "divergence"
        reconciliation["by_metric"]["bu_attendance_detail_sum"] = {
            "sum": bu["attendance_detail_sum"], "bu_attendance": bu["qtd_eleitores_compareceram"],
            "difference": bu["attendance_detail_sum"] - bu["qtd_eleitores_compareceram"], "status": "divergence"}
    return {
        "report_schema_version": 5,
        "section_key": collection["section_key"],
        "acquisition_id": collection.get("acquired_at_utc"),
        "source_files": {"manifest_sha256": collection.get("manifest_sha256"),
                         "log_sha256": metadata.get("sha256"), "bu_sha256": bu_metadata.get("sha256"),
                         "rdv_sha256": [item.get("source", {}).get("sha256") for item in rdvs]},
        "bu_schema": {"source": BU_SCHEMA_SOURCE, "bundle_sha256": BU_SCHEMA_BUNDLE_SHA256,
                      "schema_sha256": sha256(schema_path), "expected_schema_sha256": BU_SCHEMA_SHA256,
                      "matches_official_copy": sha256(schema_path) == BU_SCHEMA_SHA256},
        "bu_attendance": {"total": bu["qtd_eleitores_compareceram"], "detail": bu["attendance_detail"],
                          "detail_sum": bu["attendance_detail_sum"], "detail_sum_matches_total": detail_sum_matches,
                          "urn_type": bu["urn_type"]},
        "validation": {
            "integrity": {"manifest_sha256_matches_collection": bool(manifest_matches),
                           "log_sha256_matches_collection": log_matches,
                           "bu_sha256_matches_collection": bu_matches,
                           "log_zip_crc": "valid", "jws_signature": "unverified"},
            "timezone": {"status": "unvalidated", "reason": "log timestamps contain no timezone; no external clock reference has been authenticated"},
            "bu_clock_crosscheck": bu_time_validation,
            "urn_substitution": substitution_validation,
            "bu_envelope_identity": {"status": "matched" if identity_matches else "mismatch", **bu_identity},
            "bu_asn1": "decoded",
            "bu_attendance_detail_sum_matches": detail_sum_matches,
            "bu_reconciliation": {**reconciliation, "comparison_semantics":
                "Cargo confirmations are counted directly as computed votes by cargo. 'O voto do eleitor foi computado' is treated as a session-end marker and compared to BU attendance separately. Enablements are reported separately."},
        },
        "clock_cuts_hhmm_raw": cuts,
        "event_semantics": EVENTS,
        "event_category_keywords": EVENT_KEYWORDS,
        "event_count_total": len(events),
        "events_by_date": by_date,
        "voting_sessions": session_reconstruction,
        "log_sources": parsed_log.get("source_files", []),
        "log_diagnostics": parsed_log.get("diagnostics", []),
        "relevant_events": events,
        "rdv": rdvs,
        "privacy": {"individual_log_fields_excluded": True, "raw_message_or_elector_identifier_included": False},
        "limitations": ["log clock timezone and drift unvalidated", "JWS signature unverified", "possible urn replacement screening is not exhaustive", "event counts do not imply semantic equivalence or causality"],
    }


def process_log_file(path: Path, metadata: dict, section: str, cuts: list[str]) -> dict:
    diagnostics = []
    events = []
    counts = Counter()
    after_cuts = Counter()
    counts_by_date = defaultdict(Counter)
    after_cuts_by_date = defaultdict(Counter)
    file_hash = sha256_file(path)
    if file_hash != metadata.get("sha256"):
        diagnostics.append(diagnostic(code="LOG_SHA256_MISMATCH", stage="integrity_validation", operation="verify_sha256",
            error_type="integrity", cause="Downloaded bytes differ from acquisition metadata.", recoverability="permanent",
            section=section, file_type="log", file_name=path.name, file_path=str(path), detected_format="ZIP",
            context={"expected_sha256": metadata.get("sha256"), "observed_sha256": file_hash}))

    sources: dict[str, dict] = {}
    visited: set[str] = set()

    def inspect_archive(raw: bytes, source_name: str, source_path: str, depth: int = 0) -> None:
        source_hash = hashlib.sha256(raw).hexdigest()
        reference = {"source_file_sha256": source_hash, "source_file_name": source_name,
                     "source_path": source_path, "equipment_identifier": Path(source_name).stem,
                     "equipment_identifier_basis": "preserved_source_filename_not_verified_serial"}
        if source_hash in sources:
            sources[source_hash]["source_aliases"].append(reference)
            return
        sources[source_hash] = {"raw": raw, "source_aliases": [reference]}
        if source_hash in visited:
            return
        visited.add(source_hash)
        if depth > 8:
            diagnostics.append(diagnostic(code="LOG_NESTING_DEPTH_LIMIT", stage="decompression", operation="walk_nested_jez",
                error_type="semantic_validation", cause="Nested JEZ archive depth exceeds the limit of 8.",
                recoverability="permanent", section=section, file_type="log", file_name=source_name,
                file_path=str(path), detected_format="ZIP/JEZ", context={"source_path": source_path, "depth": depth}))
            return
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                bad_member = archive.testzip()
                if bad_member:
                    raise ValueError(f"ZIP CRC validation failed for {bad_member}")
                if "logd.dat" in archive.namelist():
                    info = archive.getinfo("logd.dat")
                    if info.file_size > 32 * 1024 * 1024:
                        diagnostics.append(diagnostic(code="LOG_MEMBER_SIZE_LIMIT", stage="semantic_validation", operation="check_member_size",
                            error_type="semantic_validation", cause="logd.dat exceeds the 32 MiB safety limit.",
                            recoverability="permanent", section=section, file_type="log", file_name=source_name,
                            file_path=str(path), detected_format="ZIP/JEZ", context={"source_path": source_path, "declared_bytes": info.file_size}))
                    else:
                        sources[source_hash]["text_data"] = archive.read("logd.dat").decode("latin1")
                for member_name in archive.namelist():
                    if member_name.lower().endswith(".jez") and not member_name.endswith("/"):
                        nested_path = f"{source_path}!/{member_name}"
                        try:
                            nested_raw = archive.read(member_name)
                        except (OSError, RuntimeError, zipfile.BadZipFile) as error:
                            diagnostics.append(diagnostic(code="LOG_NESTED_JEZ_READ_FAILED", stage="decompression", operation="read_nested_jez",
                                error_type="decompression", cause=str(error), recoverability="permanent", section=section,
                                file_type="log", file_name=Path(member_name).name, file_path=str(path), exception=error,
                                detected_format="ZIP/JEZ", context={"source_path": nested_path}))
                            continue
                        inspect_archive(nested_raw, Path(member_name).name, nested_path, depth + 1)
        except (zipfile.BadZipFile, OSError, EOFError, RuntimeError, ValueError) as error:
            code = "LOG_ZIP_OPEN_FAILED" if depth == 0 else "LOG_NESTED_JEZ_INVALID"
            diagnostics.append(diagnostic(code=code, stage="decompression", operation="open_log_archive",
                error_type="decompression", cause=str(error), recoverability="permanent", section=section,
                file_type="log", file_name=source_name, file_path=str(path), exception=error, detected_format="ZIP/JEZ",
                context={"source_path": source_path, "size_bytes": len(raw), "sha256": source_hash}))

    try:
        inspect_archive(path.read_bytes(), path.name, path.name)
    except OSError as error:
        diagnostics.append(diagnostic(code="LOG_ZIP_OPEN_FAILED", stage="decompression", operation="read_log_archive",
            error_type="decompression", cause=str(error), recoverability="permanent", section=section,
            file_type="log", file_name=path.name, file_path=str(path), exception=error, detected_format="ZIP",
            context={"size_bytes": path.stat().st_size if path.exists() else None, "sha256": file_hash}))

    substitution_matches = 0
    parsed_streams: dict[str, str] = {}
    raw_record_count = 0
    parseable_record_count = 0
    malformed_record_count = 0
    unclassified_record_count = 0
    recognized_event_candidate_count = 0
    invalid_event_timestamp_count = 0
    has_any_logd = any("text_data" in source for source in sources.values())
    for source_hash, source in sources.items():
        text_data = source.get("text_data")
        if text_data is None:
            if source_hash == file_hash and not has_any_logd:
                diagnostics.append(diagnostic(code="LOG_MEMBER_MISSING", stage="format_validation", operation="locate_log_member",
                    error_type="format_incompatibility", cause="ZIP has no logd.dat member.", recoverability="permanent",
                    section=section, file_type="log", file_name=path.name, file_path=str(path), detected_format="ZIP",
                    context={"source_path": path.name}))
            continue
        stream_hash = hashlib.sha256(text_data.encode("latin1")).hexdigest()
        source["logd_sha256"] = stream_hash
        if stream_hash in parsed_streams:
            primary_hash = parsed_streams[stream_hash]
            primary = sources[primary_hash]
            aliases = primary["source_aliases"]
            for alias in source["source_aliases"]:
                if alias not in aliases:
                    aliases.append(alias)
            source["duplicate_of_logd_sha256"] = stream_hash
            continue
        parsed_streams[stream_hash] = source_hash
        source["raw_record_count"] = 0
        source["parseable_record_count"] = 0
        source["malformed_record_count"] = 0
        source["unclassified_record_count"] = 0
        source["invalid_event_timestamp_count"] = 0
        for line_number, raw_line in enumerate(text_data.splitlines(), 1):
            raw_record_count += 1
            source["raw_record_count"] += 1
            fields = raw_line.split("\t")
            if len(fields) < 5:
                malformed_record_count += 1
                source["malformed_record_count"] += 1
                diagnostics.append(diagnostic(code="LOG_LINE_MALFORMED", stage="parsing", operation="parse_log_record",
                    error_type="parsing", cause=f"Expected at least five tab-separated fields, found {len(fields)}.",
                    recoverability="permanent", section=section, file_type="log", file_name=path.name, file_path=str(path),
                    position={"line": line_number}, detected_format="ZIP/logd.dat",
                    context={"field_count": len(fields), "source_path": source["source_aliases"][0]["source_path"]}))
                continue
            parseable_record_count += 1
            source["parseable_record_count"] += 1
            message = fields[4]
            message_lower = message.lower()
            if any(term in message_lower for term in ("contingência", "substituição", "urna substituta", "troca de urna", "recuperação dos dados")):
                substitution_matches += 1
            event_type = _event_type(message)
            if event_type is None:
                unclassified_record_count += 1
                source["unclassified_record_count"] += 1
                continue
            recognized_event_candidate_count += 1
            try:
                timestamp = datetime.strptime(fields[0], "%d/%m/%Y %H:%M:%S")
            except ValueError as error:
                invalid_event_timestamp_count += 1
                source["invalid_event_timestamp_count"] += 1
                diagnostics.append(diagnostic(code="LOG_EVENT_TIMESTAMP_INVALID", stage="parsing", operation="parse_event_timestamp",
                    error_type="parsing", cause=str(error), recoverability="permanent", section=section,
                    file_type="log", file_name=path.name, file_path=str(path), exception=error,
                    position={"line": line_number}, detected_format="ZIP/logd.dat",
                    context={"event_type": event_type, "timestamp_raw": fields[0], "source_path": source["source_aliases"][0]["source_path"]}))
                continue
            hhmm = timestamp.strftime("%H:%M")
            aliases = source["source_aliases"]
            event = {"source_file_sha256": source_hash,
                "source_file_name": aliases[0]["source_file_name"],
                "source_path": aliases[0]["source_path"],
                "source_aliases": aliases,
                "logd_sha256": stream_hash,
                "equipment_identifier": f"log-stream-sha256:{stream_hash}",
                "equipment_identification_basis": "decompressed logd.dat hash; source aliases are retained, hardware serial is not present in parsed fields",
                "line_number": line_number, "event_type": event_type,
                "event_timestamp_raw": fields[0], "event_date": timestamp.strftime("%Y-%m-%d"),
                "event_time": hhmm, "at_or_after_clock_cuts": [cut for cut in cuts if hhmm >= cut],
                "event_timezone": "unknown", "event_timestamp_utc": None,
                "event_timestamp_brt": None, "parser_status": "allowlist_match"}
            cargo_label = _cargo_confirmation_label(message)
            if cargo_label is not None:
                event["cargo"] = _cargo_slug(cargo_label)
                event["cargo_label"] = cargo_label
            elif event_type and _is_suspension_assigned_null_vote(event_type):
                cargo_label = _suspension_assigned_null_vote_label(message)
                event["cargo"] = _cargo_slug(cargo_label) if cargo_label else _cargo_from_vote_event_type(event_type)
                event["cargo_label"] = cargo_label
                event["recorded_vote_type"] = "null_after_suspension"
                event["message_raw"] = message
            if event_type == "evento_desconhecido_relevante":
                event["message_raw"] = message
                event["parser_status"] = "potentially_relevant_unclassified"
            events.append(event)
            counts[event_type] += 1
            event_date = timestamp.strftime("%Y-%m-%d")
            counts_by_date[event_date][event_type] += 1
            if event_type == "habilitacao" or event_type == "fim_sessao_votacao" or _is_cargo_vote_event(event_type):
                for cut in cuts:
                    if hhmm >= cut:
                        key = f"{event_type}_a_partir_de_{cut.replace(':', 'h')}"
                        after_cuts[key] += 1
                        after_cuts_by_date[event_date][key] += 1

    sessions = reconstruct_voting_sessions(events)
    by_date = summarize_events_by_date(events)
    return {"status": "processed_with_diagnostics" if diagnostics else "processed", "events": events,
        "diagnostics": diagnostics, "event_counts": dict(counts), "activity_after_clock_cuts": dict(after_cuts),
        "event_counts_by_date": {date: dict(day) for date, day in counts_by_date.items()},
        "activity_after_clock_cuts_by_date": {date: dict(day) for date, day in after_cuts_by_date.items()},
        "events_by_date": by_date, "voting_sessions": sessions,
        "source_files": [{"sha256": source_hash, "aliases": source["source_aliases"],
                          "contains_logd_dat": "text_data" in source,
                          "logd_sha256": source.get("logd_sha256"),
                          "duplicate_of_logd_sha256": source.get("duplicate_of_logd_sha256"),
                          "raw_record_count": source.get("raw_record_count", 0),
                          "parseable_record_count": source.get("parseable_record_count", 0),
                          "malformed_record_count": source.get("malformed_record_count", 0),
                          "unclassified_record_count": source.get("unclassified_record_count", 0),
                          "invalid_event_timestamp_count": source.get("invalid_event_timestamp_count", 0),
                          "event_count": sum(event["source_file_sha256"] == source_hash for event in events)}
                         for source_hash, source in sources.items()],
        "log_record_accounting": {
            "unique_log_stream_count": len(parsed_streams),
            "raw_record_count": raw_record_count,
            "parseable_record_count": parseable_record_count,
            "malformed_record_count": malformed_record_count,
            "unclassified_record_count": unclassified_record_count,
            "recognized_event_candidate_count": recognized_event_candidate_count,
            "invalid_event_timestamp_count": invalid_event_timestamp_count,
            "normalized_event_count": len(events),
            "parseable_records_without_normalized_event_count": parseable_record_count - len(events),
            "record_partition_matches_parseable_records": (unclassified_record_count
                + invalid_event_timestamp_count + len(events) == parseable_record_count),
            "record_partition_matches_raw_records": parseable_record_count + malformed_record_count == raw_record_count,
            "counting_unit": "records from unique decompressed logd.dat streams; duplicate streams are excluded",
        },
        "nested_archive_count": max(0, len(sources) - 1), "substitution_keyword_matches": substitution_matches,
        "sha256": file_hash, "size_bytes": path.stat().st_size, "detected_format": "ZIP/JEZ",
        "version": None, "urn_model": None}


def log_bu_reconciliation_diagnostics(validation: dict, section: str, log_path: Path,
                                      urn_model: str | None = None) -> list[dict]:
    diagnostics = []
    reconciliation = validation.get("bu_reconciliation", {})
    comparisons = list(reconciliation.get("by_attendance_metric", {}).values())
    comparisons.extend(reconciliation.get("by_metric", {}).values())
    comparisons.extend(reconciliation.get("by_cargo", []))
    for comparison in comparisons:
        if not isinstance(comparison, dict):
            continue
        if "log_confirmations" in comparison and comparison.get("log_confirmations") is None:
            diagnostics.append(diagnostic(code="LOG_BU_RECONCILIATION_INCOMPLETE", stage="reconciliation",
                operation="map_log_event_to_bu_cargo", error_type="reconciliation",
                cause="No allowlisted log event is mapped to this BU cargo.", recoverability="indeterminate",
                section=section, file_type="log", file_name=log_path.name, file_path=str(log_path),
                detected_format="ZIP/logd.dat", urn_model=urn_model, context={"comparison": comparison}))
            continue
        divergences = {key: value for key, value in comparison.items()
                       if (key.startswith("divergence_") or key.startswith("difference_"))
                       and value not in (None, 0)}
        if not divergences:
            if comparison.get("status") == "inconclusive" and comparison.get("reason"):
                diagnostics.append(diagnostic(code="LOG_BU_RECONCILIATION_INCOMPLETE", stage="reconciliation",
                    operation="select_log_events_for_bu_date", error_type="reconciliation",
                    cause=comparison["reason"],
                    recoverability="indeterminate", section=section, file_type="log", file_name=log_path.name,
                    file_path=str(log_path), detected_format="ZIP/logd.dat", urn_model=urn_model,
                    context={"comparison": comparison}))
            continue
        if comparison.get("comparison_status") == "inconclusive":
            continue
        diagnostics.append(diagnostic(code="LOG_BU_RECONCILIATION_DIVERGENCE", stage="reconciliation",
            operation="reconcile_log_bu", error_type="reconciliation",
            cause="Log event counts differ from BU attendance or vote totals.", recoverability="indeterminate",
            section=section, file_type="log", file_name=log_path.name, file_path=str(log_path),
            detected_format="ZIP/logd.dat", urn_model=urn_model, context={"comparison": comparison, "divergences": divergences}))
    return diagnostics


def analyze_collection(collection_path: Path, cuts: list[str], bu_schema_path: Path,
                       rdv_schema_path: Path, selected_logs: list[Path] | None = None,
                       selected_bus: list[Path] | None = None) -> dict:
    collection = json.loads(collection_path.read_text(encoding="utf-8"))
    section = collection.get("section_key")
    entries = collection.get("files", [])
    priority = {"bu": 0, "log": 1, "rdv": 2}
    entries = sorted(entries, key=lambda entry: priority.get(entry.get("type"), 3))
    log_filter = {path.resolve() for path in selected_logs or []}
    bu_filter = {path.resolve() for path in selected_bus or []}
    if log_filter:
        entries = [item for item in entries if item.get("type") != "log" or
                   (collection_path.parent / item.get("relative_path", "")).resolve() in log_filter]
    if bu_filter:
        entries = [item for item in entries if item.get("type") != "bu" or
                   (collection_path.parent / item.get("relative_path", "")).resolve() in bu_filter]
    diagnostics = list(collection.get("diagnostics", []))
    states = []
    for original_state in collection.get("file_states", []):
        state = dict(original_state)
        if state.get("type") == "vota" and str(state.get("name", "")).lower().endswith(".vsc"):
            state["type"] = "vota.vsc"
            state["status"] = "not_applicable"
            state["reason"] = "Signature/validation sidecar is outside the analytical scope; it is ignored by analysis."
        states.append(state)
    manifest_path = collection_path.parent / "manifest.aux.jws"
    signature_codes = {item.get("code") for item in diagnostics if item.get("error_type") == "signature_cryptographic"}
    if collection.get("signature_status") != "verified" and not signature_codes:
        diagnostics.append(diagnostic(code="JWS_SIGNATURE_NOT_PROCESSED", stage="integrity_validation",
            operation="verify_jws_signature", error_type="signature_cryptographic",
            cause="Cryptographic verification has not been completed with a trusted TSE key.",
            recoverability="indeterminate", section=section, file_type="jws", file_name=manifest_path.name,
            file_path=str(manifest_path), detected_format="JWS compact", context={"verification_status": "not_processed"}))
    if manifest_path.exists() and collection.get("manifest_sha256"):
        observed_manifest_hash = sha256_file(manifest_path)
        if observed_manifest_hash != collection["manifest_sha256"]:
            diagnostics.append(diagnostic(code="JWS_SHA256_MISMATCH", stage="integrity_validation", operation="verify_sha256",
                error_type="integrity", cause="Preserved manifest bytes differ from collection metadata.",
                recoverability="permanent", section=section, file_type="jws", file_name=manifest_path.name,
                file_path=str(manifest_path), detected_format="JWS compact",
                context={"expected_sha256": collection["manifest_sha256"], "observed_sha256": observed_manifest_hash}))
    elif not manifest_path.exists() and not any(item.get("type") == "jws" for item in states):
        diagnostics.append(diagnostic(code="ANALYZE_JWS_SOURCE_MISSING", stage="analysis_input", operation="load_manifest_bytes",
            error_type="missing", cause="Preserved JWS bytes are missing from the acquisition directory.",
            recoverability="indeterminate", section=section, file_type="jws", file_name=manifest_path.name,
            file_path=str(manifest_path), detected_format="JWS compact"))
        states.append(file_state(file_type="jws", name=manifest_path.name, section=section, status="missing",
            reason="Preserved JWS bytes are missing.", path=str(manifest_path)))
    processed = []
    log_results = []
    bu_results = []
    rdv_results = []
    for item in entries:
        file_type = item.get("type")
        if file_type == "vota" and str(item.get("name", "")).lower().endswith(".vsc"):
            file_type = "vota.vsc"
        if file_type == "vota.vsc":
            continue
        if file_type not in {"bu", "rdv", "log"}:
            continue
        relative_path = item.get("relative_path")
        if not relative_path:
            diagnostics.append(diagnostic(code=f"ANALYZE_{str(file_type).upper()}_PATH_MISSING", stage="analysis_input",
                operation="resolve_acquired_file", error_type="missing", cause="Acquisition metadata has no relative path.",
                recoverability="indeterminate", section=section, file_type=file_type, file_name=item.get("name"),
                context={"manifest_hash": item.get("manifest_hash")}))
            states.append(file_state(file_type=file_type, name=item.get("name"), section=section, status="not_processed",
                reason="No relative path was recorded."))
            continue
        path = collection_path.parent / relative_path
        if not path.exists():
            missing_diag = diagnostic(code=f"ANALYZE_{str(file_type).upper()}_SOURCE_MISSING", stage="analysis_input",
                operation="load_acquired_file", error_type="missing", cause="Manifest-listed input file is not present in the acquisition directory.",
                recoverability="indeterminate", section=section, file_type=file_type, file_name=item.get("name"),
                file_path=str(path), detected_format=Path(item.get("name", "")).suffix.lstrip("."),
                context={"relative_path": item.get("relative_path"), "manifest_hash": item.get("manifest_hash")})
            diagnostics.append(missing_diag)
            states.append(file_state(file_type=file_type, name=item.get("name"), section=section, status="missing",
                reason=missing_diag["cause"], path=str(path)))
            continue
        if file_type == "log":
            try:
                result = process_log_file(path, item, section, cuts)
            except Exception as error:
                diag = diagnostic(code="LOG_PROCESSING_FAILED", stage="log_processing", operation="process_log_file",
                    error_type="internal", cause=str(error), recoverability="indeterminate", section=section,
                    file_type="log", file_name=path.name, file_path=str(path), exception=error,
                    detected_format="ZIP/unknown", context={"bytes_read": path.stat().st_size if path.exists() else 0})
                result = {"status": "failed", "events": [], "diagnostics": [diag], "event_counts": {},
                    "sha256": sha256_file(path) if path.exists() else None,
                    "size_bytes": path.stat().st_size if path.exists() else None}
            result.update({"type": file_type, "name": item.get("name"), "path": str(path)})
            log_results.append(result)
            diagnostics.extend(result["diagnostics"])
            states.append(file_state(file_type=file_type, name=item.get("name"), section=section, status=result["status"],
                path=str(path), detected_format=result.get("detected_format"), version=result.get("version"),
                urn_model=result.get("urn_model"), sha256=result.get("sha256"), size_bytes=result.get("size_bytes")))
            if result["status"].startswith("processed"):
                processed.append(file_type)
        elif file_type == "bu":
            try:
                file_hash = sha256_file(path)
                data = decode_bu(path, bu_schema_path)
                bu_result = {"status": "processed", "type": file_type, "name": item.get("name"), "path": str(path),
                    "sha256": file_hash, "size_bytes": path.stat().st_size, "detected_format": "ASN.1 BER",
                    "version": BU_SCHEMA_SHA256, "urn_model": data.get("urn_type"), "data": data}
                if item.get("sha256") != file_hash:
                    diag = diagnostic(code="BU_SHA256_MISMATCH", stage="integrity_validation", operation="verify_sha256",
                        error_type="integrity", cause="Downloaded bytes differ from acquisition metadata.", recoverability="permanent",
                        section=section, file_type="bu", file_name=path.name, file_path=str(path), detected_format="ASN.1 BER",
                        version=BU_SCHEMA_SHA256, urn_model=data.get("urn_type"), context={"expected_sha256": item.get("sha256"), "observed_sha256": file_hash})
                    diagnostics.append(diag)
                    bu_result["status"] = "processed_with_diagnostics"
                expected = section.split("/") if isinstance(section, str) else []
                identity = data["section"]
                municipality_zone = identity["municipioZona"]
                if len(expected) == 4 and (str(municipality_zone["municipio"]) != expected[1] or f'{municipality_zone["zona"]:04d}' != expected[2] or f'{identity["secao"]:04d}' != expected[3]):
                    diagnostics.append(diagnostic(code="BU_SECTION_IDENTITY_MISMATCH", stage="semantic_validation", operation="validate_section_identity",
                        error_type="semantic_validation", cause="Decoded BU identity does not match collection section key.", recoverability="permanent",
                        section=section, file_type="bu", file_name=path.name, file_path=str(path), detected_format="ASN.1 BER",
                        version=BU_SCHEMA_SHA256, urn_model=data.get("urn_type"), context={"expected_section": section, "decoded_section": identity}))
                    bu_result["status"] = "processed_with_diagnostics"
                bu_results.append(bu_result)
                processed.append(file_type)
                states.append(file_state(file_type=file_type, name=item.get("name"), section=section, status=bu_result["status"],
                    path=str(path), detected_format="ASN.1 BER", version=BU_SCHEMA_SHA256, urn_model=data.get("urn_type"),
                    sha256=file_hash, size_bytes=path.stat().st_size))
            except Exception as error:
                if isinstance(error, BUProcessingError):
                    code, stage, error_type = error.code, error.stage, error.error_type
                    cause, recoverability, original, context = str(error), error.recoverability, error.original, error.context
                else:
                    code, stage, error_type = "BU_PROCESSING_FAILED", "bu_processing", "internal"
                    cause, recoverability, original, context = str(error), "indeterminate", error, {}
                diagnostics.append(diagnostic(code=code, stage=stage, operation="decode_bu",
                    error_type=error_type, cause=cause, recoverability=recoverability, section=section, file_type="bu",
                    file_name=path.name, file_path=str(path), exception=original, detected_format="ASN.1 BER/unknown",
                    version=BU_SCHEMA_SHA256, context={**context, "size_bytes": path.stat().st_size, "sha256": sha256_file(path),
                        "partial_interpretation": context.get("envelope_partial_interpretation", {"bytes_read": path.stat().st_size, "complete_structural_decode": False})}))
                bu_results.append({"status": "failed", "type": file_type, "name": item.get("name"), "path": str(path),
                    "sha256": sha256_file(path), "size_bytes": path.stat().st_size,
                    "partial_interpretation": {"format_detected": "ASN.1 BER/unknown", "bytes_read": path.stat().st_size, "complete_structural_decode": False}})
                states.append(file_state(file_type=file_type, name=item.get("name"), section=section, status="failed", path=str(path),
                    detected_format="ASN.1 BER/unknown", version=BU_SCHEMA_SHA256, sha256=sha256_file(path), size_bytes=path.stat().st_size))
        else:
            bu_for_rdv_result = None
            try:
                bu_for_rdv_result = next((item for item in bu_results if item.get("data")), None)
                bu_for_rdv = bu_for_rdv_result.get("data") if bu_for_rdv_result else None
                result = decode_rdv(path, collection_path, rdv_schema_path, bu_for_rdv)
            except Exception as error:
                result = diagnose_rdv_failure(path, collection, error, collection_path.parent)
            result.update({"type": file_type, "name": item.get("name"), "path": str(path)})
            rdv_results.append(result)
            diagnostics.extend(result.get("diagnostics", []))
            states.append(file_state(file_type=file_type, name=item.get("name"), section=section, status=result.get("status", "failed"),
                path=str(path), detected_format=result.get("source", {}).get("format_variant") or result.get("partial_interpretation", {}).get("format_detected"),
                version=RDV_SCHEMA_SHA256, urn_model=bu_for_rdv_result.get("urn_model") if bu_for_rdv_result else None,
                sha256=result.get("source", {}).get("sha256"), size_bytes=result.get("source", {}).get("size_bytes")))
            if result.get("status") == "decoded":
                processed.append(file_type)

    # Carry forward explicit absence/applicability states from acquisition; analyzer inputs missing from disk are added above.
    file_types = {state.get("type") for state in states}
    for file_type in ("jws", "bu", "rdv", "log", "vota.vsc", "signature_verification"):
        if file_type not in file_types:
            status = "not_processed" if file_type == "signature_verification" else "missing"
            states.append(file_state(file_type=file_type, name=None, section=section, status=status,
                reason="No acquisition state was recorded for this type."))

    legacy_report = None
    first_log = next((Path(item["path"]) for item in log_results if item["status"].startswith("processed")), None)
    first_bu = next((Path(item["path"]) for item in bu_results if item.get("data")), None)
    if first_log and first_bu:
        try:
            legacy_report = analyze(first_log, first_bu, collection_path, cuts, bu_schema_path)
            all_events_by_key = {}
            for log_result in log_results:
                for event in log_result.get("events", []):
                    key = (event.get("logd_sha256"), event.get("line_number"))
                    if key not in all_events_by_key:
                        all_events_by_key[key] = event
                    else:
                        existing = all_events_by_key[key]
                        aliases = existing.setdefault("source_aliases", [])
                        for alias in event.get("source_aliases", []):
                            if alias not in aliases:
                                aliases.append(alias)
            all_events = list(all_events_by_key.values())
            sources_by_hash = {}
            for log_result in log_results:
                for source in log_result.get("source_files", []):
                    target = sources_by_hash.setdefault(source["sha256"], dict(source, aliases=[]))
                    for alias in source.get("aliases", []):
                        if alias not in target["aliases"]:
                            target["aliases"].append(alias)
            legacy_report["log_sources"] = list(sources_by_hash.values())
            session_reconstruction = reconstruct_voting_sessions(all_events)
            unique_streams = {}
            for source in sources_by_hash.values():
                stream_hash = source.get("logd_sha256")
                if stream_hash and not source.get("duplicate_of_logd_sha256"):
                    unique_streams.setdefault(stream_hash, source)
            legacy_report["log_record_accounting"] = {
                "unique_log_stream_count": len(unique_streams),
                "raw_record_count": sum(source.get("raw_record_count", 0) for source in unique_streams.values()),
                "parseable_record_count": sum(source.get("parseable_record_count", 0) for source in unique_streams.values()),
                "malformed_record_count": sum(source.get("malformed_record_count", 0) for source in unique_streams.values()),
                "normalized_event_count": len(all_events),
                "unclassified_record_count": sum(source.get("unclassified_record_count", 0)
                    for source in unique_streams.values()),
                "invalid_event_timestamp_count": sum(source.get("invalid_event_timestamp_count", 0)
                    for source in unique_streams.values()),
                "counting_unit": "records from unique decompressed logd.dat streams; duplicate streams and source aliases are excluded",
            }
            legacy_report["voting_sessions"]["event_accounting"] = session_reconstruction["event_accounting"]
            bu_data = next(item["data"] for item in bu_results if item.get("data"))
            combined_reconciliation = reconcile_log_metrics(all_events, session_reconstruction["sessions"],
                bu_data, rdv_results, bu_data["election_date"], session_reconstruction["unassigned_events"])
            if not bu_data["attendance_detail_sum"] is None and bu_data["attendance_detail_sum"] != bu_data["qtd_eleitores_compareceram"]:
                combined_reconciliation["status"] = "divergence"
                combined_reconciliation["by_metric"]["bu_attendance_detail_sum"] = {
                    "sum": bu_data["attendance_detail_sum"], "bu_attendance": bu_data["qtd_eleitores_compareceram"],
                    "difference": bu_data["attendance_detail_sum"] - bu_data["qtd_eleitores_compareceram"],
                    "status": "divergence"}
            legacy_report["validation"]["bu_reconciliation"] = {**combined_reconciliation,
                "comparison_semantics": "Cargo confirmations are counted directly as computed votes by cargo. 'O voto do eleitor foi computado' is treated as a session-end marker and compared to BU attendance separately. Enablements are reported separately."}
            legacy_report["voting_sessions"] = session_reconstruction
            legacy_report["relevant_events"] = all_events
            legacy_report["event_count_total"] = len(all_events)
            legacy_report["events_by_date"] = summarize_events_by_date(all_events)
            diagnostics.extend(log_bu_reconciliation_diagnostics(legacy_report["validation"], section, first_log,
                legacy_report["bu_attendance"].get("urn_type")))
            if not legacy_report["validation"].get("bu_attendance_detail_sum_matches", True):
                diagnostics.append(diagnostic(code="BU_ATTENDANCE_DETAIL_RECONCILIATION_DIVERGENCE", stage="reconciliation",
                    operation="reconcile_bu_attendance_fields", error_type="reconciliation",
                    cause="BU detailed attendance fields do not sum to its attendance total.", recoverability="indeterminate",
                    section=section, file_type="bu", file_name=first_bu.name, file_path=str(first_bu),
                    detected_format="ASN.1 BER", version=BU_SCHEMA_SHA256,
                    urn_model=legacy_report["bu_attendance"].get("urn_type"), context=legacy_report["bu_attendance"]))
        except Exception as error:
            diagnostics.append(diagnostic(code="CROSS_ANALYSIS_FAILED", stage="reconciliation", operation="analyze_log_bu_pair",
                error_type="internal", cause=str(error), recoverability="indeterminate", section=section,
                file_type=None, file_name=None, file_path=None, exception=error, context={"log": str(first_log), "bu": str(first_bu)}))
    for rdv in rdv_results:
        recon = rdv.get("reconciliation_with_bu", {})
        if recon.get("status") == "partial" and not recon.get("by_cargo"):
            diagnostics.append(diagnostic(code="RDV_BU_RECONCILIATION_NOT_RUN", stage="reconciliation",
                operation="reconcile_rdv_bu", error_type="reconciliation",
                cause="No comparable election/cargo rows were available for RDV-to-BU reconciliation.",
                recoverability="indeterminate", section=section, file_type="rdv", file_name=rdv.get("name"),
                file_path=rdv.get("path"), detected_format=rdv.get("format_variant"), version=RDV_SCHEMA_SHA256,
                urn_model=next((item.get("urn_model") for item in bu_results if item.get("data")), None),
                context={"reconciliation_status": "partial"}))
        for comparison in recon.get("by_cargo", []):
            if comparison.get("status") == "no_matching_bu_cargo":
                diagnostics.append(diagnostic(code="RDV_BU_CARGO_UNMATCHED", stage="reconciliation",
                    operation="match_rdv_cargo_to_bu", error_type="reconciliation",
                    cause="RDV cargo has no matching election/cargo key in any successfully decoded BU.",
                    recoverability="indeterminate", section=section, file_type="rdv", file_name=rdv.get("name"),
                    file_path=rdv.get("path"), detected_format=rdv.get("format_variant"), version=RDV_SCHEMA_SHA256,
                    urn_model=bu_results[0].get("urn_model") if bu_results else None, context=comparison))
        if recon.get("status") == "divergence":
            for comparison in recon.get("by_cargo", []):
                if comparison.get("status") == "divergence":
                    diagnostics.append(diagnostic(code="RDV_BU_RECONCILIATION_DIVERGENCE", stage="reconciliation", operation="reconcile_rdv_bu",
                        error_type="reconciliation", cause="RDV vote totals or comparable subtotals differ from the BU.", recoverability="indeterminate",
                        section=section, file_type="rdv", file_name=rdv.get("name"), file_path=rdv.get("path"),
                        detected_format=rdv.get("format_variant"), version=RDV_SCHEMA_SHA256,
                        urn_model=bu_results[0].get("urn_model") if bu_results else None, context=comparison))

    transfer_reconciliation = {"section_key": section, "turno": collection.get("turno"),
        "status": "dados_insuficientes", "reason": "Section BU/log analysis did not produce the required comparison metric."}
    round_number = collection.get("turno")
    try:
        from temporary_transfer import load_reference, reconcile_section
        decoded_bu = next((item.get("data") for item in bu_results if item.get("data")), None)
        round_number = (decoded_bu or {}).get("turno", collection.get("turno"))
        ballot_cargos = sorted({
            str(cargo.get("cargo"))
            for election in (decoded_bu or {}).get("elections", [])
            for result in election.get("results", [])
            for cargo in result.get("cargos", [])
            if cargo.get("cargo") is not None
        }) if decoded_bu else None
        recon = (legacy_report or {}).get("validation", {}).get("bu_reconciliation", {})
        log_metric = recon.get("by_metric", {}).get("votos_confirmados_exclusivamente_presidente")
        event_date = (decoded_bu or {}).get("election_date")
        transfer_reconciliation["turno"] = round_number
        if not isinstance(section, str):
            transfer_reconciliation["reason"] = "Acquisition has no section identity."
        elif round_number != 2:
            transfer_reconciliation["reason"] = "Temporary-transfer comparison is configured only for the second round."
        else:
            transfer_reconciliation = reconcile_section(load_reference(), section, round_number, log_metric,
                                                         event_date, ballot_cargos=ballot_cargos)
    except Exception as error:
        transfer_reconciliation.update({"section_key": section, "turno": round_number,
            "status": "dados_insuficientes",
            "reason": f"The preserved TSE reference could not be loaded: {type(error).__name__}: {error}"})

    diagnostics.extend([])
    successful = [result for result in log_results if result.get("status", "").startswith("processed")]
    successful.extend(result for result in bu_results + rdv_results if result.get("status") in {"processed", "decoded"})
    overall_status = "complete" if not diagnostics and successful and all(state.get("status") in {"processed", "downloaded", "decoded"} for state in states if state.get("type") in {"jws", "bu", "rdv", "log"}) else "partial" if successful else "failed"
    validity = "valid" if overall_status == "complete" and not diagnostics else "not_valid" if overall_status == "failed" else "not_validated"
    report = legacy_report or {"report_schema_version": 4, "section_key": section, "acquisition_id": collection.get("acquired_at_utc")}
    report.update({"report_schema_version": 4, "overall_status": overall_status, "validity": validity,
        "data_id": collection.get("data_id"), "turno": collection.get("turno"),
        "source_files": {"manifest_sha256": collection.get("manifest_sha256"),
            "log_sha256": [item.get("sha256") for item in log_results], "bu_sha256": [item.get("sha256") for item in bu_results],
            "rdv_sha256": [item.get("source", {}).get("sha256") for item in rdv_results]},
        "file_processing": {"logs": log_results, "bus": bu_results, "rdvs": rdv_results,
            "states": states, "unprocessed_types": [s["type"] for s in states if s.get("status") == "not_processed"]},
        "rdv_files": rdv_results, "diagnostics": diagnostics,
        "diagnostic_summary": summarize_diagnostics(diagnostics, states),
        "reconciliation": {"log_bu": (legacy_report or {}).get("validation", {}).get("bu_reconciliation"),
            "rdv_bu": [item.get("reconciliation_with_bu") for item in rdv_results]},
        "transferencia_temporaria": transfer_reconciliation,
        "preservation": {"original_files_retained": True, "partial_files_retained": True,
            "note": "Processing outcomes never upgrade a partial or unverified input to valid."}})
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, action="append", help="limit analysis to this acquired log; repeatable")
    parser.add_argument("--bu", type=Path, action="append", help="limit analysis to this acquired BU; repeatable")
    parser.add_argument("--collection", required=True, type=Path)
    parser.add_argument("--schema", type=Path, default=Path("schemas/bu.asn1"))
    parser.add_argument("--rdv-schema", type=Path, default=Path("schemas/rdv.asn1"))
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cut", action="append", dest="cuts", help="clock cut HH:MM; repeatable; defaults to configured pilot cuts")
    args = parser.parse_args()
    cuts = parse_cuts(args.cuts or list(DEFAULT_CUTS))
    try:
        report = analyze_collection(args.collection, cuts, args.schema, args.rdv_schema, args.log, args.bu)
    except Exception as error:
        section = None
        try:
            section = json.loads(args.collection.read_text(encoding="utf-8")).get("section_key")
        except Exception:
            pass
        diag = diagnostic(code="ANALYZE_COLLECTION_FAILED", stage="analysis_input", operation="load_collection_manifest",
            error_type="parsing", cause=str(error), recoverability="permanent", section=section,
            file_type="collection", file_name=args.collection.name, file_path=str(args.collection), exception=error,
            context={"collection_bytes": args.collection.stat().st_size if args.collection.exists() else 0})
        failure_states = [file_state(file_type=kind, name=None, section=section, status="not_processed",
            reason="Collection manifest could not be loaded.") for kind in ("bu", "rdv", "log")]
        report = {"report_schema_version": 4, "section_key": section, "overall_status": "failed",
            "validity": "not_valid", "diagnostics": [diag], "file_processing": {"states": [
                *failure_states],
                "logs": [], "bus": [], "rdvs": [], "unprocessed_types": ["bu", "rdv", "log"]},
            "diagnostic_summary": summarize_diagnostics([diag], failure_states),
            "preservation": {"original_files_retained": True, "partial_files_retained": True}}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "section_key": report["section_key"],
                      "overall_status": report["overall_status"], "validity": report["validity"],
                      "diagnostic_count": report["diagnostic_summary"]["diagnostic_count"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
