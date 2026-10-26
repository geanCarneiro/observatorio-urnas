#!/usr/bin/env python3
"""Analyze a section log with an allowlist that drops identifying log fields."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import zipfile
import asn1tools
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

EVENTS = {
    "Eleitor foi habilitado": "habilitacao",
    "O voto do eleitor foi computado": "voto_computado",
    "Voto confirmado para [Governador]": "voto_confirmado_governador",
    "Voto confirmado para [Presidente]": "voto_confirmado_presidente",
    "Inicio do Encerramento": "inicio_encerramento",
    "Operador selecionou: Encerrar votação": "comando_encerramento",
    "Procedimento de encerramento confirmado": "conclusao_encerramento",
}
DEFAULT_CUTS = ("17:00", "17:15", "17:30", "17:45", "18:00", "18:30", "19:00")
BU_SCHEMA_SOURCE = "https://www.tse.jus.br/eleicoes/eleicoes-2026-content/arquivos/formato-arquivos-de-bu-rdv-e-assinatura-digital"
BU_SCHEMA_BUNDLE_SHA256 = "b5ac413e81121d53de2bb1657af611f84a45fdc84388e9d9821872994bbfbafd"
BU_SCHEMA_SHA256 = "ef64bf723f774403b423ed0b617f5a83a3f50c0023d0003e18cd29170404ab81"


def parse_cuts(values: list[str]) -> list[str]:
    for value in values:
        if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value):
            raise ValueError(f"invalid clock cut {value!r}; expected HH:MM")
    return sorted(set(values))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def decode_bu(bu_path: Path, schema_path: Path) -> dict:
    spec = asn1tools.compile_files(str(schema_path), codec="ber")
    envelope = spec.decode("EntidadeEnvelopeGenerico", bu_path.read_bytes())
    bu = spec.decode("EntidadeBoletimUrna", envelope["conteudo"])
    election_date = datetime.strptime(bu["dataHoraEmissao"], "%Y%m%dT%H%M%S").strftime("%Y-%m-%d")
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
                vote_total = sum(vote["quantidadeVotos"] for vote in cargo["votosVotaveis"])
                cargos.append({"cargo_kind": code_kind, "cargo": code_value,
                               "qtd_votos_bu": vote_total})
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
    data = {
        "phase": envelope["fase"], "envelope_type": envelope["tipoEnvelope"],
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


def analyze(log_path: Path, bu_path: Path, collection_path: Path, cuts: list[str], schema_path: Path) -> dict:
    collection = json.loads(collection_path.read_text(encoding="utf-8"))
    metadata = next((f for f in collection["files"] if f["type"] == "log" and f["name"] == log_path.name), None)
    bu_metadata = next((f for f in collection["files"] if f["type"] == "bu" and f["name"] == bu_path.name), None)
    if not metadata or not bu_metadata:
        raise ValueError("input files are not listed in the acquisition manifest")

    events = []
    counts_by_date = defaultdict(Counter)
    after_cuts = defaultdict(Counter)
    substitution_keyword_matches = 0
    with zipfile.ZipFile(log_path) as archive:
        if "logd.dat" not in archive.namelist():
            raise ValueError("ZIP has no logd.dat member")
        if archive.getinfo("logd.dat").file_size > 32 * 1024 * 1024:
            raise ValueError("logd.dat exceeds the 32 MiB safety limit")
        bad_member = archive.testzip()
        if bad_member:
            raise ValueError(f"ZIP CRC validation failed for {bad_member}")
        for line_number, raw in enumerate(archive.read("logd.dat").decode("latin1").splitlines(), 1):
            fields = raw.split("\t")
            if len(fields) < 5:
                continue
            message_lower = fields[4].lower()
            if any(term in message_lower for term in ("contingência", "substituição", "urna substituta", "troca de urna", "recuperação dos dados")):
                substitution_keyword_matches += 1
            event_type = EVENTS.get(fields[4])
            if event_type is None:
                continue
            try:
                timestamp = datetime.strptime(fields[0], "%d/%m/%Y %H:%M:%S")
            except ValueError:
                continue
            date = timestamp.strftime("%Y-%m-%d")
            hhmm = timestamp.strftime("%H:%M")
            events.append({"source_file_sha256": metadata["sha256"], "line_number": line_number,
                           "event_type": event_type, "event_timestamp_raw": fields[0],
                           "event_timezone": "unknown", "event_timestamp_utc": None,
                           "event_timestamp_brt": None, "parser_status": "allowlist_match"})
            counts_by_date[date][event_type] += 1
            if event_type in {"habilitacao", "voto_computado", "voto_confirmado_governador", "voto_confirmado_presidente"}:
                for cut in cuts:
                    if hhmm >= cut:
                        after_cuts[date][f"{event_type}_a_partir_de_{cut.replace(':', 'h')}"] += 1

    by_date = {}
    for date in sorted(counts_by_date):
        date_prefix = datetime.strptime(date, "%Y-%m-%d").strftime("%d/%m/%Y")
        day = [e for e in events if e["event_timestamp_raw"].startswith(date_prefix)]
        closure_stream = [e for e in day if e["event_type"] in {"comando_encerramento", "inicio_encerramento", "conclusao_encerramento"}]
        closure_types = [e["event_type"] for e in closure_stream]
        semantic_order = ["comando_encerramento", "inicio_encerramento", "conclusao_encerramento"]
        def last_of(kind: str):
            matches = [e["event_timestamp_raw"] for e in day if e["event_type"] == kind]
            return max(matches) if matches else None
        by_date[date] = {
            "event_counts": dict(sorted(counts_by_date[date].items())),
            "last_habilitacao_raw": last_of("habilitacao"),
            "last_voto_computado_raw": last_of("voto_computado"),
            "last_comando_encerramento_raw": last_of("comando_encerramento"),
            "last_inicio_encerramento_raw": last_of("inicio_encerramento"),
            "last_conclusao_encerramento_raw": last_of("conclusao_encerramento"),
            "closure_events_in_log_order": [{"line_number": e["line_number"], "event_type": e["event_type"], "event_timestamp_raw": e["event_timestamp_raw"]} for e in closure_stream],
            "closure_clock_order": "non_monotonic" if any(a["event_timestamp_raw"] > b["event_timestamp_raw"] for a, b in zip(closure_stream, closure_stream[1:])) else "monotonic",
            "closure_semantic_order": "matches_expected" if closure_types == semantic_order else "differs_from_expected",
            "activity_after_clock_cuts": {
                f"{kind}_a_partir_de_{cut.replace(':', 'h')}": after_cuts[date].get(f"{kind}_a_partir_de_{cut.replace(':', 'h')}", 0)
                for kind in ("habilitacao", "voto_computado", "voto_confirmado_governador", "voto_confirmado_presidente")
                for cut in cuts
            },
        }

    bu = decode_bu(bu_path, schema_path)
    bu_section = bu["section"]
    municipio_zona = bu_section["municipioZona"]
    bu_identity = {"phase": bu["phase"], "envelope_type": bu["envelope_type"],
                   "municipio": municipio_zona["municipio"], "zona": municipio_zona["zona"],
                   "secao": bu_section["secao"]}
    expected = collection["section_key"].split("/")
    identity_matches = (str(bu_identity["municipio"]) == expected[1]
                        and f'{bu_identity["zona"]:04d}' == expected[2]
                        and f'{bu_identity["secao"]:04d}' == expected[3])
    event_counts = counts_by_date.get(bu["election_date"], Counter())
    attendance_comparisons = {
        "habilitacoes_vs_bu_attendance": {"log": event_counts.get("habilitacao", 0), "bu": bu["qtd_eleitores_compareceram"]},
        "votos_computados_vs_bu_attendance": {"log": event_counts.get("voto_computado", 0), "bu": bu["qtd_eleitores_compareceram"]},
    }
    for comparison in attendance_comparisons.values():
        comparison["divergence_log_minus_bu"] = comparison["log"] - comparison["bu"]
    cargo_event = {"presidente": "voto_confirmado_presidente", "governador": "voto_confirmado_governador"}
    cargo_comparisons = []
    for election in bu["elections"]:
        for result in election["results"]:
            for cargo in result["cargos"]:
                log_kind = cargo_event.get(cargo["cargo"])
                log_count = event_counts.get(log_kind, 0) if log_kind else None
                cargo_comparisons.append({
                    "election_id": election["id_eleicao"], "cargo": cargo["cargo"],
                    "log_confirmations": log_count, "bu_attendance": result["qtd_comparecimento"],
                    "bu_vote_total": cargo["qtd_votos_bu"],
                    "divergence_log_vs_bu_attendance": log_count - result["qtd_comparecimento"] if log_count is not None else None,
                    "divergence_log_vs_bu_votes": log_count - cargo["qtd_votos_bu"] if log_count is not None else None,
                    "divergence_bu_votes_vs_attendance": cargo["qtd_votos_bu"] - result["qtd_comparecimento"],
                })
    comparisons = [*attendance_comparisons.values(), *[
        {"log": c["log_confirmations"], "bu": c["bu_attendance"], "divergence_log_minus_bu": c["divergence_log_vs_bu_attendance"]}
        for c in cargo_comparisons if c["log_confirmations"] is not None
    ]]
    detail_sum_matches = bu["attendance_detail_sum"] is None or bu["attendance_detail_sum"] == bu["qtd_eleitores_compareceram"]
    cargo_coverage_complete = bool(cargo_comparisons) and all(c["log_confirmations"] is not None for c in cargo_comparisons)
    all_reconciled = (bool(comparisons) and cargo_coverage_complete
                      and all(c["divergence_log_minus_bu"] == 0 for c in comparisons)
                      and all(c["divergence_bu_votes_vs_attendance"] == 0 for c in cargo_comparisons)
                      and all(c["divergence_log_vs_bu_votes"] == 0 for c in cargo_comparisons))
    has_divergence = (not detail_sum_matches or any(c["divergence_log_minus_bu"] != 0 for c in comparisons)
                      or any(c["divergence_bu_votes_vs_attendance"] != 0 or
                             (c["divergence_log_vs_bu_votes"] is not None and c["divergence_log_vs_bu_votes"] != 0)
                             for c in cargo_comparisons))
    reconciliation_status = "matched" if all_reconciled and detail_sum_matches else "divergence" if has_divergence else "partial"
    section_times = bu["section_times"] or {}
    closing_raw = section_times.get("dataHoraEncerramento")
    closing_event_raw = datetime.strptime(closing_raw, "%Y%m%dT%H%M%S").strftime("%d/%m/%Y %H:%M:%S") if closing_raw else None
    completion_matches = any(e["event_type"] == "conclusao_encerramento" and e["event_timestamp_raw"] == closing_event_raw for e in events)
    bu_time_validation = {"status": "clock_agreement" if completion_matches else "clock_disagreement",
                          "opening_raw": section_times.get("dataHoraAbertura"), "closing_raw": closing_raw,
                          "log_closure_matches_bu_closing": completion_matches,
                          "timezone": "timezone/UTC offset remains unvalidated; same-device clock agreement does not identify a timezone"}
    substitution_signal = (bu["urn_type"] in {"contingencia", "reservaSecao", "reservaEncerrandoSecao"}
                           or bu["unique_load_codes"] > 1 or substitution_keyword_matches > 0)
    substitution_validation = {"status": "review_signal" if substitution_signal else "no_signal_detected_in_available_fields",
                               "urn_type": bu["urn_type"], "manifest_hash_entries": len(collection["manifest_payload_unverified"].get("hashes", [])),
                               "load_history_entries": bu["load_history_entries"], "unique_load_codes": bu["unique_load_codes"],
                               "log_keyword_signal_count": substitution_keyword_matches,
                               "scope_note": "absence of a signal is not proof that no substitution occurred"}
    schema_hash = sha256(schema_path)

    integrity = {
        "manifest_sha256_matches_collection": sha256(collection_path.parent / "manifest.aux.jws") == collection["manifest_sha256"],
        "log_sha256_matches_collection": sha256(log_path) == metadata["sha256"],
        "bu_sha256_matches_collection": sha256(bu_path) == bu_metadata["sha256"],
        "log_zip_crc": "valid",
        "jws_signature": "unverified",
    }
    return {
        "section_key": collection["section_key"],
        "acquisition_id": collection["acquired_at_utc"],
        "source_files": {"manifest_sha256": collection["manifest_sha256"], "log_sha256": metadata["sha256"], "bu_sha256": bu_metadata["sha256"]},
        "bu_schema": {"source": BU_SCHEMA_SOURCE, "bundle_sha256": BU_SCHEMA_BUNDLE_SHA256,
                       "schema_sha256": schema_hash, "expected_schema_sha256": BU_SCHEMA_SHA256,
                       "matches_official_copy": schema_hash == BU_SCHEMA_SHA256},
        "bu_attendance": {"total": bu["qtd_eleitores_compareceram"], "detail": bu["attendance_detail"],
                           "detail_sum": bu["attendance_detail_sum"], "detail_sum_matches_total": detail_sum_matches,
                           "urn_type": bu["urn_type"]},
        "validation": {
            "integrity": integrity,
            "timezone": {"status": "unvalidated", "reason": "log timestamps contain no timezone; no external clock reference has been authenticated"},
            "bu_clock_crosscheck": bu_time_validation,
            "urn_substitution": substitution_validation,
            "bu_envelope_identity": {"status": "matched" if identity_matches else "mismatch", **bu_identity},
            "bu_asn1": "decoded",
            "bu_attendance_detail_sum_matches": detail_sum_matches,
            "bu_reconciliation": {"status": reconciliation_status,
                                  "by_attendance_metric": attendance_comparisons, "by_cargo": cargo_comparisons},
        },
        "clock_cuts_hhmm_raw": cuts,
        "event_semantics": EVENTS,
        "event_count_total": len(events),
        "events_by_date": by_date,
        "relevant_events": events,
        "privacy": {"individual_log_fields_excluded": True, "raw_message_or_elector_identifier_included": False},
        "limitations": ["log clock timezone and drift unvalidated", "JWS signature unverified", "possible urn replacement screening is not exhaustive", "counts describe logged events and do not imply causality or irregularity"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--bu", required=True, type=Path)
    parser.add_argument("--collection", required=True, type=Path)
    parser.add_argument("--schema", type=Path, default=Path("schemas/bu.asn1"))
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cut", action="append", dest="cuts", help="clock cut HH:MM; repeatable; defaults to configured pilot cuts")
    args = parser.parse_args()
    cuts = parse_cuts(args.cuts or list(DEFAULT_CUTS))
    report = analyze(args.log, args.bu, args.collection, cuts, args.schema)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "section_key": report["section_key"], "event_count_total": report["event_count_total"], "validation": report["validation"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
