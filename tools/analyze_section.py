#!/usr/bin/env python3
"""Analyze a section log with an allowlist that drops identifying log fields."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import zipfile
import asn1tools
from rdv import decode_rdv
from rdv import diagnose_rdv_failure
from diagnostics import diagnostic, file_state, sha256_file, summarize_diagnostics
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


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def analyze(log_path: Path, bu_path: Path, collection_path: Path, cuts: list[str], schema_path: Path,
            rdv_paths: list[Path] | None = None, rdv_schema_path: Path = Path("schemas/rdv.asn1")) -> dict:
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
    event_counts = counts_by_date.get(bu["election_date"], Counter())
    event_date_present = bu["election_date"] in counts_by_date
    attendance_comparisons = {
        "habilitacoes_vs_bu_attendance": {"log": event_counts.get("habilitacao", 0), "bu": bu["qtd_eleitores_compareceram"], "event_date_present": event_date_present},
        "votos_computados_vs_bu_attendance": {"log": event_counts.get("voto_computado", 0), "bu": bu["qtd_eleitores_compareceram"], "event_date_present": event_date_present},
    }

    for comparison in attendance_comparisons.values():
        comparison["divergence_log_minus_bu"] = comparison["log"] - comparison["bu"]
    # Confirmation events are retained as their own measurements. Their counts are
    # not treated as BU vote totals: log semantics and BU tally semantics differ.
    confirmation_events = {
        "presidente": "voto_confirmado_presidente",
        "governador": "voto_confirmado_governador",
    }
    confirmation_counts = {
        cargo: event_counts.get(event_type, 0)
        for cargo, event_type in confirmation_events.items()
    }
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
    reconciliation_status = ("partial" if not event_date_present else
                              "matched" if all(v["divergence_log_minus_bu"] == 0 for v in attendance_comparisons.values()) and detail_sum_matches
                              else "divergence")
    return {
        "report_schema_version": 4,
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
            "bu_reconciliation": {
                "status": reconciliation_status,
                "comparison_semantics": "Raw event counts are compared with BU attendance as diagnostic counts; confirmation events are not equated to vote totals.",
                "by_attendance_metric": attendance_comparisons,
                "confirmation_event_counts": confirmation_counts,
            "bu_election_date": bu["election_date"],
            "log_contains_bu_election_date": event_date_present,
                "by_cargo": [],
            },
        },
        "clock_cuts_hhmm_raw": cuts,
        "event_semantics": EVENTS,
        "event_count_total": len(events),
        "events_by_date": by_date,
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
    try:
        archive = zipfile.ZipFile(path)
    except (zipfile.BadZipFile, OSError, EOFError) as error:
        diagnostics.append(diagnostic(code="LOG_ZIP_OPEN_FAILED", stage="decompression", operation="open_zip",
            error_type="decompression", cause=str(error), recoverability="permanent", section=section,
            file_type="log", file_name=path.name, file_path=str(path), exception=error, detected_format="ZIP",
            context={"size_bytes": path.stat().st_size, "sha256": file_hash}))
        return {"status": "failed", "events": events, "diagnostics": diagnostics, "event_counts": {}}
    with archive:
        if "logd.dat" not in archive.namelist():
            error = ValueError("ZIP has no logd.dat member")
            diagnostics.append(diagnostic(code="LOG_MEMBER_MISSING", stage="format_validation", operation="locate_log_member",
                error_type="format_incompatibility", cause=str(error), recoverability="permanent", section=section,
                file_type="log", file_name=path.name, file_path=str(path), exception=error, detected_format="ZIP",
                context={"members": archive.namelist()}))
            return {"status": "failed", "events": events, "diagnostics": diagnostics, "event_counts": {}}
        member = archive.getinfo("logd.dat")
        if member.file_size > 32 * 1024 * 1024:
            error = ValueError("logd.dat exceeds the 32 MiB safety limit")
            diagnostics.append(diagnostic(code="LOG_MEMBER_SIZE_LIMIT", stage="semantic_validation", operation="check_member_size",
                error_type="semantic_validation", cause=str(error), recoverability="permanent", section=section,
                file_type="log", file_name=path.name, file_path=str(path), exception=error, detected_format="ZIP",
                context={"member": "logd.dat", "declared_bytes": member.file_size, "limit_bytes": 32 * 1024 * 1024}))
            return {"status": "failed", "events": events, "diagnostics": diagnostics, "event_counts": {}}
        try:
            bad_member = archive.testzip()
            if bad_member:
                error = ValueError(f"ZIP CRC validation failed for {bad_member}")
                diagnostics.append(diagnostic(code="LOG_ZIP_CRC_FAILED", stage="integrity_validation", operation="test_zip_crc",
                    error_type="decompression", cause=str(error), recoverability="permanent", section=section,
                    file_type="log", file_name=path.name, file_path=str(path), exception=error, detected_format="ZIP",
                    context={"member": bad_member, "member_count": len(archive.namelist())}))
                return {"status": "failed", "events": events, "diagnostics": diagnostics, "event_counts": {}}
            text_data = archive.read("logd.dat").decode("latin1")
        except (zipfile.BadZipFile, OSError, EOFError, RuntimeError) as error:
            diagnostics.append(diagnostic(code="LOG_DECOMPRESSION_FAILED", stage="decompression", operation="read_log_member",
                error_type="decompression", cause=str(error), recoverability="permanent", section=section,
                file_type="log", file_name=path.name, file_path=str(path), exception=error, detected_format="ZIP",
                context={"member": "logd.dat", "declared_bytes": member.file_size}))
            return {"status": "failed", "events": events, "diagnostics": diagnostics, "event_counts": {}}
    substitution_matches = 0
    for line_number, raw_line in enumerate(text_data.splitlines(), 1):
        fields = raw_line.split("\t")
        if len(fields) < 5:
            diagnostics.append(diagnostic(code="LOG_LINE_MALFORMED", stage="parsing", operation="parse_log_record",
                error_type="parsing", cause=f"Expected at least five tab-separated fields, found {len(fields)}.",
                recoverability="permanent", section=section, file_type="log", file_name=path.name, file_path=str(path),
                position={"line": line_number}, detected_format="ZIP/logd.dat", context={"field_count": len(fields)}))
            continue
        message = fields[4]
        message_lower = message.lower()
        if any(term in message_lower for term in ("contingência", "substituição", "urna substituta", "troca de urna", "recuperação dos dados")):
            substitution_matches += 1
        event_type = EVENTS.get(message)
        if event_type is None:
            continue
        try:
            timestamp = datetime.strptime(fields[0], "%d/%m/%Y %H:%M:%S")
        except ValueError as error:
            diagnostics.append(diagnostic(code="LOG_EVENT_TIMESTAMP_INVALID", stage="parsing", operation="parse_event_timestamp",
                error_type="parsing", cause=str(error), recoverability="permanent", section=section,
                file_type="log", file_name=path.name, file_path=str(path), exception=error,
                position={"line": line_number}, detected_format="ZIP/logd.dat", context={"event_type": event_type, "timestamp_raw": fields[0]}))
            continue
        hhmm = timestamp.strftime("%H:%M")
        events.append({"source_file_sha256": metadata["sha256"], "line_number": line_number,
            "event_type": event_type, "event_timestamp_raw": fields[0], "event_timezone": "unknown",
            "event_timestamp_utc": None, "event_timestamp_brt": None, "parser_status": "allowlist_match"})
        counts[event_type] += 1
        event_date = timestamp.strftime("%Y-%m-%d")
        counts_by_date[event_date][event_type] += 1
        if event_type in {"habilitacao", "voto_computado", "voto_confirmado_governador", "voto_confirmado_presidente"}:
            for cut in cuts:
                if hhmm >= cut:
                    key = f"{event_type}_a_partir_de_{cut.replace(':', 'h')}"
                    after_cuts[key] += 1
                    after_cuts_by_date[event_date][key] += 1
    by_date = {}
    for event_date in sorted(counts_by_date):
        day = [event for event in events if event["event_timestamp_raw"].startswith(
            datetime.strptime(event_date, "%Y-%m-%d").strftime("%d/%m/%Y"))]
        by_date[event_date] = {
            "event_counts": dict(counts_by_date[event_date]),
            "last_habilitacao_raw": max((e["event_timestamp_raw"] for e in day if e["event_type"] == "habilitacao"), default=None),
            "last_voto_computado_raw": max((e["event_timestamp_raw"] for e in day if e["event_type"] == "voto_computado"), default=None),
            "activity_after_clock_cuts": dict(after_cuts_by_date[event_date]),
            "relevant_events": day,
        }
    return {"status": "processed_with_diagnostics" if diagnostics else "processed", "events": events,
        "diagnostics": diagnostics, "event_counts": dict(counts), "activity_after_clock_cuts": dict(after_cuts),
        "event_counts_by_date": {date: dict(day) for date, day in counts_by_date.items()},
        "activity_after_clock_cuts_by_date": {date: dict(day) for date, day in after_cuts_by_date.items()},
        "events_by_date": by_date,
        "substitution_keyword_matches": substitution_matches, "sha256": file_hash,
        "size_bytes": path.stat().st_size, "detected_format": "ZIP", "version": None, "urn_model": None}


def log_bu_reconciliation_diagnostics(validation: dict, section: str, log_path: Path,
                                      urn_model: str | None = None) -> list[dict]:
    diagnostics = []
    reconciliation = validation.get("bu_reconciliation", {})
    comparisons = list(reconciliation.get("by_attendance_metric", {}).values())
    comparisons.extend(reconciliation.get("by_cargo", []))
    for comparison in comparisons:
        if "log_confirmations" in comparison and comparison.get("log_confirmations") is None:
            diagnostics.append(diagnostic(code="LOG_BU_RECONCILIATION_INCOMPLETE", stage="reconciliation",
                operation="map_log_event_to_bu_cargo", error_type="reconciliation",
                cause="No allowlisted log event is mapped to this BU cargo.", recoverability="indeterminate",
                section=section, file_type="log", file_name=log_path.name, file_path=str(log_path),
                detected_format="ZIP/logd.dat", urn_model=urn_model, context={"comparison": comparison}))
            continue
        divergences = {key: value for key, value in comparison.items()
                       if key.startswith("divergence_") and value not in (None, 0)}
        if not divergences:
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
            diagnostics.extend(log_bu_reconciliation_diagnostics(legacy_report["validation"], section, first_log,
                legacy_report["bu_attendance"].get("urn_type")))
            if legacy_report["validation"]["bu_reconciliation"].get("status") == "partial":
                diagnostics.append(diagnostic(code="LOG_BU_DATE_NOT_PRESENT", stage="reconciliation",
                    operation="select_log_events_for_bu_date", error_type="reconciliation",
                    cause="No allowlisted event was found on the BU emission date, so log-to-BU count comparison is incomplete.",
                    recoverability="indeterminate", section=section, file_type="log", file_name=first_log.name,
                    file_path=str(first_log), detected_format="ZIP/logd.dat", urn_model=legacy_report["bu_attendance"].get("urn_type"),
                    context={"bu_election_date": legacy_report["validation"]["bu_reconciliation"].get("bu_election_date")}))
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
        "preservation": {"original_files_retained": True, "partial_files_retained": True,
            "note": "Processing outcomes never upgrade a partial or unverified input to valid."}})
    return report
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
        "report_schema_version": 4,
        "section_key": collection["section_key"],
        "acquisition_id": collection["acquired_at_utc"],
        "source_files": {"manifest_sha256": collection["manifest_sha256"], "log_sha256": metadata["sha256"], "bu_sha256": bu_metadata["sha256"],
                         "rdv_sha256": [rdv["source"]["sha256"] for rdv in rdvs]},
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
        "rdv_files": rdvs,
        "privacy": {"individual_log_fields_excluded": True, "raw_message_or_elector_identifier_included": False},
        "limitations": ["log clock timezone and drift unvalidated", "JWS signature unverified", "possible urn replacement screening is not exhaustive", "counts describe logged events and do not imply causality or irregularity"],
    }


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
