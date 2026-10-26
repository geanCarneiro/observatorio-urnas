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


def der_children(data: bytes) -> list[tuple[int, bytes]]:
    """Read one bounded definite-length BER/DER constructed value."""
    if not data or data[0] & 0x20 == 0:
        raise ValueError("expected constructed ASN.1 value")
    pos = 1
    first = data[pos]
    pos += 1
    if first < 0x80:
        length = first
    else:
        width = first & 0x7f
        if width == 0 or width > 4 or pos + width > len(data):
            raise ValueError("unsupported ASN.1 length")
        length = int.from_bytes(data[pos:pos + width], "big")
        pos += width
    end = pos + length
    if end != len(data):
        raise ValueError("ASN.1 length does not match input")
    children = []
    while pos < end:
        start = pos
        tag = data[pos]
        pos += 1
        first = data[pos]
        pos += 1
        if first < 0x80:
            size = first
        else:
            width = first & 0x7f
            if width == 0 or width > 4 or pos + width > end:
                raise ValueError("unsupported nested ASN.1 length")
            size = int.from_bytes(data[pos:pos + width], "big")
            pos += width
        child_end = pos + size
        if child_end > end:
            raise ValueError("ASN.1 child exceeds parent")
        content = data[pos:child_end]
        pos = child_end
        children.append((tag, data[start:child_end]))
    return children


def bu_section_times(bu_path: Path, schema_path: Path) -> dict:
    spec = asn1tools.compile_files(str(schema_path), codec="ber")
    envelope = spec.decode("EntidadeEnvelopeGenerico", bu_path.read_bytes())
    root = der_children(envelope["conteudo"])
    if len(root) < 6 or root[5][0] != 0xa0:
        raise ValueError("BU payload does not contain the expected section-data choice")
    values = [value[2:].decode("ascii") for tag, value in der_children(root[5][1]) if tag == 0x1b]
    if len(values) != 2:
        raise ValueError("could not read BU opening/closing timestamps")
    for value in values:
        datetime.strptime(value, "%Y%m%dT%H%M%S")
    return {"opening_raw": values[0], "closing_raw": values[1]}


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

    bu_outer = asn1tools.compile_files(str(schema_path), codec="ber").decode("EntidadeEnvelopeGenerico", bu_path.read_bytes())
    identification = bu_outer["identificacao"]
    section_id = identification[1].get("municipioZona", {}) if identification[0] == "identificacaoSecaoEleitoral" else {}
    bu_identity = {"phase": bu_outer["fase"], "envelope_type": bu_outer["tipoEnvelope"],
                   "municipio": section_id.get("municipio"), "zona": section_id.get("zona"),
                   "secao": identification[1].get("secao") if identification[0] == "identificacaoSecaoEleitoral" else None}
    identity_matches = (str(bu_identity["municipio"]) == collection["section_key"].split("/")[1]
                        and f'{bu_identity["zona"]:04d}' == collection["section_key"].split("/")[2]
                        and f'{bu_identity["secao"]:04d}' == collection["section_key"].split("/")[3])
    try:
        asn1tools.compile_files(str(schema_path), codec="ber").decode("EntidadeBoletimUrna", bu_outer["conteudo"])
        inner_schema_status = "decoded"
    except Exception as error:
        inner_schema_status = f"schema_mismatch:{type(error).__name__}"
    try:
        bu_times = bu_section_times(bu_path, schema_path)
        completion_matches = any(e["event_type"] == "conclusao_encerramento" and e["event_timestamp_raw"] == datetime.strptime(bu_times["closing_raw"], "%Y%m%dT%H%M%S").strftime("%d/%m/%Y %H:%M:%S") for e in events)
        bu_time_validation = {"status": "clock_agreement" if completion_matches else "clock_disagreement", **bu_times,
                              "log_closure_matches_bu_closing": completion_matches,
                              "timezone": "timezone/UTC offset remains unvalidated; matching same-device clocks do not identify a timezone"}
    except (ValueError, IndexError) as error:
        bu_time_validation = {"status": "unavailable", "reason": str(error)}

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
        "validation": {
            "integrity": integrity,
            "timezone": {"status": "unvalidated", "reason": "log timestamps contain no timezone; no external clock reference has been authenticated"},
            "bu_clock_crosscheck": bu_time_validation,
            "urn_substitution": {"status": "inconclusive", "manifest_hash_entries": len(collection["manifest_payload_unverified"].get("hashes", [])), "log_keyword_signal_count": substitution_keyword_matches, "reason": "manifest and log keyword screening are not a complete urn-substitution history; BU correspondence fields could not be decoded"},
            "bu_envelope_identity": {"status": "matched" if identity_matches else "mismatch", **bu_identity},
            "bu_inner_schema": inner_schema_status,
            "bu_reconciliation": {"status": "unavailable", "log_vote_events_by_date": {d: c.get("voto_computado", 0) for d, c in sorted(counts_by_date.items())}, "bu_vote_counts": None, "divergences": None, "reason": "current inner BU ASN.1 schema is incompatible with this payload; numeric reconciliation and divergence calculation remain unavailable"},
        },
        "clock_cuts_hhmm_raw": cuts,
        "event_semantics": EVENTS,
        "event_count_total": len(events),
        "events_by_date": by_date,
        "relevant_events": events,
        "privacy": {"individual_log_fields_excluded": True, "raw_message_or_elector_identifier_included": False},
        "limitations": ["log clock timezone and drift unvalidated", "JWS signature unverified", "BU inner schema mismatch prevents reconciliation", "counts describe logged events and do not imply causality or irregularity"],
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
