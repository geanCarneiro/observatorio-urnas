#!/usr/bin/env python3
"""Write one structured JSON extraction beside each acquired source file."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

from analyze_section import analyze_collection
from election_turns import turn_for_pleito
from temporary_transfer import reference_identity, write_reconciliation, write_section_excerpts, write_validation


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".extract-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            json.dump(payload, output, ensure_ascii=False, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _source_metadata(collection: dict, kind: str, name: str) -> dict:
    return next((item for item in collection.get("files", [])
                 if item.get("type") == kind and item.get("name") == name), {})


def _file_diagnostics(diagnostics: list[dict], kind: str, name: str) -> list[dict]:
    return [item for item in diagnostics if item.get("file_type") == kind and item.get("file_name") == name]


def extract_collection(acquisition_path: Path, bu_schema: Path, rdv_schema: Path,
                      reference_excerpt: dict | None = None) -> None:
    acquisition = json.loads(acquisition_path.read_text(encoding="utf-8"))
    extraction = analyze_collection(acquisition_path, [], bu_schema, rdv_schema)
    acquisition_source = {
        "data_id": acquisition.get("data_id"),
        "pleito_id": acquisition.get("pleito_id"),
        "turno_from_bu": acquisition.get("turno"),
        "acquired_at_utc": acquisition.get("acquired_at_utc"),
    }

    for kind, result_key, data_key in (("bu", "bus", "data"), ("rdv", "rdvs", None), ("log", "logs", None)):
        for result in extraction.get("file_processing", {}).get(result_key, []):
            source_path = Path(result["path"])
            name = result.get("name") or source_path.name
            metadata = _source_metadata(acquisition, kind, name)
            if kind == "bu":
                extracted = result.get(data_key)
                pleito_id = (extracted or {}).get("pleito_id")
                payload = {
                    "source": metadata,
                    "acquisition": acquisition_source,
                    "section_key": acquisition.get("section_key"),
                    "turno": turn_for_pleito(pleito_id),
                    "pleito_id": pleito_id,
                    "status": result.get("status"),
                    "detected_format": result.get("detected_format"),
                    "schema_sha256": result.get("version"),
                    "urn_model": result.get("urn_model"),
                    "data": extracted,
                    "diagnostics": _file_diagnostics(extraction.get("diagnostics", []), kind, name),
                }
            elif kind == "rdv":
                payload = dict(result)
                pleito_id = payload.get("entity_metadata", {}).get("pleito")
                payload["turno"] = turn_for_pleito(pleito_id)
                payload["section_key"] = acquisition.get("section_key")
                payload["acquisition"] = acquisition_source
                payload["source_manifest"] = metadata
                payload["diagnostics"] = result.get("diagnostics") or _file_diagnostics(
                    extraction.get("diagnostics", []), kind, name)
            else:
                payload = {
                    "source": metadata,
                    "acquisition": acquisition_source,
                    "section_key": acquisition.get("section_key"),
                    "status": result.get("status"),
                    "events": [{key: value for key, value in event.items()
                                if key != "at_or_after_clock_cuts"}
                               for event in result.get("events", [])],
                    "diagnostics": result.get("diagnostics") or _file_diagnostics(
                        extraction.get("diagnostics", []), kind, name),
                    "record_policy": "Each allowlisted source event is a separate object in events[]; raw message fields are omitted.",
                }
            _write_json(source_path.with_name(source_path.name + ".json"), payload)

    section_diagnostics = [item for item in extraction.get("diagnostics", [])
                           if not item.get("file_type") or not item.get("file_name")]
    if section_diagnostics:
        acquisition["analysis_diagnostics"] = section_diagnostics
        _write_json(acquisition_path, acquisition)

    transfer_reconciliation = extraction.get("transferencia_temporaria")
    if transfer_reconciliation and reference_excerpt:
        transfer_reconciliation["reference_excerpt"] = reference_excerpt
    if transfer_reconciliation and transfer_reconciliation.get("turno") == 2:
        output_root = acquisition_path.parent
        write_reconciliation([transfer_reconciliation],
            output_root / "transferencia-temporaria-reconciliacao.json",
            output_root / "transferencia-temporaria-reconciliacao.csv",
            "section_analysis_log_sessions")
        write_validation([transfer_reconciliation],
            output_root / "transferencia-temporaria-validation.json",
            source_note="section_analysis_log_sessions")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acquisition", type=Path, action="append", required=True,
                        help="acquisition.json; repeat for multiple sections")
    parser.add_argument("--schema", type=Path, default=Path("schemas/bu.asn1"))
    parser.add_argument("--rdv-schema", type=Path, default=Path("schemas/rdv.asn1"))
    args = parser.parse_args()
    acquisitions = []
    targets = []
    for acquisition in args.acquisition:
        collection = json.loads(acquisition.read_text(encoding="utf-8"))
        acquisitions.append((acquisition, collection))
        section_key = collection.get("section_key")
        turn = collection.get("turno") or {"3220": 1, "3221": 2}.get(str(collection.get("data_id")))
        if isinstance(section_key, str) and turn in {1, 2, "1", "2"}:
            targets.append({"section_key": section_key, "turn": int(turn),
                            "output_path": acquisition.parent / "transferencia-temporaria-elegiveis.csv"})
    try:
        excerpt_metadata = write_section_excerpts(targets)
    except Exception as error:
        identity = reference_identity()
        excerpt_metadata = {str(target["output_path"]): {
            "status": "not_generated", "reason": f"{type(error).__name__}: {error}",
            "reference_zip_sha256": identity.get("zip_sha256"),
            "reference_version": identity.get("version"),
        } for target in targets}
    for acquisition, _collection in acquisitions:
        excerpt_path = acquisition.parent / "transferencia-temporaria-elegiveis.csv"
        extract_collection(acquisition, args.schema, args.rdv_schema,
                           excerpt_metadata.get(str(excerpt_path)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
