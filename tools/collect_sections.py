#!/usr/bin/env python3
"""Collect multiple sections independently; a failed invocation does not stop the batch."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from diagnostics import diagnostic, summarize_diagnostics
from section_aggregation import is_aggregated

MAX_BATCH_SIZE = 100

def collect_sections(sections: list[dict], output_root: Path, runner=subprocess.run, data_id: str = "3221",
                    offset: int = 0, limit: int = MAX_BATCH_SIZE) -> dict:
    diagnostics = []
    results = []
    script = Path(__file__).with_name("collect_section.py")
    if offset < 0:
        raise ValueError("offset must be zero or greater")
    if not 1 <= limit <= MAX_BATCH_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_BATCH_SIZE}")
    selected = sections[offset:offset + limit]
    for index, section in enumerate(selected, start=offset):
        key = "/".join((str(section.get("uf", "")).lower(), str(section.get("municipio", "")),
                        str(section.get("zona", "")), str(section.get("secao", ""))))
        if is_aggregated(section):
            results.append({"section": key, "status": "aggregated", "return_code": 0,
                "acquisition": None, "principal_section_key": section.get("principal_section_key"),
                "diagnostic_count": 0, "file_states": []})
            continue
        command = [sys.executable, str(script), "--uf", str(section.get("uf", "")),
                   "--municipio", str(section.get("municipio", "")), "--zona", str(section.get("zona", "")),
                   "--secao", str(section.get("secao", "")), "--data-id", data_id,
                   "--output", str(output_root)]
        try:
            completed = runner(command, capture_output=True, text=True, check=False)
            try:
                summary = json.loads(completed.stdout) if completed.stdout.strip() else {}
            except json.JSONDecodeError as error:
                summary = {}
                diagnostics.append(diagnostic(code="BATCH_COLLECTOR_OUTPUT_INVALID", stage="batch_collection",
                    operation="read_collector_result", error_type="parsing", cause=str(error), recoverability="permanent",
                    section=key, file_type="collection_result", file_name=None, file_path=None, exception=error,
                    position={"section_index": index}, context={"stderr": completed.stderr[-2000:]}))
            if completed.returncode != 0:
                diagnostics.append(diagnostic(code="BATCH_SECTION_COLLECTION_FAILED", stage="batch_collection",
                    operation="collect_section", error_type="internal", cause=completed.stderr.strip() or "collector exited non-zero",
                    recoverability="indeterminate", section=key, file_type="section", file_name=None, file_path=None,
                    position={"section_index": index}, context={"return_code": completed.returncode}))
            result_diagnostics = summary.get("diagnostics", [])
            diagnostics.extend(result_diagnostics)
            if completed.returncode == 0 and (not summary.get("section_key") or not summary.get("acquisition")):
                diagnostics.append(diagnostic(code="BATCH_COLLECTOR_RESULT_INCOMPLETE", stage="batch_collection",
                    operation="validate_collector_result", error_type="semantic_validation",
                    cause="Collector completed without section identity or acquisition metadata path.", recoverability="indeterminate",
                    section=key, file_type="collection_result", file_name=None, file_path=None,
                    context={"return_code": completed.returncode}))
            states = summary.get("file_states", [])
            core_states = [state for state in states if state.get("type") in {"bu", "rdv", "log"}]
            downloaded_count = sum(state.get("status") == "downloaded" for state in core_states)
            failed_statuses = {"missing", "not_found", "failed", "partial"}
            has_real_failure = any(state.get("status") in failed_statuses for state in core_states)
            if completed.returncode != 0 or not core_states or downloaded_count == 0:
                section_status = "failed"
            elif has_real_failure:
                section_status = "partial"
            else:
                section_status = "collected"
            results.append({"section": summary.get("section_key", key),
                "status": section_status,
                "return_code": completed.returncode, "acquisition": summary.get("acquisition"),
                "downloaded_source_file_count": downloaded_count,
                "diagnostic_count": len(result_diagnostics), "file_states": states})
        except Exception as error:
            diagnostics.append(diagnostic(code="BATCH_SECTION_INVOCATION_FAILED", stage="batch_collection",
                operation="invoke_collector", error_type="internal", cause=str(error), recoverability="indeterminate",
                section=key, file_type="section", file_name=None, file_path=None, exception=error,
                position={"section_index": index}, context={"command": command}))
            results.append({"section": key, "status": "failed", "return_code": None, "acquisition": None,
                "diagnostic_count": 1, "file_states": []})
    counts = {"registered_sections": len(selected),
        "aggregated_sections": sum(item["status"] == "aggregated" for item in results),
        "principal_collectable_sections": sum(section.get("collectable", not is_aggregated(section))
                                               for section in selected),
        "effectively_collected_sections": sum(item["status"] in {"collected", "partial"} for item in results),
        "fully_collected_sections": sum(item["status"] == "collected" for item in results),
        "partial_sections": sum(item["status"] == "partial" for item in results),
        "real_failures": sum(item["status"] in {"partial", "failed"} for item in results)}
    return {"report_type": "batch_collection", "data_id": data_id, "section_counts": counts,
            "section_count": len(results), "sections": results,
            "diagnostics": diagnostics, "diagnostic_summary": summarize_diagnostics(diagnostics)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sections", type=Path, required=True,
                        help="JSON array or section-inventory report with a sections[] array")
    parser.add_argument("--data-id", default="3221", help="TSE arquivo-urna dataset ID (default: 3221)")
    parser.add_argument("--output", type=Path, default=Path("dados-brutos"))
    parser.add_argument("--offset", type=int, default=0, help="start index in the selected inventory")
    parser.add_argument("--limit", type=int, default=MAX_BATCH_SIZE,
                        help=f"maximum sections in this batch (1..{MAX_BATCH_SIZE}; default: {MAX_BATCH_SIZE})")
    args = parser.parse_args()
    if args.offset < 0:
        parser.error("offset must be zero or greater")
    if not 1 <= args.limit <= MAX_BATCH_SIZE:
        parser.error(f"limit must be between 1 and {MAX_BATCH_SIZE}")
    try:
        sections = json.loads(args.sections.read_text(encoding="utf-8"))
        if isinstance(sections, dict):
            sections = sections.get("sections")
        if not isinstance(sections, list):
            raise ValueError("sections input must be an array or an inventory report with sections[]")
        if not (len(args.data_id) == 4 and args.data_id.isdigit()):
            raise ValueError("data-id must contain exactly four digits")
    except Exception as error:
        sections = []
        diagnostics = [diagnostic(code="BATCH_SECTION_INVENTORY_INVALID", stage="batch_collection",
            operation="read_section_inventory", error_type="parsing", cause=str(error), recoverability="permanent",
            section=None, file_type="inventory", file_name=args.sections.name, file_path=str(args.sections), exception=error)]
        result = {"report_type": "batch_collection", "section_count": 0, "sections": [],
                  "diagnostics": diagnostics, "diagnostic_summary": summarize_diagnostics(diagnostics)}
    else:
        result = collect_sections(sections, args.output, data_id=args.data_id,
                                  offset=args.offset, limit=args.limit)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
