#!/usr/bin/env python3
"""Analyze every acquired section independently and consolidate diagnostics."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from analyze_section import DEFAULT_CUTS, analyze_collection, parse_cuts
from diagnostics import consolidate_reports, diagnostic, file_state, summarize_diagnostics


def analyze_sections(collections: list[Path], output_dir: Path, cuts: list[str],
                     bu_schema: Path, rdv_schema: Path) -> dict:
    reports = []
    section_index = []
    for index, collection_path in enumerate(collections, 1):
        try:
            report = analyze_collection(collection_path, cuts, bu_schema, rdv_schema)
        except Exception as error:
            report = {"report_schema_version": 4, "section_key": None, "overall_status": "failed", "validity": "not_valid",
                "diagnostics": [diagnostic(code="ANALYZE_SECTION_FAILED", stage="batch_analysis", operation="analyze_section",
                    error_type="internal", cause=str(error), recoverability="indeterminate", section=None,
                    file_type="collection", file_name=collection_path.name, file_path=str(collection_path), exception=error)],
                "file_processing": {"states": [file_state(file_type=kind, name=None, section=None, status="not_processed",
                    reason="Section collection could not be processed.") for kind in ("bu", "rdv", "log")],
                    "logs": [], "bus": [], "rdvs": [], "unprocessed_types": ["bu", "rdv", "log"]}}
            report["diagnostic_summary"] = summarize_diagnostics(report["diagnostics"], report["file_processing"]["states"])
        filename = f"section-{index:05d}.json"
        output_path = output_dir / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        reports.append(report)
        section_index.append({"section": report.get("section_key"), "source_collection": str(collection_path),
            "report": str(output_path), "overall_status": report.get("overall_status"), "validity": report.get("validity")})
    consolidated = consolidate_reports(reports)
    consolidated["section_reports"] = section_index
    consolidated["diagnostics_summary"] = consolidated.pop("summary")
    consolidated_path = output_dir / "diagnosticos-consolidados.json"
    consolidated_path.write_text(json.dumps(consolidated, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return consolidated


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collection", type=Path, action="append", required=True,
                        help="acquisition collection.json; repeat to process multiple sections")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cut", action="append", dest="cuts")
    parser.add_argument("--schema", type=Path, default=Path("schemas/bu.asn1"))
    parser.add_argument("--rdv-schema", type=Path, default=Path("schemas/rdv.asn1"))
    args = parser.parse_args()
    result = analyze_sections(args.collection, args.output_dir, parse_cuts(args.cuts or list(DEFAULT_CUTS)), args.schema, args.rdv_schema)
    print(json.dumps({"section_count": result["section_count"], "diagnostic_count": result["diagnostics_summary"]["diagnostic_count"],
        "output": str(args.output_dir / "diagnosticos-consolidados.json")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
