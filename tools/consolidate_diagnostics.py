#!/usr/bin/env python3
"""Merge per-section analysis artifacts without stopping on a bad report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from diagnostics import consolidate_reports, diagnostic, summarize_diagnostics


def consolidate_paths(paths: list[Path]) -> dict:
    reports = []
    input_errors = []
    for path in paths:
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(report, dict):
                raise ValueError("section report JSON root must be an object")
            reports.append(report)
        except Exception as error:
            input_errors.append(diagnostic(code="CONSOLIDATE_REPORT_READ_FAILED", stage="consolidation",
                operation="read_section_report", error_type="parsing", cause=str(error), recoverability="permanent",
                section=None, file_type="analysis_report", file_name=path.name, file_path=str(path), exception=error,
                context={"bytes_read": path.stat().st_size if path.exists() else 0}))
    result = consolidate_reports(reports)
    result["diagnostics"].extend(input_errors)
    result["summary"] = summarize_diagnostics(result["diagnostics"], result["summary"]["files"])
    result["input_report_count"] = len(paths)
    result["loaded_report_count"] = len(reports)
    result["input_error_count"] = len(input_errors)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, action="append", required=True,
                        help="per-section analysis JSON; repeat to consolidate a batch")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = consolidate_paths(args.report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "section_count": report["section_count"],
        "diagnostic_count": report["summary"]["diagnostic_count"], "input_error_count": report["input_error_count"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
