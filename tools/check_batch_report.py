#!/usr/bin/env python3
"""Fail a worker job when the batch report contains real collection failures."""

import argparse
import json
from pathlib import Path


def validate_batch_report(path: Path, collection_outcome: str) -> tuple[bool, str]:
    if collection_outcome != "success":
        return False, f"collector process outcome was {collection_outcome}"
    if not path.is_file():
        return False, "collector did not write a batch report"
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return False, f"batch report is unreadable: {error}"
    counts = report.get("section_counts", {})
    failures = counts.get("real_failures")
    if not isinstance(failures, int):
        return False, "batch report has no integer real_failures count"
    if failures:
        return False, f"batch report contains {failures} real section failure(s)"
    return True, "batch report has no real collection failures"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--collector-outcome", required=True)
    args = parser.parse_args()
    valid, message = validate_batch_report(args.report, args.collector_outcome)
    print(message)
    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
