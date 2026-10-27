#!/usr/bin/env python3
"""Update selected inventory rows with actual collection outcomes."""

from __future__ import annotations

import argparse
import json
import tempfile
import os
from collections import Counter
from pathlib import Path

from section_aggregation import is_aggregated


CORE_TYPES = {"bu", "rdv", "log"}
FAILURE_STATUSES = {"missing", "not_found", "failed", "partial"}


def update_inventory_collection_status(inventory_path: Path, data_root: Path) -> dict:
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    config_id = str(inventory.get("config_id", ""))
    section_statuses = Counter()

    for section in inventory.get("sections", []):
        if is_aggregated(section):
            section.update({"collection_status": "aggregated", "downloaded_source_file_count": 0,
                "collection_failures": []})
            section_statuses["aggregated"] += 1
            continue

        section_root = data_root / section["uf"] / section["municipio"] / section["zona"] / section["secao"]
        acquisitions = []
        for path in section_root.glob("turno-*/acquisition.json"):
            try:
                item = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if str(item.get("data_id")) == config_id and item.get("section_key") == "/".join((
                    section["uf"], section["municipio"], section["zona"], section["secao"])):
                acquisitions.append(item)

        if not acquisitions:
            section.update({"collection_status": "not_processed", "downloaded_source_file_count": 0,
                "collection_failures": [], "collection_processing_state": "no_acquisition_recorded"})
            section_statuses["not_processed"] += 1
            continue

        acquisition = sorted(acquisitions, key=lambda item: item.get("acquired_at_utc", ""))[-1]
        core_states = [item for item in acquisition.get("file_states", []) if item.get("type") in CORE_TYPES]
        downloaded = sum(item.get("status") == "downloaded" for item in core_states)
        failures = [{"type": item.get("type"), "status": item.get("status"), "reason": item.get("reason")}
                    for item in core_states if item.get("status") in FAILURE_STATUSES]
        if len(core_states) < len(CORE_TYPES):
            known = {item.get("type") for item in core_states}
            failures.extend({"type": kind, "status": "state_not_recorded"} for kind in sorted(CORE_TYPES - known))

        if not failures and len(core_states) == len(CORE_TYPES) and downloaded == len(CORE_TYPES):
            status = "collected"
        elif downloaded:
            status = "partial"
        else:
            status = "failed"
            if not failures:
                failures.append({"type": "section", "status": "no_source_file_downloaded"})
        section.update({"collection_status": status, "downloaded_source_file_count": downloaded,
            "collection_failures": failures, "collection_processing_state": "processed",
            "acquisition_path": str(section_root / acquisition.get("turn_directory", "") / "acquisition.json")})
        section_statuses[status] += 1

    selected_registered = len(inventory.get("sections", []))
    selected_aggregated = section_statuses["aggregated"]
    selected_collectable = sum(section.get("collectable", not is_aggregated(section))
                               for section in inventory.get("sections", []))
    counts = inventory.setdefault("section_counts", {})
    counts.update({
        "selected_registered_sections": selected_registered,
        "selected_aggregated_sections": selected_aggregated,
        "selected_principal_collectable_sections": selected_collectable,
        "effectively_collected_sections": section_statuses["collected"] + section_statuses["partial"],
        "fully_collected_sections": section_statuses["collected"],
        "partially_collected_sections": section_statuses["partial"],
        "not_collected_sections": section_statuses["failed"],
        "not_processed_sections": section_statuses["not_processed"],
        "real_failures": section_statuses["partial"] + section_statuses["failed"],
    })
    fd, temporary = tempfile.mkstemp(prefix=".inventory-status-", dir=inventory_path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            json.dump(inventory, output, ensure_ascii=False, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, inventory_path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return inventory


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    args = parser.parse_args()
    result = update_inventory_collection_status(args.inventory, args.data_root)
    print(json.dumps({"config_id": result.get("config_id"), "section_counts": result.get("section_counts")},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
