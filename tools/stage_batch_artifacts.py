#!/usr/bin/env python3
"""Split a collected worker batch into smaller, independently downloadable artifacts."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


def stage_parts(inventory: dict, data_root: Path, output_root: Path, *, offset: int,
                limit: int, part_size: int, report_path: Path | None = None) -> list[Path]:
    sections = inventory.get("sections")
    if not isinstance(sections, list):
        raise ValueError("inventory must contain sections[]")
    if offset < 0 or limit < 1 or part_size < 1:
        raise ValueError("offset must be nonnegative and limits must be positive")
    selected = sections[offset:offset + limit]
    if len(selected) != limit:
        raise ValueError("requested batch exceeds the inventory")
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True)
    report = json.loads(report_path.read_text(encoding="utf-8")) if report_path else None
    output_parts = []
    for part_index, start in enumerate(range(0, len(selected), part_size)):
        part_sections = selected[start:start + part_size]
        part_dir = output_root / f"part-{part_index:02d}"
        staged_data = part_dir / "dados"
        for section in part_sections:
            key = "/".join((str(section["uf"]).lower(), str(section["municipio"]),
                            str(section["zona"]), str(section["secao"])))
            source = data_root / key
            if source.exists():
                destination = staged_data / key
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(source, destination, dirs_exist_ok=True)
        report_sections = report.get("sections", []) if report is not None else []
        status_by_key = {item.get("section"): item.get("status") for item in report_sections}
        section_rows = []
        for section in part_sections:
            key = "/".join((str(section["uf"]).lower(), str(section["municipio"]),
                            str(section["zona"]), str(section["secao"])))
            section_rows.append({**section, "collection_status": status_by_key.get(key, "not_processed")})
        inventory_excerpt = {
            "report_type": "collected_section_inventory_excerpt",
            "config_id": inventory.get("config_id"),
            "acquired_at_utc": inventory.get("acquired_at_utc"),
            "filters": inventory.get("filters"),
            "catalog_sources": inventory.get("catalog_sources", []),
            "sections": section_rows,
        }
        (part_dir / "section-inventory.json").write_text(
            json.dumps(inventory_excerpt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        if report is not None:
            keys = {"/".join((str(item["uf"]).lower(), str(item["municipio"]),
                              str(item["zona"]), str(item["secao"]))) for item in part_sections}
            part_report = {**report, "sections": [item for item in report_sections if item.get("section") in keys]}
            part_report["section_count"] = len(part_report["sections"])
            statuses = [item.get("status") for item in part_report["sections"]]
            part_report["section_counts"] = {
                "registered_sections": len(part_sections),
                "aggregated_sections": sum(item.get("status") == "aggregated" for item in part_report["sections"]),
                "principal_collectable_sections": sum(item.get("status") != "aggregated" for item in part_report["sections"]),
                "effectively_collected_sections": sum(status in {"collected", "partial"} for status in statuses),
                "fully_collected_sections": sum(status == "collected" for status in statuses),
                "partial_sections": sum(status == "partial" for status in statuses),
                "real_failures": sum(status in {"partial", "failed"} for status in statuses),
            }
            (part_dir / "batch-collection.json").write_text(
                json.dumps(part_report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        output_parts.append(part_dir)
    return output_parts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, default=Path("dados"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--offset", type=int, required=True)
    parser.add_argument("--limit", type=int, required=True)
    parser.add_argument("--part-size", type=int, default=500)
    args = parser.parse_args()
    inventory = json.loads(args.inventory.read_text(encoding="utf-8"))
    parts = stage_parts(inventory, args.data_root, args.output, offset=args.offset,
                        limit=args.limit, part_size=args.part_size, report_path=args.report)
    print(json.dumps({"part_count": len(parts), "paths": [str(path) for path in parts]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
