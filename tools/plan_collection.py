#!/usr/bin/env python3
"""Create bounded worker shards from one immutable section inventory."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def build_plan(inventories: dict[str, dict], *, batch_size: int = 1000,
               jobs_per_worker: int = 100) -> dict:
    if batch_size < 1 or jobs_per_worker < 1:
        raise ValueError("batch_size and jobs_per_worker must be positive")
    batches = []
    for turn in sorted(inventories):
        inventory = inventories[turn]
        sections = inventory.get("sections")
        if not isinstance(sections, list):
            raise ValueError(f"inventory for turn {turn} must contain sections[]")
        data_id = str(inventory.get("config_id", "3220" if turn == "1" else "3221"))
        base_offset = int(inventory.get("selection", {}).get("offset", 0))
        inventory_path = f"inventario/turno{turn}/section-inventory.json"
        for local_offset in range(0, len(sections), batch_size):
            count = min(batch_size, len(sections) - local_offset)
            start = base_offset + local_offset
            end = start + count
            batches.append({
                "batch_id": f"turno{turn}-{start:08d}-{end:08d}",
                "turno": turn,
                "data_id": data_id,
                "inventory_path": inventory_path,
                "offset": local_offset,
                "limit": count,
                "range_start": start,
                "range_end": end,
            })
    if not batches:
        raise ValueError("no sections were selected")
    shards = []
    for start in range(0, len(batches), jobs_per_worker):
        shard_batches = batches[start:start + jobs_per_worker]
        shards.append({
            "shard_index": len(shards),
            "batch_ids": [batch["batch_id"] for batch in shard_batches],
        })
    return {
        "plan_schema_version": 1,
        "batch_size": batch_size,
        "jobs_per_worker": jobs_per_worker,
        "total_sections": sum(batch["limit"] for batch in batches),
        "total_batches": len(batches),
        "shards": shards,
        "batches": batches,
    }


def failed_batch_ids(plan: dict, result_index: dict) -> set[str]:
    """Return only batches whose worker run ended unsuccessfully or never completed."""
    known = {batch["batch_id"] for batch in plan.get("batches", [])}
    status_by_id = result_index.get("batch_status", {})
    return {batch_id for batch_id in known if status_by_id.get(batch_id) != "success"}


def retain_batches(plan: dict, batch_ids: set[str]) -> dict:
    batches = [batch for batch in plan.get("batches", []) if batch["batch_id"] in batch_ids]
    jobs_per_worker = int(plan.get("jobs_per_worker", 100))
    shards = []
    for start in range(0, len(batches), jobs_per_worker):
        group = batches[start:start + jobs_per_worker]
        shards.append({"shard_index": len(shards), "batch_ids": [batch["batch_id"] for batch in group]})
    return {**plan, "batches": batches, "shards": shards,
            "total_sections": sum(batch["limit"] for batch in batches),
            "total_batches": len(batches), "resumed_only_failed": True}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory-dir", type=Path, required=True)
    parser.add_argument("--turns", required=True, help="comma-separated turn numbers")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--jobs-per-worker", type=int, default=100)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    inventories = {}
    for turn in args.turns.split(","):
        turn = turn.strip()
        path = args.inventory_dir / f"turno{turn}" / "section-inventory.json"
        inventories[turn] = json.loads(path.read_text(encoding="utf-8"))
    plan = build_plan(inventories, batch_size=args.batch_size, jobs_per_worker=args.jobs_per_worker)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"total_sections": plan["total_sections"], "total_batches": plan["total_batches"],
                      "worker_runs": len(plan["shards"])}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
