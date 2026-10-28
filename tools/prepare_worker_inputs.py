#!/usr/bin/env python3
"""Materialize only one worker shard's section slices from the immutable full plan."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from render_worker_matrix import shard_matrix


def prepare_worker_inputs(plan: dict, inventory_root: Path, output_root: Path,
                          shard_index: int) -> dict:
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True)
    batches = []
    for batch in shard_matrix(plan, shard_index):
        source_path = inventory_root / batch["inventory_path"]
        source_inventory = json.loads(source_path.read_text(encoding="utf-8"))
        source_sections = source_inventory.get("sections")
        if not isinstance(source_sections, list):
            raise ValueError(f"inventory {source_path} has no sections[]")
        start, count = int(batch["offset"]), int(batch["limit"])
        sections = source_sections[start:start + count]
        if len(sections) != count:
            raise ValueError(f"batch {batch['batch_id']} exceeds its source inventory")
        target_path = output_root / "batches" / f"{batch['batch_id']}.json"
        target_path.parent.mkdir(parents=True, exist_ok=True)
        mini_inventory = {**source_inventory, "sections": sections,
                          "source_inventory_path": batch["inventory_path"],
                          "selection": {**source_inventory.get("selection", {}),
                                        "offset": int(batch["range_start"]),
                                        "limit": count, "selected_count": count}}
        target_path.write_text(json.dumps(mini_inventory, ensure_ascii=False, indent=2) + "\n",
                               encoding="utf-8")
        batches.append({**batch, "inventory_path": f"batches/{batch['batch_id']}.json",
                        "offset": 0})
    worker_plan = {**plan, "batches": batches,
                   "shards": [{"shard_index": shard_index,
                               "batch_ids": [batch["batch_id"] for batch in batches]}]}
    (output_root / "worker-plan.json").write_text(
        json.dumps(worker_plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return worker_plan


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--inventory-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    args = parser.parse_args()
    plan = json.loads(args.plan.read_text(encoding="utf-8"))
    result = prepare_worker_inputs(plan, args.inventory_root, args.output, args.shard_index)
    print(json.dumps({"total_batches": result["total_batches"],
                      "prepared_sections": result["total_sections"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
