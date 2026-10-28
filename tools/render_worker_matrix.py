#!/usr/bin/env python3
"""Render one bounded GitHub Actions worker matrix from a persisted plan."""

import argparse
import json
from pathlib import Path


def shard_matrix(plan: dict, shard_index: int) -> list[dict]:
    shards = plan.get("shards", [])
    shard = next((item for item in shards if int(item["shard_index"]) == shard_index), None)
    if shard is None:
        raise ValueError(f"worker shard {shard_index} is absent from the plan")
    by_id = {batch["batch_id"]: batch for batch in plan.get("batches", [])}
    return [by_id[batch_id] for batch_id in shard["batch_ids"]]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    args = parser.parse_args()
    matrix = shard_matrix(json.loads(args.plan.read_text(encoding="utf-8")), args.shard_index)
    if not 1 <= len(matrix) <= 100:
        raise SystemExit("worker matrix must contain between 1 and 100 jobs")
    print(json.dumps({"include": matrix}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
