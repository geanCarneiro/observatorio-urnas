#!/usr/bin/env python3
"""Dispatch bounded worker workflow runs and resume only failed worker jobs."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path


def _gh_json(args: list[str]) -> dict:
    result = subprocess.run(["gh", "api", *args], capture_output=True, text=True, check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "gh api failed")
    return json.loads(result.stdout)


def _worker_runs(repo: str, workflow_file: str) -> list[dict]:
    endpoint = f"repos/{repo}/actions/workflows/{workflow_file}/runs"
    payload = _gh_json([endpoint, "-f", "event=workflow_dispatch", "-f", "per_page=100"])
    return payload.get("workflow_runs", [])


def _find_run(repo: str, workflow_file: str, display_title: str) -> dict | None:
    return next((run for run in _worker_runs(repo, workflow_file)
                 if run.get("display_title") == display_title), None)


def dispatch_plan(plan: dict, *, parent_run_id: str, repo: str, ref: str,
                  workflow_file: str, index_path: Path, lookup_attempts: int = 30) -> dict:
    index = {"index_schema_version": 1, "parent_run_id": str(parent_run_id),
             "plan_run_id": str(parent_run_id), "ref": ref,
             "workflow_file": workflow_file, "worker_runs": [], "status": "dispatching"}
    for shard in plan.get("shards", []):
        shard_index = int(shard["shard_index"])
        title = f"coleta-plan-{parent_run_id}-shard-{shard_index}"
        record = {"shard_index": shard_index, "batch_ids": shard["batch_ids"],
                  "artifact_names": [f"dados-lote-{batch_id}-parte-{part}"
                                     for batch_id in shard["batch_ids"] for part in (1, 2)],
                  "display_title": title, "run_id": None, "status": "dispatching"}
        index["worker_runs"].append(record)
        index_path.write_text(json.dumps(index, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        endpoint = f"repos/{repo}/actions/workflows/{workflow_file}/dispatches"
        result = subprocess.run(["gh", "api", "--method", "POST", endpoint,
            "-f", f"ref={ref}", "-f", f"inputs[plan_run_id]={parent_run_id}",
            "-f", f"inputs[shard_index]={shard_index}"], capture_output=True, text=True, check=False)
        if result.returncode:
            record["status"] = "dispatch_failed"
            record["error"] = result.stderr.strip()
            index["status"] = "partial_dispatch"
            index_path.write_text(json.dumps(index, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            continue
        record["dispatch_attempted"] = True
        for _ in range(lookup_attempts):
            run = _find_run(repo, workflow_file, title)
            if run:
                record["run_id"] = run["id"]
                record["html_url"] = run.get("html_url")
                record["status"] = run.get("status", "queued")
                break
            time.sleep(4)
        if record["run_id"] is None:
            record["status"] = "dispatched_run_id_pending"
        index_path.write_text(json.dumps(index, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if index["status"] == "dispatching":
        index["status"] = "dispatched"
    index_path.write_text(json.dumps(index, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return index


def resume_index(index: dict, *, repo: str, plan: dict | None = None,
                 workflow_file: str = "coleta-abrangencia-worker.yml",
                 lookup_attempts: int = 30) -> dict:
    """Skip successful/active worker runs; ask Actions to retry failed jobs only."""
    outcomes = []
    workers_by_shard = {int(worker["shard_index"]): worker for worker in index.get("worker_runs", [])}
    if plan:
        for shard in plan.get("shards", []):
            shard_index = int(shard["shard_index"])
            workers_by_shard.setdefault(shard_index, {
                "shard_index": shard_index, "batch_ids": shard["batch_ids"], "run_id": None,
                "display_title": f"coleta-plan-{index['plan_run_id']}-shard-{shard_index}",
                "artifact_names": [f"dados-lote-{batch_id}-parte-{part}"
                                   for batch_id in shard["batch_ids"] for part in (1, 2)],
                "status": "planned",
            })
    index["worker_runs"] = [workers_by_shard[key] for key in sorted(workers_by_shard)]
    for worker in index.get("worker_runs", []):
        run_id = worker.get("run_id")
        if not run_id:
            found = _find_run(repo, workflow_file, worker.get("display_title", ""))
            if found:
                run_id = worker["run_id"] = found["id"]
                worker["html_url"] = found.get("html_url")
            else:
                if worker.get("dispatch_attempted"):
                    worker["resume_status"] = "unresolved_run_id"
                    outcomes.append("unresolved")
                    continue
                endpoint = f"repos/{repo}/actions/workflows/{workflow_file}/dispatches"
                result = subprocess.run(["gh", "api", "--method", "POST", endpoint,
                    "-f", f"ref={index.get('ref', 'main')}",
                    "-f", f"inputs[plan_run_id]={index['plan_run_id']}",
                    "-f", f"inputs[shard_index]={worker['shard_index']}"],
                    capture_output=True, text=True, check=False)
                if result.returncode:
                    worker["resume_status"] = "dispatch_failed"
                    worker["resume_error"] = result.stderr.strip()
                    outcomes.append("retry_failed")
                    continue
                worker["dispatch_attempted"] = True
                for _ in range(lookup_attempts):
                    found = _find_run(repo, workflow_file, worker["display_title"])
                    if found:
                        run_id = worker["run_id"] = found["id"]
                        worker["html_url"] = found.get("html_url")
                        break
                    time.sleep(4)
                if not run_id:
                    worker["resume_status"] = "dispatched_run_id_pending"
                    outcomes.append("unresolved")
                    continue
        details = _gh_json([f"repos/{repo}/actions/runs/{run_id}"])
        status, conclusion = details.get("status"), details.get("conclusion")
        worker["status"] = status
        worker["conclusion"] = conclusion
        worker["run_attempt"] = details.get("run_attempt")
        if status != "completed":
            worker["resume_status"] = "already_active"
            outcomes.append("active")
        elif conclusion == "success":
            worker["resume_status"] = "already_complete"
            outcomes.append("success")
        elif conclusion in {"failure", "cancelled", "timed_out", "action_required"}:
            result = subprocess.run(["gh", "run", "rerun", str(run_id), "--failed"],
                                    capture_output=True, text=True, check=False)
            if result.returncode:
                worker["resume_status"] = "retry_failed"
                worker["resume_error"] = result.stderr.strip()
                outcomes.append("retry_failed")
            else:
                worker["resume_status"] = "retry_started_failed_jobs_only"
                outcomes.append("retry_started")
        else:
            worker["resume_status"] = "no_retry_needed"
            outcomes.append("success")
    if outcomes and all(item == "success" for item in outcomes):
        index["resume_result"] = "already_complete_no_collection_repeated"
    elif "retry_failed" in outcomes or "unresolved" in outcomes:
        index["resume_result"] = "partial_retry"
    elif "retry_started" in outcomes:
        index["resume_result"] = "failed_jobs_retried"
    else:
        index["resume_result"] = "workers_still_active_no_duplicate_dispatch"
    return index


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    dispatch_parser = subparsers.add_parser("dispatch")
    dispatch_parser.add_argument("--plan", type=Path, required=True)
    dispatch_parser.add_argument("--parent-run-id", required=True)
    dispatch_parser.add_argument("--repo", required=True)
    dispatch_parser.add_argument("--ref", required=True)
    dispatch_parser.add_argument("--workflow-file", default="coleta-abrangencia-worker.yml")
    dispatch_parser.add_argument("--index", type=Path, required=True)
    resume_parser = subparsers.add_parser("resume")
    resume_parser.add_argument("--index", type=Path, required=True)
    resume_parser.add_argument("--plan", type=Path, required=True)
    resume_parser.add_argument("--repo", required=True)
    args = parser.parse_args()
    if args.command == "dispatch":
        plan = json.loads(args.plan.read_text(encoding="utf-8"))
        result = dispatch_plan(plan, parent_run_id=args.parent_run_id, repo=args.repo, ref=args.ref,
                               workflow_file=args.workflow_file, index_path=args.index)
        print(json.dumps(result, ensure_ascii=False))
        return 1 if result.get("status") == "partial_dispatch" else 0
    index = json.loads(args.index.read_text(encoding="utf-8"))
    plan = json.loads(args.plan.read_text(encoding="utf-8"))
    result = resume_index(index, repo=args.repo, plan=plan,
                          workflow_file=index.get("workflow_file", "coleta-abrangencia-worker.yml"))
    args.index.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"resume_result": result["resume_result"]}, ensure_ascii=False))
    return 1 if result["resume_result"] == "partial_retry" else 0


if __name__ == "__main__":
    raise SystemExit(main())
