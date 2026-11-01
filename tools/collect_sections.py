#!/usr/bin/env python3
"""Collect multiple sections independently; a failed invocation does not stop the batch."""

from __future__ import annotations

import argparse
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
import subprocess
import sys
import threading
from typing import Callable
from pathlib import Path

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from diagnostics import diagnostic, summarize_diagnostics
from section_aggregation import is_aggregated
from collect_section import complete_existing_acquisition

MAX_BATCH_SIZE = 1000
DEFAULT_WORKERS = 15
QUEUE_PREVIEW_SIZE = 20
PROGRESS_PREFIX = "@@OBSERVATORIO_PROGRESS@@"

def collect_sections(sections: list[dict], output_root: Path, runner=subprocess.run, data_id: str = "3221",
                    offset: int = 0, limit: int = MAX_BATCH_SIZE, *, incremental: bool = False,
                    staging_root: Path = Path(".estado-local/staging"), progress_callback: Callable | None = None,
                    stop_event=None, job_progress_callback: Callable | None = None,
                    max_workers: int = DEFAULT_WORKERS) -> dict:
    diagnostics = []
    results = []
    script = Path(__file__).with_name("collect_section.py")
    if offset < 0:
        raise ValueError("offset must be zero or greater")
    if not 1 <= limit <= MAX_BATCH_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_BATCH_SIZE}")
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    selected = sections[offset:offset + limit]

    def notify(result: dict) -> None:
        if progress_callback:
            progress_callback(result)

    def run_collector(command: list[str], job: dict) -> subprocess.CompletedProcess:
        if runner is not subprocess.run:
            return runner(command, capture_output=True, text=True, check=False)
        stdout_lines = []
        stderr_lines = []
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, encoding="utf-8", errors="replace", bufsize=1) as process:
            for line in process.stdout:
                if line.startswith(PROGRESS_PREFIX):
                    try:
                        update = json.loads(line[len(PROGRESS_PREFIX):])
                    except json.JSONDecodeError:
                        stderr_lines.append(line)
                        continue
                    with active_lock:
                        active[job["job_id"]] = {**active.get(job["job_id"], {}), **update}
                    if job_progress_callback:
                        job_progress_callback({"type": "job_progress", **job, **update})
                else:
                    try:
                        json.loads(line)
                        stdout_lines.append(line)
                    except json.JSONDecodeError:
                        stderr_lines.append(line)
            return_code = process.wait()
        return subprocess.CompletedProcess(command, return_code, "".join(stdout_lines), "".join(stderr_lines))

    pending_jobs = deque()
    for index, section in enumerate(selected, start=offset):
        key = "/".join((str(section.get("uf", "")).lower(), str(section.get("municipio", "")),
                        str(section.get("zona", "")), str(section.get("secao", ""))))
        if is_aggregated(section):
            result = {"section": key, "status": "aggregated", "return_code": 0,
                "acquisition": None, "principal_section_key": section.get("principal_section_key"),
                "diagnostic_count": 0, "file_states": []}
            results.append(result)
            notify(result)
            continue
        if stop_event and stop_event.is_set():
            break
        if incremental:
            existing = complete_existing_acquisition(output_root / key, key, data_id)
            if existing:
                result = {"section": key, "status": "skipped_existing", "return_code": 0,
                    "acquisition": str(existing), "downloaded_source_file_count": 0,
                    "diagnostic_count": 0, "file_states": [], "reason": "complete_local_acquisition"}
                results.append(result)
                notify(result)
                continue
        command = [sys.executable, str(script), "--uf", str(section.get("uf", "")),
                   "--municipio", str(section.get("municipio", "")), "--zona", str(section.get("zona", "")),
                   "--secao", str(section.get("secao", "")), "--data-id", data_id,
                   "--output", str(output_root), "--staging-root", str(staging_root), "--progress-jsonl"]
        if incremental:
            command.append("--incremental")
        turn = {"3220": 1, "3221": 2}.get(str(data_id))
        job = {"job_id": f"{key}:turno-{turn or '?'}", "section": key, "turn": turn}
        pending_jobs.append((index, key, command, job))

    active = {}
    active_lock = threading.Lock()
    futures = {}
    network_blocked = False

    def queue_snapshot() -> None:
        if not job_progress_callback:
            return
        with active_lock:
            active_snapshot = list(active.values())
        job_progress_callback({"type": "job_queue", "worker_limit": max_workers,
            "active_jobs": active_snapshot, "waiting_count": len(pending_jobs),
            "waiting_jobs": [dict(item[3], status="queued", progress=0.0, stage="Aguardando na fila")
                              for item in list(pending_jobs)[:QUEUE_PREVIEW_SIZE]]})

    def run_one(index: int, key: str, command: list[str], job: dict):
        try:
            completed = run_collector(command, job)
            try:
                summary = json.loads(completed.stdout) if completed.stdout.strip() else {}
            except json.JSONDecodeError as error:
                summary = {}
                item_diagnostics = [diagnostic(code="BATCH_COLLECTOR_OUTPUT_INVALID", stage="batch_collection",
                    operation="read_collector_result", error_type="parsing", cause=str(error), recoverability="permanent",
                    section=key, file_type="collection_result", file_name=None, file_path=None, exception=error,
                    position={"section_index": index}, context={"stderr": completed.stderr[-2000:]})]
            else:
                item_diagnostics = []
            if completed.returncode != 0:
                item_diagnostics.append(diagnostic(code="BATCH_SECTION_COLLECTION_FAILED", stage="batch_collection",
                    operation="collect_section", error_type="internal", cause=completed.stderr.strip() or "collector exited non-zero",
                    recoverability="indeterminate", section=key, file_type="section", file_name=None, file_path=None,
                    position={"section_index": index}, context={"return_code": completed.returncode}))
            result_diagnostics = summary.get("diagnostics", [])
            item_diagnostics.extend(result_diagnostics)
            if completed.returncode == 0 and (not summary.get("section_key") or not summary.get("acquisition")):
                item_diagnostics.append(diagnostic(code="BATCH_COLLECTOR_RESULT_INCOMPLETE", stage="batch_collection",
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
            result = {"section": summary.get("section_key", key),
                "status": section_status,
                "return_code": completed.returncode, "acquisition": summary.get("acquisition"),
                "downloaded_source_file_count": downloaded_count,
                "diagnostic_count": len(result_diagnostics), "file_states": states}
            network_error = next((item for item in result_diagnostics
                                  if item.get("code") == "NETWORK_ACCESS_DENIED"), None)
            if network_error:
                result["reason"] = network_error.get("cause") or "Acesso à rede bloqueado (WinError 10013)."
            return result, item_diagnostics
        except Exception as error:
            item_diagnostic = diagnostic(code="BATCH_SECTION_INVOCATION_FAILED", stage="batch_collection",
                operation="invoke_collector", error_type="internal", cause=str(error), recoverability="indeterminate",
                section=key, file_type="section", file_name=None, file_path=None, exception=error,
                position={"section_index": index}, context={"command": command})
            result = {"section": key, "status": "failed", "return_code": None, "acquisition": None,
                "diagnostic_count": 1, "file_states": []}
            return result, [item_diagnostic]

    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="urna-collect")
    try:
        while pending_jobs or futures:
            while (pending_jobs and len(futures) < max_workers and not network_blocked
                   and not (stop_event and stop_event.is_set())):
                index, key, command, job = pending_jobs.popleft()
                notify({"section": key, "status": "running", "return_code": None,
                        "acquisition": None, "file_states": []})
                active_item = {**job, "status": "running", "progress": 0.0, "stage": "Iniciando coleta"}
                with active_lock:
                    active[job["job_id"]] = active_item
                if job_progress_callback:
                    job_progress_callback({"type": "job_started", **active_item})
                futures[executor.submit(run_one, index, key, command, job)] = job
                queue_snapshot()
            queue_snapshot()
            if not futures:
                break
            done, _remaining = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                job = futures.pop(future)
                result, item_diagnostics = future.result()
                diagnostics.extend(item_diagnostics)
                results.append(result)
                notify(result)
                if any(item.get("code") == "NETWORK_ACCESS_DENIED" for item in item_diagnostics):
                    network_blocked = True
                    pending_jobs.clear()
                    message = ("Coleta interrompida: o Windows ou o ambiente de execução bloqueou o acesso à rede "
                               "(WinError 10013). Os workers ativos serão encerrados e as seções restantes ficarão pendentes.")
                    if job_progress_callback:
                        job_progress_callback({"type": "worker_error", "error": message})
                with active_lock:
                    active.pop(job["job_id"], None)
                if job_progress_callback:
                    job_progress_callback({"type": "job_finished", **job, "status": result["status"],
                                           "progress": 100.0, "stage": "Coleta concluída"})
                queue_snapshot()
    finally:
        executor.shutdown(wait=True, cancel_futures=bool(stop_event and stop_event.is_set()))
    counts = {"registered_sections": len(selected),
        "aggregated_sections": sum(item["status"] == "aggregated" for item in results),
        "principal_collectable_sections": sum(section.get("collectable", not is_aggregated(section))
                                               for section in selected),
        "effectively_collected_sections": sum(item["status"] in {"collected", "partial"} for item in results),
        "fully_collected_sections": sum(item["status"] == "collected" for item in results),
        "skipped_existing_sections": sum(item["status"] == "skipped_existing" for item in results),
        "partial_sections": sum(item["status"] == "partial" for item in results),
        "real_failures": sum(item["status"] in {"partial", "failed"} for item in results)}
    return {"report_type": "batch_collection", "data_id": data_id, "section_counts": counts,
            "section_count": len(results), "sections": results,
            "diagnostics": diagnostics, "diagnostic_summary": summarize_diagnostics(diagnostics),
            "network_blocked": network_blocked}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sections", type=Path, required=True,
                        help="JSON array or section-inventory report with a sections[] array")
    parser.add_argument("--data-id", default="3221", help="TSE arquivo-urna dataset ID (default: 3221)")
    parser.add_argument("--output", type=Path, default=Path("dados"))
    parser.add_argument("--staging-root", type=Path, default=Path(".estado-local/staging"))
    parser.add_argument("--incremental", action="store_true",
                        help="skip complete acquisitions and reuse unchanged local files")
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
                                  offset=args.offset, limit=args.limit, incremental=args.incremental,
                                  staging_root=args.staging_root)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
