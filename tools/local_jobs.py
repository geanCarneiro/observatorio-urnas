#!/usr/bin/env python3
"""Run, resume, and inspect local collection and reprocessing jobs."""

from __future__ import annotations

import argparse
import functools
import hashlib
import io
import json
import multiprocessing
import os
import subprocess
import shutil
import sqlite3
import sys
import threading
import time
import traceback
import uuid
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from queue import Empty

TOOLS_ROOT = Path(__file__).resolve().parent
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

from collect_sections import DEFAULT_WORKERS as COLLECTION_WORKERS, collect_sections
from discover_sections import DEFAULT_CATALOG_WORKERS, discover
from collect_section import complete_existing_acquisition, save_atomic, main as collect_section_main
from election_turns import folder_for_pleito
from analyze_section import analyze_collection
from extract_section_data import extract_collection, process_acquisitions
from temporary_transfer import reference_identity, write_reconciliation, write_section_excerpts, write_validation
from plan_collection import build_plan, build_section_unit_plan
from batch_consolidation import (PROXY_RULE_VERSION,
    build_summary_row, clean_data_root, clean_section_data, preserve_section_evidence, write_batch_csv,
    verify_tree_copy, write_batch_manifest, _reparse_point)


ROOT = TOOLS_ROOT.parent
DATA_ROOT = ROOT / "dados"
STATE_ROOT = ROOT / ".estado-local"
CATALOG_CACHE_ROOT = STATE_ROOT / "catalog-cache"
BU_SCHEMA = ROOT / "schemas" / "bu.asn1"
RDV_SCHEMA = ROOT / "schemas" / "rdv.asn1"
TURN_CONFIG = {"1": "3220", "2": "3221"}
COLLECTION_BATCH_SIZE = 500
REPROCESS_BATCH_SIZE = 250
DEFAULT_BATCH_SECTIONS = 500
DEFAULT_BATCH_WORKERS = 15
MAX_WINDOWS_PROCESS_WORKERS = 61
CONSOLIDATION_ROOT = ROOT / "consolidados"
_PROCESS_PROGRESS_QUEUE = None
_PROCESS_WORKER_STATE_PATH: Path | None = None
_PROCESS_WORKER_STATE: dict = {}
_PROCESS_WORKER_LAST_WRITE = 0.0
_PROCESS_WORKER_WRITE_INTERVAL = 0.5


def _configure_process_executable() -> None:
    """Use the windowless interpreter for spawned workers when it is available."""
    if os.name != "nt":
        return
    windowless_python = Path(sys.executable).with_name("pythonw.exe")
    if windowless_python.is_file():
        multiprocessing.set_executable(str(windowless_python))


def validate_worker_count(workers: int) -> int:
    if workers < 1:
        raise ValueError("workers deve ser pelo menos 1")
    if os.name == "nt" and workers > MAX_WINDOWS_PROCESS_WORKERS:
        raise ValueError(f"no Windows, ProcessPoolExecutor aceita no máximo {MAX_WINDOWS_PROCESS_WORKERS} workers")
    return workers


def _prepare_process_worker_state_dir(run_dir: Path) -> Path:
    """Start a fresh display snapshot set for this process-pool lifetime."""
    worker_state_dir = run_dir / "workers"
    worker_state_dir.mkdir(parents=True, exist_ok=True)
    for stale_snapshot in worker_state_dir.glob("worker-*.json"):
        stale_snapshot.unlink(missing_ok=True)
    return worker_state_dir


def _initialize_process_worker(progress_queue, worker_state_dir: str | Path, run_id: str) -> None:
    global _PROCESS_PROGRESS_QUEUE, _PROCESS_WORKER_STATE_PATH, _PROCESS_WORKER_STATE
    global _PROCESS_WORKER_LAST_WRITE
    _PROCESS_PROGRESS_QUEUE = progress_queue
    worker_state_dir = Path(worker_state_dir)
    worker_state_dir.mkdir(parents=True, exist_ok=True)
    pid = os.getpid()
    _PROCESS_WORKER_STATE_PATH = worker_state_dir / f"worker-{pid}.json"
    _PROCESS_WORKER_STATE = {"worker_id": f"pid-{pid}", "process_id": pid,
        "run_id": run_id, "status": "idle", "progress": 0.0,
        "stage": "Parado", "job_id": None, "section": None, "turn": None,
        "file": None, "last_task_stage": None, "last_task_section": None,
        "last_task_turn": None, "last_task_file": None, "last_task_outcome": None,
        "last_task_completed_at_utc": None, "finished_task_count": 0}
    _PROCESS_WORKER_LAST_WRITE = 0.0
    _write_process_worker_state({}, force=True)


def _write_process_worker_state(update: dict, *, force: bool = False) -> None:
    global _PROCESS_WORKER_STATE, _PROCESS_WORKER_LAST_WRITE
    if _PROCESS_WORKER_STATE_PATH is None:
        return
    now = time.monotonic()
    next_state = {**_PROCESS_WORKER_STATE, **{key: value for key, value in update.items()
        if key != "type"}}
    changed_identity = any(next_state.get(key) != _PROCESS_WORKER_STATE.get(key)
        for key in ("job_id", "stage", "status"))
    if not force and not changed_identity and now - _PROCESS_WORKER_LAST_WRITE < _PROCESS_WORKER_WRITE_INTERVAL:
        return
    next_state["updated_at_utc"] = _utc_now()
    _write_json(_PROCESS_WORKER_STATE_PATH, next_state)
    _PROCESS_WORKER_STATE = next_state
    _PROCESS_WORKER_LAST_WRITE = now


def _process_progress(task: dict, event: dict) -> None:
    pid = os.getpid()
    _write_process_worker_state({
        "worker_id": f"pid-{pid}", "process_id": pid,
        "run_id": task.get("run_id"), "job_id": task.get("job_id"),
        "section": task.get("section_key") or task.get("section"),
        "turn": task.get("turn"), "status": "running",
        **event,
    })
    if _PROCESS_PROGRESS_QUEUE is None:
        return
    _PROCESS_PROGRESS_QUEUE.put({**event, "job_id": task.get("job_id"),
        "run_id": task.get("run_id"), "section": task.get("section_key") or task.get("section"),
        "turn": task.get("turn"), "process_id": pid})


class _CollectorProgressStream:
    def __init__(self, task: dict, progress_scale: float = 1.0, progress_offset: float = 0.0):
        self.task = task
        self.progress_scale = progress_scale
        self.progress_offset = progress_offset
        self.buffer = ""
        self.errors = []

    def write(self, value: str) -> int:
        self.buffer += value
        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            prefix = "@@OBSERVATORIO_PROGRESS@@"
            if line.startswith(prefix):
                try:
                    event = json.loads(line[len(prefix):])
                    event["progress"] = self.progress_offset + float(event.get("progress") or 0) * self.progress_scale
                    event["stage"] = f"Coleta: {event.get('stage') or 'processando'}"
                    _process_progress(self.task, {"type": "job_progress", **event})
                except (TypeError, ValueError, json.JSONDecodeError):
                    self.errors.append(line)
            elif line.strip():
                self.errors.append(line)
        return len(value)

    def flush(self) -> None:
        if self.buffer:
            self.write("\n")


def _collect_section_in_process(task: dict, *, progress_scale: float = 1.0,
                                progress_offset: float = 0.0) -> dict:
    section = task["section"]
    turn = str(task["turn"])
    data_id = str(task.get("data_id") or TURN_CONFIG[turn])
    argv = ["--uf", str(section["uf"]), "--municipio", str(section["municipio"]),
        "--zona", str(section["zona"]), "--secao", str(section["secao"]),
        "--data-id", data_id, "--output", str(task.get("data_root") or DATA_ROOT),
        "--staging-root", str(task["staging_root"]), "--progress-jsonl"]
    if task.get("incremental"):
        argv.append("--incremental")
    stdout = io.StringIO()
    progress_stream = _CollectorProgressStream(task, progress_scale, progress_offset)
    try:
        with redirect_stdout(stdout), redirect_stderr(progress_stream):
            return_code = collect_section_main(argv)
        summary = json.loads(stdout.getvalue()) if stdout.getvalue().strip() else {}
    except BaseException as error:
        return {"status": "failed", "error": f"{type(error).__name__}: {error}",
                "traceback": traceback.format_exc(), "return_code": 1,
                "stderr": "\n".join(progress_stream.errors[-20:])}
    states = summary.get("file_states", [])
    core_states = [item for item in states if item.get("type") in {"bu", "rdv", "log"}]
    failed_states = {"missing", "not_found", "failed", "partial"}
    if return_code != 0 or not core_states:
        status = "failed"
    elif any(item.get("status") in failed_states for item in core_states):
        status = "partial"
    else:
        status = "collected"
    return {**summary, "status": status, "return_code": return_code,
        "reason": "\n".join(progress_stream.errors[-20:]) or None}


def _execute_process_task(task: dict) -> dict:
    """Execute one section unit in a persistent OS worker process."""
    operation = task.get("operation")
    _process_progress(task, {"type": "job_progress", "progress": 0.0,
        "stage": task.get("stage_label") or task.get("stage") or "Iniciando", "process_id": os.getpid()})
    try:
        if operation == "collect_extract":
            section, turn = task["section"], str(task["turn"])
            collect_task = {**task, "data_id": TURN_CONFIG[turn], "incremental": task.get("incremental", True)}
            collection = _collect_section_in_process(collect_task, progress_scale=0.5)
            raw_status = collection.get("status", "failed")
            acquisition = collection.get("acquisition")
            extraction = None
            if raw_status in {"collected", "partial"} and acquisition and Path(acquisition).is_file():
                _process_progress(task, {"type": "job_progress", "progress": 50.0,
                    "stage": "Extração: iniciando análise da seção"})
                extraction_results = []
                process_acquisitions([Path(acquisition)], BU_SCHEMA, RDV_SCHEMA,
                    extraction_results.append, None,
                    job_progress_callback=lambda event: _process_progress(task, {
                        "type": "job_progress", "progress": 50 + float(event.get("progress") or 0) * 0.5,
                        "stage": f"Extração: {event.get('stage') or 'processando'}",
                        "file": event.get("file")}), max_workers=1, generate_excerpts=False)
                extraction = extraction_results[-1] if extraction_results else {"status": "failed"}
            return {"turn": turn, "section": task["section_key"], "raw_status": raw_status,
                "acquisition": acquisition, "reason": collection.get("reason"),
                "traceback": collection.get("traceback"),
                "extract_status": extraction.get("status") if extraction else (
                    "not_needed" if raw_status in {"skipped_existing", "aggregated"} else "pending"),
                "extract_error": extraction.get("error") if extraction else None,
                "network_blocked": any(item.get("code") == "NETWORK_ACCESS_DENIED" for item in
                    collection.get("diagnostics", [])), "result": collection,
                "process_id": os.getpid()}
        if operation == "batch_pipeline":
            item = task["item"]
            section = item["section"]
            turn = str(item["turn"])
            raw_status, acquisition = item["raw_status"], item.get("acquisition_path")
            collection_error = None
            network_blocked = False
            if not (raw_status in {"collected", "partial"} and acquisition and Path(acquisition).is_file()):
                collection = _collect_section_in_process({**task, "section": section, "turn": turn,
                    "data_id": TURN_CONFIG[turn], "incremental": False,
                    "staging_root": task["staging_root"]}, progress_scale=0.5)
                raw_status, acquisition = collection.get("status", "failed"), collection.get("acquisition")
                collection_error = collection.get("reason") or collection.get("error")
                network_blocked = any(item.get("code") == "NETWORK_ACCESS_DENIED"
                    or "10013" in str(item.get("cause", "")) for item in collection.get("diagnostics", []))
            if acquisition and Path(acquisition).is_file():
                _process_progress(task, {"type": "job_progress", "progress": 50.0,
                    "stage": "Análise determinística da seção"})
                report = analyze_collection(Path(acquisition), [], BU_SCHEMA, RDV_SCHEMA,
                    progress_callback=lambda update: _process_progress(task, {
                        "type": "job_progress", "progress": 50 + min(95, max(0,
                            95 * float(update.get("processed") or 0) / max(1, float(update.get("total") or 0)))) * 0.5,
                        "stage": "Análise determinística da seção", "file": update.get("file")}))
            else:
                report = {"file_processing": {}, "diagnostics": ([{"error": collection_error}]
                    if collection_error else [])}
            row, keep_evidence = build_summary_row(section, int(turn), report)
            extract_status = "complete" if row["status_processamento"] == "processado" else "failed"
            error_message = collection_error or (None if extract_status == "complete" else
                "análise insuficiente para consolidar todas as fontes")
            return {"item": item, "cancelled": False, "raw_status": raw_status,
                "acquisition_path": acquisition, "network_blocked": network_blocked,
                "row": row, "keep_evidence": keep_evidence, "extract_status": extract_status,
                "error": error_message, "process_id": os.getpid()}
        if operation == "reprocess_one":
            acquisition_path = Path(task["acquisition_path"])
            output_path = acquisition_path.parent / "transferencia-temporaria-elegiveis.csv"
            extract_collection(acquisition_path, BU_SCHEMA, RDV_SCHEMA,
                task.get("excerpt_metadata", {}).get(str(output_path)),
                progress_callback=lambda update: _process_progress(task, {
                    "type": "job_progress", "progress": float(update.get("progress") or 0),
                    "stage": update.get("stage") or "Reprocessando aquisição",
                    "file": update.get("file")}))
            return {"acquisition": str(acquisition_path), "section": task["section_key"],
                "turn": task["turn"], "status": "complete", "process_id": os.getpid()}
        if operation == "write_excerpts":
            try:
                metadata = write_section_excerpts(task["targets"], progress_callback=lambda update:
                    _process_progress(task, {"type": "job_progress", "stage": update.get("stage"),
                        "progress": update.get("progress"), "file": update.get("file"),
                        "processed": update.get("rows_processed"), "total": update.get("rows_total")}))
                return {"excerpt_metadata": metadata, "error": None, "process_id": os.getpid()}
            except Exception as error:
                identity = reference_identity()
                metadata = {str(target["output_path"]): {"status": "not_generated",
                    "reason": f"{type(error).__name__}: {error}",
                    "reference_zip_sha256": identity.get("zip_sha256"),
                    "reference_version": identity.get("version")} for target in task["targets"]}
                return {"excerpt_metadata": metadata, "error": f"{type(error).__name__}: {error}",
                    "traceback": traceback.format_exc(), "process_id": os.getpid()}
        if operation == "section_stage":
            section, key, stage = task["section"], task["section_key"], task["stage"]
            outcomes = []
            if stage == "audit":
                turn_checks, expected_paths = [], set()
                section_root = DATA_ROOT.joinpath(*(str(section[name]) for name in
                    ("uf", "municipio", "zona", "secao")))
                for turn in task["turns"]:
                    summary = task["summaries"].get(turn)
                    progress = lambda done, total, name, turn=turn: _process_progress(task, {
                        "type": "job_progress", "turn": int(turn),
                        "progress": round(100 * done / max(1, total), 2),
                        "stage": "Auditoria física", "detail": "Verificação de integridade", "file": name})
                    inspected = _inspect_physical_acquisition(section, turn, progress) if section_root.exists() else {
                        "turn": turn, "present": False, "valid": False, "reason": "raiz bruta ausente"}
                    if inspected.get("valid"):
                        expected_paths.update(inspected.get("expected_paths", set()))
                    summary_row = summary.get("row") if summary else None
                    summary_hash = hashlib.sha256(json.dumps(summary_row, ensure_ascii=False,
                        separators=(",", ":")).encode("utf-8")).hexdigest() if summary_row else None
                    summary_valid = bool(summary_row and summary["extract_status"] != "failed"
                        and summary.get("consolidation_status") == "complete"
                        and summary_row.get("status_processamento") in {"processado", "agregada"}
                        and (not summary.get("consolidation_row_sha256")
                            or summary.get("consolidation_row_sha256") == summary_hash))
                    if section.get("aggregation_role") == "aggregated":
                        action = "keep" if summary_valid else "consolidate"
                    elif inspected.get("present") and not inspected.get("valid"):
                        action = "collect"
                    elif inspected.get("valid") and (not summary_valid or
                            not _summary_matches_acquisition(summary_row, inspected)):
                        action = "consolidate"
                    elif summary_valid and not inspected.get("present") and summary.get("acquisition_path"):
                        action = "collect"
                    elif summary_valid:
                        action = "keep"
                    else:
                        action = "collect"
                    turn_checks.append({"turn": turn, "action": action,
                        "summary_valid": summary_valid, "acquisition": inspected})
                untracked = []
                if (section_root.exists()
                        and set(task["turns"]) == set(str(turn) for turn in turns_by_key[key])):
                    for current, directories, files in os.walk(section_root, topdown=True, followlinks=False):
                        current_path = Path(current)
                        for name in directories:
                            candidate = current_path / name
                            if _reparse_point(candidate):
                                untracked.append(str(candidate))
                        for name in files:
                            candidate = current_path / name
                            if _reparse_point(candidate) or not candidate.is_file():
                                untracked.append(str(candidate))
                            elif candidate.resolve() not in expected_paths:
                                untracked.append(str(candidate))
                return {"task": task, "success": True, "outcomes": [],
                    "audit": {"turns": turn_checks, "untracked_files": untracked[:20],
                        "untracked_count": len(untracked)}, "process_id": os.getpid()}
            if stage == "collect":
                for turn in task["collect_turns"]:
                    collection = _collect_section_in_process({**task, "turn": turn,
                        "data_id": TURN_CONFIG[turn], "incremental": False,
                        "staging_root": task["staging_root"]}, progress_scale=1.0)
                    acquisition = complete_existing_acquisition(DATA_ROOT.joinpath(
                        *(str(section[name]) for name in ("uf", "municipio", "zona", "secao"))),
                        key, TURN_CONFIG[turn])
                    outcomes.append({"turn": turn,
                        "status": "collected" if acquisition else collection.get("status", "failed"),
                        "acquisition": str(acquisition) if acquisition else collection.get("acquisition"),
                        "error": collection.get("reason") or collection.get("error"),
                        "traceback": collection.get("traceback"),
                        "network_blocked": any(item.get("code") == "NETWORK_ACCESS_DENIED"
                            or "10013" in str(item.get("cause", ""))
                            for item in collection.get("diagnostics", []))})
            elif stage == "consolidate":
                for turn in task["turns"]:
                    row_traceback = None
                    if section.get("aggregation_role") == "aggregated":
                        report, raw_status = {"file_processing": {}, "diagnostics": []}, "aggregated"
                    else:
                        acquisition = complete_existing_acquisition(DATA_ROOT.joinpath(
                            *(str(section[name]) for name in ("uf", "municipio", "zona", "secao"))),
                            key, TURN_CONFIG[turn])
                        if acquisition:
                            report = analyze_collection(acquisition, [], BU_SCHEMA, RDV_SCHEMA,
                                progress_callback=lambda event, turn=turn: _process_progress(task, {
                                    "type": "job_progress", "turn": int(turn),
                                    "progress": round(100 * float(event.get("processed") or 0) /
                                        max(1, float(event.get("total") or 1)), 2),
                                    "stage": "Conciliação determinística",
                                    "detail": "Análise determinística", "file": event.get("file")}))
                            raw_status = "collected"
                        else:
                            report, raw_status = {"file_processing": {},
                                "diagnostics": [{"error": "aquisição incompleta"}]}, "failed"
                    try:
                        row, evidence = build_summary_row(section, int(turn), report)
                        extract_status = "complete" if row["status_processamento"] in {"processado", "agregada"} else "failed"
                        error = None if extract_status == "complete" else "análise insuficiente para consolidar todas as fontes"
                    except Exception as row_error:
                        row, evidence = build_summary_row(section, int(turn),
                            {"file_processing": {}, "diagnostics": [{"error": str(row_error)}]})
                        row["status_processamento"] = "falha"
                        extract_status, error = "failed", f"{type(row_error).__name__}: {row_error}"
                        row_traceback = traceback.format_exc()
                    outcomes.append({"turn": turn, "row": row, "evidence_required": evidence,
                        "raw_status": raw_status, "extract_status": extract_status, "error": error,
                        "traceback": row_traceback,
                        "acquisition": str(acquisition) if stage == "consolidate" and section.get("aggregation_role") != "aggregated" and acquisition else None})
            elif stage == "cleanup":
                section_root = DATA_ROOT.joinpath(*(str(section[name]) for name in
                    ("uf", "municipio", "zona", "secao")))
                physical_validation = _validate_physical_section_for_cleanup(section, task["summary_rows"],
                    task.get("raw_expected_turns", set()))
                if task["evidence_turns"] and physical_validation["raw_tree_present"]:
                    evidence_copy = preserve_section_evidence(DATA_ROOT, Path(task["output_root"]) / "evidencias", section)
                    if evidence_copy is None:
                        raise ValueError(f"seção {key}: cópia de evidência não foi criada")
                    physical_validation["evidence_copy"] = verify_tree_copy(section_root, evidence_copy)
                clean_section_data(DATA_ROOT, ROOT, section)
                if section_root.exists():
                    raise RuntimeError(f"seção {key}: a árvore bruta permaneceu após a limpeza")
                return {"task": task, "outcomes": [], "success": True,
                    "physical_validation": physical_validation, "process_id": os.getpid()}
            return {"task": task, "outcomes": outcomes, "success": True,
                "network_blocked": any(item.get("network_blocked", False) for item in outcomes),
                "process_id": os.getpid()}
    except Exception as error:
        error_traceback = traceback.format_exc()
        if operation == "collect_extract":
            return {"turn": str(task.get("turn")), "section": task.get("section_key"),
                "raw_status": "failed", "extract_status": "pending",
                "reason": f"{type(error).__name__}: {error}", "traceback": error_traceback,
                "process_id": os.getpid()}
        if operation == "batch_pipeline":
            item = task["item"]
            section = item["section"]
            row, keep_evidence = build_summary_row(section, int(item["turn"]),
                {"file_processing": {}, "diagnostics": [{"error": str(error)}]})
            row["status_processamento"] = "falha"
            return {"item": item, "cancelled": False, "raw_status": "failed",
                "acquisition_path": item.get("acquisition_path"), "network_blocked": False,
                "row": row, "keep_evidence": keep_evidence, "extract_status": "failed",
                "error": f"{type(error).__name__}: {error}", "traceback": error_traceback,
                "process_id": os.getpid()}
        if operation == "reprocess_one":
            return {"acquisition": str(task.get("acquisition_path", "")),
                "section": task.get("section_key"), "turn": task.get("turn"),
                "status": "failed", "error": f"{type(error).__name__}: {error}",
                "traceback": error_traceback, "process_id": os.getpid()}
        if operation == "section_stage":
            return {"task": task.get("task") or task, "outcomes": [], "success": False,
                "error": f"{type(error).__name__}: {error}", "network_blocked": False,
                "traceback": error_traceback, "process_id": os.getpid()}
        raise


def _process_task_outcome(result: dict | None) -> str:
    """Summarize a worker result for the live dashboard without replacing its full result."""
    if not isinstance(result, dict):
        return "completed"
    if result.get("cancelled"):
        return "cancelled"
    if result.get("success") is False or result.get("error") or result.get("extract_error"):
        return "failed"

    statuses = [result.get(name) for name in ("status", "raw_status", "extract_status")]
    for outcome in result.get("outcomes", []) if isinstance(result.get("outcomes"), list) else []:
        if isinstance(outcome, dict):
            statuses.extend(outcome.get(name) for name in ("status", "raw_status", "extract_status"))
            if outcome.get("error"):
                return "failed"
    normalized = {str(status).casefold() for status in statuses if status is not None}
    if normalized & {"failed", "error", "falha"}:
        return "failed"
    if normalized & {"partial", "parcial"}:
        return "partial"
    return "completed"


def _run_process_task(task: dict) -> dict:
    """Run a task and retain its outcome in the worker snapshot after it becomes idle."""
    result = None
    outcome = "failed"
    try:
        result = _execute_process_task(task)
        outcome = _process_task_outcome(result)
        return result
    finally:
        pid = os.getpid()
        previous = _PROCESS_WORKER_STATE
        finished_task_count = int(previous.get("finished_task_count") or 0) + 1
        _write_process_worker_state({"worker_id": f"pid-{pid}", "process_id": pid,
            "run_id": task.get("run_id"), "status": "idle", "job_id": None,
            "last_task_stage": previous.get("stage") or task.get("stage_label") or task.get("stage") or task.get("operation"),
            "last_task_section": previous.get("section") or task.get("section_key"),
            "last_task_turn": previous.get("turn") or task.get("turn"),
            "last_task_file": previous.get("file"), "last_task_outcome": outcome,
            "last_task_completed_at_utc": _utc_now(),
            "finished_task_count": finished_task_count,
            "stage": "Aguardando próxima tarefa", "section": None, "turn": None,
            "file": None, "progress": 0.0}, force=True)


def _drain_process_progress(progress_queue, active_jobs: dict, active_lock: threading.Lock,
                            progress_callback: Callable | None) -> None:
    while True:
        try:
            event = progress_queue.get_nowait()
        except Empty:
            return
        job_id = event.get("job_id")
        if job_id:
            with active_lock:
                # A multiprocessing queue can deliver the last progress message
                # after the coordinator has already collected the future and
                # removed its job. Never let that late message resurrect a
                # completed task in the active-worker snapshot.
                if job_id not in active_jobs:
                    continue
                active_jobs[job_id] = {**active_jobs[job_id], **event}
        _emit(progress_callback, event)


@contextmanager
def _borrow_executor(executor):
    """Use a persistent executor across phases without closing it at each phase boundary."""
    yield executor


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]


def _physical_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hash_set(value: object) -> set[str]:
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = value
    else:
        parsed = value
    values = parsed if isinstance(parsed, list) else [parsed]
    return {str(item).casefold() for item in values if item not in (None, "")}


def _inspect_physical_acquisition(section: dict, turn: str, progress_callback: Callable | None = None) -> dict:
    """Check the acquisition manifest and every recorded file, including content hashes."""
    key = "/".join(str(section[name]) for name in ("uf", "municipio", "zona", "secao"))
    section_root = DATA_ROOT.joinpath(*(str(section[name]) for name in ("uf", "municipio", "zona", "secao")))
    acquisition_candidate = section_root / folder_for_pleito(TURN_CONFIG[turn]) / "acquisition.json"
    acquisition_path = complete_existing_acquisition(section_root, key, TURN_CONFIG[turn])
    if acquisition_path is None:
        return {"turn": turn, "present": acquisition_candidate.exists(),
                "valid": False, "reason": "manifesto ausente, inválido ou aquisição incompleta"}
    try:
        if _reparse_point(section_root) or _reparse_point(acquisition_path):
            raise ValueError("raiz da seção ou manifesto de aquisição é link/reparse point")
        acquisition = json.loads(acquisition_path.read_text(encoding="utf-8"))
        manifest_path = acquisition_path.parent / "manifest.aux.jws"
        if _reparse_point(manifest_path):
            raise ValueError("manifesto JWS é link/reparse point")
        observed_manifest_hash = _physical_sha256(manifest_path)
        expected_manifest_hash = str(acquisition.get("manifest_sha256") or "").casefold()
        if not expected_manifest_hash or observed_manifest_hash != expected_manifest_hash:
            raise ValueError("hash físico do manifesto JWS divergente")
        files_by_type: dict[str, set[str]] = {kind: set() for kind in ("bu", "rdv", "log")}
        files = acquisition.get("files", [])
        total_files = len(files) + 1
        verified_step = 1
        if progress_callback:
            progress_callback(verified_step, total_files, manifest_path.name)
        expected_paths = {acquisition_path.resolve(), manifest_path.resolve()}
        verified_files = 0
        verified_bytes = 0
        section_resolved = section_root.resolve()
        for item in files:
            kind = item.get("type")
            relative_path = item.get("relative_path")
            if not isinstance(relative_path, str):
                raise ValueError(f"arquivo {kind} sem caminho relativo")
            unresolved_path = acquisition_path.parent / relative_path
            cursor = unresolved_path
            while cursor != section_root and section_root in cursor.parents:
                if _reparse_point(cursor):
                    raise ValueError(f"caminho contém link/reparse point ({kind}: {relative_path})")
                cursor = cursor.parent
            physical_path = unresolved_path.resolve()
            try:
                physical_path.relative_to(section_resolved)
            except ValueError as error:
                raise ValueError(f"arquivo {kind} fora da pasta da seção") from error
            if not physical_path.is_file():
                raise ValueError(f"arquivo físico ausente ({kind}: {relative_path})")
            expected_paths.add(physical_path)
            observed_size = physical_path.stat().st_size
            if item.get("size_bytes") is not None and observed_size != int(item["size_bytes"]):
                raise ValueError(f"tamanho físico divergente ({kind}: {relative_path})")
            observed_hash = _physical_sha256(physical_path)
            expected_hash = str(item.get("sha256") or "").casefold()
            if not expected_hash or observed_hash != expected_hash:
                raise ValueError(f"hash físico divergente ({kind}: {relative_path})")
            if kind in files_by_type:
                files_by_type[kind].add(observed_hash)
            verified_files += 1
            verified_bytes += observed_size
            verified_step += 1
            if progress_callback:
                progress_callback(verified_step, total_files, str(relative_path))
        for kind, hashes in files_by_type.items():
            if not hashes:
                raise ValueError(f"aquisição sem arquivos completos de {kind}")
        return {"turn": turn, "present": True, "valid": True,
                "acquisition_path": str(acquisition_path),
                "manifest_sha256": observed_manifest_hash, "hashes": files_by_type,
                "files": verified_files + 1, "bytes": verified_bytes + manifest_path.stat().st_size,
                "expected_paths": expected_paths}
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
        return {"turn": turn, "present": True, "valid": False, "reason": str(error)}


def _summary_matches_acquisition(row: dict, inspected: dict) -> bool:
    if not inspected.get("valid"):
        return False
    if str(row.get("hash_manifesto") or "").casefold() != inspected["manifest_sha256"]:
        return False
    return all(_hash_set(row.get(field)) == inspected["hashes"][kind]
               for kind, field in (("bu", "hash_bu"), ("rdv", "hash_rdv"),
                                   ("log", "hashes_log_json")))


def _validate_physical_section_for_cleanup(section: dict, summary_rows: list[dict],
                                           raw_expected_turns: set[str] | None = None) -> dict:
    """Refuse raw cleanup unless durable summaries match verified physical acquisitions."""
    key = "/".join(str(section[name]) for name in ("uf", "municipio", "zona", "secao"))
    section_root = DATA_ROOT.joinpath(*(str(section[name]) for name in ("uf", "municipio", "zona", "secao")))
    if not section_root.exists():
        return {"section_key": key, "raw_tree_present": False, "verified_turns": 0,
                "verified_files": 0, "verified_bytes": 0}
    if section.get("aggregation_role") == "aggregated":
        raise ValueError(f"seção agregada {key} tem dados brutos físicos; limpeza recusada")
    rows_by_turn = {str(int(row.get("turno"))): row for row in summary_rows}
    if not rows_by_turn:
        raise ValueError(f"seção {key} não tem resumos físicos para validar")

    verified_files = 0
    verified_bytes = 0
    manifest_hashes = []
    inspected_by_turn = {}
    for turn, row in sorted(rows_by_turn.items(), key=lambda item: int(item[0])):
        if row.get("status_processamento") != "processado":
            raise ValueError(f"seção {key}, turno {turn}: resumo não está processado")
        acquisition_candidate = section_root / folder_for_pleito(TURN_CONFIG[turn]) / "acquisition.json"
        if not acquisition_candidate.exists() and turn not in (raw_expected_turns or set()):
            # A prior cleanup may already have removed this turn's raw files; its durable summary remains.
            continue
        inspected = _inspect_physical_acquisition(section, turn)
        inspected_by_turn[turn] = inspected
        if not inspected.get("valid"):
            raise ValueError(f"seção {key}, turno {turn}: {inspected.get('reason')}")
        if not _summary_matches_acquisition(row, inspected):
            raise ValueError(f"seção {key}, turno {turn}: hashes do resumo não conferem com os arquivos")
        if not inspected.get("expected_paths"):
            raise ValueError(f"seção {key}, turno {turn}: inventário físico não foi produzido")
        for kind, summary_field in (("bu", "hash_bu"), ("rdv", "hash_rdv"), ("log", "hashes_log_json")):
            if not _hash_set(row.get(summary_field)):
                raise ValueError(f"seção {key}, turno {turn}: hashes de {kind} no resumo não conferem com os arquivos")
        verified_files += int(inspected["files"])
        verified_bytes += int(inspected["bytes"])
        manifest_hashes.append(inspected["manifest_sha256"])

    expected_paths = set()
    for turn in rows_by_turn:
        inspected = inspected_by_turn.get(turn) or _inspect_physical_acquisition(section, turn)
        if inspected.get("valid"):
            expected_paths.update(inspected.get("expected_paths", set()))
    actual_paths = set()
    for current, directories, files in os.walk(section_root, topdown=True, followlinks=False):
        current_path = Path(current)
        for name in directories:
            candidate = current_path / name
            if _reparse_point(candidate):
                raise ValueError(f"seção {key}: link/reparse point não pode ser validado: {candidate}")
        for name in files:
            candidate = current_path / name
            if candidate.is_symlink() or not candidate.is_file():
                raise ValueError(f"seção {key}: arquivo inseguro não pode ser validado: {candidate}")
            actual_paths.add(candidate.resolve())
    untracked = actual_paths - expected_paths
    if untracked:
        sample = sorted(str(path.relative_to(section_root.resolve())) for path in untracked)[:5]
        raise ValueError(f"seção {key}: {len(untracked)} arquivo(s) não referenciado(s) por aquisições válidas; limpeza recusada: {sample}")

    return {"section_key": key, "raw_tree_present": True, "verified_turns": len(rows_by_turn),
            "verified_files": verified_files, "verified_bytes": verified_bytes,
            "manifest_sha256": manifest_hashes}


class _RunStopSignal:
    """Combine the owner's in-process event with a stop request from another UI process."""

    def __init__(self, local_event: threading.Event | None, request_path: Path):
        self.local_event = local_event
        self.request_path = request_path

    def is_set(self) -> bool:
        return bool((self.local_event and self.local_event.is_set()) or self.request_path.exists())

    def set(self) -> None:
        if self.local_event:
            self.local_event.set()
        self.request_path.parent.mkdir(parents=True, exist_ok=True)
        self.request_path.touch(exist_ok=True)


class _RunPauseRequested(Exception):
    """Internal signal used to exit resumable preparation without marking the run failed."""


def request_active_run_stop() -> str:
    active = next((run for run in list_runs() if run.get("status") in {"preparing", "running"}), None)
    if not active:
        raise RuntimeError("não há execução ativa para parar")
    if not active.get("cross_process_stop_supported"):
        raise RuntimeError("esta execução foi iniciada por uma versão sem controle de parada entre processos; use a UI original que iniciou o job")
    run_id = str(active.get("run_id") or "")
    if not run_id or any(character not in "0123456789T-abcdefghijklmnopqrstuvwxyz" for character in run_id):
        raise RuntimeError("identificador da execução ativa inválido")
    run_dir = (STATE_ROOT / "runs" / run_id).resolve()
    runs_root = (STATE_ROOT / "runs").resolve()
    if runs_root not in run_dir.parents:
        raise RuntimeError("caminho da execução ativa inválido")
    marker = run_dir / "stop.request"
    marker.touch(exist_ok=True)
    return run_id


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(min(0.05 * (2 ** attempt), 0.5))
    finally:
        temporary.unlink(missing_ok=True)


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("""CREATE TABLE IF NOT EXISTS section_jobs (
        turn TEXT NOT NULL,
        data_id TEXT NOT NULL,
        section_key TEXT NOT NULL,
        section_json TEXT NOT NULL,
        raw_status TEXT NOT NULL DEFAULT 'pending',
        extract_status TEXT NOT NULL DEFAULT 'pending',
        consolidation_status TEXT NOT NULL DEFAULT 'pending',
        consolidated_at_utc TEXT,
        consolidation_row_sha256 TEXT,
        acquisition_path TEXT,
        error TEXT,
        batch_index INTEGER,
        PRIMARY KEY (turn, section_key)
    )""")
    columns = {row[1] for row in connection.execute("PRAGMA table_info(section_jobs)")}
    if "batch_index" not in columns:
        connection.execute("ALTER TABLE section_jobs ADD COLUMN batch_index INTEGER")
    if "consolidation_status" not in columns:
        connection.execute("ALTER TABLE section_jobs ADD COLUMN consolidation_status TEXT NOT NULL DEFAULT 'pending'")
    if "consolidated_at_utc" not in columns:
        connection.execute("ALTER TABLE section_jobs ADD COLUMN consolidated_at_utc TEXT")
    if "consolidation_row_sha256" not in columns:
        connection.execute("ALTER TABLE section_jobs ADD COLUMN consolidation_row_sha256 TEXT")
    connection.execute("""CREATE TABLE IF NOT EXISTS section_summaries (
        turn TEXT NOT NULL,
        section_key TEXT NOT NULL,
        batch_index INTEGER NOT NULL,
        row_json TEXT NOT NULL,
        evidence_required INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (turn, section_key)
    )""")
    connection.execute("""CREATE TABLE IF NOT EXISTS section_audits (
        section_key TEXT PRIMARY KEY,
        result_json TEXT NOT NULL,
        audited_at_utc TEXT NOT NULL
    )""")
    connection.execute("""CREATE TABLE IF NOT EXISTS processing_batches (
        batch_index INTEGER PRIMARY KEY,
        batch_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        row_count INTEGER NOT NULL DEFAULT 0,
        csv_path TEXT,
        manifest_path TEXT,
        csv_sha256 TEXT,
        state_outputs_json TEXT,
        error TEXT
    )""")
    batch_columns = {row[1] for row in connection.execute("PRAGMA table_info(processing_batches)")}
    if "state_outputs_json" not in batch_columns:
        connection.execute("ALTER TABLE processing_batches ADD COLUMN state_outputs_json TEXT")
    legacy_cursor = connection.execute("""SELECT j.turn,j.section_key,s.row_json
        FROM section_jobs j JOIN section_summaries s
        ON s.turn=j.turn AND s.section_key=j.section_key
        WHERE j.consolidation_status='pending' AND j.extract_status IN ('complete','not_needed')""")
    while legacy_rows := legacy_cursor.fetchmany(250):
        for row in legacy_rows:
            try:
                summary = json.loads(row["row_json"])
                if summary.get("status_processamento") not in {"processado", "agregada"}:
                    continue
                canonical_row = json.dumps(summary, ensure_ascii=False, separators=(",", ":"))
                row_hash = hashlib.sha256(canonical_row.encode("utf-8")).hexdigest()
                connection.execute("""UPDATE section_jobs SET consolidation_status='complete',
                    consolidated_at_utc=COALESCE(consolidated_at_utc,?),
                    consolidation_row_sha256=COALESCE(consolidation_row_sha256,?)
                    WHERE turn=? AND section_key=?""",
                    (_utc_now(), row_hash, row["turn"], row["section_key"]))
            except (TypeError, json.JSONDecodeError, AttributeError):
                continue
    connection.commit()
    return connection


def _emit(callback: Callable | None, event: dict) -> None:
    if callback:
        callback(event)


def _persistent_callback(run_dir: Path, callback: Callable | None, worker_limit: int = DEFAULT_BATCH_WORKERS) -> Callable:
    event_path = run_dir / "events.jsonl"
    event_lock = threading.Lock()
    transient_job_events = {"job_queue", "job_progress"}
    heartbeat_path = run_dir / "heartbeat.json"
    heartbeat_interval = 3.0
    last_heartbeat = 0.0
    last_worker_snapshot = 0.0
    active_workers: dict[str, dict] = {}
    waiting_jobs: list[dict] = []
    waiting_count = 0
    worker_limit = validate_worker_count(int(worker_limit))
    # A resumed run may have a stale worker-state snapshot from before it was
    # paused. Publish an empty, current snapshot before the UI attaches again.
    _write_json(run_dir / "worker-state.json", {
        "worker_limit": worker_limit, "active_jobs": [], "waiting_count": 0,
        "waiting_jobs": [], "updated_at_utc": _utc_now()})
    heartbeat_critical = {"run_started", "run_finished", "run_failed", "status", "checkpoint_queued",
                         "audit_started", "audit_complete"}

    def record(event: dict) -> None:
        nonlocal last_heartbeat, last_worker_snapshot, waiting_jobs, waiting_count, worker_limit
        event_type = event.get("type")
        with event_lock:
            now = time.monotonic()
            if event_type in heartbeat_critical or now - last_heartbeat >= heartbeat_interval:
                heartbeat = {"run_id": event.get("run_id"), "heartbeat_at_utc": _utc_now(),
                    "event": event_type, "stage": event.get("stage"),
                    "progress": event.get("progress"), "processed": event.get("processed"),
                    "total": event.get("total"),
                    "phase_processed": event.get("phase_processed", event.get("processed")),
                    "phase_total": event.get("phase_total", event.get("total")),
                    "unique_sections": event.get("unique_sections"),
                    "sections_total": event.get("sections_total"),
                    "checkpoint_id": event.get("checkpoint_id"), "section": event.get("section"),
                    "checkpoint_processed": event.get("checkpoint_processed"),
                    "checkpoint_total": event.get("checkpoint_total"),
                    "audit_active": event.get("audit_active"),
                    "audit_summary": event.get("summary"),
                    "turn": event.get("turn"), "job_id": event.get("job_id"),
                    "message": event.get("message")}
                _write_json(heartbeat_path, heartbeat)
                last_heartbeat = now
            if event_type not in transient_job_events:
                persisted_event = event
                if event_type in {"job_started", "job_finished"}:
                    # Keep lifecycle activity restorable without duplicating the full
                    # worker payload (inventory metadata and progress details).
                    persisted_event = {key: event[key] for key in (
                        "type", "run_id", "section", "stage", "stage_kind", "turn", "turns",
                        "status", "error", "traceback") if key in event}
                with event_path.open("a", encoding="utf-8") as output:
                    output.write(json.dumps({"recorded_at_utc": _utc_now(), **persisted_event}, ensure_ascii=False) + "\n")
            if event_type == "job_queue":
                active_workers.clear()
                active_workers.update({item["job_id"]: item for item in event.get("active_jobs", [])
                                       if item.get("job_id")})
                waiting_jobs = list(event.get("waiting_jobs") or [])
                waiting_count = int(event.get("waiting_count") or 0)
                worker_limit = int(event.get("worker_limit") or worker_limit)
            elif event_type == "job_started" and event.get("job_id"):
                active_workers[event["job_id"]] = dict(event)
            elif event_type == "job_progress" and event.get("job_id"):
                if event["job_id"] in active_workers:
                    active_workers[event["job_id"]] = {**active_workers[event["job_id"]], **event}
            elif event_type == "job_finished":
                active_workers.pop(event.get("job_id"), None)
            if event_type in {"job_queue", "job_started", "job_finished"} or (
                    event_type == "job_progress" and now - last_worker_snapshot >= 1.0):
                _write_json(run_dir / "worker-state.json", {
                    "worker_limit": worker_limit, "active_jobs": list(active_workers.values()),
                    "waiting_count": waiting_count, "waiting_jobs": waiting_jobs,
                    "updated_at_utc": _utc_now()})
                last_worker_snapshot = now
        _emit(callback, event)

    return record


@contextmanager
def _job_lock():
    STATE_ROOT.mkdir(parents=True, exist_ok=True)
    lock_path = STATE_ROOT / "active-job.lock"
    with lock_path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise RuntimeError("another local job is already running") from error
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _serialized_job(function):
    @functools.wraps(function)
    def wrapped(*args, **kwargs):
        with _job_lock():
            return function(*args, **kwargs)
    return wrapped


def _request(turns: list[str], uf: str | None, municipio: str | None, zona: str | None,
             secao: str | None, offset: int, limit: int | None, incremental: bool) -> dict:
    return {"turns": turns, "uf": uf or "", "municipio": municipio or "", "zona": zona or "",
            "secao": secao or "", "offset": offset, "limit": limit, "incremental": incremental}


def _selection_only(request: dict) -> dict:
    return {key: request[key] for key in
            ("turns", "uf", "municipio", "zona", "secao", "offset", "limit") if key in request}


def _parse_turns(turns: list[str] | str) -> list[str]:
    if isinstance(turns, str):
        turns = [part.strip() for part in turns.split(",") if part.strip()]
    normalized = list(dict.fromkeys(turns))
    if not normalized or any(turn not in TURN_CONFIG for turn in normalized):
        raise ValueError("turns must contain 1, 2, or both")
    return normalized


def _seed_catalog_cache_from_existing_inventories() -> None:
    """Reuse validated catalog files produced before the shared cache existed."""
    marker = CATALOG_CACHE_ROOT / ".seeded-existing-inventories"
    if marker.is_file():
        return
    CATALOG_CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    inventory_paths = list((STATE_ROOT / "previews").glob("*/turno*/section-inventory.json"))
    inventory_paths.extend((STATE_ROOT / "runs").glob("*/inventario/turno*/section-inventory.json"))
    inventory_paths.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    for inventory_path in inventory_paths:
        try:
            inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        config_id = str(inventory.get("config_id", ""))
        if not config_id.isdigit():
            continue
        for source in inventory.get("catalog_sources", []):
            state = str(source.get("uf", "")).lower()
            source_path = Path(source.get("path") or "")
            if len(state) != 2 or not source_path.is_file():
                continue
            cache_path = CATALOG_CACHE_ROOT / config_id / source_path.name
            metadata_path = cache_path.with_suffix(cache_path.suffix + ".json")
            if cache_path.is_file() and metadata_path.is_file():
                continue
            try:
                raw = source_path.read_bytes()
            except OSError:
                continue
            digest = hashlib.sha256(raw).hexdigest()
            if source.get("sha256") and source["sha256"] != digest:
                continue
            cache_metadata = {"sha256": digest, "http": source.get("http") or {},
                              "url": source.get("url"),
                              "updated_at_utc": inventory.get("acquired_at_utc")}
            save_atomic(cache_path, raw)
            save_atomic(metadata_path, json.dumps(cache_metadata, ensure_ascii=False, indent=2).encode("utf-8") + b"\n")
    marker.write_text("Existing inventory catalogs were checked for cache seeding.\n", encoding="utf-8")


def preview_collection(*, turns: list[str] | str, uf: str = "", municipio: str = "", zona: str = "",
                       secao: str = "", offset: int = 0, limit: int | None = None,
                       progress_callback: Callable | None = None) -> dict:
    selected_turns = _parse_turns(turns)
    preview_id = _new_id()
    preview_dir = STATE_ROOT / "previews" / preview_id
    _seed_catalog_cache_from_existing_inventories()
    preview_request = _selection_only(_request(selected_turns, uf, municipio, zona, secao, offset, limit, True))
    _emit(progress_callback, {"type": "status", "stage": "preview", "message": "Consultando inventário oficial"})
    summaries = []
    for turn in selected_turns:
        output = preview_dir / f"turno{turn}"
        report = discover(config_id=TURN_CONFIG[turn], output=output, uf=uf, municipio=municipio,
                          zona=zona, secao=secao, offset=offset, limit=limit,
                          cache_dir=CATALOG_CACHE_ROOT, progress_callback=progress_callback,
                          catalog_workers=DEFAULT_CATALOG_WORKERS)
        if report.get("status") == "failed" or any(item.get("code") == "NETWORK_ACCESS_DENIED"
                                                     for item in report.get("diagnostics", [])):
            if any(item.get("code") == "NETWORK_ACCESS_DENIED" for item in report.get("diagnostics", [])):
                raise RuntimeError(
                    "A prévia foi interrompida: o Windows ou o ambiente de execução bloqueou o acesso à rede "
                    "(WinError 10013). Nenhum retry foi feito para as demais UFs. Verifique a política de rede/firewall "
                    "ou execute a UI/CLI em um ambiente com acesso externo."
                )
            raise RuntimeError(f"falha ao descobrir inventário do turno {turn}: {report.get('diagnostics', [])}")
        summaries.append({"turn": int(turn), "data_id": TURN_CONFIG[turn],
                          "selected_sections": report.get("selection", {}).get("selected_count", 0),
                          "registered_sections": report.get("section_counts", {}).get("selected_registered_sections", 0),
                          "aggregated_sections": report.get("section_counts", {}).get("selected_aggregated_sections", 0),
                          "collectable_sections": report.get("section_counts", {}).get("selected_principal_collectable_sections", 0),
                          "status": report.get("status")})
    result = {"preview_id": preview_id, "preview_dir": str(preview_dir), "request": preview_request,
              "turns": summaries, "total_selected_sections": sum(row["selected_sections"] for row in summaries),
              "created_at_utc": _utc_now()}
    _write_json(preview_dir / "preview.json", result)
    _emit(progress_callback, {"type": "preview_complete", **result})
    return result


def _prepare_run(run_id: str, run_dir: Path, record: dict) -> sqlite3.Connection:
    run_dir.mkdir(parents=True, exist_ok=False)
    _write_json(run_dir / "run.json", record)
    return _connect(run_dir / "state.sqlite3")


def _copy_preview(preview_dir: Path, run_dir: Path, request: dict) -> dict[str, dict]:
    preview = json.loads((preview_dir / "preview.json").read_text(encoding="utf-8"))
    if preview.get("request") != _selection_only(request):
        raise ValueError("the selection changed after preview; preview the current selection again")
    inventories = {}
    for turn in request["turns"]:
        source = preview_dir / f"turno{turn}"
        destination = run_dir / "inventario" / f"turno{turn}"
        if not (source / "section-inventory.json").is_file():
            raise ValueError(f"preview inventory is missing for turn {turn}")
        shutil.copytree(source, destination)
        inventories[turn] = json.loads((destination / "section-inventory.json").read_text(encoding="utf-8"))
    return inventories


def _discover_for_run(request: dict, run_dir: Path, progress_callback: Callable | None) -> dict[str, dict]:
    _seed_catalog_cache_from_existing_inventories()
    inventories = {}
    for turn in request["turns"]:
        output = run_dir / "inventario" / f"turno{turn}"
        _emit(progress_callback, {"type": "status", "stage": "discovery", "turn": int(turn),
                                 "message": f"Descobrindo seções do turno {turn}"})
        report = discover(config_id=TURN_CONFIG[turn], output=output, uf=request["uf"],
                          municipio=request["municipio"], zona=request["zona"], secao=request["secao"],
                          offset=request["offset"], limit=request["limit"],
                          cache_dir=CATALOG_CACHE_ROOT, progress_callback=progress_callback,
                          catalog_workers=DEFAULT_CATALOG_WORKERS)
        if report.get("status") == "failed" or any(item.get("code") == "NETWORK_ACCESS_DENIED"
                                                     for item in report.get("diagnostics", [])):
            if any(item.get("code") == "NETWORK_ACCESS_DENIED" for item in report.get("diagnostics", [])):
                raise RuntimeError(
                    "A descoberta foi interrompida: o Windows ou o ambiente de execução bloqueou o acesso à rede "
                    "(WinError 10013). Nenhum retry foi feito para as demais UFs. Verifique a política de rede/firewall "
                    "ou execute a UI/CLI em um ambiente com acesso externo."
                )
            raise RuntimeError(f"falha ao descobrir inventário do turno {turn}: {report.get('diagnostics', [])}")
        inventories[turn] = report
    return inventories


def _store_collection_plan(run_dir: Path, connection: sqlite3.Connection,
                           inventories: dict[str, dict]) -> dict:
    plan = build_plan(inventories, batch_size=COLLECTION_BATCH_SIZE, jobs_per_worker=100)
    _write_json(run_dir / "coleta-plan.json", plan)
    for turn, inventory in inventories.items():
        connection.executemany(
            "INSERT OR IGNORE INTO section_jobs(turn,data_id,section_key,section_json) VALUES(?,?,?,?)",
            [(turn, TURN_CONFIG[turn], "/".join((item["uf"], item["municipio"], item["zona"], item["secao"])),
              json.dumps(item, ensure_ascii=False)) for item in inventory.get("sections", [])])
    connection.commit()
    return plan


@_serialized_job
def start_collection(*, turns: list[str] | str, uf: str = "", municipio: str = "", zona: str = "",
                     secao: str = "", offset: int = 0, limit: int | None = None,
                     workers: int = COLLECTION_WORKERS,
                     incremental: bool = True, preview_dir: str | Path | None = None,
                     progress_callback: Callable | None = None, stop_event: threading.Event | None = None) -> dict:
    selected_turns = _parse_turns(turns)
    if offset < 0 or (limit is not None and limit < 1):
        raise ValueError("offset must be nonnegative and limit must be positive")
    validate_worker_count(workers)
    request = _request(selected_turns, uf, municipio, zona, secao, offset, limit, incremental)
    request["workers"] = workers
    run_id = _new_id()
    run_dir = STATE_ROOT / "runs" / run_id
    record = {"run_id": run_id, "kind": "collect", "status": "preparing",
              "cross_process_stop_supported": True, "request": request,
              "data_root": str(DATA_ROOT), "created_at_utc": _utc_now(), "updated_at_utc": _utc_now()}
    connection = _prepare_run(run_id, run_dir, record)
    on_progress = _persistent_callback(run_dir, progress_callback, workers)
    try:
        if preview_dir:
            inventories = _copy_preview(Path(preview_dir), run_dir, request)
        else:
            inventories = _discover_for_run(request, run_dir, on_progress)
        plan = _store_collection_plan(run_dir, connection, inventories)
        record.update({"status": "running", "total_sections": plan["total_sections"],
                       "total_batches": plan["total_batches"], "started_at_utc": _utc_now(),
                       "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        _emit(on_progress, {"type": "run_started", "run_id": run_id,
                            "total_sections": plan["total_sections"], "kind": "collect"})
        return _execute_collection(run_dir, connection, record, on_progress, stop_event)
    except Exception as error:
        error_traceback = traceback.format_exc()
        record.update({"status": "failed", "error": f"{type(error).__name__}: {error}",
                       "traceback": error_traceback,
                       "finished_at_utc": _utc_now(), "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        _emit(on_progress, {"type": "run_failed", "run_id": run_id, "error": record["error"],
                            "traceback": error_traceback})
        raise
    finally:
        connection.close()


def _execute_collection(run_dir: Path, connection: sqlite3.Connection, record: dict,
                        progress_callback: Callable | None, stop_event: threading.Event | None) -> dict:
    stop_event = _RunStopSignal(stop_event, run_dir / "stop.request")
    plan = json.loads((run_dir / "coleta-plan.json").read_text(encoding="utf-8"))
    inventories = {}
    for turn in record["request"]["turns"]:
        inventories[turn] = json.loads((run_dir / "inventario" / f"turno{turn}" / "section-inventory.json")
                                       .read_text(encoding="utf-8"))
    total = int(record.get("total_sections", 0))
    progress_total = total * 2
    status_rows = connection.execute("""SELECT raw_status,extract_status,COUNT(*) AS amount
        FROM section_jobs GROUP BY raw_status,extract_status""").fetchall()
    processed = sum(row["amount"] for row in status_rows
                    if row["raw_status"] in {"collected", "skipped_existing", "aggregated"})
    no_extraction_processed = sum(row["amount"] for row in status_rows
                                   if row["raw_status"] in {"skipped_existing", "aggregated"})
    extraction_processed = sum(row["amount"] for row in status_rows
                               if row["raw_status"] == "collected" and row["extract_status"] == "complete")
    completed_units = processed + no_extraction_processed + extraction_processed
    network_blocked = False
    worker_limit = int(record.get("request", {}).get("workers", COLLECTION_WORKERS))
    completed_sections = 0
    active_jobs: dict[str, dict] = {}
    active_lock = threading.Lock()
    total_waiting = connection.execute("""SELECT COUNT(*) FROM section_jobs
        WHERE raw_status NOT IN ('collected','skipped_existing','aggregated')""").fetchone()[0]
    _emit(progress_callback, {"type": "progress_update", "stage": "collection",
                             "progress_processed": completed_units, "progress_total": progress_total,
                             "raw_processed": processed, "raw_total": total,
                             "extraction_processed": extraction_processed,
                             "extraction_total": total,
                             "run_id": record["run_id"]})
    def task_iterator():
        """Yield section parameters continuously; plan batches are only inventory offsets."""
        for batch in plan.get("batches", []):
            turn = str(batch["turno"])
            sections = inventories[turn].get("sections", [])[batch["offset"]:batch["offset"] + batch["limit"]]
            for section in sections:
                key = "/".join((section["uf"], section["municipio"], section["zona"], section["secao"]))
                row = connection.execute("SELECT raw_status FROM section_jobs WHERE turn=? AND section_key=?",
                                         (turn, key)).fetchone()
                if row and row["raw_status"] not in {"collected", "skipped_existing", "aggregated"}:
                    yield turn, key, section

    task_source = iter(task_iterator())
    pending = deque()
    source_exhausted = False
    futures = {}
    locked_sections: set[str] = set()
    _configure_process_executable()
    process_context = multiprocessing.get_context("spawn")
    process_progress = process_context.Queue()
    worker_state_dir = _prepare_process_worker_state_dir(run_dir)
    worker_pool = ProcessPoolExecutor(max_workers=worker_limit, mp_context=process_context,
        initializer=_initialize_process_worker,
        initargs=(process_progress, worker_state_dir, record["run_id"]))

    def queue_snapshot() -> None:
        with active_lock:
            active_snapshot = [active_jobs.get(item[3]["job_id"], item[3])
                for future, item in futures.items() if not future.done()]
            waiting = max(0, total_waiting - completed_sections - len(futures))
            preview = [dict(item[3], status="queued", progress=0.0, stage="Aguardando na fila")
                       for item in list(pending)[:25]]
        _emit(progress_callback, {"type": "job_queue", "worker_limit": worker_limit,
            "active_jobs": active_snapshot, "waiting_count": waiting, "waiting_jobs": preview,
            "run_id": record["run_id"]})

    try:
        while not source_exhausted or pending or futures:
            # Keep a small look-ahead buffer for the UI while the remaining queue stays lazy.
            while len(pending) < worker_limit + 25 and not source_exhausted:
                try:
                    turn, key, section = next(task_source)
                    with active_lock:
                        pending.append((turn, key, section, {"job_id": f"{key}:turno-{turn}",
                            "section": key, "turn": int(turn)}))
                except StopIteration:
                    source_exhausted = True
            while (len(futures) < worker_limit and not network_blocked
                   and not (stop_event and stop_event.is_set())):
                with active_lock:
                    has_pending = bool(pending)
                if not has_pending:
                    break
                scheduled = False
                with active_lock:
                    pending_length = len(pending)
                for _ in range(pending_length):
                    with active_lock:
                        item = pending.popleft()
                    section_lock = item[1].casefold()
                    if section_lock in locked_sections:
                        with active_lock:
                            pending.append(item)
                        continue
                    turn, key, section, info = item
                    locked_sections.add(section_lock)
                    connection.execute("UPDATE section_jobs SET raw_status='running' WHERE turn=? AND section_key=?",
                                       (turn, key))
                    connection.commit()
                    with active_lock:
                        active_jobs[info["job_id"]] = {**info, "status": "running", "progress": 0.0,
                                                       "stage": "Iniciando coleta"}
                    _emit(progress_callback, {"type": "section_started", "stage": "collection", "turn": int(turn),
                        "section": key, "processed": processed, "total": total, "run_id": record["run_id"]})
                    _emit(progress_callback, {"type": "job_started", **active_jobs[info["job_id"]],
                                             "run_id": record["run_id"]})
                    process_task = {"operation": "collect_extract", "turn": turn,
                        "section": section, "section_key": key, "job_id": info["job_id"],
                        "run_id": record["run_id"], "incremental": record["request"]["incremental"],
                        "data_root": str(DATA_ROOT), "staging_root": str(run_dir / "staging"),
                        "stage_label": "Iniciando coleta"}
                    futures[worker_pool.submit(_run_process_task, process_task)] = (turn, key, section_lock, info)
                    scheduled = True
                    break
                queue_snapshot()
                if not scheduled:
                    break
            queue_snapshot()
            if not futures:
                if network_blocked:
                    break
                if stop_event and stop_event.is_set():
                    break
                if pending and not source_exhausted:
                    continue
                break
            done, _remaining = wait(futures, timeout=0.5, return_when=FIRST_COMPLETED)
            _drain_process_progress(process_progress, active_jobs, active_lock, progress_callback)
            for future in done:
                turn, key, section_lock, info = futures.pop(future)
                locked_sections.discard(section_lock)
                result = future.result()
                if result.get("status") == "cancelled":
                    connection.execute("UPDATE section_jobs SET raw_status='pending' WHERE turn=? AND section_key=?",
                                       (turn, key))
                    connection.commit()
                    with active_lock:
                        active_jobs.pop(info["job_id"], None)
                    _emit(progress_callback, {"type": "job_finished", **info, "status": "cancelled",
                                             "run_id": record["run_id"]})
                    continue
                raw_status = result.get("raw_status", "failed")
                extraction_status = result.get("extract_status", "pending")
                acquisition_path = result.get("acquisition")
                error = result.get("extract_error") or result.get("reason") or (
                    "collector failed" if raw_status == "failed" else None)
                connection.execute("""UPDATE section_jobs SET raw_status=?,extract_status=?,
                    acquisition_path=COALESCE(?,acquisition_path),error=? WHERE turn=? AND section_key=?""",
                    (raw_status, extraction_status, acquisition_path, error, turn, key))
                connection.commit()
                processed += 1
                completed_sections += 1
                completed_units += 2 if extraction_status in {"complete", "failed", "not_needed"} or raw_status in {"failed", "aggregated", "skipped_existing"} else 1
                if raw_status in {"skipped_existing", "aggregated", "failed"}:
                    no_extraction_processed += 1
                if extraction_status == "complete":
                    extraction_processed += 1
                _emit(progress_callback, {"type": "section_complete", "stage": "collection", "turn": int(turn),
                    "section": key, "status": raw_status, "processed": processed, "total": total,
                    "progress_processed": completed_units, "progress_total": progress_total,
                    "raw_processed": processed, "raw_total": total, "run_id": record["run_id"]})
                if raw_status in {"collected", "partial"}:
                    _emit(progress_callback, {"type": "extraction_complete", "stage": "extraction",
                        "turn": int(turn), "section": key, "status": extraction_status,
                        "processed": extraction_processed, "total": total,
                        "progress_processed": completed_units, "progress_total": progress_total,
                        "raw_processed": processed, "raw_total": total,
                        "extraction_processed": extraction_processed, "extraction_total": total,
                        "run_id": record["run_id"]})
                with active_lock:
                    active_jobs.pop(info["job_id"], None)
                _emit(progress_callback, {"type": "job_finished", **info,
                    "status": extraction_status if raw_status in {"collected", "partial"} else raw_status,
                    "progress": 100.0, "process_id": result.get("process_id"), "error": error,
                    "traceback": result.get("traceback"), "run_id": record["run_id"]})
                if result.get("network_blocked"):
                    network_blocked = True
                    message = ("Coleta interrompida: o Windows ou o ambiente bloqueou o acesso à rede "
                               "(WinError 10013). As seções restantes ficaram pendentes.")
                    _emit(progress_callback, {"type": "worker_error", "error": message, "run_id": record["run_id"]})
            queue_snapshot()
    finally:
        worker_pool.shutdown(wait=True, cancel_futures=bool(stop_event and stop_event.is_set()))
        process_progress.close()

    excerpt_targets = []
    for row in connection.execute("""SELECT turn,section_key,acquisition_path FROM section_jobs
        WHERE raw_status IN ('collected','partial') AND acquisition_path IS NOT NULL
        ORDER BY turn,section_key"""):
        acquisition_path = Path(row["acquisition_path"])
        if acquisition_path.is_file():
            excerpt_targets.append({"section_key": row["section_key"], "turn": int(row["turn"]),
                "output_path": acquisition_path.parent / "transferencia-temporaria-elegiveis.csv"})
    if excerpt_targets:
        target_list = excerpt_targets
        _emit(progress_callback, {"type": "status", "stage": "temporary_transfer_excerpt",
            "message": f"Gerando recortes da referência para {len(target_list):,} seções…",
            "run_id": record["run_id"]})
        def on_excerpt_progress(update: dict) -> None:
            rows_seen = int(update.get("rows_processed") or 0)
            stage = update.get("stage") or "Gerando recortes da referência"
            _emit(progress_callback, {"type": "status", "stage": "temporary_transfer_excerpt",
                "message": f"{stage} · {rows_seen:,} registros verificados",
                "rows_processed": rows_seen, "run_id": record["run_id"]})
        try:
            excerpt_metadata = write_section_excerpts(target_list, progress_callback=on_excerpt_progress)
        except Exception as error:
            identity = reference_identity()
            reason = f"{type(error).__name__}: {error}"
            excerpt_metadata = {str(target["output_path"]): {
                "status": "not_generated", "reason": reason,
                "reference_zip_sha256": identity.get("zip_sha256"),
                "reference_version": identity.get("version"),
            } for target in target_list}
            _emit(progress_callback, {"type": "worker_error",
                "error": f"Não foi possível gerar os recortes TTE: {reason}", "run_id": record["run_id"]})
        for target in target_list:
            if target["turn"] != 2:
                continue
            report_json = target["output_path"].parent / "transferencia-temporaria-reconciliacao.json"
            if not report_json.is_file():
                continue
            try:
                payload = json.loads(report_json.read_text(encoding="utf-8"))
                rows = payload.get("sections", [])
                for row in rows:
                    if row.get("section_key") == target["section_key"]:
                        row["reference_excerpt"] = excerpt_metadata.get(str(target["output_path"]), {
                            "status": "not_generated", "reason": "metadata indisponível"})
                write_reconciliation(rows, report_json,
                    target["output_path"].parent / "transferencia-temporaria-reconciliacao.csv",
                    payload.get("log_metric_source", "section_analysis_log_sessions"))
                write_validation(rows,
                    target["output_path"].parent / "transferencia-temporaria-validation.json",
                    source_note="section_analysis_log_sessions")
            except (OSError, ValueError, TypeError) as error:
                _emit(progress_callback, {"type": "worker_error",
                    "error": f"Não foi possível atualizar a reconciliação TTE de {target['section_key']}: {error}",
                    "run_id": record["run_id"]})

    counts = _run_counts(connection)
    if stop_event and stop_event.is_set():
        status = "paused"
    elif network_blocked:
        status = "partial"
    elif counts["pending"] or counts["failed"] or counts["partial"]:
        status = "partial"
    else:
        status = "complete"
    record.update({"status": status, "counts": counts, "finished_at_utc": _utc_now(), "updated_at_utc": _utc_now()})
    _write_json(run_dir / "run.json", record)
    result = {"run_id": record["run_id"], "status": status, "counts": counts, "run_dir": str(run_dir)}
    _emit(progress_callback, {"type": "run_finished", **result})
    return result


def _latest_batch_process_run(exclude_run_id: str | None = None) -> tuple[dict | None, Path | None]:
    runs_root = STATE_ROOT / "runs"
    if not runs_root.is_dir():
        return None, None
    candidates = []
    for run_path in runs_root.glob("*/run.json"):
        try:
            record = json.loads(run_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if record.get("run_id") == exclude_run_id or record.get("kind") != "batch_process":
            continue
        if record.get("status") == "preparing":
            continue
        candidates.append((str(record.get("created_at_utc") or ""), record, run_path.parent))
    if not candidates:
        return None, None
    _, record, run_dir = max(candidates, key=lambda item: item[0])
    return record, run_dir


def _copy_reused_evidence(source_run: dict, output_root: Path, row: dict) -> bool:
    """Copy a previously preserved evidence tree for a reused anomalous section."""
    if not source_run.get("consolidation_root"):
        return False
    parts = [str(row.get(name) or "") for name in ("uf", "municipio", "zona", "secao")]
    if any(not value or value in {".", ".."} or "/" in value or "\\" in value for value in parts):
        return False
    source = Path(source_run["consolidation_root"]).joinpath("evidencias", *parts)
    if not source.is_dir():
        return False
    destination = output_root.joinpath("evidencias", *parts)
    try:
        if not destination.exists():
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(source, destination)
        verify_tree_copy(source, destination)
    except Exception:
        if destination.exists():
            shutil.rmtree(destination, ignore_errors=True)
        raise
    return True


def _reuse_completed_section_summaries(source_record: dict | None, source_run_dir: Path | None,
                                       connection: sqlite3.Connection, plan: dict,
                                       sections_per_batch: int, output_root: Path) -> dict:
    """Import trusted per-turn summaries from the latest batch run without rescanning raw data."""
    result = {"source_run_id": None, "source_status": None, "reused_turns": 0,
              "reused_sections": 0, "uncertain_sections": [],
              "evidence_pending_sections": [], "cleanup_pending_sections": [],
              "source_reason": None, "source_error": None}
    if not source_record or not source_run_dir:
        return result
    previous_status = source_record.get("status")
    result.update({"source_run_id": source_record.get("run_id"),
                   "source_status": "interrupted" if previous_status == "running" else previous_status})
    source_db = source_run_dir / "state.sqlite3"
    if not source_db.is_file():
        result["source_reason"] = "state.sqlite3 ausente"
        return result
    source_root_value = source_record.get("consolidation_root")
    source_manifest_path = Path(source_root_value) / "manifest.json" if source_root_value else None
    current_schema_hashes = {"bu": hashlib.sha256(BU_SCHEMA.read_bytes()).hexdigest(),
                             "rdv": hashlib.sha256(RDV_SCHEMA.read_bytes()).hexdigest()}
    if source_manifest_path and source_manifest_path.is_file():
        try:
            source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
            manifest_hashes = source_manifest.get("schema_hashes") or {}
            if manifest_hashes and manifest_hashes != current_schema_hashes:
                result["source_reason"] = "schemas incompatíveis"
                return result
        except (OSError, json.JSONDecodeError):
            result["source_reason"] = "manifesto anterior inválido"
            return result

    source_uri = f"{source_db.as_uri()}?mode=ro"
    try:
        source_connection = sqlite3.connect(source_uri, uri=True, timeout=5)
    except sqlite3.Error as error:
        source_status = source_record.get("status")
        result.update({"source_run_id": source_record.get("run_id"),
                       "source_status": "interrupted" if source_status == "running" else source_status,
                       "source_reason": "estado anterior ilegível",
                       "source_error": f"{type(error).__name__}: {error}"})
        return result
    source_connection.row_factory = sqlite3.Row
    try:
        table_names = {row[0] for row in source_connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"section_jobs", "section_summaries"}.issubset(table_names):
            result["source_reason"] = "estado anterior sem tabelas de consolidação"
            return result
        job_columns = {row[1] for row in source_connection.execute("PRAGMA table_info(section_jobs)")}
        has_consolidation_flag = "consolidation_status" in job_columns
        has_hash = "consolidation_row_sha256" in job_columns
        source_connection.execute("""CREATE TEMP TABLE requested_sections (
            turn TEXT NOT NULL, section_key TEXT NOT NULL, batch_index INTEGER NOT NULL,
            PRIMARY KEY (turn,section_key))""")
        chunk = []
        for section_index, key in enumerate(plan["section_keys"]):
            batch_index = section_index // sections_per_batch
            for turn in plan["section_turns"][key]:
                chunk.append((str(turn), key, batch_index))
                if len(chunk) >= 5000:
                    source_connection.executemany("INSERT OR REPLACE INTO requested_sections VALUES(?,?,?)", chunk)
                    chunk.clear()
        if chunk:
            source_connection.executemany("INSERT OR REPLACE INTO requested_sections VALUES(?,?,?)", chunk)

        flag_sql = "j.consolidation_status" if has_consolidation_flag else "NULL AS consolidation_status"
        hash_sql = "j.consolidation_row_sha256" if has_hash else "NULL AS consolidation_row_sha256"
        query = f"""SELECT j.turn,j.section_key,j.raw_status,j.extract_status,j.acquisition_path,
            {flag_sql},{hash_sql},s.row_json,s.evidence_required,r.batch_index
            FROM requested_sections r
            JOIN section_jobs j ON j.turn=r.turn AND j.section_key=r.section_key
            LEFT JOIN section_summaries s ON s.turn=j.turn AND s.section_key=j.section_key"""
        reused_pairs = set()
        evidence_pending = set()
        cleanup_pending = set()
        for prior in source_connection.execute(query):
            if (prior["raw_status"] in {"running", "failed", "partial"}
                    or prior["extract_status"] in {"running", "failed"}
                    or prior["consolidation_status"] in {"running", "failed"}):
                result["uncertain_sections"].append(prior["section_key"])
            if not prior["row_json"]:
                continue
            try:
                row = json.loads(prior["row_json"])
            except (TypeError, json.JSONDecodeError):
                if prior["consolidation_status"] == "complete":
                    result["uncertain_sections"].append(prior["section_key"])
                continue
            row_section_key = "/".join(str(row.get(name) or "")
                for name in ("uf", "municipio", "zona", "secao"))
            if (row.get("status_processamento") not in {"processado", "agregada"}
                    or row.get("versao_regra_proxy") != PROXY_RULE_VERSION
                    or row_section_key != prior["section_key"]
                    or str(row.get("turno")) != str(prior["turn"])):
                if prior["consolidation_status"] == "complete":
                    result["uncertain_sections"].append(prior["section_key"])
                continue
            if prior["extract_status"] not in {"complete", "not_needed"}:
                continue
            is_flagged_complete = prior["consolidation_status"] == "complete"
            legacy_complete = (prior["consolidation_status"] is None
                and prior["extract_status"] in {"complete", "not_needed"}
                and prior["raw_status"] in {"collected", "aggregated", "skipped_existing", "partial"})
            if not (is_flagged_complete or legacy_complete):
                if prior["raw_status"] in {"running", "failed", "partial"} or prior["extract_status"] in {"running", "failed"}:
                    result["uncertain_sections"].append(prior["section_key"])
                continue
            row_json = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
            row_hash = hashlib.sha256(row_json.encode("utf-8")).hexdigest()
            if prior["consolidation_row_sha256"] and prior["consolidation_row_sha256"] != row_hash:
                result["uncertain_sections"].append(prior["section_key"])
                continue
            evidence_required = int(prior["evidence_required"] or 0)
            raw_section = DATA_ROOT.joinpath(*(str(row.get(name) or "") for name in
                ("uf", "municipio", "zona", "secao")))
            if raw_section.is_dir():
                # Raw acquisitions are temporary. Even when their summary is
                # reusable, verify this section before its deferred cleanup.
                cleanup_pending.add(prior["section_key"])
            if evidence_required:
                if raw_section.is_dir():
                    # The raw tree is still present; let the cleanup stage validate
                    # it and create a fresh evidence copy before removing it.
                    evidence_pending.add(prior["section_key"])
                else:
                    try:
                        evidence_copied = _copy_reused_evidence(source_record, output_root, row)
                    except (OSError, ValueError):
                        evidence_copied = False
                    if not evidence_copied:
                        result["uncertain_sections"].append(prior["section_key"])
                        continue
            aggregated = row.get("status_processamento") == "agregada"
            if prior["acquisition_path"] and raw_section.is_dir():
                cleanup_pending.add(prior["section_key"])
            connection.execute("""UPDATE section_jobs SET raw_status=?,extract_status=?,
                consolidation_status='complete',consolidated_at_utc=COALESCE(consolidated_at_utc,?),
                consolidation_row_sha256=?,acquisition_path=?,error=NULL,batch_index=?
                WHERE turn=? AND section_key=?""",
                ("aggregated" if aggregated else "skipped_existing",
                 "not_needed" if aggregated else "complete", _utc_now(), row_hash,
                 prior["acquisition_path"],
                 prior["batch_index"], prior["turn"], prior["section_key"]))
            connection.execute("""INSERT OR REPLACE INTO section_summaries
                (turn,section_key,batch_index,row_json,evidence_required) VALUES(?,?,?,?,?)""",
                (prior["turn"], prior["section_key"], prior["batch_index"], row_json, evidence_required))
            reused_pairs.add((str(prior["turn"]), prior["section_key"]))
            result["reused_turns"] += 1
        connection.commit()
        reused_keys = {key for key in plan["section_keys"]
            if all((str(turn), key) in reused_pairs for turn in plan["section_turns"][key])}
        result.update({"source_run_id": source_record.get("run_id"),
                       "source_status": "interrupted" if source_record.get("status") == "running"
                           else source_record.get("status"),
                       "reused_sections": len(reused_keys),
                       "uncertain_sections": sorted(set(result["uncertain_sections"])),
                       "evidence_pending_sections": sorted(evidence_pending),
                       "cleanup_pending_sections": sorted(cleanup_pending),
                       "source_reason": None})
        return result
    except sqlite3.Error as error:
        connection.rollback()
        result.update({"source_run_id": source_record.get("run_id"),
                       "source_status": "interrupted" if source_record.get("status") == "running"
                           else source_record.get("status"),
                       "source_reason": "estado anterior ilegível",
                       "source_error": f"{type(error).__name__}: {error}",
                       "reused_turns": 0, "reused_sections": 0,
                       "uncertain_sections": [], "evidence_pending_sections": [],
                       "cleanup_pending_sections": []})
        return result
    finally:
        source_connection.close()


@_serialized_job
def start_batch_processing(*, turns: list[str] | str, uf: str = "", municipio: str = "", zona: str = "",
                          secao: str = "", offset: int = 0, limit: int | None = None,
                          sections_per_batch: int = DEFAULT_BATCH_SECTIONS,
                          workers: int = DEFAULT_BATCH_WORKERS, preview_dir: str | Path | None = None,
                          confirm_processing: bool = False, confirm_clean_data: bool | None = None,
                          progress_callback: Callable | None = None,
                          stop_event: threading.Event | None = None) -> dict:
    """Collect and deterministically consolidate bounded batches, cleaning successful sections only."""
    if confirm_clean_data is not None:
        confirm_processing = confirm_processing or confirm_clean_data
    if not confirm_processing:
        raise ValueError("confirme o início do processamento por remessas")
    selected_turns = _parse_turns(turns)
    if offset < 0 or (limit is not None and limit < 1):
        raise ValueError("offset must be nonnegative and limit must be positive")
    if not 1 <= sections_per_batch <= 10000:
        raise ValueError("sections_per_batch must be between 1 and 10000")
    validate_worker_count(workers)
    request = _request(selected_turns, uf, municipio, zona, secao, offset, limit, False)
    request.update({"sections_per_batch": sections_per_batch, "workers": workers,
                    "confirm_processing": True, "proxy_rule_version": PROXY_RULE_VERSION})
    run_id = _new_id()
    run_dir = STATE_ROOT / "runs" / run_id
    output_dir = CONSOLIDATION_ROOT / run_id
    record = {"run_id": run_id, "kind": "batch_process", "status": "preparing",
              "cross_process_stop_supported": True, "request": request,
              "data_root": str(DATA_ROOT), "consolidation_root": str(output_dir),
              "initial_cleanup_done": True, "initial_cleanup_entries_removed": 0,
              "initial_cleanup_skipped": True,
              "created_at_utc": _utc_now(), "updated_at_utc": _utc_now()}
    connection = _prepare_run(run_id, run_dir, record)
    on_progress = _persistent_callback(run_dir, progress_callback, workers)
    try:
        if preview_dir:
            inventories = _copy_preview(Path(preview_dir), run_dir, request)
        else:
            inventories = _discover_for_run(request, run_dir, on_progress)
        plan = build_section_unit_plan(inventories, sections_per_checkpoint=sections_per_batch)
        if not plan.get("total_sections"):
            raise ValueError("nenhuma seção selecionada; dados/ não foi limpo")
        _write_json(run_dir / "remessas-plan.json", plan)
        section_by_key = {}
        for turn, inventory in inventories.items():
            for section in inventory.get("sections", []):
                key = "/".join(str(section[name]) for name in ("uf", "municipio", "zona", "secao"))
                section_by_key.setdefault(key, section)
        for checkpoint in plan["checkpoints"]:
            connection.execute("INSERT OR IGNORE INTO processing_batches(batch_index,batch_id,status) VALUES(?,?, 'pending')",
                               (checkpoint["checkpoint_index"], checkpoint["checkpoint_id"]))
        for section_index, key in enumerate(plan["section_keys"]):
            batch_index = section_index // sections_per_batch
            section = section_by_key[key]
            for turn in plan["section_turns"][key]:
                connection.execute("""INSERT OR IGNORE INTO section_jobs
                    (turn,data_id,section_key,section_json,batch_index)
                    VALUES(?,?,?,?,?)""",
                    (turn, TURN_CONFIG[turn], key, json.dumps(section, ensure_ascii=False), batch_index))
        connection.commit()
        source_record, source_run_dir = _latest_batch_process_run(exclude_run_id=run_id)
        reuse_state = _reuse_completed_section_summaries(source_record, source_run_dir,
            connection, plan, sections_per_batch, output_dir)
        record.update({"reuse_source_run_id": reuse_state["source_run_id"],
            "reuse_source_status": reuse_state["source_status"],
            "reuse_source_reason": reuse_state["source_reason"],
            "reuse_source_error": reuse_state["source_error"],
            "reused_sections": reuse_state["reused_sections"],
            "reused_turns": reuse_state["reused_turns"],
            "uncertain_source_sections": reuse_state["uncertain_sections"],
            "reuse_evidence_pending_sections": reuse_state["evidence_pending_sections"],
            "reuse_cleanup_pending_sections": reuse_state["cleanup_pending_sections"],
            "processing_compatibility": {"proxy_rule_version": PROXY_RULE_VERSION,
                "schema_hashes": {"bu": hashlib.sha256(BU_SCHEMA.read_bytes()).hexdigest(),
                                  "rdv": hashlib.sha256(RDV_SCHEMA.read_bytes()).hexdigest()}}})
        if reuse_state["source_run_id"]:
            _emit(on_progress, {"type": "status", "stage": "data_reuse",
                "message": (f"Reaproveitadas {reuse_state['reused_sections']} seções consolidadas "
                    f"({reuse_state['reused_turns']} turno(s)) do job {reuse_state['source_run_id']}; "
                    "as demais serão coletadas ou verificadas conforme o estado físico."
                    + (f" Reuso não aplicado: {reuse_state['source_reason']}."
                        if reuse_state["source_reason"] else "")
                    + (f" Detalhe: {reuse_state['source_error']}" if reuse_state["source_error"] else "")),
                "run_id": run_id})
        record.update({"status": "running", "total_sections": plan["total_sections"],
                       "total_batches": len(plan["checkpoints"]), "processing_format_version": 2,
                       "progress_total_units": plan["total_section_turns"] * 2,
                       "started_at_utc": _utc_now(),
                       "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        _emit(on_progress, {"type": "run_started", "run_id": run_id,
                            "total_sections": plan["total_sections"],
                            "total_batches": len(plan["checkpoints"]),
                            "kind": "batch_process"})
        return _execute_batch_processing(run_dir, connection, record, on_progress, stop_event)
    except Exception as error:
        error_traceback = traceback.format_exc()
        record.update({"status": "failed", "error": f"{type(error).__name__}: {error}",
                       "traceback": error_traceback,
                       "finished_at_utc": _utc_now(), "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        _emit(on_progress, {"type": "run_failed", "run_id": run_id, "error": record["error"],
                            "traceback": error_traceback})
        raise
    finally:
        connection.close()


def _upgrade_batch_plan_for_resume(run_dir: Path, connection: sqlite3.Connection,
                                  record: dict, progress_callback: Callable | None = None,
                                  stop_signal: _RunStopSignal | None = None) -> dict:
    """Upgrade a persisted per-turn plan without discarding valid section summaries."""
    plan_path = run_dir / "remessas-plan.json"
    previous = json.loads(plan_path.read_text(encoding="utf-8"))
    if int(previous.get("plan_schema_version", 1)) >= 2:
        return previous
    turns = [str(turn) for turn in record["request"]["turns"]]
    total_inventory_files = max(1, len(turns))
    inventories = {}

    def report(stage: str, processed: int, total: int, *, turn: str | None = None,
               unique_sections: int | None = None, sections_total: int | None = None,
               checkpoint_id: str | None = None, checkpoint_processed: int | None = None,
               checkpoint_total: int | None = None) -> None:
        if stop_signal and stop_signal.is_set():
            raise _RunPauseRequested("Parada solicitada durante a migração do plano")
        denominator = max(1, total)
        if stage == "loading_inventories":
            percent = 10.0 * processed / denominator
            label = "Carregando inventários para migrar o plano"
        elif stage == "indexing_sections":
            percent = 10.0 + 70.0 * processed / denominator
            label = "Indexando seções físicas dos turnos"
        elif stage == "remapping_state":
            percent = 80.0 + 19.0 * processed / denominator
            label = "Reassociando o estado salvo às novas remessas"
        else:
            percent = 0.0
            label = stage
        details = {"stage": label, "turn": turn, "processed": processed, "total": total,
                   "phase_processed": processed, "phase_total": total,
                   "unique_sections": unique_sections, "sections_total": sections_total,
                   "checkpoint_id": checkpoint_id, "checkpoint_processed": checkpoint_processed,
                   "checkpoint_total": checkpoint_total, "progress": round(percent, 2)}
        record.update({"resume_phase": stage, "resume_progress": round(percent, 2),
                       "resume_progress_detail": details, "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        _emit(progress_callback, {"type": "progress_update", "stage": label,
            "processed": processed, "total": total, "progress": round(percent, 2),
            "turn": turn, "message": label, "run_id": record["run_id"],
            "unique_sections": unique_sections, "sections_total": sections_total,
            "checkpoint_id": checkpoint_id, "checkpoint_processed": checkpoint_processed,
            "checkpoint_total": checkpoint_total, "resume_progress_detail": details})

    report("loading_inventories", 0, total_inventory_files)
    for index, turn in enumerate(turns, start=1):
        if stop_signal and stop_signal.is_set():
            raise _RunPauseRequested("Parada solicitada durante a leitura dos inventários")
        inventories[turn] = json.loads((run_dir / "inventario" / f"turno{turn}" /
            "section-inventory.json").read_text(encoding="utf-8"))
        report("loading_inventories", index, total_inventory_files, turn=turn)
    size = int(record.get("request", {}).get("sections_per_batch", DEFAULT_BATCH_SECTIONS))
    total_entries = sum(len(item.get("sections", [])) for item in inventories.values())
    plan = build_section_unit_plan(inventories, sections_per_checkpoint=size,
        progress_callback=lambda progress: report("indexing_sections", progress["processed"],
            max(1, total_entries), turn=progress.get("turn"),
            unique_sections=progress.get("unique_sections")))
    report("remapping_state", 0, plan["total_sections"], sections_total=plan["total_sections"])
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute("DELETE FROM processing_batches")
        for checkpoint in plan["checkpoints"]:
            connection.execute("""INSERT INTO processing_batches(batch_index,batch_id,status)
                VALUES(?,?, 'pending')""", (checkpoint["checkpoint_index"], checkpoint["checkpoint_id"]))
        for checkpoint in plan["checkpoints"]:
            if stop_signal and stop_signal.is_set():
                raise _RunPauseRequested("Parada solicitada durante a migração do estado salvo")
            index = int(checkpoint["checkpoint_index"])
            start = int(checkpoint["section_offset"])
            keys = plan["section_keys"][start:start + int(checkpoint["section_count"])]
            for chunk_start in range(0, len(keys), 100):
                chunk = keys[chunk_start:chunk_start + 100]
                assignments = [(index, turn, key) for key in chunk
                    for turn in plan["section_turns"][key]]
                connection.executemany("""UPDATE section_jobs SET batch_index=?
                    WHERE turn=? AND section_key=?""", assignments)
                connection.executemany("""UPDATE section_summaries SET batch_index=?
                    WHERE turn=? AND section_key=?""", assignments)
                assigned_sections = start + chunk_start + len(chunk)
                report("remapping_state", assigned_sections, plan["total_sections"],
                    unique_sections=assigned_sections, sections_total=plan["total_sections"],
                    checkpoint_id=checkpoint["checkpoint_id"],
                    checkpoint_processed=index + 1, checkpoint_total=len(plan["checkpoints"]))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    _write_json(plan_path, plan)
    record.update({"processing_format_version": 2,
                   "total_sections": plan["total_sections"],
                   "total_batches": len(plan["checkpoints"]),
                   "progress_total_units": plan["total_section_turns"] * 2,
                   "upgraded_from_plan_schema": int(previous.get("plan_schema_version", 1)),
                   "resume_phase": "plan_migrated", "resume_progress": 100.0,
                   "updated_at_utc": _utc_now()})
    _write_json(run_dir / "run.json", record)
    return plan


def _execute_section_unit_processing_legacy(run_dir: Path, connection: sqlite3.Connection, record: dict,
                                     progress_callback: Callable | None,
                                     stop_event: threading.Event | None, plan: dict) -> dict:
    """Run a rolling queue of physical sections, each covering all selected turns."""
    stop_event = _RunStopSignal(stop_event, run_dir / "stop.request")
    output_root = Path(record["consolidation_root"])
    output_root.mkdir(parents=True, exist_ok=True)
    workers = int(record.get("request", {}).get("workers", DEFAULT_BATCH_WORKERS))
    turns_by_key = plan["section_turns"]
    section_by_key: dict[str, dict] = {}
    inventories = {str(turn): json.loads((run_dir / "inventario" / f"turno{turn}" /
        "section-inventory.json").read_text(encoding="utf-8")) for turn in record["request"]["turns"]}
    for inventory in inventories.values():
        for section in inventory.get("sections", []):
            key = "/".join(str(section[name]) for name in ("uf", "municipio", "zona", "secao"))
            section_by_key.setdefault(key, section)
    schema_hashes = {"bu": hashlib.sha256(BU_SCHEMA.read_bytes()).hexdigest(),
                     "rdv": hashlib.sha256(RDV_SCHEMA.read_bytes()).hexdigest()}

    if not record.get("initial_cleanup_done"):
        _emit(progress_callback, {"type": "status", "stage": "initial_cleanup",
            "message": "Limpando dados/ antes da primeira remessa", "run_id": record["run_id"]})
        removed = clean_data_root(DATA_ROOT, ROOT)
        record.update({"initial_cleanup_done": True, "initial_cleanup_entries_removed": removed,
                       "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)

    section_position = {key: index for index, key in enumerate(plan["section_keys"])}
    pending = deque()
    next_checkpoint = 0
    checkpoints = plan["checkpoints"]
    active_jobs: dict[str, dict] = {}
    active_lock = threading.Lock()
    futures = {}
    active_sections: set[str] = set()
    network_blocked = False
    low_water = max(workers * 2, min(100, int(plan.get("sections_per_checkpoint", 500)) // 4))

    def summary_is_valid(turn: str, key: str) -> bool:
        row = connection.execute("""SELECT s.row_json,j.extract_status FROM section_summaries s
            JOIN section_jobs j ON j.turn=s.turn AND j.section_key=s.section_key
            WHERE s.turn=? AND s.section_key=?""", (turn, key)).fetchone()
        if row is None or row["extract_status"] == "failed":
            return False
        try:
            return json.loads(row["row_json"]).get("status_processamento") == "processado"
        except (TypeError, json.JSONDecodeError):
            return False

    def unit_needs_work(key: str) -> bool:
        turns = turns_by_key[key]
        if any(not summary_is_valid(turn, key) for turn in turns):
            return True
        section = section_by_key[key]
        raw_path = DATA_ROOT.joinpath(*(str(section[name]) for name in ("uf", "municipio", "zona", "secao")))
        return raw_path.exists()

    def activate_checkpoints() -> None:
        nonlocal next_checkpoint
        while (next_checkpoint < len(checkpoints) and len(pending) <= low_water
               and not stop_event.is_set() and not network_blocked):
            checkpoint = checkpoints[next_checkpoint]
            start = int(checkpoint["section_offset"])
            end = start + int(checkpoint["section_count"])
            loaded = 0
            for key in plan["section_keys"][start:end]:
                if unit_needs_work(key):
                    pending.append(key)
                    loaded += 1
            connection.execute("UPDATE processing_batches SET status=? WHERE batch_index=?",
                ("processing" if loaded else "consolidated", checkpoint["checkpoint_index"]))
            connection.commit()
            next_checkpoint += 1
            record.update({"current_batch": checkpoint["checkpoint_id"],
                           "next_checkpoint_index": next_checkpoint,
                           "updated_at_utc": _utc_now()})
            _write_json(run_dir / "run.json", record)
            _emit(progress_callback, {"type": "checkpoint_queued", "run_id": record["run_id"],
                "checkpoint_id": checkpoint["checkpoint_id"], "checkpoint_index": checkpoint["checkpoint_index"],
                "section_count": loaded, "queue_waiting": len(pending), "total_checkpoints": len(checkpoints)})

    def queue_snapshot() -> None:
        with active_lock:
            active_snapshot = [active_jobs[job_id] for future, (_key, _lock_key, job_id) in futures.items()
                if not future.done() and job_id in active_jobs]
            waiting = len(pending)
            waiting_jobs = []
            for key in list(pending)[:25]:
                waiting_jobs.append({**section_by_key[key], "section": key, "status": "queued",
                    "stage": "Aguardando coleta/consolidação/limpeza"})
        _emit(progress_callback, {"type": "job_queue", "worker_limit": workers,
            "active_jobs": active_snapshot, "waiting_count": waiting, "waiting_jobs": waiting_jobs,
            "run_id": record["run_id"], "current_batch": record.get("current_batch")})

    def update_worker(job_id: str, update: dict) -> None:
        with active_lock:
            active_jobs[job_id] = {**active_jobs.get(job_id, {}), **update}
            current = active_jobs[job_id].copy()
        queue_snapshot()
        _emit(progress_callback, {"type": "job_progress", **current, "run_id": record["run_id"]})

    def process_unit(key: str, turns_to_process: list[str], raw_states: dict[str, dict]) -> dict:
        section = section_by_key[key]
        job_id = f"{key}:turnos-{','.join(turns_by_key[key])}"
        outcomes = []
        any_network_blocked = False
        errors = []
        for turn_index, turn in enumerate(turns_to_process):
            if stop_event.is_set():
                return {"key": key, "section": section, "cancelled": True,
                        "outcomes": outcomes, "network_blocked": any_network_blocked}
            job = raw_states.get(turn, {})
            raw_status = job.get("raw_status", "pending")
            acquisition_path = complete_existing_acquisition(DATA_ROOT.joinpath(
                *(str(section[name]) for name in ("uf", "municipio", "zona", "secao"))),
                key, TURN_CONFIG[turn])
            if acquisition_path is not None:
                raw_status = "collected"
            try:
                if section.get("aggregation_role") == "aggregated":
                    report = {"file_processing": {}, "diagnostics": []}
                    raw_status = "aggregated"
                else:
                    if acquisition_path is None:
                        events = []
                        turn_progress_base = turn_index * 100 / max(1, len(turns_by_key[key]))
                        result = collect_sections([section], DATA_ROOT, data_id=TURN_CONFIG[turn],
                            offset=0, limit=1, incremental=False, staging_root=run_dir / "staging",
                            progress_callback=events.append, stop_event=stop_event, max_workers=1,
                            job_progress_callback=lambda event, base=turn_progress_base: update_worker(job_id, {
                                "progress": min(95.0, base + float(event.get("progress") or 0) /
                                    max(1, len(turns_by_key[key]))),
                                "stage": f"Turno {turn}: coleta {event.get('stage', 'processando')}",
                                "file": event.get("file"), "status": "running"}))
                        final = next((event for event in reversed(events) if event.get("status") != "running"), {})
                        raw_status = final.get("status", "failed")
                        acquisition_path = final.get("acquisition")
                        any_network_blocked = any_network_blocked or bool(result.get("network_blocked"))
                        if stop_event.is_set():
                            return {"key": key, "cancelled": True, "outcomes": outcomes,
                                    "network_blocked": any_network_blocked}
                    if acquisition_path and Path(acquisition_path).is_file():
                        report = analyze_collection(Path(acquisition_path), [], BU_SCHEMA, RDV_SCHEMA,
                            progress_callback=lambda event: update_worker(job_id, {
                                "progress": min(99.0, 50.0 + float(event.get("processed") or 0) * 50 /
                                    max(1, float(event.get("total") or 1))),
                                "stage": f"Turno {turn}: análise determinística da seção",
                                "file": event.get("file"), "status": "running"}))
                    else:
                        report = {"file_processing": {}, "diagnostics": [{"error": "aquisição incompleta"}]}
                row, keep_evidence = build_summary_row(section, int(turn), report)
                extract_status = "complete" if row["status_processamento"] == "processado" else "failed"
                error = None if extract_status == "complete" else "análise insuficiente para consolidar todas as fontes"
            except Exception as error_value:
                row, keep_evidence = build_summary_row(section, int(turn),
                    {"file_processing": {}, "diagnostics": [{"error": str(error_value)}]})
                row["status_processamento"] = "falha"
                extract_status = "failed"
                error = f"{type(error_value).__name__}: {error_value}"
                if not acquisition_path:
                    raw_status = "failed"
            outcomes.append({"turn": turn, "row": row, "keep_evidence": keep_evidence,
                "raw_status": raw_status, "extract_status": extract_status,
                "acquisition_path": str(acquisition_path) if acquisition_path else None, "error": error})
            if error:
                errors.append(error)
        return {"key": key, "section": section, "outcomes": outcomes,
                "network_blocked": any_network_blocked, "errors": errors}

    def submit_available(pool: ThreadPoolExecutor) -> None:
        nonlocal network_blocked
        activate_checkpoints()
        while pending and len(futures) < workers and not stop_event.is_set() and not network_blocked:
            key = pending.popleft()
            lock_key = key.casefold()
            if lock_key in active_sections:
                pending.append(key)
                break
            active_sections.add(lock_key)
            job_id = f"{key}:turnos-{','.join(turns_by_key[key])}"
            info = {**section_by_key[key], "job_id": job_id, "section": key,
                "turns": turns_by_key[key], "status": "running", "progress": 0.0,
                "stage": "Iniciando seção física"}
            with active_lock:
                active_jobs[job_id] = info
            _emit(progress_callback, {"type": "job_started", **info, "run_id": record["run_id"]})
            turns_to_process = [turn for turn in turns_by_key[key] if not summary_is_valid(turn, key)]
            raw_states = {}
            for turn in turns_to_process:
                row = connection.execute("""SELECT raw_status,acquisition_path FROM section_jobs
                    WHERE turn=? AND section_key=?""", (turn, key)).fetchone()
                raw_states[turn] = dict(row) if row else {}
            futures[pool.submit(process_unit, key, turns_to_process, raw_states)] = (key, lock_key, job_id)
            activate_checkpoints()
        queue_snapshot()

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="urna-section-unit") as pool:
        while True:
            submit_available(pool)
            if not futures:
                if stop_event.is_set() or network_blocked:
                    break
                if pending or next_checkpoint < len(checkpoints):
                    continue
                break
            done, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                key, lock_key, job_id = futures.pop(future)
                active_sections.discard(lock_key)
                result = future.result()
                for outcome in result["outcomes"]:
                    if outcome.get("already_consolidated"):
                        continue
                    connection.execute("""UPDATE section_jobs SET raw_status=?,extract_status=?,
                        acquisition_path=COALESCE(?,acquisition_path),error=?,batch_index=?
                        WHERE turn=? AND section_key=?""",
                        (outcome["raw_status"], outcome["extract_status"], outcome["acquisition_path"],
                         outcome["error"], section_position[key] // int(plan["sections_per_checkpoint"]),
                         outcome["turn"], key))
                    row_json = json.dumps(outcome["row"], ensure_ascii=False, separators=(",", ":"))
                    successful = (outcome["extract_status"] in {"complete", "not_needed"}
                        and outcome["row"].get("status_processamento") in {"processado", "agregada"})
                    connection.execute("""UPDATE section_jobs SET consolidation_status=?,
                        consolidated_at_utc=?,consolidation_row_sha256=? WHERE turn=? AND section_key=?""",
                        ("complete" if successful else "failed", _utc_now() if successful else None,
                         hashlib.sha256(row_json.encode("utf-8")).hexdigest() if successful else None,
                         outcome["turn"], key))
                    connection.execute("""INSERT OR REPLACE INTO section_summaries
                        (turn,section_key,batch_index,row_json,evidence_required) VALUES(?,?,?,?,?)""",
                        (outcome["turn"], key,
                         section_position[key] // int(plan["sections_per_checkpoint"]),
                         row_json,
                         int(outcome["keep_evidence"])))
                connection.commit()
                all_valid = all(summary_is_valid(turn, key) for turn in turns_by_key[key])
                if all_valid:
                    saved = connection.execute("SELECT 1 FROM section_summaries WHERE section_key=? AND evidence_required=1 LIMIT 1",
                                               (key,)).fetchone()
                    if saved:
                        preserve_section_evidence(DATA_ROOT, output_root / "evidencias", section_by_key[key])
                    clean_section_data(DATA_ROOT, ROOT, section_by_key[key])
                    for turn in turns_by_key[key]:
                        connection.execute("UPDATE section_jobs SET acquisition_path=NULL WHERE turn=? AND section_key=?",
                                           (turn, key))
                    connection.commit()
                else:
                    network_blocked = network_blocked or bool(result.get("network_blocked"))
                with active_lock:
                    active_jobs.pop(job_id, None)
                _emit(progress_callback, {"type": "section_complete", "stage": "section_unit_pipeline",
                    "section": key, "turns": turns_by_key[key],
                    "status": "complete" if all_valid else "partial", "run_id": record["run_id"]})
                current_counts = _run_counts(connection)
                _emit(progress_callback, {"type": "progress_update", "stage": "section_unit_pipeline",
                    "progress_processed": max(0, current_counts["total"] - current_counts["pending"]),
                    "progress_total": max(1, current_counts["total"]), "run_id": record["run_id"]})
                _emit(progress_callback, {"type": "job_finished", "job_id": job_id,
                    "section": key, "turns": turns_by_key[key], "status": "complete" if all_valid else "partial",
                    "progress": 100.0, "run_id": record["run_id"]})
                if result.get("network_blocked"):
                    network_blocked = True

    # Rebuild final tables from the durable per-turn rows. The single writer prevents collisions.
    grouped: dict[tuple[str, str, str], list[dict]] = {}
    for stored in connection.execute("SELECT row_json FROM section_summaries ORDER BY turn,section_key"):
        row = json.loads(stored[0])
        identity = (str(row.get("uf") or "XX"), str(row.get("municipio") or ""), str(row.get("zona") or ""))
        grouped.setdefault(identity, []).append(row)
    final_job = {"job_id": f"{record['run_id']}:consolidacao-final", "section": "Remessa completa",
        "stage": "Consolidação final por estado, município e zona", "status": "running", "progress": 0.0}
    _emit(progress_callback, {"type": "job_queue", "worker_limit": workers, "active_jobs": [final_job],
        "waiting_count": 0, "waiting_jobs": [], "run_id": record["run_id"]})
    _emit(progress_callback, {"type": "job_started", **final_job, "run_id": record["run_id"]})

    def write_final_outputs() -> list[dict]:
        zone_outputs = []
        zone_count = max(1, len(grouped))
        for index, ((state, municipality, zone), rows) in enumerate(sorted(grouped.items()), start=1):
            zone_dir = output_root / state / municipality
            csv_path = zone_dir / f"{zone}.csv"
            csv_hash = write_batch_csv(csv_path, rows)
            manifest_path = zone_dir / f"{zone}.manifest.json"
            write_batch_manifest(manifest_path, batch_id="consolidacao-final", row_count=len(rows),
                csv_path=csv_path, schema_hashes=schema_hashes, state_code=state)
            zone_outputs.append({"uf": state, "municipio": municipality, "zona": zone,
                "row_count": len(rows), "csv_path": str(csv_path), "csv_sha256": csv_hash,
                "manifest_path": str(manifest_path)})
            _emit(progress_callback, {"type": "job_progress", **final_job,
                "progress": round(index * 100 / zone_count, 2), "file": csv_path.name,
                "run_id": record["run_id"]})
        return zone_outputs

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="urna-final-consolidator") as finalizer:
        zone_outputs = finalizer.submit(write_final_outputs).result()
    _emit(progress_callback, {"type": "job_finished", **final_job, "status": "complete",
        "progress": 100.0, "run_id": record["run_id"]})
    counts = _run_counts(connection)
    incomplete = counts["pending"] or counts["failed"] or counts["extraction_failed"]
    status = "paused" if stop_event.is_set() else ("partial" if incomplete or network_blocked else "complete")
    _write_json(output_root / "manifest.json", {"run_id": record["run_id"], "status": status,
        "section_count": len(plan["section_keys"]), "section_turn_count": plan["total_section_turns"],
        "zone_outputs": zone_outputs, "proxy_rule_version": PROXY_RULE_VERSION,
        "schema_hashes": schema_hashes})
    for checkpoint in checkpoints:
        start = int(checkpoint["section_offset"])
        end = start + int(checkpoint["section_count"])
        keys = plan["section_keys"][start:end]
        failed = any(not all(summary_is_valid(turn, key) for turn in turns_by_key[key]) for key in keys)
        connection.execute("UPDATE processing_batches SET status=? WHERE batch_index=?",
            ("partial" if failed else "complete", checkpoint["checkpoint_index"]))
    connection.commit()
    record.update({"status": status, "counts": counts,
        "completed_batches": sum(row[0] == "complete" for row in connection.execute(
            "SELECT status FROM processing_batches")), "finished_at_utc": _utc_now(),
        "updated_at_utc": _utc_now(), "consolidation_outputs": zone_outputs})
    _write_json(run_dir / "run.json", record)
    result = {"run_id": record["run_id"], "status": status, "counts": counts,
        "run_dir": str(run_dir), "consolidation_root": str(output_root)}
    _emit(progress_callback, {"type": "run_finished", **result})
    return result


def _execute_section_unit_processing(run_dir: Path, connection: sqlite3.Connection, record: dict,
                                     progress_callback: Callable | None,
                                     stop_event: threading.Event | None, plan: dict) -> dict:
    """Feed collect, consolidate, and cleanup stages through one continuously reused worker pool."""
    stop_event = _RunStopSignal(stop_event, run_dir / "stop.request")
    output_root = Path(record["consolidation_root"])
    output_root.mkdir(parents=True, exist_ok=True)
    workers = int(record.get("request", {}).get("workers", DEFAULT_BATCH_WORKERS))
    turns_by_key = plan["section_turns"]
    sections: dict[str, dict] = {}
    inventories = {str(turn): json.loads((run_dir / "inventario" / f"turno{turn}" /
        "section-inventory.json").read_text(encoding="utf-8")) for turn in record["request"]["turns"]}
    for inventory in inventories.values():
        for section in inventory.get("sections", []):
            key = "/".join(str(section[name]) for name in ("uf", "municipio", "zona", "secao"))
            sections.setdefault(key, section)
    schema_hashes = {"bu": hashlib.sha256(BU_SCHEMA.read_bytes()).hexdigest(),
                     "rdv": hashlib.sha256(RDV_SCHEMA.read_bytes()).hexdigest()}

    if not record.get("initial_cleanup_done"):
        # Version 2 jobs preserve existing acquisitions and clean only after a
        # section has a durable, successful summary.
        record.update({"initial_cleanup_done": True, "initial_cleanup_entries_removed": 0,
                       "initial_cleanup_skipped": True, "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)

    valid_summary_cache: dict[tuple[str, str], bool] = {}

    def valid_summary(turn: str, key: str) -> bool:
        cache_key = (str(turn), key)
        if cache_key in valid_summary_cache:
            return valid_summary_cache[cache_key]
        stored = connection.execute("""SELECT s.row_json,j.extract_status,
            j.consolidation_status,j.consolidation_row_sha256 FROM section_summaries s
            JOIN section_jobs j ON j.turn=s.turn AND j.section_key=s.section_key
            WHERE s.turn=? AND s.section_key=?""", (turn, key)).fetchone()
        if (not stored or stored["extract_status"] == "failed"
                or stored["consolidation_status"] != "complete"):
            valid_summary_cache[cache_key] = False
            return False
        try:
            valid = json.loads(stored["row_json"]).get("status_processamento") in {"processado", "agregada"}
            canonical_row = json.dumps(json.loads(stored["row_json"]), ensure_ascii=False,
                separators=(",", ":"))
            row_hash = hashlib.sha256(canonical_row.encode("utf-8")).hexdigest()
            result = valid and (not stored["consolidation_row_sha256"]
                or stored["consolidation_row_sha256"] == row_hash)
            valid_summary_cache[cache_key] = result
            return result
        except (TypeError, json.JSONDecodeError):
            valid_summary_cache[cache_key] = False
            return False

    def raw_acquisition(key: str, turn: str) -> Path | None:
        section = sections[key]
        return complete_existing_acquisition(DATA_ROOT.joinpath(
            *(str(section[name]) for name in ("uf", "municipio", "zona", "secao"))),
            key, TURN_CONFIG[turn])

    def cleanup_task(key: str) -> dict:
        summary_rows = [json.loads(row[0]) for row in connection.execute(
            "SELECT row_json FROM section_summaries WHERE section_key=? ORDER BY turn", (key,))]
        evidence_turns = [str(int(row["turno"])) for row in connection.execute(
            "SELECT turn FROM section_summaries WHERE section_key=? AND evidence_required=1 ORDER BY turn",
            (key,))]
        raw_expected_turns = {str(int(row[0])) for row in connection.execute(
            "SELECT turn FROM section_jobs WHERE section_key=? AND acquisition_path IS NOT NULL", (key,))}
        return {"stage": "cleanup", "section_key": key, "turns": turns_by_key[key],
                "evidence_turns": evidence_turns, "summary_rows": summary_rows,
                "raw_expected_turns": raw_expected_turns, "section": sections[key]}

    def make_initial_task(key: str, turns: list[str] | None = None) -> dict | None:
        summaries = {}
        selected_turns = [str(turn) for turn in (turns or turns_by_key[key])]
        for turn in selected_turns:
            stored = connection.execute("""SELECT s.row_json,s.evidence_required,j.extract_status,
                j.consolidation_status,j.consolidation_row_sha256,j.acquisition_path,j.raw_status
                FROM section_jobs j LEFT JOIN section_summaries s
                ON s.turn=j.turn AND s.section_key=j.section_key
                WHERE j.turn=? AND j.section_key=?""", (turn, key)).fetchone()
            try:
                stored_row = json.loads(stored["row_json"]) if stored and stored["row_json"] else None
            except (TypeError, json.JSONDecodeError):
                stored_row = None
            summaries[turn] = ({"row": stored_row,
                "evidence_required": int(stored["evidence_required"] or 0),
                "extract_status": stored["extract_status"],
                "consolidation_status": stored["consolidation_status"],
                "consolidation_row_sha256": stored["consolidation_row_sha256"],
                "acquisition_path": stored["acquisition_path"],
                "raw_status": stored["raw_status"]} if stored and stored["row_json"] else (
                    {"row": None, "evidence_required": 0,
                     "extract_status": stored["extract_status"],
                     "consolidation_status": stored["consolidation_status"],
                     "consolidation_row_sha256": stored["consolidation_row_sha256"],
                     "acquisition_path": stored["acquisition_path"],
                     "raw_status": stored["raw_status"]} if stored else None))
        return {"stage": "audit", "section_key": key, "turns": selected_turns,
                "summaries": summaries, "section": sections[key]}

    def run_stage(task: dict) -> dict:
        stage, key, section = task["stage"], task["section_key"], task["section"]
        job_id = f"{key}:{stage}"
        stage_labels = {"audit": "Auditoria física", "collect": "Coleta da seção", "consolidate": "Conciliação determinística",
                        "cleanup": "Limpeza dos dados brutos"}
        outcomes = []
        try:
            if stage == "audit":
                turn_checks = []
                expected_paths = set()
                section_root = DATA_ROOT.joinpath(*(str(section[name]) for name in
                    ("uf", "municipio", "zona", "secao")))
                for turn in task["turns"]:
                    summary = task["summaries"].get(turn)
                    progress = lambda done, total, name, turn=turn: _emit(progress_callback, {
                        "type": "job_progress", "job_id": job_id,
                        "stage": stage_labels[stage], "section": key, "turn": int(turn),
                        "progress": round(100 * done / max(1, total), 2),
                        "detail": "Verificação de integridade", "file": name,
                        "run_id": record["run_id"]})
                    inspected = _inspect_physical_acquisition(section, turn, progress) if section_root.exists() else {
                        "turn": turn, "present": False, "valid": False, "reason": "raiz bruta ausente"}
                    if inspected.get("valid"):
                        expected_paths.update(inspected.get("expected_paths", set()))
                    summary_row = summary.get("row") if summary else None
                    summary_valid = bool(summary_row and summary["extract_status"] != "failed"
                        and summary_row.get("status_processamento") in {"processado", "agregada"})
                    if section.get("aggregation_role") == "aggregated":
                        action = "keep" if summary_valid else "consolidate"
                    elif inspected.get("present") and not inspected.get("valid"):
                        action = "collect"
                    elif inspected.get("valid") and (not summary_valid or
                            not _summary_matches_acquisition(summary_row, inspected)):
                        action = "consolidate"
                    elif summary_valid and not inspected.get("present") and summary.get("acquisition_path"):
                        action = "collect"
                    elif summary_valid:
                        action = "keep"
                    else:
                        action = "collect"
                    turn_checks.append({"turn": turn, "action": action,
                        "summary_valid": summary_valid, "acquisition": inspected})
                untracked = []
                if section_root.exists():
                    for current, directories, files in os.walk(section_root, topdown=True, followlinks=False):
                        current_path = Path(current)
                        for name in directories:
                            candidate = current_path / name
                            if _reparse_point(candidate):
                                untracked.append(str(candidate))
                        for name in files:
                            candidate = current_path / name
                            if _reparse_point(candidate) or not candidate.is_file():
                                untracked.append(str(candidate))
                            elif candidate.resolve() not in expected_paths:
                                untracked.append(str(candidate))
                return {"task": task, "success": True, "outcomes": [],
                    "audit": {"turns": turn_checks, "untracked_files": untracked[:20],
                              "untracked_count": len(untracked)}}
            elif stage == "collect":
                for turn in task["collect_turns"]:
                    if stop_event.is_set():
                        break
                    events = []
                    result = collect_sections([section], DATA_ROOT, data_id=TURN_CONFIG[turn],
                        offset=0, limit=1, incremental=False, staging_root=run_dir / "staging",
                        progress_callback=events.append, stop_event=stop_event, max_workers=1,
                        job_progress_callback=lambda event, turn=turn: _emit(progress_callback, {
                            "type": "job_progress", "job_id": job_id, "stage": stage_labels[stage], "section": key,
                            "turn": int(turn), "progress": event.get("progress"),
                            "detail": event.get("stage"), "file": event.get("file"),
                            "run_id": record["run_id"]}))
                    final = next((event for event in reversed(events) if event.get("status") != "running"), {})
                    acquisition = complete_existing_acquisition(DATA_ROOT.joinpath(
                        *(str(section[name]) for name in ("uf", "municipio", "zona", "secao"))),
                        key, TURN_CONFIG[turn])
                    outcomes.append({"turn": turn, "status": "collected" if acquisition else
                        final.get("status", "failed"), "acquisition": str(acquisition) if acquisition else
                        final.get("acquisition"), "error": final.get("reason") or final.get("error"),
                        "network_blocked": bool(result.get("network_blocked"))})
            elif stage == "consolidate":
                for index, turn in enumerate(task["turns"]):
                    if section.get("aggregation_role") == "aggregated":
                        report = {"file_processing": {}, "diagnostics": []}
                        raw_status = "aggregated"
                    else:
                        acquisition = raw_acquisition(key, turn)
                        if acquisition:
                            report = analyze_collection(acquisition, [], BU_SCHEMA, RDV_SCHEMA,
                                progress_callback=lambda event, turn=turn: _emit(progress_callback, {
                                    "type": "job_progress", "job_id": job_id,
                                    "stage": stage_labels[stage], "section": key,
                                    "turn": int(turn), "progress": round(100 *
                                        float(event.get("processed") or 0) /
                                        max(1, float(event.get("total") or 1)), 2),
                                    "detail": "Análise determinística", "file": event.get("file"),
                                    "run_id": record["run_id"]}))
                            raw_status = "collected"
                        else:
                            report = {"file_processing": {}, "diagnostics": [{"error": "aquisição incompleta"}]}
                            raw_status = "failed"
                    try:
                        row, evidence = build_summary_row(section, int(turn), report)
                        extract_status = "complete" if row["status_processamento"] in {"processado", "agregada"} else "failed"
                        error = None if extract_status == "complete" else "análise insuficiente para consolidar todas as fontes"
                    except Exception as error_value:
                        row, evidence = build_summary_row(section, int(turn),
                            {"file_processing": {}, "diagnostics": [{"error": str(error_value)}]})
                        row["status_processamento"] = "falha"
                        extract_status, error = "failed", f"{type(error_value).__name__}: {error_value}"
                    outcomes.append({"turn": turn, "row": row, "evidence_required": evidence,
                        "raw_status": raw_status, "extract_status": extract_status, "error": error,
                        "acquisition": str(raw_acquisition(key, turn) or "") or None})
            elif stage == "cleanup":
                physical_validation = _validate_physical_section_for_cleanup(section, task["summary_rows"],
                    task.get("raw_expected_turns", set()))
                _emit(progress_callback, {"type": "cleanup_validation", "run_id": record["run_id"],
                    "section": key, "result": "passed", **physical_validation})
                if task["evidence_turns"] and physical_validation["raw_tree_present"]:
                    evidence_copy = preserve_section_evidence(DATA_ROOT, output_root / "evidencias", section)
                    if evidence_copy is None:
                        raise ValueError(f"seção {key}: cópia de evidência não foi criada")
                    physical_validation["evidence_copy"] = verify_tree_copy(
                        section_root, evidence_copy)
                clean_section_data(DATA_ROOT, ROOT, section)
                if section_root.exists():
                    raise RuntimeError(f"seção {key}: a árvore bruta permaneceu após a limpeza")
            return {"task": task, "outcomes": outcomes, "success": True,
                    "network_blocked": any(item.get("network_blocked", False) for item in outcomes),
                    "physical_validation": physical_validation if stage == "cleanup" else None}
        except Exception as error:
            return {"task": task, "outcomes": outcomes, "success": False,
                    "error": f"{type(error).__name__}: {error}",
                    "traceback": traceback.format_exc(),
                    "network_blocked": any(item.get("network_blocked", False) for item in outcomes)}

    checkpoints = plan["checkpoints"]
    futures = {}
    active_jobs = {}
    active_lock = threading.Lock()
    network_blocked = False
    cleanup_failed_sections: set[str] = set()
    audit_failed_sections: set[str] = set()

    # Reuse durable summaries and audit only sessions that still need work and
    # either contain physical data or were left uncertain by a failed run.
    uncertain_source_sections = set(record.get("uncertain_source_sections") or [])
    reuse_cleanup_pending_sections = set(record.get("reuse_cleanup_pending_sections") or [])
    audit_section_keys = []
    for key in plan["section_keys"]:
        pending_turns = [str(turn) for turn in turns_by_key[key] if not valid_summary(str(turn), key)]
        needs_cleanup_validation = key in reuse_cleanup_pending_sections
        if not pending_turns and not needs_cleanup_validation:
            continue
        section_root = DATA_ROOT.joinpath(*(str(sections[key][name]) for name in
            ("uf", "municipio", "zona", "secao")))
        if key in uncertain_source_sections or section_root.exists():
            audit_section_keys.append(key)

    audit_total = len(audit_section_keys)
    audit_section_key_set = set(audit_section_keys)
    audit_summary_path = run_dir / "audit-summary.json"
    audit_results_path = run_dir / "audit-results.jsonl"
    audit_summary = {
        "run_id": record["run_id"], "status": "running", "sections_total": audit_total,
        "sections_reused": int(record.get("reused_sections") or 0),
        "sections_skipped_without_physical_data": max(0, len(plan["section_keys"])
            - audit_total - int(record.get("reused_sections") or 0)),
        "results_file": str(audit_results_path),
        "sections_verified": 0, "sections_failed": 0, "turns_checked": 0,
        "actions": {"keep": 0, "collect": 0, "consolidate": 0},
        "acquisitions": {"valid": 0, "invalid": 0, "missing": 0, "not_applicable": 0},
        "untracked_file_count": 0, "verified_files": 0, "verified_bytes": 0,
    }
    connection.execute("DELETE FROM section_audits")
    reuse_evidence_pending_sections = set(record.get("reuse_evidence_pending_sections") or [])
    def direct_decision(key: str) -> dict:
        turns = []
        section_root = DATA_ROOT.joinpath(*(str(sections[key][name]) for name in
            ("uf", "municipio", "zona", "secao")))
        trusted_reuse = (key not in reuse_evidence_pending_sections
            and key not in reuse_cleanup_pending_sections
            and not section_root.exists())
        for turn in turns_by_key[key]:
            turn = str(turn)
            summary_valid = valid_summary(turn, key)
            if summary_valid:
                action = "keep"
                acquisition = {"present": False, "valid": False,
                    "reason": "resultado consolidado reutilizado"}
            else:
                trusted_reuse = False
                action = "consolidate" if sections[key].get("aggregation_role") == "aggregated" else "collect"
                acquisition = {"present": False, "valid": False, "reason": "aquisição física ausente"}
            turns.append({"turn": turn, "action": action, "summary_valid": summary_valid,
                          "acquisition": acquisition})
        return {"aggregation_role": sections[key].get("aggregation_role"),
            "trusted_reuse": trusted_reuse, "turns": turns,
            "untracked_count": 0, "untracked_files": []}
    connection.commit()
    record.update({"resume_phase": "physical_audit", "audit_total_sections": audit_total,
                   "audit_completed_sections": 0, "updated_at_utc": _utc_now()})
    if audit_total == 0:
        record.update({"resume_phase": "processing_sections", "audit_status": "complete"})
        audit_summary.update({"status": "complete", "sections_completed": 0})
    _write_json(run_dir / "run.json", record)
    _write_json(audit_summary_path, audit_summary)
    if audit_total:
        _emit(progress_callback, {"type": "audit_started", "run_id": record["run_id"],
            "sections_total": audit_total, "workers": workers,
            "sections_reused": int(record.get("reused_sections") or 0)})
    else:
        _emit(progress_callback, {"type": "status", "stage": "audit_skipped",
            "message": "Auditoria física dispensada: não há sessões pendentes com dados físicos ou estado incerto.",
            "run_id": record["run_id"]})
    _configure_process_executable()
    process_context = multiprocessing.get_context("spawn")
    process_progress = process_context.Queue()
    worker_state_dir = _prepare_process_worker_state_dir(run_dir)
    process_pool = ProcessPoolExecutor(max_workers=workers, mp_context=process_context,
        initializer=_initialize_process_worker,
        initargs=(process_progress, worker_state_dir, record["run_id"]))

    def compact_audit(audit: dict, section: dict) -> dict:
        return {
            "aggregation_role": section.get("aggregation_role"),
            "trusted_reuse": bool(audit.get("trusted_reuse")),
            "turns": [{"turn": checked["turn"], "action": checked["action"],
                "summary_valid": checked["summary_valid"],
                "acquisition": {name: checked["acquisition"].get(name) for name in
                    ("present", "valid", "reason", "acquisition_path", "manifest_sha256", "files", "bytes")}}
                for checked in audit["turns"]],
            "untracked_count": audit["untracked_count"],
            "untracked_files": audit["untracked_files"],
        }

    def add_audit_counts(compact: dict) -> None:
        audit_summary["sections_verified"] += 1
        audit_summary["untracked_file_count"] += int(compact.get("untracked_count") or 0)
        for checked in compact.get("turns", []):
            audit_summary["turns_checked"] += 1
            action = checked.get("action")
            if action in audit_summary["actions"]:
                audit_summary["actions"][action] += 1
            acquisition = checked.get("acquisition") or {}
            if compact.get("aggregation_role") == "aggregated":
                audit_summary["acquisitions"]["not_applicable"] += 1
            elif acquisition.get("valid"):
                audit_summary["acquisitions"]["valid"] += 1
                audit_summary["verified_files"] += int(acquisition.get("files") or 0)
                audit_summary["verified_bytes"] += int(acquisition.get("bytes") or 0)
            elif acquisition.get("present"):
                audit_summary["acquisitions"]["invalid"] += 1
            else:
                audit_summary["acquisitions"]["missing"] += 1

    audit_started_at = time.monotonic()
    last_audit_report_at = 0.0
    last_queue_report_at = 0.0
    audit_iterator = iter(audit_section_keys)
    audit_next_index = 0
    audit_completed = 0
    with audit_results_path.open("w", encoding="utf-8", buffering=1) as audit_output:
        with _borrow_executor(process_pool) as audit_pool:
            while True:
                while len(futures) < workers and not stop_event.is_set():
                    try:
                        key = next(audit_iterator)
                    except StopIteration:
                        break
                    pending_turns = [str(turn) for turn in turns_by_key[key]
                        if not valid_summary(str(turn), key)]
                    task = make_initial_task(key, pending_turns)
                    job_id = f"{key}:audit"
                    info = {**task["section"], "job_id": job_id, "section": key,
                        "turns": task["turns"], "stage": "Auditoria física",
                        "stage_kind": "audit", "status": "running", "progress": 0.0}
                    with active_lock:
                        active_jobs[job_id] = info
                    _emit(progress_callback, {"type": "job_started", **info,
                        "run_id": record["run_id"]})
                    process_task = {**task, "operation": "section_stage", "job_id": job_id,
                        "run_id": record["run_id"], "staging_root": str(run_dir / "staging"),
                        "output_root": str(output_root), "stage_label": "Auditoria física"}
                    futures[audit_pool.submit(_run_process_task, process_task)] = (task, job_id)
                    audit_next_index += 1

                now = time.monotonic()
                if now - last_queue_report_at >= 0.5 or not futures:
                    with active_lock:
                        active = [active_jobs[job_id] for future, (_task, job_id) in futures.items()
                            if not future.done() and job_id in active_jobs]
                    _emit(progress_callback, {"type": "job_queue", "worker_limit": workers,
                        "active_jobs": active, "waiting_count": max(0, audit_total - audit_next_index),
                        "waiting_jobs": [{"section": key, "stage": "Auditoria física", "status": "queued"}
                            for key in audit_section_keys[audit_next_index:audit_next_index + 25]],
                        "active_stage_counts": {"audit": len(active)},
                        "waiting_stage_counts": {"audit": max(0, audit_total - audit_next_index)},
                        "run_id": record["run_id"]})
                    last_queue_report_at = now

                if not futures:
                    break
                done, _ = wait(futures, timeout=0.5, return_when=FIRST_COMPLETED)
                _drain_process_progress(process_progress, active_jobs, active_lock, progress_callback)
                for future in done:
                    task, job_id = futures.pop(future)
                    result = future.result()
                    if result["success"]:
                        compact = compact_audit(result["audit"], task["section"])
                        audit_json = json.dumps(compact, ensure_ascii=False, separators=(",", ":"))
                        connection.execute("INSERT OR REPLACE INTO section_audits(section_key,result_json,audited_at_utc) VALUES(?,?,?)",
                            (task["section_key"], audit_json, _utc_now()))
                        audit_output.write(json.dumps({"section": task["section_key"], **compact},
                            ensure_ascii=False, separators=(",", ":")) + "\n")
                        add_audit_counts(compact)
                    else:
                        audit_failed_sections.add(task["section_key"])
                        audit_summary["sections_failed"] += 1
                        error_item = {"section": task["section_key"], "error": result.get("error")}
                        connection.execute("INSERT OR REPLACE INTO section_audits(section_key,result_json,audited_at_utc) VALUES(?,?,?)",
                            (task["section_key"], json.dumps(error_item, ensure_ascii=False), _utc_now()))
                        audit_output.write(json.dumps(error_item, ensure_ascii=False,
                            separators=(",", ":")) + "\n")
                    audit_completed += 1
                    with active_lock:
                        active_jobs.pop(job_id, None)
                    _emit(progress_callback, {"type": "job_finished", "job_id": job_id,
                        "section": task["section_key"], "stage": "Auditoria física", "stage_kind": "audit",
                        "status": "complete" if result["success"] else "failed",
                        "progress": 100.0, "process_id": result.get("process_id"), "run_id": record["run_id"]})

                now = time.monotonic()
                if audit_completed == audit_total or audit_completed % 100 == 0 or now - last_audit_report_at >= 2.0:
                    audit_output.flush()
                    connection.commit()
                    audit_summary.update({"sections_completed": audit_completed,
                        "status": "complete" if audit_completed == audit_total else
                                  "paused" if stop_event.is_set() and not futures else "running",
                        "elapsed_seconds": round(now - audit_started_at, 3)})
                    record.update({"audit_completed_sections": audit_completed,
                        "updated_at_utc": _utc_now()})
                    _write_json(run_dir / "run.json", record)
                    _write_json(audit_summary_path, audit_summary)
                    _emit(progress_callback, {"type": "audit_progress", "run_id": record["run_id"],
                        "processed": audit_completed, "total": audit_total,
                        "audit_active": True,
                        "progress": round(audit_completed * 100 / max(1, audit_total), 2),
                        "summary": dict(audit_summary)})
                    last_audit_report_at = now

                if stop_event.is_set() and not futures:
                    break

    audit_finished = audit_completed == audit_total and not stop_event.is_set()
    audit_summary.update({"sections_completed": audit_completed,
        "status": "complete" if audit_finished else "paused" if stop_event.is_set() else "partial",
        "elapsed_seconds": round(time.monotonic() - audit_started_at, 3)})
    _write_json(audit_summary_path, audit_summary)
    record.update({"audit_completed_sections": audit_completed,
        "audit_status": audit_summary["status"],
        "audit_summary_path": str(audit_summary_path),
        "audit_results_path": str(audit_results_path),
        "updated_at_utc": _utc_now()})
    _write_json(run_dir / "run.json", record)
    if audit_total:
        _emit(progress_callback, {"type": "audit_complete", "run_id": record["run_id"],
            "summary": dict(audit_summary)})

    stage_queue = deque()
    next_checkpoint = 0
    futures = {}
    active_jobs = {}
    active_lock = threading.Lock()
    low_water = max(workers * 2, min(100, int(plan["sections_per_checkpoint"]) // 4))

    raw_done_states = {"collected", "skipped_existing", "aggregated", "partial", "failed"}
    terminal_extract_states = {"complete", "failed", "not_needed"}
    initial_groups = connection.execute("""SELECT raw_status,extract_status,COUNT(*)
        FROM section_jobs GROUP BY raw_status,extract_status""").fetchall()
    raw_done_count = sum(row[2] for row in initial_groups if row[0] in raw_done_states)
    extract_done_count = sum(row[2] for row in initial_groups
        if row[0] in raw_done_states and row[1] in terminal_extract_states)

    def update_job_status(turn: str, key: str, *, raw_status: str | None = None,
                          extract_status: str | None = None, acquisition_path: str | None = None,
                          error: str | None = None) -> None:
        """Persist one unit's status and maintain cached progress counts in constant time."""
        nonlocal raw_done_count, extract_done_count
        previous = connection.execute("""SELECT raw_status,extract_status FROM section_jobs
            WHERE turn=? AND section_key=?""", (turn, key)).fetchone()
        if not previous:
            return
        old_raw, old_extract = previous["raw_status"], previous["extract_status"]
        new_raw = raw_status if raw_status is not None else old_raw
        new_extract = extract_status if extract_status is not None else old_extract
        raw_done_count += int(new_raw in raw_done_states) - int(old_raw in raw_done_states)
        old_extract_done = old_raw in raw_done_states and old_extract in terminal_extract_states
        new_extract_done = new_raw in raw_done_states and new_extract in terminal_extract_states
        extract_done_count += int(new_extract_done) - int(old_extract_done)
        connection.execute("""UPDATE section_jobs SET raw_status=?,extract_status=?,
            acquisition_path=COALESCE(?,acquisition_path),error=? WHERE turn=? AND section_key=?""",
            (new_raw, new_extract, acquisition_path, error, turn, key))

    def emit_pipeline_progress(stage: str) -> None:
        processed = raw_done_count + extract_done_count
        total = max(1, int(plan["total_section_turns"]) * 2)
        percent = min(98.0, round(processed * 98 / total, 2))
        _emit(progress_callback, {"type": "progress_update", "stage": "Progresso geral da execução",
            "last_completed_stage": stage,
            "processed": processed, "total": total, "progress": percent,
            "run_id": record["run_id"]})

    record["progress_total_units"] = int(plan["total_section_turns"]) * 2
    record["processing_format_version"] = 2
    if audit_finished:
        record["resume_phase"] = "processing_sections"
        record["updated_at_utc"] = _utc_now()
        _write_json(run_dir / "run.json", record)
        emit_pipeline_progress("Fila de coleta e conciliação")

    def fill_checkpoints() -> None:
        nonlocal next_checkpoint
        while (audit_finished and next_checkpoint < len(checkpoints) and len(stage_queue) <= low_water
               and not stop_event.is_set() and not network_blocked):
            checkpoint = checkpoints[next_checkpoint]
            start = int(checkpoint["section_offset"])
            end = start + int(checkpoint["section_count"])
            enqueued = 0
            for key in plan["section_keys"][start:end]:
                task = make_initial_task(key)
                if task:
                    stored_audit = connection.execute(
                        "SELECT result_json FROM section_audits WHERE section_key=?", (key,)).fetchone()
                    audit_result = json.loads(stored_audit["result_json"]) if stored_audit else direct_decision(key)
                    if "error" in audit_result:
                        continue
                    append_next_stage({"task": task, "success": True,
                        "audit": audit_result, "outcomes": []})
                    enqueued += 1
            connection.execute("UPDATE processing_batches SET status=? WHERE batch_index=?",
                ("processing" if enqueued else "consolidated", checkpoint["checkpoint_index"]))
            connection.commit()
            next_checkpoint += 1
            record.update({"current_batch": checkpoint["checkpoint_id"],
                "next_checkpoint_index": next_checkpoint, "updated_at_utc": _utc_now()})
            _write_json(run_dir / "run.json", record)
            _emit(progress_callback, {"type": "checkpoint_queued", "run_id": record["run_id"],
                "checkpoint_id": checkpoint["checkpoint_id"], "section_count": enqueued,
                "queue_waiting": len(stage_queue), "total_checkpoints": len(checkpoints)})

    def queue_snapshot() -> None:
        with active_lock:
            active = [active_jobs[job_id] for future, (_task, job_id) in futures.items()
                if not future.done() and job_id in active_jobs]
            waiting = len(stage_queue)
            waiting_jobs = [{**task["section"], "section": task["section_key"], "stage": task["stage"],
                "status": "queued"} for task in list(stage_queue)[:25]]
            active_stage_counts = {}
            for item in active:
                stage = item.get("stage_kind") or "unknown"
                active_stage_counts[stage] = active_stage_counts.get(stage, 0) + 1
        waiting_stage_counts = {}
        for task in stage_queue:
            stage = task.get("stage") or "unknown"
            waiting_stage_counts[stage] = waiting_stage_counts.get(stage, 0) + 1
        _emit(progress_callback, {"type": "job_queue", "worker_limit": workers,
            "active_jobs": active, "waiting_count": waiting, "waiting_jobs": waiting_jobs,
            "active_stage_counts": active_stage_counts, "waiting_stage_counts": waiting_stage_counts,
            "run_id": record["run_id"], "current_batch": record.get("current_batch")})

    def append_next_stage(result: dict) -> None:
        task = result["task"]
        key, stage = task["section_key"], task["stage"]
        if stage == "audit":
            if not result["success"]:
                return
            audit = result["audit"]
            collect_turns = []
            consolidate_turns = []
            for checked in audit["turns"]:
                turn = checked["turn"]
                if checked["action"] in {"collect", "consolidate"}:
                    connection.execute("DELETE FROM section_summaries WHERE turn=? AND section_key=?", (turn, key))
                    reason = (checked["acquisition"].get("reason") if checked["action"] == "collect"
                              else "resumo ausente ou divergente; reconsolidação determinística agendada")
                    if checked["action"] == "collect":
                        collect_turns.append(turn)
                        update_job_status(turn, key, raw_status="pending",
                            extract_status="pending", error=reason)
                    else:
                        consolidate_turns.append(turn)
                        update_job_status(turn, key, extract_status="pending", error=reason)
                    connection.execute("""UPDATE section_jobs SET consolidation_status='pending',
                        consolidated_at_utc=NULL,consolidation_row_sha256=NULL
                        WHERE turn=? AND section_key=?""", (turn, key))
                    valid_summary_cache[(str(turn), key)] = False
            connection.commit()
            if collect_turns:
                reconcile_turns = list(dict.fromkeys(collect_turns + consolidate_turns))
                stage_queue.append({"stage": "collect", "section_key": key, "turns": reconcile_turns,
                    "collect_turns": collect_turns, "section": task["section"]})
            elif consolidate_turns:
                stage_queue.append({"stage": "consolidate", "section_key": key,
                    "turns": consolidate_turns, "section": task["section"]})
            if not collect_turns and not consolidate_turns and not audit.get("trusted_reuse"):
                section_root = DATA_ROOT.joinpath(*(str(task["section"][name]) for name in
                    ("uf", "municipio", "zona", "secao")))
                if section_root.exists() and task["section"].get("aggregation_role") != "aggregated":
                    stage_queue.append(cleanup_task(key))
        elif stage == "collect":
            for outcome in result["outcomes"]:
                update_job_status(outcome["turn"], key, raw_status=outcome["status"],
                    acquisition_path=outcome["acquisition"], error=outcome["error"])
            connection.commit()
            stage_queue.append({"stage": "consolidate", "section_key": key,
                "turns": task["turns"], "section": task["section"]})
        elif stage == "consolidate":
            checkpoint_index = section_position[key] // int(plan["sections_per_checkpoint"])
            for outcome in result["outcomes"]:
                update_job_status(outcome["turn"], key, raw_status=outcome["raw_status"],
                    extract_status=outcome["extract_status"],
                    acquisition_path=outcome["acquisition"], error=outcome["error"])
                connection.execute("UPDATE section_jobs SET batch_index=? WHERE turn=? AND section_key=?",
                    (checkpoint_index, outcome["turn"], key))
                row_json = json.dumps(outcome["row"], ensure_ascii=False, separators=(",", ":"))
                successful = (outcome["extract_status"] in {"complete", "not_needed"}
                    and outcome["row"].get("status_processamento") in {"processado", "agregada"})
                connection.execute("""UPDATE section_jobs SET consolidation_status=?,
                    consolidated_at_utc=?,consolidation_row_sha256=? WHERE turn=? AND section_key=?""",
                    ("complete" if successful else "failed", _utc_now() if successful else None,
                     hashlib.sha256(row_json.encode("utf-8")).hexdigest() if successful else None,
                     outcome["turn"], key))
                connection.execute("""INSERT OR REPLACE INTO section_summaries
                    (turn,section_key,batch_index,row_json,evidence_required) VALUES(?,?,?,?,?)""",
                    (outcome["turn"], key, checkpoint_index,
                     row_json,
                     int(outcome["evidence_required"])))
                valid_summary_cache[(str(outcome["turn"]), key)] = successful
            connection.commit()
            if all(valid_summary(turn, key) for turn in turns_by_key[key]):
                stage_queue.append(cleanup_task(key))
        elif stage == "cleanup" and result["success"]:
            connection.execute("UPDATE section_jobs SET acquisition_path=NULL WHERE section_key=?", (key,))
            connection.commit()

    section_position = {key: index for index, key in enumerate(plan["section_keys"])}
    with _borrow_executor(process_pool) as pool:
        while True:
            fill_checkpoints()
            while stage_queue and len(futures) < workers and not stop_event.is_set():
                task = stage_queue.popleft()
                key, stage = task["section_key"], task["stage"]
                job_id = f"{key}:{stage}"
                stage_labels = {"audit": "Auditoria física", "collect": "Coleta da seção", "consolidate": "Conciliação determinística",
                "cleanup": "Limpeza dos dados brutos"}
                info = {**task["section"], "job_id": job_id, "section": key,
                    "turns": task.get("turns", []), "stage": stage_labels[stage], "stage_kind": stage,
                    "status": "running", "progress": 0.0}
                with active_lock:
                    active_jobs[job_id] = info
                _emit(progress_callback, {"type": "job_started", **info, "run_id": record["run_id"]})
                process_task = {**task, "operation": "section_stage", "job_id": job_id,
                    "run_id": record["run_id"], "staging_root": str(run_dir / "staging"),
                    "output_root": str(output_root), "stage_label": stage_labels[stage]}
                futures[pool.submit(_run_process_task, process_task)] = (task, job_id)
                fill_checkpoints()
            queue_snapshot()
            if not futures:
                if stop_event.is_set() or network_blocked:
                    break
                if stage_queue or (audit_finished and next_checkpoint < len(checkpoints)):
                    continue
                break
            done, _ = wait(futures, timeout=0.5, return_when=FIRST_COMPLETED)
            _drain_process_progress(process_progress, active_jobs, active_lock, progress_callback)
            for future in done:
                task, job_id = futures.pop(future)
                result = future.result()
                network_blocked = network_blocked or bool(result.get("network_blocked"))
                append_next_stage(result)
                emit_pipeline_progress({"audit": "Auditoria física",
                    "collect": "Coleta e consolidação das seções",
                    "consolidate": "Conciliação determinística das seções",
                    "cleanup": "Limpeza das seções"}[task["stage"]])
                with active_lock:
                    active_jobs.pop(job_id, None)
                outcome_error = next((item.get("error") for item in result.get("outcomes", [])
                    if item.get("error")), None)
                outcome_traceback = next((item.get("traceback") for item in result.get("outcomes", [])
                    if item.get("traceback")), None)
                task_error = result.get("error") or outcome_error
                task_traceback = result.get("traceback") or outcome_traceback
                _emit(progress_callback, {"type": "stage_complete", "section": task["section_key"],
                    "stage": task["stage"], "status": "complete" if result["success"] else "failed",
                    "error": task_error, "traceback": task_traceback,
                    "process_id": result.get("process_id"),
                    "run_id": record["run_id"]})
                if not result["success"] or task_traceback:
                    _emit(progress_callback, {"type": "worker_error", "error": task_error,
                        "traceback": task_traceback,
                        "section": task["section_key"], "run_id": record["run_id"]})
                    if task["stage"] == "cleanup":
                        cleanup_failed_sections.add(task["section_key"])
                elif task["stage"] == "cleanup":
                    cleanup_failed_sections.discard(task["section_key"])
            if network_blocked:
                # Finish already queued deterministic work but stop adding new network collections.
                stage_queue = deque(task for task in stage_queue if task["stage"] != "collect")

    process_pool.shutdown(wait=True, cancel_futures=False)
    process_progress.close()

    grouped: dict[tuple[str, str, str], list[dict]] = {}
    for stored in connection.execute("SELECT row_json FROM section_summaries ORDER BY turn,section_key"):
        row = json.loads(stored[0])
        identity = (str(row.get("uf") or "XX"), str(row.get("municipio") or ""), str(row.get("zona") or ""))
        grouped.setdefault(identity, []).append(row)
    zone_outputs = []
    for (state, municipality, zone), rows in sorted(grouped.items()):
        zone_dir = output_root / state / municipality
        csv_path = zone_dir / f"{zone}.csv"
        csv_hash = write_batch_csv(csv_path, rows)
        manifest_path = zone_dir / f"{zone}.manifest.json"
        write_batch_manifest(manifest_path, batch_id="consolidacao-final", row_count=len(rows),
            csv_path=csv_path, schema_hashes=schema_hashes, state_code=state)
        zone_outputs.append({"uf": state, "municipio": municipality, "zona": zone,
            "row_count": len(rows), "csv_path": str(csv_path), "csv_sha256": csv_hash,
            "manifest_path": str(manifest_path)})
    counts = _run_counts(connection)
    incomplete = (counts["pending"] or counts["failed"] or counts["extraction_failed"]
        or audit_failed_sections)
    status = "paused" if stop_event.is_set() else (
        "partial" if incomplete or network_blocked or cleanup_failed_sections else "complete")
    _write_json(output_root / "manifest.json", {"run_id": record["run_id"], "status": status,
        "section_count": len(plan["section_keys"]), "section_turn_count": plan["total_section_turns"],
        "zone_outputs": zone_outputs, "proxy_rule_version": PROXY_RULE_VERSION,
        "schema_hashes": schema_hashes,
        "reuse_source_run_id": record.get("reuse_source_run_id"),
        "reused_sections": record.get("reused_sections", 0),
        "cleanup_failed_sections": sorted(cleanup_failed_sections)})
    for checkpoint in checkpoints:
        start = int(checkpoint["section_offset"])
        keys = plan["section_keys"][start:start + int(checkpoint["section_count"])]
        failed = any(not all(valid_summary(turn, key) for turn in turns_by_key[key]) for key in keys)
        connection.execute("UPDATE processing_batches SET status=? WHERE batch_index=?",
            ("partial" if failed else "complete", checkpoint["checkpoint_index"]))
    connection.commit()
    record.update({"status": status, "counts": counts,
        "completed_batches": sum(row[0] == "complete" for row in connection.execute(
            "SELECT status FROM processing_batches")), "finished_at_utc": _utc_now(),
        "updated_at_utc": _utc_now(), "consolidation_outputs": zone_outputs,
        "cleanup_failed_sections": sorted(cleanup_failed_sections)})
    _write_json(run_dir / "run.json", record)
    result = {"run_id": record["run_id"], "status": status, "counts": counts,
        "run_dir": str(run_dir), "consolidation_root": str(output_root)}
    _emit(progress_callback, {"type": "run_finished", **result})
    return result


def _execute_batch_processing(run_dir: Path, connection: sqlite3.Connection, record: dict,
                              progress_callback: Callable | None,
                              stop_event: threading.Event | None) -> dict:
    plan_path = run_dir / "remessas-plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    if int(plan.get("plan_schema_version", 1)) >= 2:
        return _execute_section_unit_processing(run_dir, connection, record, progress_callback,
                                                stop_event, plan)
    stop_event = _RunStopSignal(stop_event, run_dir / "stop.request")
    inventories = {turn: json.loads((run_dir / "inventario" / f"turno{turn}" / "section-inventory.json")
                                  .read_text(encoding="utf-8")) for turn in record["request"]["turns"]}
    output_root = Path(record["consolidation_root"])
    output_root.mkdir(parents=True, exist_ok=True)
    total = int(record.get("total_sections", 0))
    workers = int(record["request"].get("workers", DEFAULT_BATCH_WORKERS))
    schema_hashes = {"bu": hashlib.sha256(BU_SCHEMA.read_bytes()).hexdigest(),
                     "rdv": hashlib.sha256(RDV_SCHEMA.read_bytes()).hexdigest()}
    processed = connection.execute("SELECT COUNT(*) FROM section_summaries").fetchone()[0]
    raw_processed = connection.execute("SELECT COUNT(*) FROM section_jobs WHERE raw_status IN ('collected','partial','failed','skipped_existing','aggregated')").fetchone()[0]

    if not record.get("initial_cleanup_done"):
        _emit(progress_callback, {"type": "status", "stage": "initial_cleanup",
                                 "message": "Limpando dados/ antes da primeira remessa", "run_id": record["run_id"]})
        removed = clean_data_root(DATA_ROOT, ROOT)
        record.update({"initial_cleanup_done": True, "initial_cleanup_entries_removed": removed,
                       "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        _emit(progress_callback, {"type": "status", "stage": "initial_cleanup",
            "message": f"Base bruta limpa ({removed} itens removidos)", "run_id": record["run_id"]})

    network_blocked = False
    for batch_index, batch in enumerate(plan.get("batches", [])):
        batch_state = connection.execute("SELECT status,batch_id,csv_path,manifest_path FROM processing_batches WHERE batch_index=?",
                                         (batch_index,)).fetchone()
        batch_id = batch_state["batch_id"] if batch_state else f"remessa-{batch_index + 1:05d}"
        if batch_state and batch_state["status"] == "complete":
            continue
        if stop_event.is_set():
            break
        turn = str(batch["turno"])
        sections = inventories[turn].get("sections", [])[batch["offset"]:batch["offset"] + batch["limit"]]
        work_rows = []
        for section in sections:
            key = "/".join(section[name] for name in ("uf", "municipio", "zona", "secao"))
            row = connection.execute("SELECT raw_status,extract_status,acquisition_path,error FROM section_jobs WHERE turn=? AND section_key=?",
                                     (turn, key)).fetchone()
            summary = connection.execute("SELECT row_json FROM section_summaries WHERE turn=? AND section_key=?",
                                         (turn, key)).fetchone()
            if summary is not None and row and row["extract_status"] == "failed":
                connection.execute("DELETE FROM section_summaries WHERE turn=? AND section_key=?", (turn, key))
                connection.execute("UPDATE section_jobs SET extract_status='pending' WHERE turn=? AND section_key=?",
                                   (turn, key))
                summary = None
            work_rows.append({"turn": turn, "key": key, "section": section,
                              "raw_status": row["raw_status"] if row else "pending",
                              "extract_status": row["extract_status"] if row else "pending",
                              "acquisition_path": row["acquisition_path"] if row else None,
                              "error": row["error"] if row else None,
                              "has_summary": summary is not None})
        for item in work_rows:
            if item["section"].get("aggregation_role") != "aggregated" or item["has_summary"]:
                continue
            row, _ = build_summary_row(item["section"], int(turn), {})
            row_json = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
            connection.execute("""UPDATE section_jobs SET raw_status='aggregated',extract_status='not_needed',
                consolidation_status='complete',consolidated_at_utc=?,consolidation_row_sha256=?,
                acquisition_path=NULL,error=NULL WHERE turn=? AND section_key=?""",
                (_utc_now(), hashlib.sha256(row_json.encode("utf-8")).hexdigest(), turn, item["key"]))
            connection.execute("""INSERT OR REPLACE INTO section_summaries
                (turn,section_key,batch_index,row_json,evidence_required) VALUES(?,?,?,?,0)""",
                (turn, item["key"], batch_index, row_json))
            item["has_summary"] = True
            raw_processed += 1
            processed += 1
        connection.commit()
        connection.execute("UPDATE processing_batches SET status='processing',error=NULL WHERE batch_index=?",
                           (batch_index,))
        connection.commit()
        record.update({"current_batch": batch_id, "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        _emit(progress_callback, {"type": "batch_started", "run_id": record["run_id"],
            "batch_id": batch_id, "batch_index": batch_index, "total_batches": len(plan.get("batches", [])),
            "section_count": len(work_rows)})

        # Each queued section is one end-to-end task: acquire if needed, then analyze.
        # Workers therefore overlap collection and processing across different sections.
        pending = deque(item for item in work_rows if not item["has_summary"])
        futures = {}
        active_jobs: dict[str, dict] = {}
        active_lock = threading.Lock()
        active_sections: set[str] = set()
        worker_pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="urna-batch-pipeline")

        def queue_snapshot() -> None:
            with active_lock:
                active_snapshot = list(active_jobs.values())
                waiting = len(pending)
                queued = [dict(item["section"], status="queued", turn=int(item["turn"]),
                               stage="Aguardando coleta/processamento") for item in list(pending)[:25]]
            _emit(progress_callback, {"type": "job_queue", "worker_limit": workers,
                "active_jobs": active_snapshot, "waiting_count": waiting, "waiting_jobs": queued,
                "run_id": record["run_id"], "batch_id": batch_id})

        def update_worker(job_id: str, update: dict) -> None:
            with active_lock:
                current = active_jobs.get(job_id, {})
                updated = {**current, **update}
                active_jobs[job_id] = updated
            queue_snapshot()
            _emit(progress_callback, {"type": "job_progress", **updated,
                "run_id": record["run_id"], "batch_id": batch_id})

        def section_pipeline(item: dict) -> dict:
            info = {"job_id": f"{item['key']}:turno-{item['turn']}", "section": item["key"],
                    "turn": int(item["turn"]), "batch_id": batch_id}
            if stop_event.is_set():
                return {**info, "cancelled": True, "item": item,
                        "raw_status": item["raw_status"], "acquisition_path": item["acquisition_path"]}
            events = []
            raw_status = item["raw_status"]
            acquisition_path = item["acquisition_path"]
            network_blocked_result = False
            collection_error = None
            try:
                reusable = raw_status in {"collected", "partial"} and acquisition_path and Path(acquisition_path).is_file()
                if not reusable:
                    result = collect_sections([item["section"]], DATA_ROOT, data_id=TURN_CONFIG[item["turn"]],
                        offset=0, limit=1, incremental=False, staging_root=run_dir / "staging",
                        progress_callback=events.append, stop_event=stop_event,
                        max_workers=1, job_progress_callback=lambda event: update_worker(info["job_id"], {
                            "progress": float(event.get("progress") or 0) * 0.5,
                            "stage": f"Coleta: {event.get('stage', 'processando')}",
                            "file": event.get("file"), "status": "running"}))
                    final = next((event for event in reversed(events) if event.get("status") != "running"), {})
                    raw_status = final.get("status", "failed")
                    acquisition_path = final.get("acquisition")
                    collection_error = final.get("reason") or final.get("error")
                    network_blocked_result = bool(result.get("network_blocked", False))
                    if raw_status == "cancelled":
                        return {**info, "item": item, "cancelled": True, "raw_status": raw_status,
                                "acquisition_path": acquisition_path, "network_blocked": network_blocked_result}

                if acquisition_path and Path(acquisition_path).is_file():
                    update_worker(info["job_id"], {"progress": 50.0,
                        "stage": "Análise determinística da seção", "file": None, "status": "running"})
                    def on_analysis_progress(update: dict) -> None:
                        denominator = max(1.0, float(update.get("total") or 0))
                        processed_files = max(0.0, float(update.get("processed") or 0))
                        percentage = min(95.0, max(0.0, 95.0 * processed_files / denominator))
                        update_worker(info["job_id"], {"progress": 50.0 + percentage * 0.5,
                            "stage": "Análise determinística da seção", "file": update.get("file"),
                            "status": "running"})
                    report = analyze_collection(Path(acquisition_path), [], BU_SCHEMA, RDV_SCHEMA,
                                                progress_callback=on_analysis_progress)
                else:
                    report = {"file_processing": {}, "diagnostics": ([{"error": collection_error}]
                        if collection_error else [])}
                row, keep_evidence = build_summary_row(item["section"], int(item["turn"]), report)
                extract_status = "complete" if row["status_processamento"] == "processado" else "failed"
                error_message = collection_error or (None if extract_status == "complete" else
                    "análise insuficiente para consolidar todas as fontes")
            except Exception as pipeline_error:
                if not acquisition_path or not Path(acquisition_path).is_file():
                    raw_status = "failed"
                row, keep_evidence = build_summary_row(item["section"], int(item["turn"]),
                    {"file_processing": {}, "diagnostics": [{"error": str(pipeline_error)}]})
                row["status_processamento"] = "falha"
                extract_status = "failed"
                error_message = f"{type(pipeline_error).__name__}: {pipeline_error}"
            return {**info, "item": item, "cancelled": False, "raw_status": raw_status,
                "acquisition_path": acquisition_path, "network_blocked": network_blocked_result,
                "row": row, "keep_evidence": keep_evidence, "extract_status": extract_status,
                "error": error_message}

        def submit_available() -> None:
            while pending and len(futures) < workers and not stop_event.is_set() and not network_blocked:
                scheduled = False
                for _ in range(len(pending)):
                    with active_lock:
                        item = pending.popleft()
                    lock_key = item["key"].casefold()
                    if lock_key in active_sections:
                        with active_lock:
                            pending.append(item)
                        continue
                    active_sections.add(lock_key)
                    info = {"job_id": f"{item['key']}:turno-{item['turn']}",
                            "section": item["key"], "turn": int(item["turn"]),
                            "status": "running", "progress": 0.0, "stage": "Iniciando seção",
                            "batch_id": batch_id}
                    with active_lock:
                        active_jobs[info["job_id"]] = info
                    _emit(progress_callback, {"type": "section_started", "stage": "batch_pipeline",
                        "turn": int(item["turn"]), "section": item["key"], "run_id": record["run_id"],
                        "batch_id": batch_id})
                    _emit(progress_callback, {"type": "job_started", **info, "run_id": record["run_id"]})
                    futures[worker_pool.submit(section_pipeline, item)] = (item, lock_key, info["job_id"])
                    scheduled = True
                    break
                if not scheduled:
                    break
            queue_snapshot()

        try:
            while pending or futures:
                submit_available()
                if not futures:
                    break
                done, _ = wait(futures, return_when=FIRST_COMPLETED)
                for future in done:
                    item, lock_key, job_id = futures.pop(future)
                    active_sections.discard(lock_key)
                    result = future.result()
                    raw_status = result.get("raw_status", "failed")
                    if result.get("cancelled"):
                        if raw_status == "cancelled":
                            raw_status = "pending"
                        connection.execute("""UPDATE section_jobs SET raw_status=?,
                            acquisition_path=COALESCE(?,acquisition_path),batch_index=?
                            WHERE turn=? AND section_key=?""",
                            (raw_status, result.get("acquisition_path"), batch_index, item["turn"], item["key"]))
                    else:
                        connection.execute("""UPDATE section_jobs SET raw_status=?,extract_status=?,
                            acquisition_path=COALESCE(?,acquisition_path),error=?,batch_index=?
                            WHERE turn=? AND section_key=?""",
                            (raw_status, result["extract_status"], result.get("acquisition_path"),
                             result.get("error"), batch_index, item["turn"], item["key"]))
                        row_json = json.dumps(result["row"], ensure_ascii=False, separators=(",", ":"))
                        successful = (result["extract_status"] in {"complete", "not_needed"}
                            and result["row"].get("status_processamento") in {"processado", "agregada"})
                        connection.execute("""UPDATE section_jobs SET consolidation_status=?,
                            consolidated_at_utc=?,consolidation_row_sha256=? WHERE turn=? AND section_key=?""",
                            ("complete" if successful else "failed", _utc_now() if successful else None,
                             hashlib.sha256(row_json.encode("utf-8")).hexdigest() if successful else None,
                             item["turn"], item["key"]))
                        connection.execute("""INSERT OR REPLACE INTO section_summaries
                            (turn,section_key,batch_index,row_json,evidence_required) VALUES(?,?,?,?,?)""",
                            (item["turn"], item["key"], batch_index, row_json,
                             int(result["keep_evidence"])))
                    connection.commit()
                    raw_processed = connection.execute("""SELECT COUNT(*) FROM section_jobs WHERE raw_status IN
                        ('collected','partial','failed','skipped_existing','aggregated')""").fetchone()[0]
                    if result.get("network_blocked"):
                        network_blocked = True
                    with active_lock:
                        active_jobs.pop(job_id, None)
                    section_status = "cancelled" if result.get("cancelled") else result["extract_status"]
                    _emit(progress_callback, {"type": "section_complete", "stage": "batch_pipeline",
                        "turn": int(item["turn"]), "section": item["key"], "status": section_status,
                        "processed": raw_processed, "total": total, "run_id": record["run_id"],
                        "batch_id": batch_id})
                    completed_summaries = connection.execute(
                        "SELECT COUNT(*) FROM section_summaries").fetchone()[0]
                    processed = completed_summaries
                    _emit(progress_callback, {"type": "progress_update", "stage": "batch_pipeline",
                        "progress_processed": min(total * 2, raw_processed + completed_summaries),
                        "progress_total": max(1, total * 2), "raw_processed": raw_processed,
                        "raw_total": total, "extraction_processed": completed_summaries,
                        "extraction_total": total, "run_id": record["run_id"], "batch_id": batch_id})
                    # Refill the slot before handling another already-completed future.
                    # This keeps the pool at its configured width when many sections finish together.
                    submit_available()
                    _emit(progress_callback, {"type": "job_finished", "job_id": job_id,
                        "section": item["key"], "turn": int(item["turn"]),
                        "status": section_status, "progress": 100.0,
                        "run_id": record["run_id"], "batch_id": batch_id})
        finally:
            worker_pool.shutdown(wait=True, cancel_futures=stop_event.is_set())

        if stop_event.is_set() or network_blocked or pending:
            record.update({"status": "paused" if stop_event.is_set() else "partial",
                           "current_batch": batch_id,
                           "error": "Parada solicitada; dados brutos da remessa foram preservados." if stop_event.is_set()
                                    else "A coleta foi interrompida; dados brutos da remessa foram preservados.",
                           "updated_at_utc": _utc_now()})
            counts = _run_counts(connection)
            record.update({"counts": counts, "finished_at_utc": _utc_now()})
            _write_json(run_dir / "run.json", record)
            result = {"run_id": record["run_id"], "status": record["status"], "counts": counts,
                      "run_dir": str(run_dir), "consolidation_root": str(output_root)}
            _emit(progress_callback, {"type": "run_finished", **result})
            return result

        summary_rows = [json.loads(row[0]) for row in connection.execute(
            "SELECT row_json FROM section_summaries WHERE batch_index=? ORDER BY turn,section_key", (batch_index,))]
        if len(summary_rows) != len(work_rows):
            raise RuntimeError(f"remessa {batch_id} tem {len(summary_rows)} resumos para {len(work_rows)} seções")

        rows_by_state: dict[str, list[dict]] = {}
        for row in summary_rows:
            state_code = str(row.get("uf") or "XX").upper()
            rows_by_state.setdefault(state_code, []).append(row)

        finalize_job_id = f"{batch_id}:consolidacao-final"
        finalize_info = {"job_id": finalize_job_id, "section": "Remessa inteira", "turn": "",
                         "status": "running", "progress": 0.0,
                         "stage": "Consolidação final por estado", "batch_id": batch_id}
        _emit(progress_callback, {"type": "job_queue", "worker_limit": workers,
            "active_jobs": [finalize_info], "waiting_count": 0, "waiting_jobs": [],
            "run_id": record["run_id"], "batch_id": batch_id})
        _emit(progress_callback, {"type": "job_started", **finalize_info, "run_id": record["run_id"]})

        def finalize_batch() -> dict:
            outputs = []
            state_count = max(1, len(rows_by_state))
            for state_index, (state_code, state_rows) in enumerate(sorted(rows_by_state.items()), start=1):
                state_batch_dir = output_root / state_code / batch_id
                csv_path = state_batch_dir / "consolidacao.csv"
                state_manifest_path = state_batch_dir / "manifest.json"
                csv_hash = write_batch_csv(csv_path, state_rows)
                write_batch_manifest(state_manifest_path, batch_id=batch_id, row_count=len(state_rows),
                    csv_path=csv_path, schema_hashes=schema_hashes, state_code=state_code)
                outputs.append({"uf": state_code, "row_count": len(state_rows),
                    "csv_path": str(csv_path), "manifest_path": str(state_manifest_path),
                    "csv_sha256": csv_hash})
                _emit(progress_callback, {"type": "job_progress", **finalize_info,
                    "progress": round(state_index * 100 / state_count, 2),
                    "stage": f"Gravando consolidado de {state_code}", "file": csv_path.name,
                    "run_id": record["run_id"]})
            manifest_path = output_root / "_remessas" / f"{batch_id}.json"
            _write_json(manifest_path, {"batch_id": batch_id, "row_count": len(summary_rows),
                "state_outputs": outputs, "schema_hashes": schema_hashes,
                "proxy_rule_version": PROXY_RULE_VERSION})
            return {"outputs": outputs, "manifest_path": str(manifest_path)}

        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="urna-batch-finalize") as finalize_pool:
            finalization = finalize_pool.submit(finalize_batch).result()
        state_outputs = finalization["outputs"]
        manifest_path = finalization["manifest_path"]
        single_state_csv = state_outputs[0]["csv_path"] if len(state_outputs) == 1 else ""
        single_state_hash = state_outputs[0]["csv_sha256"] if len(state_outputs) == 1 else ""
        connection.execute("""UPDATE processing_batches SET status='consolidated',row_count=?,csv_path=?,
            manifest_path=?,csv_sha256=?,state_outputs_json=? WHERE batch_index=?""",
            (len(summary_rows), single_state_csv, manifest_path, single_state_hash,
             json.dumps(state_outputs, ensure_ascii=False, separators=(",", ":")), batch_index))
        connection.commit()
        _emit(progress_callback, {"type": "job_finished", **finalize_info,
            "status": "complete", "progress": 100.0, "run_id": record["run_id"]})

        evidence_root = output_root / "evidencias" / batch_id
        for item in work_rows:
            saved = connection.execute("SELECT evidence_required FROM section_summaries WHERE turn=? AND section_key=?",
                                       (item["turn"], item["key"])).fetchone()
            if saved and saved[0]:
                preserve_section_evidence(DATA_ROOT, evidence_root, item["section"])
                evidence_path = evidence_root.joinpath(item["section"]["uf"], item["section"]["municipio"],
                    item["section"]["zona"], item["section"]["secao"],
                    f"turno-{item['turn']}", "acquisition.json")
                if evidence_path.is_file():
                    connection.execute("UPDATE section_jobs SET acquisition_path=? WHERE turn=? AND section_key=?",
                                       (str(evidence_path), item["turn"], item["key"]))
                else:
                    connection.execute("UPDATE section_jobs SET acquisition_path=NULL WHERE turn=? AND section_key=?",
                                       (item["turn"], item["key"]))
            else:
                connection.execute("UPDATE section_jobs SET acquisition_path=NULL WHERE turn=? AND section_key=?",
                                   (item["turn"], item["key"]))
        # Evidence and the consolidated output are durable before this cleanup.
        clean_data_root(DATA_ROOT, ROOT)
        staging = run_dir / "staging"
        if staging.exists():
            shutil.rmtree(staging)
        batch_has_failures = connection.execute("""SELECT 1 FROM section_jobs
            WHERE batch_index=? AND (raw_status='failed' OR extract_status='failed') LIMIT 1""",
            (batch_index,)).fetchone() is not None
        connection.execute("UPDATE processing_batches SET status=? WHERE batch_index=?",
                           ("partial" if batch_has_failures else "complete", batch_index))
        connection.commit()
        _emit(progress_callback, {"type": "batch_finished", "run_id": record["run_id"],
            "batch_id": batch_id, "row_count": len(summary_rows), "csv_paths": state_outputs,
            "manifest_path": manifest_path})

    batch_statuses = [row[0] for row in connection.execute("SELECT status FROM processing_batches")]
    counts = _run_counts(connection)
    if stop_event.is_set():
        status = "paused"
    elif network_blocked or any(value != "complete" for value in batch_statuses):
        status = "partial"
    elif counts["failed"] or counts["extraction_failed"]:
        status = "partial"
    else:
        status = "complete"
    record.update({"status": status, "counts": counts,
                   "completed_batches": sum(value == "complete" for value in batch_statuses),
                   "finished_at_utc": _utc_now(), "updated_at_utc": _utc_now()})
    _write_json(run_dir / "run.json", record)
    result = {"run_id": record["run_id"], "status": status, "counts": counts,
              "run_dir": str(run_dir), "consolidation_root": str(output_root)}
    _emit(progress_callback, {"type": "run_finished", **result})
    return result


def _run_counts(connection: sqlite3.Connection) -> dict:
    rows = connection.execute("SELECT raw_status,extract_status,COUNT(*) AS amount FROM section_jobs GROUP BY raw_status,extract_status").fetchall()
    counts = {"total": 0, "collected": 0, "skipped_existing": 0, "aggregated": 0,
              "partial": 0, "failed": 0, "pending": 0, "extraction_failed": 0,
              "extraction_pending": 0}
    for row in rows:
        raw_status, extract_status, amount = row["raw_status"], row["extract_status"], row["amount"]
        counts["total"] += amount
        if raw_status in {"collected", "skipped_existing", "aggregated", "partial", "failed"}:
            counts[raw_status] += amount
        if raw_status in {"pending", "running"}:
            counts["pending"] += amount
        if raw_status in {"collected", "partial"} and extract_status == "pending":
            counts["pending"] += amount
            counts["extraction_pending"] += amount
        if extract_status == "failed":
            counts["extraction_failed"] += amount
            counts["failed"] += amount
    return counts


@_serialized_job
def resume_run(run_id: str, *, progress_callback: Callable | None = None,
               stop_event: threading.Event | None = None) -> dict:
    run_dir = STATE_ROOT / "runs" / run_id
    record = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
    if record.get("status") == "complete":
        return {"run_id": run_id, "status": "complete", "counts": record.get("counts", {}), "run_dir": str(run_dir)}
    connection = _connect(run_dir / "state.sqlite3")
    on_progress = _persistent_callback(run_dir, progress_callback,
        int((record.get("request") or {}).get("workers", DEFAULT_BATCH_WORKERS)))
    try:
        (run_dir / "stop.request").unlink(missing_ok=True)
        stop_signal = _RunStopSignal(stop_event, run_dir / "stop.request")
        record.update({"status": "running", "cross_process_stop_supported": True,
                       "resumed_at_utc": _utc_now(), "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        if record.get("kind") == "collect":
            plan_path = run_dir / "coleta-plan.json"
            job_count = connection.execute("SELECT COUNT(*) FROM section_jobs").fetchone()[0]
            if not plan_path.is_file():
                inventories = _discover_for_run(record["request"], run_dir, on_progress)
                _store_collection_plan(run_dir, connection, inventories)
                record["total_sections"] = sum(len(item.get("sections", [])) for item in inventories.values())
                record["total_batches"] = json.loads(plan_path.read_text(encoding="utf-8"))["total_batches"]
                _write_json(run_dir / "run.json", record)
            elif job_count == 0:
                inventories = {turn: json.loads((run_dir / "inventario" / f"turno{turn}" / "section-inventory.json")
                               .read_text(encoding="utf-8")) for turn in record["request"]["turns"]}
                plan = json.loads(plan_path.read_text(encoding="utf-8"))
                for turn, inventory in inventories.items():
                    connection.executemany(
                        "INSERT OR IGNORE INTO section_jobs(turn,data_id,section_key,section_json) VALUES(?,?,?,?)",
                        [(turn, TURN_CONFIG[turn], "/".join((item["uf"], item["municipio"], item["zona"], item["secao"])),
                          json.dumps(item, ensure_ascii=False)) for item in inventory.get("sections", [])])
                connection.commit()
                record.update({"total_sections": plan["total_sections"], "total_batches": plan["total_batches"]})
                _write_json(run_dir / "run.json", record)
            return _execute_collection(run_dir, connection, record, on_progress, stop_event)
        if record.get("kind") == "batch_process":
            plan = _upgrade_batch_plan_for_resume(run_dir, connection, record, on_progress, stop_signal)
            if plan.get("plan_schema_version", 1) >= 2:
                record["status"] = "running"
                _write_json(run_dir / "run.json", record)
            return _execute_section_unit_processing(run_dir, connection, record, on_progress,
                                                    stop_event, plan)
        return _execute_reprocess(run_dir, connection, record, on_progress, stop_event)
    except _RunPauseRequested as error:
        counts = _run_counts(connection)
        record.update({"status": "paused", "error": str(error), "counts": counts,
                       "finished_at_utc": _utc_now(), "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        result = {"run_id": run_id, "status": "paused", "counts": counts, "run_dir": str(run_dir),
                  "consolidation_root": record.get("consolidation_root")}
        _emit(on_progress, {"type": "run_finished", **result})
        return result
    except Exception as error:
        error_traceback = traceback.format_exc()
        record.update({"status": "failed", "error": f"{type(error).__name__}: {error}",
                       "traceback": error_traceback,
                       "finished_at_utc": _utc_now(), "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        _emit(on_progress, {"type": "run_failed", "run_id": run_id, "error": record["error"],
                            "traceback": error_traceback})
        raise
    finally:
        connection.close()


@_serialized_job
def start_reprocessing(*, data_root: str | Path = DATA_ROOT, workers: int = DEFAULT_BATCH_WORKERS,
                       progress_callback: Callable | None = None,
                       stop_event: threading.Event | None = None) -> dict:
    data_root = Path(data_root).resolve()
    validate_worker_count(workers)
    acquisitions = sorted(data_root.rglob("acquisition.json"))
    if not acquisitions:
        raise ValueError(f"no acquisition.json files found under {data_root}")
    run_id = _new_id()
    run_dir = STATE_ROOT / "runs" / run_id
    record = {"run_id": run_id, "kind": "reprocess", "status": "running",
              "cross_process_stop_supported": True, "data_root": str(data_root),
              "request": {"workers": workers},
              "total_sections": len(acquisitions), "created_at_utc": _utc_now(),
              "started_at_utc": _utc_now(), "updated_at_utc": _utc_now()}
    connection = _prepare_run(run_id, run_dir, record)
    on_progress = _persistent_callback(run_dir, progress_callback, workers)
    try:
        for acquisition_path in acquisitions:
            collection = json.loads(acquisition_path.read_text(encoding="utf-8"))
            turn = str(collection.get("turno") or {"3220": 1, "3221": 2}.get(str(collection.get("data_id"))) or "?")
            section_key = str(collection.get("section_key") or acquisition_path.parent.as_posix())
            connection.execute("""INSERT OR REPLACE INTO section_jobs
                (turn,data_id,section_key,section_json,raw_status,extract_status,acquisition_path)
                VALUES(?,?,?,?,?,?,?)""",
                (turn, str(collection.get("data_id", "")), section_key, json.dumps(collection, ensure_ascii=False),
                 "collected", "pending", str(acquisition_path)))
        connection.commit()
        _emit(on_progress, {"type": "run_started", "run_id": run_id,
                            "total_sections": len(acquisitions), "kind": "reprocess"})
        return _execute_reprocess(run_dir, connection, record, on_progress, stop_event)
    except Exception as error:
        error_traceback = traceback.format_exc()
        record.update({"status": "failed", "error": f"{type(error).__name__}: {error}",
                       "traceback": error_traceback,
                       "finished_at_utc": _utc_now(), "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        _emit(on_progress, {"type": "run_failed", "run_id": run_id, "error": record["error"],
                            "traceback": error_traceback})
        raise
    finally:
        connection.close()


def _execute_reprocess(run_dir: Path, connection: sqlite3.Connection, record: dict,
                       progress_callback: Callable | None, stop_event: threading.Event | None) -> dict:
    stop_event = _RunStopSignal(stop_event, run_dir / "stop.request")
    total = int(record.get("total_sections", 0))
    processed = 0
    workers = int(record.get("request", {}).get("workers", DEFAULT_BATCH_WORKERS))
    _configure_process_executable()
    process_context = multiprocessing.get_context("spawn")
    process_progress = process_context.Queue()
    worker_state_dir = _prepare_process_worker_state_dir(run_dir)
    process_pool = ProcessPoolExecutor(max_workers=workers, mp_context=process_context,
        initializer=_initialize_process_worker,
        initargs=(process_progress, worker_state_dir, record["run_id"]))
    active_jobs: dict[str, dict] = {}
    active_lock = threading.Lock()
    active_sections: set[str] = set()
    try:
        while not stop_event.is_set():
            rows = connection.execute("""SELECT turn,section_key,acquisition_path FROM section_jobs
                WHERE extract_status IN ('pending','failed') ORDER BY turn,section_key LIMIT ?""",
                (REPROCESS_BATCH_SIZE,)).fetchall()
            candidates = [row for row in rows if row["acquisition_path"] and
                Path(row["acquisition_path"]).is_file()]
            if not candidates:
                break
            targets = []
            for row in candidates:
                acquisition_path = Path(row["acquisition_path"])
                targets.append({"section_key": row["section_key"], "turn": int(row["turn"]),
                    "output_path": acquisition_path.parent / "transferencia-temporaria-elegiveis.csv"})
            _emit(progress_callback, {"type": "status", "stage": "reprocessing",
                "message": f"Preparando recortes e reprocessando lote de {len(candidates):,} aquisições",
                "run_id": record["run_id"]})
            excerpt_job = {"job_id": "referencia:recortes", "section": "referência", "turn": None,
                "status": "running", "progress": 0.0,
                "stage": "Preparando recortes de transferência", "stage_kind": "preparation"}
            with active_lock:
                active_jobs[excerpt_job["job_id"]] = excerpt_job
            _emit(progress_callback, {"type": "job_started", **excerpt_job, "run_id": record["run_id"]})
            excerpt_future = process_pool.submit(_run_process_task, {"operation": "write_excerpts",
                "targets": targets, **excerpt_job, "section_key": "referência",
                "run_id": record["run_id"], "stage_label": "Preparando recortes de transferência"})
            while not excerpt_future.done():
                _drain_process_progress(process_progress, active_jobs, active_lock, progress_callback)
                wait([excerpt_future], timeout=0.5)
            _drain_process_progress(process_progress, active_jobs, active_lock, progress_callback)
            try:
                excerpt_result = excerpt_future.result()
            except Exception as error:
                error_traceback = traceback.format_exc()
                with active_lock:
                    active_jobs.pop(excerpt_job["job_id"], None)
                _emit(progress_callback, {"type": "job_finished", **excerpt_job, "status": "failed",
                    "error": f"{type(error).__name__}: {error}", "traceback": error_traceback,
                    "run_id": record["run_id"]})
                raise
            excerpt_metadata = excerpt_result.get("excerpt_metadata", {})
            with active_lock:
                active_jobs.pop(excerpt_job["job_id"], None)
            _emit(progress_callback, {"type": "job_finished", **excerpt_job, "status": "complete",
                "progress": 100.0, "process_id": excerpt_result.get("process_id"),
                "run_id": record["run_id"]})
            pending = deque(candidates)
            futures = {}
            active_sections.clear()

            def queue_snapshot() -> None:
                with active_lock:
                    active_snapshot = [active_jobs[info["job_id"]] for future, (_row, _lock_key, info)
                        in futures.items() if not future.done() and info["job_id"] in active_jobs]
                    waiting = len(pending)
                    waiting_preview = [{"section": row["section_key"], "turn": row["turn"],
                        "status": "queued", "stage": "Aguardando reprocessamento"}
                        for row in list(pending)[:25]]
                _emit(progress_callback, {"type": "job_queue", "worker_limit": workers,
                    "active_jobs": active_snapshot, "waiting_count": waiting,
                    "waiting_jobs": waiting_preview, "run_id": record["run_id"]})

            while pending or futures:
                while pending and len(futures) < workers and not stop_event.is_set():
                    scheduled = False
                    for _ in range(len(pending)):
                        row = pending.popleft()
                        lock_key = str(row["section_key"]).casefold()
                        if lock_key in active_sections:
                            pending.append(row)
                            continue
                        active_sections.add(lock_key)
                        acquisition_path = Path(row["acquisition_path"])
                        job_id = f"{row['section_key']}:turno-{row['turn']}"
                        info = {"job_id": job_id, "section": row["section_key"],
                            "turn": int(row["turn"]), "stage": "Reprocessamento determinístico",
                            "stage_kind": "reprocess", "status": "running", "progress": 0.0}
                        with active_lock:
                            active_jobs[job_id] = info
                        _emit(progress_callback, {"type": "job_started", **info,
                            "run_id": record["run_id"]})
                        excerpt_path = str(acquisition_path.parent / "transferencia-temporaria-elegiveis.csv")
                        process_task = {"operation": "reprocess_one", "acquisition_path": str(acquisition_path),
                            "section_key": row["section_key"], "turn": str(row["turn"]),
                            "excerpt_metadata": {excerpt_path: excerpt_metadata.get(excerpt_path)},
                            "job_id": job_id, "run_id": record["run_id"],
                            "stage_label": "Reprocessamento determinístico"}
                        futures[process_pool.submit(_run_process_task, process_task)] = (row, lock_key, info)
                        scheduled = True
                        break
                    if not scheduled:
                        break
                queue_snapshot()
                if not futures:
                    break
                done, _ = wait(futures, timeout=0.5, return_when=FIRST_COMPLETED)
                _drain_process_progress(process_progress, active_jobs, active_lock, progress_callback)
                for future in done:
                    row, lock_key, info = futures.pop(future)
                    active_sections.discard(lock_key)
                    result = future.result()
                    status, error = result.get("status", "failed"), result.get("error")
                    connection.execute("UPDATE section_jobs SET extract_status=?,error=? WHERE turn=? AND section_key=?",
                        (status, error, row["turn"], row["section_key"]))
                    connection.commit()
                    processed += 1
                    with active_lock:
                        active_jobs.pop(info["job_id"], None)
                    _emit(progress_callback, {"type": "reprocessing_complete", "stage": "reprocessing",
                        "turn": row["turn"], "section": row["section_key"], "status": status,
                        "processed": processed, "total": total, "run_id": record["run_id"]})
                    _emit(progress_callback, {"type": "job_finished", **info, "status": status,
                        "progress": 100.0, "process_id": result.get("process_id"),
                        "run_id": record["run_id"]})
            if stop_event.is_set():
                break
    finally:
        process_pool.shutdown(wait=True, cancel_futures=False)
        process_progress.close()
    counts = _run_counts(connection)
    status = "paused" if stop_event and stop_event.is_set() else (
        "partial" if counts["pending"] or counts["failed"] or counts["extraction_failed"] else "complete")
    record.update({"status": status, "counts": counts, "finished_at_utc": _utc_now(), "updated_at_utc": _utc_now()})
    _write_json(run_dir / "run.json", record)
    result = {"run_id": record["run_id"], "status": status, "counts": counts, "run_dir": str(run_dir)}
    _emit(progress_callback, {"type": "run_finished", **result})
    return result


def list_runs() -> list[dict]:
    result = []
    for path in sorted((STATE_ROOT / "runs").glob("*/run.json"), reverse=True):
        try:
            result.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            continue
    return result


def reconcile_interrupted_runs() -> list[str]:
    """Mark persisted active runs as paused when no process owns the local job lock."""
    recovered = []
    try:
        with _job_lock():
            for run in list_runs():
                if run.get("status") not in {"preparing", "running"}:
                    continue
                # Older runs did not advertise cross-process locking, so their liveness
                # cannot safely be inferred from the current lock file.
                if run.get("cross_process_stop_supported") is not True:
                    continue
                run_id = str(run.get("run_id") or "")
                if not run_id or Path(run_id).name != run_id or "\\" in run_id or "/" in run_id:
                    continue
                run_dir = STATE_ROOT / "runs" / run_id
                run_path = run_dir / "run.json"
                if not run_path.is_file():
                    continue
                current = json.loads(run_path.read_text(encoding="utf-8"))
                if current.get("status") not in {"preparing", "running"}:
                    continue
                now = _utc_now()
                message = "Processo do job encerrado sem registrar estado final; execução disponível para retomada."
                current.update({"status": "paused", "error": message,
                                "finished_at_utc": now, "updated_at_utc": now,
                                "recovered_as_interrupted": True})
                _write_json(run_path, current)
                event_path = run_dir / "events.jsonl"
                with event_path.open("a", encoding="utf-8") as output:
                    output.write(json.dumps({"recorded_at_utc": now, "type": "run_interrupted",
                        "run_id": run_id, "status": "paused", "message": message}, ensure_ascii=False) + "\n")
                recovered.append(run_id)
    except RuntimeError as error:
        if str(error) != "another local job is already running":
            raise
    return recovered


_CLI_ACTIVE_JOBS: dict[str, dict] = {}
_CLI_WAITING_JOBS: list[dict] = []
_CLI_WAITING_COUNT = 0
_CLI_WORKER_LIMIT = 15
_CLI_LAST_JOB_SNAPSHOT = 0.0
_CLI_EVENT_LOCK = threading.Lock()


def launch_worker_process(operation: str, payload: dict) -> int:
    """Start a detached job worker; run state and events remain in STATE_ROOT."""
    if operation not in {"collect", "batch-process", "reprocess", "resume"}:
        raise ValueError(f"operação de worker inválida: {operation}")
    requests_root = STATE_ROOT / "worker-requests"
    requests_root.mkdir(parents=True, exist_ok=True)
    request_path = requests_root / f"{uuid.uuid4().hex}.json"
    _write_json(request_path, {"operation": operation, "payload": payload})
    command = [sys.executable, str(Path(__file__).resolve()), "_worker", "--request-file", str(request_path)]
    options = {"stdin": subprocess.DEVNULL, "stdout": subprocess.DEVNULL,
               "stderr": subprocess.DEVNULL, "close_fds": True}
    if os.name == "nt":
        options["creationflags"] = (getattr(subprocess, "DETACHED_PROCESS", 0x00000008)
                                    | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200))
    else:
        options["start_new_session"] = True
    try:
        process = subprocess.Popen(command, **options)
    except Exception:
        request_path.unlink(missing_ok=True)
        raise
    return int(process.pid)


def run_worker_request(request_file: str | Path) -> int:
    request_path = Path(request_file).resolve()
    allowed_root = (STATE_ROOT / "worker-requests").resolve()
    if allowed_root not in request_path.parents or request_path.suffix.lower() != ".json":
        raise ValueError("arquivo de comando fora da área de controle local")
    request = json.loads(request_path.read_text(encoding="utf-8"))
    request_path.unlink(missing_ok=True)
    operation, payload = request.get("operation"), request.get("payload") or {}
    try:
        if operation == "collect":
            result = start_collection(turns=payload["turns"], uf=payload["uf"], municipio=payload["municipio"],
                zona=payload["zona"], secao=payload["secao"], offset=payload["offset"], limit=payload["limit"],
                workers=payload["workers"], incremental=payload["incremental"], preview_dir=payload["preview_dir"])
        elif operation == "batch-process":
            result = start_batch_processing(turns=payload["turns"], uf=payload["uf"], municipio=payload["municipio"],
                zona=payload["zona"], secao=payload["secao"], offset=payload["offset"], limit=payload["limit"],
                sections_per_batch=payload["sections_per_batch"], workers=payload["workers"],
                preview_dir=payload["preview_dir"],
                confirm_processing=bool(payload.get("confirm_processing", payload.get("confirm_clean_data", False))))
        elif operation == "reprocess":
            result = start_reprocessing(data_root=payload.get("data_root", DATA_ROOT),
                workers=int(payload.get("workers", DEFAULT_BATCH_WORKERS)))
        elif operation == "resume":
            result = resume_run(payload["run_id"])
        else:
            raise ValueError(f"operação de worker inválida: {operation}")
    except Exception as error:
        error_path = STATE_ROOT / "worker-errors.jsonl"
        error_path.parent.mkdir(parents=True, exist_ok=True)
        with error_path.open("a", encoding="utf-8") as output:
            output.write(json.dumps({"type": "worker_error", "operation": operation,
                "error": f"{type(error).__name__}: {error}", "traceback": traceback.format_exc(),
                "recorded_at_utc": _utc_now()},
                ensure_ascii=False) + "\n")
        raise
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return 0 if result.get("status") == "complete" else 1


def _cli_event(event: dict) -> None:
    global _CLI_WAITING_COUNT, _CLI_WAITING_JOBS, _CLI_WORKER_LIMIT, _CLI_LAST_JOB_SNAPSHOT
    with _CLI_EVENT_LOCK:
        event_type = event.get("type")
        if event_type in {"job_queue", "job_started", "job_progress", "job_finished"}:
            if event_type == "job_queue":
                _CLI_WAITING_COUNT = int(event.get("waiting_count") or 0)
                _CLI_WAITING_JOBS = list(event.get("waiting_jobs") or [])
                _CLI_WORKER_LIMIT = int(event.get("worker_limit") or _CLI_WORKER_LIMIT)
                _CLI_ACTIVE_JOBS.clear()
                _CLI_ACTIVE_JOBS.update({item["job_id"]: item for item in event.get("active_jobs", [])
                                         if item.get("job_id")})
            elif event_type == "job_started" and event.get("job_id"):
                _CLI_ACTIVE_JOBS[event["job_id"]] = dict(event)
            elif event_type == "job_progress" and event.get("job_id"):
                if event["job_id"] in _CLI_ACTIVE_JOBS:
                    _CLI_ACTIVE_JOBS[event["job_id"]] = {**_CLI_ACTIVE_JOBS[event["job_id"]], **event}
            elif event_type == "job_finished":
                _CLI_ACTIVE_JOBS.pop(event.get("job_id"), None)
            now = time.monotonic()
            if now - _CLI_LAST_JOB_SNAPSHOT >= 1.0 or event_type == "job_finished":
                snapshot = {"type": "job_snapshot", "worker_limit": _CLI_WORKER_LIMIT,
                            "active_jobs": list(_CLI_ACTIVE_JOBS.values()),
                            "waiting_count": _CLI_WAITING_COUNT, "waiting_jobs": _CLI_WAITING_JOBS}
                print(json.dumps(snapshot, ensure_ascii=False), flush=True)
                _CLI_LAST_JOB_SNAPSHOT = now
            return
        print(json.dumps(event, ensure_ascii=False), flush=True)


def _watch_run(run_id: str | None) -> int:
    runs = list_runs()
    run = next((item for item in runs if item.get("run_id") == run_id), None) if run_id else next(
        (item for item in runs if item.get("status") in {"preparing", "running"}), None)
    if not run:
        raise ValueError("não há execução ativa; informe --run-id para acompanhar uma execução encerrada")
    run_id = str(run["run_id"])
    run_dir = STATE_ROOT / "runs" / run_id
    if run_dir.resolve().parent != (STATE_ROOT / "runs").resolve():
        raise ValueError("identificador de execução inválido")
    event_path = run_dir / "events.jsonl"
    tail = deque(maxlen=50)
    if event_path.is_file():
        with event_path.open("rb") as source:
            source.seek(0, os.SEEK_END)
            position = source.tell()
            buffer = b""
            while position > 0 and buffer.count(b"\n") <= 50:
                size = min(8192, position)
                position -= size
                source.seek(position)
                buffer = source.read(size) + buffer
        for line in buffer.splitlines()[-50:]:
            try:
                tail.append(json.loads(line.decode("utf-8")))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
    for event in tail:
        print(json.dumps(event, ensure_ascii=False), flush=True)
    offset = event_path.stat().st_size if event_path.exists() else 0
    worker_state_path = run_dir / "worker-state.json"
    worker_state_stamp = None
    while True:
        if event_path.is_file():
            with event_path.open("rb") as source:
                source.seek(offset)
                data = source.read()
                offset = source.tell()
            for line in data.splitlines():
                try:
                    print(json.dumps(json.loads(line.decode("utf-8")), ensure_ascii=False), flush=True)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
        if worker_state_path.is_file():
            stat = worker_state_path.stat()
            stamp = (stat.st_mtime_ns, stat.st_size)
            if stamp != worker_state_stamp:
                try:
                    worker_state = json.loads(worker_state_path.read_text(encoding="utf-8"))
                    print(json.dumps({"type": "job_snapshot", **worker_state}, ensure_ascii=False), flush=True)
                    worker_state_stamp = stamp
                except (OSError, json.JSONDecodeError):
                    pass
        try:
            current = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            current = {}
        if current.get("status") not in {"preparing", "running"}:
            return 0 if current.get("status") == "complete" else 1
        time.sleep(0.5)


def _start_cli_worker(operation: str, payload: dict) -> int:
    active = next((run for run in list_runs() if run.get("status") in {"preparing", "running"}), None)
    if active:
        raise RuntimeError(f"a execução {active.get('run_id')} já está ativa; use status, watch ou stop")
    previous_run_ids = {run.get("run_id") for run in list_runs()}
    previous_resume = next((run for run in list_runs() if run.get("run_id") == payload.get("run_id")), None)
    worker_errors_path = STATE_ROOT / "worker-errors.jsonl"
    error_offset = worker_errors_path.stat().st_size if worker_errors_path.exists() else 0
    pid = launch_worker_process(operation, payload)
    run_id = payload.get("run_id") if operation == "resume" else None
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        current_runs = list_runs()
        if operation == "resume":
            resumed = next((run for run in current_runs if run.get("run_id") == run_id), None)
            if resumed:
                if resumed.get("status") in {"preparing", "running"}:
                    break
                if resumed.get("updated_at_utc") != (previous_resume or {}).get("updated_at_utc"):
                    break
        else:
            new_run = next((run for run in current_runs if run.get("run_id") not in previous_run_ids), None)
            if new_run:
                run_id = str(new_run.get("run_id"))
                break
        if worker_errors_path.is_file():
            with worker_errors_path.open("rb") as source:
                source.seek(error_offset)
                error_data = source.read()
                error_offset = source.tell()
            if error_data:
                print(error_data.decode("utf-8", errors="replace"), file=sys.stderr, flush=True)
                return 1
        time.sleep(0.2)
    if run_id is None or time.monotonic() >= deadline:
        raise RuntimeError(f"worker {pid} não registrou uma execução em 30 segundos")
    print(json.dumps({"type": "worker_process_started", "pid": pid, "run_id": run_id}, ensure_ascii=False),
          flush=True)
    return _watch_run(str(run_id))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    worker = commands.add_parser("_worker", help="internal detached worker process")
    worker.add_argument("--request-file", required=True, type=Path)
    collect = commands.add_parser("collect", help="discover and collect locally")
    collect.add_argument("--turn", choices=("1", "2", "ambos"), default="2")
    collect.add_argument("--uf", default="")
    collect.add_argument("--municipio", default="")
    collect.add_argument("--zona", default="")
    collect.add_argument("--secao", default="")
    collect.add_argument("--offset", type=int, default=0)
    collect.add_argument("--max-secoes", type=int)
    collect.add_argument("--workers", type=int, default=COLLECTION_WORKERS,
                         help="quantidade de processos worker simultâneos (padrão: 15; no Windows, máximo 61)")
    collect.add_argument("--refresh", action="store_true", help="force raw downloads and replace matching files")
    reprocess = commands.add_parser("reprocess", help="rebuild derived outputs from local raw acquisitions only")
    reprocess.add_argument("--data-root", type=Path, default=DATA_ROOT)
    reprocess.add_argument("--workers", type=int, default=DEFAULT_BATCH_WORKERS,
                           help="quantidade de processos worker simultâneos")
    batch = commands.add_parser("batch-process", help="collect, summarize and clean bounded section/turn batches")
    batch.add_argument("--turn", choices=("1", "2", "ambos"), default="ambos")
    batch.add_argument("--uf", default="")
    batch.add_argument("--municipio", default="")
    batch.add_argument("--zona", default="")
    batch.add_argument("--secao", default="")
    batch.add_argument("--offset", type=int, default=0)
    batch.add_argument("--max-secoes", type=int)
    batch.add_argument("--secoes-por-remessa", type=int, default=DEFAULT_BATCH_SECTIONS)
    batch.add_argument("--workers", type=int, default=DEFAULT_BATCH_WORKERS)
    batch.add_argument("--confirmar-processamento", "--confirmar-limpeza",
                       dest="confirmar_processamento", action="store_true",
                       help="confirmar o início do processamento por remessas")
    resume = commands.add_parser("resume", help="resume a paused or incomplete local job")
    resume.add_argument("--run-id", required=True)
    commands.add_parser("list-runs", help="list recent local jobs")
    commands.add_parser("status", help="show the active local job, if any")
    commands.add_parser("stop", help="request a safe stop for the active local job")
    watch = commands.add_parser("watch", help="follow activity from the active or selected local job")
    watch.add_argument("--run-id")
    args = parser.parse_args()
    try:
        if args.command == "_worker":
            return run_worker_request(args.request_file)
        if args.command == "status":
            reconcile_interrupted_runs()
            active = next((run for run in list_runs() if run.get("status") in {"preparing", "running"}), None)
            print(json.dumps({"active_run": active, "recent_runs": list_runs()[:20]}, ensure_ascii=False, indent=2))
            return 0
        if args.command == "stop":
            run_id = request_active_run_stop()
            print(json.dumps({"run_id": run_id, "status": "stop_requested"}, ensure_ascii=False))
            return 0
        if args.command == "watch":
            return _watch_run(args.run_id)
        if args.command == "collect":
            turns = ["1", "2"] if args.turn == "ambos" else [args.turn]
            return _start_cli_worker("collect", {"turns": turns, "uf": args.uf,
                "municipio": args.municipio, "zona": args.zona, "secao": args.secao,
                "offset": args.offset, "limit": args.max_secoes, "workers": args.workers,
                "incremental": not args.refresh, "preview_dir": None})
        elif args.command == "reprocess":
            return _start_cli_worker("reprocess", {"data_root": str(args.data_root), "workers": args.workers})
        elif args.command == "batch-process":
            turns = ["1", "2"] if args.turn == "ambos" else [args.turn]
            return _start_cli_worker("batch-process", {"turns": turns, "uf": args.uf,
                "municipio": args.municipio, "zona": args.zona, "secao": args.secao,
                "offset": args.offset, "limit": args.max_secoes,
                "sections_per_batch": args.secoes_por_remessa, "workers": args.workers,
                "confirm_processing": args.confirmar_processamento, "preview_dir": None})
        elif args.command == "resume":
            return _start_cli_worker("resume", {"run_id": args.run_id})
        else:
            print(json.dumps(list_runs(), ensure_ascii=False, indent=2))
            return 0
    except Exception as error:
        print(json.dumps({"type": "error", "error": f"{type(error).__name__}: {error}"}, ensure_ascii=False),
              file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("status") == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
