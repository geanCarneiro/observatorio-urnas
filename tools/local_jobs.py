#!/usr/bin/env python3
"""Run, resume, and inspect local collection and reprocessing jobs."""

from __future__ import annotations

import argparse
import functools
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import threading
import time
import uuid
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

TOOLS_ROOT = Path(__file__).resolve().parent
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

from collect_sections import DEFAULT_WORKERS as COLLECTION_WORKERS, collect_sections
from discover_sections import DEFAULT_CATALOG_WORKERS, discover
from collect_section import save_atomic
from extract_section_data import process_acquisitions
from temporary_transfer import reference_identity, write_reconciliation, write_section_excerpts, write_validation
from plan_collection import build_plan
from batch_consolidation import (PROXY_RULE_VERSION,
    build_summary_row, clean_data_root, preserve_section_evidence, write_batch_csv,
    write_batch_manifest)


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
CONSOLIDATION_ROOT = ROOT / "consolidados"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]


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
        acquisition_path TEXT,
        error TEXT,
        batch_index INTEGER,
        PRIMARY KEY (turn, section_key)
    )""")
    columns = {row[1] for row in connection.execute("PRAGMA table_info(section_jobs)")}
    if "batch_index" not in columns:
        connection.execute("ALTER TABLE section_jobs ADD COLUMN batch_index INTEGER")
    connection.execute("""CREATE TABLE IF NOT EXISTS section_summaries (
        turn TEXT NOT NULL,
        section_key TEXT NOT NULL,
        batch_index INTEGER NOT NULL,
        row_json TEXT NOT NULL,
        evidence_required INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (turn, section_key)
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
    connection.commit()
    return connection


def _emit(callback: Callable | None, event: dict) -> None:
    if callback:
        callback(event)


def _persistent_callback(run_dir: Path, callback: Callable | None) -> Callable:
    event_path = run_dir / "events.jsonl"
    event_lock = threading.Lock()
    transient_job_events = {"job_queue", "job_started", "job_progress", "job_finished"}

    def record(event: dict) -> None:
        if event.get("type") not in transient_job_events:
            with event_lock:
                with event_path.open("a", encoding="utf-8") as output:
                    output.write(json.dumps({"recorded_at_utc": _utc_now(), **event}, ensure_ascii=False) + "\n")
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
    if workers < 1:
        raise ValueError("workers must be at least 1")
    request = _request(selected_turns, uf, municipio, zona, secao, offset, limit, incremental)
    request["workers"] = workers
    run_id = _new_id()
    run_dir = STATE_ROOT / "runs" / run_id
    record = {"run_id": run_id, "kind": "collect", "status": "preparing",
              "cross_process_stop_supported": True, "request": request,
              "data_root": str(DATA_ROOT), "created_at_utc": _utc_now(), "updated_at_utc": _utc_now()}
    connection = _prepare_run(run_id, run_dir, record)
    on_progress = _persistent_callback(run_dir, progress_callback)
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
        record.update({"status": "failed", "error": f"{type(error).__name__}: {error}",
                       "finished_at_utc": _utc_now(), "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        _emit(on_progress, {"type": "run_failed", "run_id": run_id, "error": record["error"]})
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
    worker_pool = ThreadPoolExecutor(max_workers=worker_limit, thread_name_prefix="urna-section")

    def queue_snapshot() -> None:
        with active_lock:
            active_snapshot = list(active_jobs.values())
            waiting = max(0, total_waiting - completed_sections - len(active_snapshot))
            preview = [dict(item[3], status="queued", progress=0.0, stage="Aguardando na fila")
                       for item in list(pending)[:25]]
        _emit(progress_callback, {"type": "job_queue", "worker_limit": worker_limit,
            "active_jobs": active_snapshot, "waiting_count": waiting, "waiting_jobs": preview,
            "run_id": record["run_id"]})

    def update_worker(job_id: str, update: dict) -> None:
        with active_lock:
            current = active_jobs.get(job_id, {})
            active_jobs[job_id] = {**current, **update}
        queue_snapshot()
        _emit(progress_callback, {"type": "job_progress", **active_jobs.get(job_id, {}), **update,
                                 "run_id": record["run_id"]})

    def run_section(turn: str, key: str, section: dict, info: dict) -> dict:
        def on_job_progress(event: dict) -> None:
            event_type = event.get("type")
            if event_type == "job_progress":
                progress = float(event.get("progress") or 0)
                stage = event.get("stage") or "Coletando arquivos"
                update_worker(info["job_id"], {"progress": progress * 0.5, "stage": f"Coleta: {stage}",
                    "file": event.get("file"), "status": "running"})

        if stop_event and stop_event.is_set():
            return {"turn": turn, "section": key, "status": "cancelled"}
        try:
            collected = []
            batch_result = collect_sections([section], DATA_ROOT, data_id=TURN_CONFIG[turn], offset=0, limit=1,
                incremental=record["request"]["incremental"], staging_root=run_dir / "staging",
                progress_callback=collected.append, stop_event=stop_event,
                job_progress_callback=on_job_progress, max_workers=1)
            collection = next((item for item in reversed(collected) if item.get("status") != "running"), {})
            raw_status = collection.get("status", "failed")
            acquisition_path = collection.get("acquisition")
            extraction = None
            if raw_status in {"collected", "partial"} and acquisition_path and Path(acquisition_path).is_file():
                update_worker(info["job_id"], {"progress": 50.0, "stage": "Extração: iniciando análise da seção",
                                                "file": None, "status": "running"})
                extraction_results = []
                def on_extraction_job(event: dict) -> None:
                    if event.get("type") == "job_progress":
                        progress = float(event.get("progress") or 0)
                        update_worker(info["job_id"], {"progress": 50.0 + progress * 0.5,
                            "stage": f"Extração: {event.get('stage') or 'Processando'}",
                            "file": event.get("file"), "status": "running"})
                process_acquisitions([Path(acquisition_path)], BU_SCHEMA, RDV_SCHEMA,
                    extraction_results.append, None, job_progress_callback=on_extraction_job,
                    max_workers=1, generate_excerpts=False)
                extraction = extraction_results[-1] if extraction_results else {
                    "status": "failed", "error": "extração interrompida"}
            return {"turn": turn, "section": key, "raw_status": raw_status,
                    "acquisition": acquisition_path, "reason": collection.get("reason"),
                    "network_blocked": batch_result.get("network_blocked", False),
                    "extract_status": extraction.get("status") if extraction else (
                        "not_needed" if raw_status in {"skipped_existing", "aggregated"} else "pending"),
                    "extract_error": extraction.get("error") if extraction else None,
                    "result": collection}
        except Exception as error:
            return {"turn": turn, "section": key, "raw_status": "failed", "extract_status": "pending",
                    "reason": f"{type(error).__name__}: {error}", "network_blocked": False}

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
                    futures[worker_pool.submit(run_section, turn, key, section, info)] = (turn, key, section_lock, info)
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
            done, _remaining = wait(futures, return_when=FIRST_COMPLETED)
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
                    "progress": 100.0, "run_id": record["run_id"]})
                if result.get("network_blocked"):
                    network_blocked = True
                    message = ("Coleta interrompida: o Windows ou o ambiente bloqueou o acesso à rede "
                               "(WinError 10013). As seções restantes ficaram pendentes.")
                    _emit(progress_callback, {"type": "worker_error", "error": message, "run_id": record["run_id"]})
            queue_snapshot()
    finally:
        worker_pool.shutdown(wait=True, cancel_futures=bool(stop_event and stop_event.is_set()))

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


@_serialized_job
def start_batch_processing(*, turns: list[str] | str, uf: str = "", municipio: str = "", zona: str = "",
                          secao: str = "", offset: int = 0, limit: int | None = None,
                          sections_per_batch: int = DEFAULT_BATCH_SECTIONS,
                          workers: int = DEFAULT_BATCH_WORKERS, preview_dir: str | Path | None = None,
                          confirm_clean_data: bool = False, progress_callback: Callable | None = None,
                          stop_event: threading.Event | None = None) -> dict:
    """Collect and deterministically consolidate bounded batches, clearing raw data between them."""
    if not confirm_clean_data:
        raise ValueError("confirme a limpeza inicial de dados/ para iniciar o processamento por remessas")
    selected_turns = _parse_turns(turns)
    if offset < 0 or (limit is not None and limit < 1):
        raise ValueError("offset must be nonnegative and limit must be positive")
    if not 1 <= sections_per_batch <= 10000:
        raise ValueError("sections_per_batch must be between 1 and 10000")
    if workers < 1:
        raise ValueError("workers must be at least 1")
    request = _request(selected_turns, uf, municipio, zona, secao, offset, limit, False)
    request.update({"sections_per_batch": sections_per_batch, "workers": workers,
                    "confirm_clean_data": True, "proxy_rule_version": PROXY_RULE_VERSION})
    run_id = _new_id()
    run_dir = STATE_ROOT / "runs" / run_id
    output_dir = CONSOLIDATION_ROOT / run_id
    record = {"run_id": run_id, "kind": "batch_process", "status": "preparing",
              "cross_process_stop_supported": True, "request": request,
              "data_root": str(DATA_ROOT), "consolidation_root": str(output_dir),
              "initial_cleanup_done": False, "created_at_utc": _utc_now(), "updated_at_utc": _utc_now()}
    connection = _prepare_run(run_id, run_dir, record)
    on_progress = _persistent_callback(run_dir, progress_callback)
    try:
        if preview_dir:
            inventories = _copy_preview(Path(preview_dir), run_dir, request)
        else:
            inventories = _discover_for_run(request, run_dir, on_progress)
        plan = build_plan(inventories, batch_size=sections_per_batch, jobs_per_worker=1)
        if not plan.get("total_sections"):
            raise ValueError("nenhuma seção selecionada; dados/ não foi limpo")
        _write_json(run_dir / "remessas-plan.json", plan)
        for batch_index, batch in enumerate(plan.get("batches", [])):
            turn = str(batch["turno"])
            batch_id = f"remessa-{batch_index + 1:05d}"
            connection.execute("INSERT INTO processing_batches(batch_index,batch_id,status) VALUES(?,?, 'pending')",
                               (batch_index, batch_id))
            sections = inventories[turn].get("sections", [])[batch["offset"]:batch["offset"] + batch["limit"]]
            for section in sections:
                key = "/".join(section[name] for name in ("uf", "municipio", "zona", "secao"))
                connection.execute("""INSERT OR IGNORE INTO section_jobs
                    (turn,data_id,section_key,section_json,batch_index)
                    VALUES(?,?,?,?,?)""",
                    (turn, TURN_CONFIG[turn], key, json.dumps(section, ensure_ascii=False), batch_index))
        connection.commit()
        record.update({"status": "running", "total_sections": plan["total_sections"],
                       "total_batches": plan["total_batches"], "started_at_utc": _utc_now(),
                       "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        _emit(on_progress, {"type": "run_started", "run_id": run_id,
                            "total_sections": plan["total_sections"], "total_batches": plan["total_batches"],
                            "kind": "batch_process"})
        return _execute_batch_processing(run_dir, connection, record, on_progress, stop_event)
    except Exception as error:
        record.update({"status": "failed", "error": f"{type(error).__name__}: {error}",
                       "finished_at_utc": _utc_now(), "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        _emit(on_progress, {"type": "run_failed", "run_id": run_id, "error": record["error"]})
        raise
    finally:
        connection.close()


def _execute_batch_processing(run_dir: Path, connection: sqlite3.Connection, record: dict,
                              progress_callback: Callable | None,
                              stop_event: threading.Event | None) -> dict:
    stop_event = _RunStopSignal(stop_event, run_dir / "stop.request")
    plan_path = run_dir / "remessas-plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
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
            connection.execute("""UPDATE section_jobs SET raw_status='aggregated',extract_status='not_needed',
                acquisition_path=NULL,error=NULL WHERE turn=? AND section_key=?""", (turn, item["key"]))
            connection.execute("""INSERT OR REPLACE INTO section_summaries
                (turn,section_key,batch_index,row_json,evidence_required) VALUES(?,?,?,?,0)""",
                (turn, item["key"], batch_index, json.dumps(row, ensure_ascii=False, separators=(",", ":"))))
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
                        connection.execute("""INSERT OR REPLACE INTO section_summaries
                            (turn,section_key,batch_index,row_json,evidence_required) VALUES(?,?,?,?,?)""",
                            (item["turn"], item["key"], batch_index,
                             json.dumps(result["row"], ensure_ascii=False, separators=(",", ":")),
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
    on_progress = _persistent_callback(run_dir, progress_callback)
    try:
        (run_dir / "stop.request").unlink(missing_ok=True)
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
            return _execute_batch_processing(run_dir, connection, record, on_progress, stop_event)
        return _execute_reprocess(run_dir, connection, record, on_progress, stop_event)
    except Exception as error:
        record.update({"status": "failed", "error": f"{type(error).__name__}: {error}",
                       "finished_at_utc": _utc_now(), "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        _emit(on_progress, {"type": "run_failed", "run_id": run_id, "error": record["error"]})
        raise
    finally:
        connection.close()


@_serialized_job
def start_reprocessing(*, data_root: str | Path = DATA_ROOT, progress_callback: Callable | None = None,
                       stop_event: threading.Event | None = None) -> dict:
    data_root = Path(data_root).resolve()
    acquisitions = sorted(data_root.rglob("acquisition.json"))
    if not acquisitions:
        raise ValueError(f"no acquisition.json files found under {data_root}")
    run_id = _new_id()
    run_dir = STATE_ROOT / "runs" / run_id
    record = {"run_id": run_id, "kind": "reprocess", "status": "running",
              "cross_process_stop_supported": True, "data_root": str(data_root),
              "total_sections": len(acquisitions), "created_at_utc": _utc_now(),
              "started_at_utc": _utc_now(), "updated_at_utc": _utc_now()}
    connection = _prepare_run(run_id, run_dir, record)
    on_progress = _persistent_callback(run_dir, progress_callback)
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
        record.update({"status": "failed", "error": f"{type(error).__name__}: {error}",
                       "finished_at_utc": _utc_now(), "updated_at_utc": _utc_now()})
        _write_json(run_dir / "run.json", record)
        _emit(on_progress, {"type": "run_failed", "run_id": run_id, "error": record["error"]})
        raise
    finally:
        connection.close()


def _execute_reprocess(run_dir: Path, connection: sqlite3.Connection, record: dict,
                       progress_callback: Callable | None, stop_event: threading.Event | None) -> dict:
    stop_event = _RunStopSignal(stop_event, run_dir / "stop.request")
    total = int(record.get("total_sections", 0))
    processed = 0
    while True:
        if stop_event and stop_event.is_set():
            break
        rows = connection.execute("""SELECT turn,section_key,acquisition_path FROM section_jobs
            WHERE extract_status IN ('pending','failed') ORDER BY turn,section_key LIMIT ?""",
            (REPROCESS_BATCH_SIZE,)).fetchall()
        paths = [Path(row["acquisition_path"]) for row in rows if row["acquisition_path"] and Path(row["acquisition_path"]).is_file()]
        if not paths:
            break
        path_to_key = {str(Path(row["acquisition_path"])): (row["turn"], row["section_key"]) for row in rows}
        _emit(progress_callback, {"type": "status", "stage": "reprocessing",
                                 "message": f"Reprocessando lote de {len(paths)} aquisições"})

        def on_extract(result: dict) -> None:
            nonlocal processed
            key_info = path_to_key.get(result.get("acquisition"))
            if not key_info:
                return
            turn, key = key_info
            status, error = result.get("status", "failed"), result.get("error")
            connection.execute("UPDATE section_jobs SET extract_status=?,error=? WHERE turn=? AND section_key=?",
                               (status, error, turn, key))
            connection.commit()
            processed += 1
            _emit(progress_callback, {"type": "reprocessing_complete", "stage": "reprocessing", "turn": turn,
                                     "section": key, "status": status, "processed": processed,
                                     "total": total, "run_id": record["run_id"]})

        def on_job_progress(event: dict) -> None:
            _emit(progress_callback, {**event, "stage": event.get("stage", "reprocessamento"),
                                      "run_id": record["run_id"]})

        process_acquisitions(paths, BU_SCHEMA, RDV_SCHEMA, on_extract, stop_event,
                             job_progress_callback=on_job_progress)
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
                _CLI_ACTIVE_JOBS[event["job_id"]] = {**_CLI_ACTIVE_JOBS.get(event["job_id"], {}), **event}
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    collect = commands.add_parser("collect", help="discover and collect locally")
    collect.add_argument("--turn", choices=("1", "2", "ambos"), default="2")
    collect.add_argument("--uf", default="")
    collect.add_argument("--municipio", default="")
    collect.add_argument("--zona", default="")
    collect.add_argument("--secao", default="")
    collect.add_argument("--offset", type=int, default=0)
    collect.add_argument("--max-secoes", type=int)
    collect.add_argument("--workers", type=int, default=COLLECTION_WORKERS,
                         help="quantidade de workers simultâneos (padrão: 15; sem teto fixo)")
    collect.add_argument("--refresh", action="store_true", help="force raw downloads and replace matching files")
    reprocess = commands.add_parser("reprocess", help="rebuild derived outputs from local raw acquisitions only")
    reprocess.add_argument("--data-root", type=Path, default=DATA_ROOT)
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
    batch.add_argument("--confirmar-limpeza", action="store_true",
                       help="confirm that all contents of dados/ are deleted before the first batch")
    resume = commands.add_parser("resume", help="resume a paused or incomplete local job")
    resume.add_argument("--run-id", required=True)
    commands.add_parser("list-runs", help="list recent local jobs")
    args = parser.parse_args()
    try:
        if args.command == "collect":
            turns = ["1", "2"] if args.turn == "ambos" else [args.turn]
            result = start_collection(turns=turns, uf=args.uf, municipio=args.municipio, zona=args.zona,
                                      secao=args.secao, offset=args.offset, limit=args.max_secoes,
                                      workers=args.workers,
                                      incremental=not args.refresh, progress_callback=_cli_event)
        elif args.command == "reprocess":
            result = start_reprocessing(data_root=args.data_root, progress_callback=_cli_event)
        elif args.command == "batch-process":
            turns = ["1", "2"] if args.turn == "ambos" else [args.turn]
            result = start_batch_processing(turns=turns, uf=args.uf, municipio=args.municipio,
                zona=args.zona, secao=args.secao, offset=args.offset, limit=args.max_secoes,
                sections_per_batch=args.secoes_por_remessa, workers=args.workers,
                confirm_clean_data=args.confirmar_limpeza, progress_callback=_cli_event)
        elif args.command == "resume":
            result = resume_run(args.run_id, progress_callback=_cli_event)
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
