#!/usr/bin/env python3
"""Write one structured JSON extraction beside each acquired source file."""

from __future__ import annotations

import argparse
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Callable

import sys

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from analyze_section import analyze_collection
from election_turns import turn_for_pleito
from temporary_transfer import reference_identity, write_reconciliation, write_section_excerpts, write_validation

DEFAULT_WORKERS = 15
QUEUE_PREVIEW_SIZE = 25


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".extract-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            json.dump(payload, output, ensure_ascii=False, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _source_metadata(collection: dict, kind: str, name: str) -> dict:
    return next((item for item in collection.get("files", [])
                 if item.get("type") == kind and item.get("name") == name), {})


def _file_diagnostics(diagnostics: list[dict], kind: str, name: str) -> list[dict]:
    return [item for item in diagnostics if item.get("file_type") == kind and item.get("file_name") == name]


def extract_collection(acquisition_path: Path, bu_schema: Path, rdv_schema: Path,
                      reference_excerpt: dict | None = None,
                      progress_callback: Callable | None = None) -> None:
    acquisition = json.loads(acquisition_path.read_text(encoding="utf-8"))
    if progress_callback:
        progress_callback({"progress": 0.0, "stage": "Lendo aquisição"})

    def analysis_progress(update: dict) -> None:
        if not progress_callback:
            return
        total = int(update.get("total") or 0)
        processed = int(update.get("processed") or 0)
        percent = 70.0 * processed / total if total else 70.0
        progress_callback({"progress": percent, "stage": "Analisando arquivos",
                           "file": update.get("file"), "file_type": update.get("file_type"),
                           "processed": processed, "total": total})

    extraction = analyze_collection(acquisition_path, [], bu_schema, rdv_schema,
                                    progress_callback=analysis_progress)
    acquisition_source = {
        "data_id": acquisition.get("data_id"),
        "pleito_id": acquisition.get("pleito_id"),
        "turno_from_bu": acquisition.get("turno"),
        "acquired_at_utc": acquisition.get("acquired_at_utc"),
    }

    outputs = [(kind, result_key, data_key, result) for kind, result_key, data_key in (
        ("bu", "bus", "data"), ("rdv", "rdvs", None), ("log", "logs", None))
        for result in extraction.get("file_processing", {}).get(result_key, [])]
    output_total = len(outputs)
    for output_index, (kind, result_key, data_key, result) in enumerate(outputs, start=1):
        source_path = Path(result["path"])
        name = result.get("name") or source_path.name
        metadata = _source_metadata(acquisition, kind, name)
        if kind == "bu":
            extracted = result.get(data_key)
            pleito_id = (extracted or {}).get("pleito_id")
            payload = {
                "source": metadata,
                "acquisition": acquisition_source,
                "section_key": acquisition.get("section_key"),
                "turno": turn_for_pleito(pleito_id),
                "pleito_id": pleito_id,
                "status": result.get("status"),
                "detected_format": result.get("detected_format"),
                "schema_sha256": result.get("version"),
                "urn_model": result.get("urn_model"),
                "data": extracted,
                "diagnostics": _file_diagnostics(extraction.get("diagnostics", []), kind, name),
            }
        elif kind == "rdv":
            payload = dict(result)
            pleito_id = payload.get("entity_metadata", {}).get("pleito")
            payload["turno"] = turn_for_pleito(pleito_id)
            payload["section_key"] = acquisition.get("section_key")
            payload["acquisition"] = acquisition_source
            payload["source_manifest"] = metadata
            payload["diagnostics"] = result.get("diagnostics") or _file_diagnostics(
                extraction.get("diagnostics", []), kind, name)
        else:
            payload = {
                "source": metadata,
                "acquisition": acquisition_source,
                "section_key": acquisition.get("section_key"),
                "status": result.get("status"),
                "event_counts": result.get("event_counts", {}),
                "log_record_accounting": result.get("log_record_accounting", {}),
                "source_files": result.get("source_files", []),
                "events": [{key: value for key, value in event.items()
                            if key != "at_or_after_clock_cuts"}
                           for event in result.get("events", [])],
                "diagnostics": result.get("diagnostics") or _file_diagnostics(
                    extraction.get("diagnostics", []), kind, name),
                "record_policy": "Recognized and potentially relevant unclassified events remain individual objects in events[]. The fifth log message field is retained for potentially relevant unclassified events, suspension-assigned null votes, and explicit vote-registration failures; explicit failures carry vote_registration_status and, when present, the stated error code. Other raw tabular fields are omitted.",
            }
        _write_json(source_path.with_name(source_path.name + ".json"), payload)
        if progress_callback:
            progress_callback({"progress": 70.0 + 25.0 * output_index / max(1, output_total),
                               "stage": "Gravando extrações", "file": name,
                               "processed": output_index, "total": output_total})

    section_diagnostics = [item for item in extraction.get("diagnostics", [])
                           if not item.get("file_type") or not item.get("file_name")]
    if section_diagnostics:
        acquisition["analysis_diagnostics"] = section_diagnostics
        _write_json(acquisition_path, acquisition)

    transfer_reconciliation = extraction.get("transferencia_temporaria")
    if transfer_reconciliation and reference_excerpt:
        transfer_reconciliation["reference_excerpt"] = reference_excerpt
    if transfer_reconciliation and transfer_reconciliation.get("turno") == 2:
        output_root = acquisition_path.parent
        write_reconciliation([transfer_reconciliation],
            output_root / "transferencia-temporaria-reconciliacao.json",
            output_root / "transferencia-temporaria-reconciliacao.csv",
            "section_analysis_log_sessions")
        write_validation([transfer_reconciliation],
            output_root / "transferencia-temporaria-validation.json",
            source_note="section_analysis_log_sessions")
    if progress_callback:
        progress_callback({"progress": 100.0, "stage": "Extração concluída"})


def process_acquisitions(acquisition_paths: list[Path], bu_schema: Path, rdv_schema: Path,
                         progress_callback: Callable | None = None,
                         stop_event: threading.Event | None = None,
                         job_progress_callback: Callable | None = None,
                         max_workers: int = DEFAULT_WORKERS,
                         generate_excerpts: bool = True) -> list[dict]:
    """Reprocess local acquisitions and atomically refresh their derived outputs."""
    acquisitions = []
    targets = []
    for acquisition_path in acquisition_paths:
        collection = json.loads(acquisition_path.read_text(encoding="utf-8"))
        acquisitions.append((acquisition_path, collection))
        section_key = collection.get("section_key")
        turn = collection.get("turno") or {"3220": 1, "3221": 2}.get(str(collection.get("data_id")))
        if isinstance(section_key, str) and turn in {1, 2, "1", "2"}:
            targets.append({"section_key": section_key, "turn": int(turn),
                            "output_path": acquisition_path.parent / "transferencia-temporaria-elegiveis.csv"})
    if generate_excerpts:
        try:
            excerpt_metadata = write_section_excerpts(targets)
        except Exception as error:
            identity = reference_identity()
            excerpt_metadata = {str(target["output_path"]): {
                "status": "not_generated", "reason": f"{type(error).__name__}: {error}",
                "reference_zip_sha256": identity.get("zip_sha256"),
                "reference_version": identity.get("version"),
            } for target in targets}
    else:
        excerpt_metadata = {}
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")

    def identity(acquisition_path: Path, collection: dict) -> dict:
        section = collection.get("section_key") or acquisition_path.parent.parent.name
        turn = collection.get("turno") or {"3220": 1, "3221": 2}.get(str(collection.get("data_id")))
        return {"job_id": f"{section}:turno-{turn or '?'}", "section": section, "turn": turn}

    jobs = deque((path, collection, identity(path, collection)) for path, collection in acquisitions)
    results_by_path: dict[str, dict] = {}
    active: dict[str, dict] = {}
    active_sections: set[str] = set()
    active_lock = threading.Lock()
    futures = {}
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="urna-extract")

    def queue_snapshot() -> None:
        if job_progress_callback:
            with active_lock:
                active_snapshot = list(active.values())
            job_progress_callback({"type": "job_queue", "worker_limit": max_workers,
                "waiting_count": len(jobs), "active_jobs": active_snapshot,
                "waiting_jobs": [dict(info, status="queued") for _path, _collection, info
                                  in list(jobs)[:QUEUE_PREVIEW_SIZE]]})

    def run_one(path: Path, collection: dict, info: dict) -> dict:
        excerpt_path = path.parent / "transferencia-temporaria-elegiveis.csv"

        def report(update: dict) -> None:
            if job_progress_callback:
                with active_lock:
                    active[info["job_id"]] = {**active.get(info["job_id"], {}), **update,
                                                "stage": update.get("stage", active.get(info["job_id"], {}).get("stage"))}
                job_progress_callback({"type": "job_progress", **info, **update})

        try:
            report({"progress": 0.0, "stage": "Iniciando extração"})
            extract_collection(path, bu_schema, rdv_schema, excerpt_metadata.get(str(excerpt_path)),
                               progress_callback=report)
            return {"acquisition": str(path), "status": "complete", **info}
        except Exception as error:
            return {"acquisition": str(path), "status": "failed",
                    "error": f"{type(error).__name__}: {error}", **info}

    try:
        while jobs or futures:
            while jobs and len(futures) < max_workers and not (stop_event and stop_event.is_set()):
                scheduled = False
                for _ in range(len(jobs)):
                    path, collection, info = jobs.popleft()
                    section_lock = str(info["section"]).casefold()
                    if section_lock in active_sections:
                        jobs.append((path, collection, info))
                        continue
                    scheduled = True
                    active_sections.add(section_lock)
                    with active_lock:
                        active[info["job_id"]] = {**info, "status": "running", "progress": 0.0,
                                                   "stage": "Iniciando extração"}
                    if job_progress_callback:
                        job_progress_callback({"type": "job_started", **active[info["job_id"]]})
                    futures[executor.submit(run_one, path, collection, info)] = (path, info, section_lock)
                    break
                if not scheduled:
                    break
            queue_snapshot()
            if not futures:
                break
            done, _pending = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                path, info, section_lock = futures.pop(future)
                active_sections.discard(section_lock)
                result = future.result()
                results_by_path[str(path)] = result
                with active_lock:
                    active.pop(info["job_id"], None)
                if progress_callback:
                    progress_callback(result)
                if job_progress_callback:
                    job_progress_callback({"type": "job_finished", **info,
                        "status": result.get("status"), "progress": 100.0 if result.get("status") == "complete" else None,
                        "error": result.get("error")})
            queue_snapshot()
    finally:
        executor.shutdown(wait=True, cancel_futures=True)

    return [results_by_path[str(path)] for path, _collection in acquisitions if str(path) in results_by_path]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acquisition", type=Path, action="append", required=True,
                        help="acquisition.json; repeat for multiple sections")
    parser.add_argument("--schema", type=Path, default=Path("schemas/bu.asn1"))
    parser.add_argument("--rdv-schema", type=Path, default=Path("schemas/rdv.asn1"))
    args = parser.parse_args()
    results = process_acquisitions(args.acquisition, args.schema, args.rdv_schema)
    return 1 if any(result["status"] != "complete" for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
