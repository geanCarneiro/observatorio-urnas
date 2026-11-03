#!/usr/bin/env python3
"""Download, preserve, and file one section's official TSE data by section and turn."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import http.client
import os
import re
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.request
from urllib.parse import quote
from datetime import datetime, timezone
from pathlib import Path

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from diagnostics import diagnostic, file_state, verify_jws_signature
from election_turns import folder_for_pleito

BASE_TEMPLATE = "https://resultados.tse.jus.br/oficial/ele2026/arquivo-urna/{data_id}/dados"
USER_AGENT = "observatorio-urnas/0.1 (section-level research collection)"
MAX_BYTES = 64 * 1024 * 1024


class NetworkAccessDeniedError(ConnectionError):
    """Raised when Windows or the execution environment blocks outbound sockets."""


def _is_network_access_denied(error: BaseException) -> bool:
    pending = [error]
    seen = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if getattr(current, "winerror", None) == 10013 or getattr(current, "errno", None) == 10013:
            return True
        if "winerror 10013" in str(current).lower():
            return True
        for attribute in ("reason", "__cause__", "__context__"):
            nested = getattr(current, attribute, None)
            if isinstance(nested, BaseException):
                pending.append(nested)
    return False


def fetch(url: str, *, cached_body: bytes | None = None,
          cached_metadata: dict[str, str] | None = None,
          progress_callback=None) -> tuple[bytes, dict[str, str]]:
    last_partial = b""
    last_metadata = None
    for attempt in range(6):
        headers = {"User-Agent": USER_AGENT}
        if cached_body is not None and cached_metadata:
            if cached_metadata.get("etag"):
                headers["If-None-Match"] = cached_metadata["etag"]
            if cached_metadata.get("last_modified"):
                headers["If-Modified-Since"] = cached_metadata["last_modified"]
        request = urllib.request.Request(url, headers=headers)
        data = bytearray()
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                metadata = {
                    "status": str(response.status),
                    "url": response.geturl(),
                    "content_type": response.headers.get("Content-Type", ""),
                    "content_length": response.headers.get("Content-Length", ""),
                    "last_modified": response.headers.get("Last-Modified", ""),
                    "etag": response.headers.get("ETag", ""),
                    "cache_status": "downloaded",
                }
                last_metadata = metadata
                content_length = metadata["content_length"]
                if progress_callback:
                    progress_callback(0, int(content_length) if content_length.isdigit() else None)
                while True:
                    try:
                        chunk = response.read(min(1024 * 1024, MAX_BYTES + 1 - len(data)))
                    except http.client.IncompleteRead as error:
                        data.extend(error.partial)
                        raise
                    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
                        last_partial = bytes(data)
                        raise
                    if not chunk:
                        break
                    data.extend(chunk)
                    if progress_callback:
                        progress_callback(len(data), int(content_length) if content_length.isdigit() else None)
                    if len(data) > MAX_BYTES:
                        raise DownloadIncompleteError(f"response exceeds {MAX_BYTES} bytes", bytes(data[:MAX_BYTES]), metadata)
                if metadata["content_length"].isdigit() and int(metadata["content_length"]) != len(data):
                    raise DownloadIncompleteError(
                        f"expected {metadata['content_length']} bytes, received {len(data)}", data, metadata
                    )
                return bytes(data), metadata
        except urllib.error.HTTPError as error:
            if error.code == 304 and cached_body is not None:
                metadata = dict(cached_metadata or {})
                metadata.update({"status": "304", "url": url, "cache_status": "revalidated"})
                if error.headers.get("ETag"):
                    metadata["etag"] = error.headers["ETag"]
                if error.headers.get("Last-Modified"):
                    metadata["last_modified"] = error.headers["Last-Modified"]
                error.close()
                return cached_body, metadata
            if error.code not in (429, 503) or attempt == 5:
                raise
            retry_after = error.headers.get("Retry-After", "")
            delay = int(retry_after) if retry_after.isdigit() else min(60, 2**attempt)
            time.sleep(delay)
        except http.client.IncompleteRead as error:
            last_partial = bytes(data)
            if attempt == 5:
                raise DownloadIncompleteError(f"HTTP body ended early: {error}", last_partial, last_metadata) from error
            time.sleep(min(60, 2**attempt))
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as error:
            if _is_network_access_denied(error):
                raise NetworkAccessDeniedError(
                    "O Windows bloqueou a conexão de saída (WinError 10013). "
                    "O processo que executa a coleta não tem permissão para acessar a rede. "
                    "Confira a política de rede/firewall e execute a UI/CLI em um ambiente com acesso externo."
                ) from error
            if data:
                last_partial = bytes(data)
            if attempt == 5:
                if last_partial:
                    raise DownloadIncompleteError(f"network interrupted after {len(last_partial)} response bytes: {error}", last_partial, last_metadata) from error
                raise
            time.sleep(min(60, 2**attempt))
    raise AssertionError("unreachable")


class DownloadIncompleteError(ValueError):
    def __init__(self, message: str, body: bytes = b"", http_metadata: dict | None = None):
        super().__init__(message)
        self.body = body
        self.http_metadata = http_metadata or {}


def save_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".download-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def complete_existing_acquisition(section_root: Path, section_key: str, data_id: str) -> Path | None:
    """Return a complete local acquisition for this section and dataset, if present."""
    turn_directory = folder_for_pleito(data_id)
    acquisition_path = section_root / turn_directory / "acquisition.json"
    if not acquisition_path.is_file():
        return None
    try:
        acquisition = json.loads(acquisition_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if (acquisition.get("section_key") != section_key
            or str(acquisition.get("data_id")) != str(data_id)
            or acquisition.get("turn_directory") != turn_directory):
        return None

    core_states = [state for state in acquisition.get("file_states", [])
                   if state.get("type") in {"bu", "rdv", "log"}]
    if not core_states or any(state.get("status") != "downloaded" for state in core_states):
        return None
    if not all(any(state.get("type") == kind for state in core_states) for kind in ("bu", "rdv", "log")):
        return None

    manifest_path = acquisition_path.parent / "manifest.aux.jws"
    if not manifest_path.is_file():
        return None
    expected_manifest_hash = acquisition.get("manifest_sha256")
    if expected_manifest_hash and hashlib.sha256(manifest_path.read_bytes()).hexdigest() != expected_manifest_hash:
        return None

    files = [item for item in acquisition.get("files", []) if item.get("type") in {"bu", "rdv", "log"}]
    if not {"bu", "rdv", "log"}.issubset({item.get("type") for item in files}):
        return None
    section_root = section_root.resolve()
    for item in files:
        relative_path = item.get("relative_path")
        if not isinstance(relative_path, str):
            return None
        candidate = (acquisition_path.parent / relative_path).resolve()
        try:
            candidate.relative_to(section_root)
        except ValueError:
            return None
        if not candidate.is_file():
            return None
        expected_size = item.get("size_bytes")
        if expected_size is not None and candidate.stat().st_size != expected_size:
            return None
    return acquisition_path


def decode_unverified_jws(raw: bytes) -> tuple[dict, dict]:
    parts = raw.decode("ascii").strip().split(".")
    if len(parts) != 3:
        raise ValueError("expected compact JWS with three segments")
    header = json.loads(base64.urlsafe_b64decode(parts[0] + "=" * (-len(parts[0]) % 4)))
    payload_bytes = base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4))
    payload = json.loads(payload_bytes)
    return header, payload


def identifier(value: str, width: int, label: str) -> str:
    if not re.fullmatch(rf"\d{{1,{width}}}", value):
        raise argparse.ArgumentTypeError(f"{label} must be 1..{width} digits")
    return value.zfill(width)


def canonical_manifest_file_type(raw_type: object, name: object) -> str:
    """Resolve the concrete format when the manifest uses a broad type code."""
    if raw_type == "vota" and isinstance(name, str) and name.lower().endswith(".vsc"):
        return "vota.vsc"
    return raw_type if isinstance(raw_type, str) else "unknown"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uf", required=True, type=lambda x: x.lower())
    parser.add_argument("--municipio", required=True)
    parser.add_argument("--zona", required=True)
    parser.add_argument("--secao", required=True)
    parser.add_argument("--data-id", default="3221", help="TSE arquivo-urna dataset ID (default: 3221)")
    parser.add_argument("--output", type=Path, default=Path("dados"))
    parser.add_argument("--staging-root", type=Path, default=Path(".estado-local/staging"),
                        help="temporary collection files are staged outside the persistent data tree")
    parser.add_argument("--incremental", action="store_true",
                        help="reuse already present files whose current manifest hash and local size match")
    parser.add_argument("--progress-jsonl", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[a-z]{2}", args.uf):
        parser.error("UF must be a two-letter code")
    municipality = identifier(args.municipio, 5, "municipio")
    zone = identifier(args.zona, 4, "zona")
    section = identifier(args.secao, 4, "secao")
    if not re.fullmatch(r"\d{4}", args.data_id):
        parser.error("data-id must contain exactly four digits")
    base = BASE_TEMPLATE.format(data_id=args.data_id)
    key = f"{args.uf}/{municipality}/{zone}/{section}"
    stem = f"p00{args.data_id}-{args.uf}-m{municipality}-z{zone}-s{section}-aux.jws"
    manifest_url = f"{base}/{key}/{stem}"
    acquisition_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    section_root = args.output / key
    root = args.staging_root / acquisition_id / key
    run = {"section_key": key, "data_id": args.data_id,
           "turno": {"3220": "1", "3221": "2"}.get(args.data_id), "manifest_url": manifest_url,
           "acquired_at_utc": datetime.now(timezone.utc).isoformat(), "signature_status": "unverified",
           "files": [], "file_states": [], "diagnostics": []}

    def emit_progress(progress: float, stage: str, *, file: str | None = None,
                      bytes_downloaded: int | None = None, bytes_total: int | None = None) -> None:
        if not args.progress_jsonl:
            return
        event = {"progress": max(0.0, min(100.0, round(progress, 2))), "stage": stage, "file": file}
        if bytes_downloaded is not None:
            event["bytes_downloaded"] = bytes_downloaded
        if bytes_total is not None:
            event["bytes_total"] = bytes_total
        print("@@OBSERVATORIO_PROGRESS@@" + json.dumps(event, ensure_ascii=False), file=sys.stderr, flush=True)

    manifest_path = root / "manifest.aux.jws"
    manifest_status = "failed"
    emit_progress(0, "Baixando manifesto", file=manifest_path.name)
    try:
        manifest_bytes, manifest_http = fetch(
            manifest_url,
            progress_callback=lambda downloaded, total: emit_progress(
                10 * downloaded / total if total else 0, "Baixando manifesto", file=manifest_path.name,
                bytes_downloaded=downloaded, bytes_total=total),
        )
        save_atomic(manifest_path, manifest_bytes)
        run["manifest_http"] = manifest_http
        run["manifest_sha256"] = hashlib.sha256(manifest_bytes).hexdigest()
        manifest_status = "downloaded"
        emit_progress(10, "Manifesto baixado", file=manifest_path.name)
    except urllib.error.HTTPError as error:
        manifest_status = "not_found" if error.code == 404 else "failed"
        code = "COLLECT_JWS_HTTP_ERROR"
        run["diagnostics"].append(diagnostic(code=code, stage="collection", operation="fetch_manifest",
            error_type="http", cause=f"HTTP {error.code}: {error.reason}", recoverability="permanent" if error.code == 404 else "recoverable",
            section=key, file_type="jws", file_name=manifest_path.name, file_path=str(manifest_path), exception=error,
            context={"http_status": error.code, "url": manifest_url}))
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as error:
        network_blocked = isinstance(error, NetworkAccessDeniedError)
        run["diagnostics"].append(diagnostic(code="NETWORK_ACCESS_DENIED" if network_blocked else "COLLECT_JWS_NETWORK_ERROR",
            stage="collection", operation="fetch_manifest",
            error_type="network", cause=str(getattr(error, "reason", error)), recoverability="recoverable", section=key,
            file_type="jws", file_name=manifest_path.name, file_path=str(manifest_path), exception=error,
            context={"url": manifest_url, **({"winerror": 10013} if network_blocked else {})}))
    except DownloadIncompleteError as error:
        manifest_status = "partial"
        partial_path = manifest_path.with_suffix(manifest_path.suffix + ".partial")
        if error.body:
            save_atomic(partial_path, error.body)
        run["diagnostics"].append(diagnostic(code="COLLECT_JWS_DOWNLOAD_INCOMPLETE", stage="collection", operation="fetch_manifest",
            error_type="download_incomplete", cause=str(error), recoverability="recoverable", section=key,
            file_type="jws", file_name=manifest_path.name, file_path=str(partial_path), exception=error,
            position={"partial_bytes": len(error.body)}, context={"partial_bytes": len(error.body), "url": manifest_url,
                "http": error.http_metadata}))
    except Exception as error:
        run["diagnostics"].append(diagnostic(code="COLLECT_JWS_FETCH_ERROR", stage="collection", operation="fetch_manifest",
            error_type="internal", cause=str(error), recoverability="indeterminate", section=key,
            file_type="jws", file_name=manifest_path.name, file_path=str(manifest_path), exception=error,
            context={"url": manifest_url}))

    payload = None
    if manifest_path.exists():
        try:
            raw_manifest = manifest_path.read_bytes()
        except OSError as error:
            raw_manifest = b""
            run["diagnostics"].append(diagnostic(code="COLLECT_JWS_READ_FAILED", stage="analysis_input", operation="read_manifest_bytes",
                error_type="integrity", cause=str(error), recoverability="indeterminate", section=key,
                file_type="jws", file_name=manifest_path.name, file_path=str(manifest_path), exception=error))
        signature_status, signature_diagnostic = verify_jws_signature(raw_manifest, section=key, file_name=manifest_path.name)
        run["signature_status"] = signature_status
        if signature_diagnostic:
            run["diagnostics"].append(signature_diagnostic)
        try:
            header, payload = decode_unverified_jws(raw_manifest)
            run["jws_header_unverified"] = header
            run["manifest_payload_unverified"] = payload
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
            run["diagnostics"].append(diagnostic(code="COLLECT_JWS_PARSE_ERROR", stage="parsing", operation="decode_compact_jws",
                error_type="parsing", cause=str(error), recoverability="permanent", section=key,
                file_type="jws", file_name=manifest_path.name, file_path=str(manifest_path), exception=error,
                detected_format="JWS compact", context={"sha256": run.get("manifest_sha256")}))

    listed_types = set()
    progress_file_positions = {}
    if payload is not None and isinstance(payload.get("hashes"), list):
        for entry_index, entry in enumerate(payload["hashes"]):
            if not isinstance(entry, dict) or not isinstance(entry.get("arq", []), list):
                continue
            for file_index, item in enumerate(entry.get("arq", [])):
                if not isinstance(item, dict):
                    continue
                file_type = canonical_manifest_file_type(item.get("tp"), item.get("nm"))
                name = item.get("nm")
                if (file_type in {"bu", "rdv", "log"} and isinstance(name, str)
                        and Path(name).name == name and "/" not in name and "\\" not in name
                        and name not in {".", ".."}):
                    progress_file_positions[(entry_index, file_index)] = len(progress_file_positions) + 1
    total_progress_files = len(progress_file_positions)
    if not total_progress_files:
        emit_progress(95, "Manifesto analisado; sem arquivos brutos para baixar")
    if payload is not None:
        hashes = payload.get("hashes")
        if not isinstance(hashes, list):
            error = ValueError("manifest payload does not contain hashes[]")
            run["diagnostics"].append(diagnostic(code="COLLECT_JWS_SCHEMA_INVALID", stage="semantic_validation",
                operation="validate_manifest_payload", error_type="semantic_validation", cause=str(error),
                recoverability="permanent", section=key, file_type="jws", file_name=manifest_path.name,
                file_path=str(manifest_path), exception=error, detected_format="JWS compact", context={"field": "hashes"}))
            hashes = []
        for entry_index, entry in enumerate(hashes):
            if not isinstance(entry, dict):
                run["diagnostics"].append(diagnostic(code="COLLECT_JWS_HASH_ENTRY_INVALID", stage="semantic_validation",
                    operation="validate_manifest_entry", error_type="semantic_validation", cause="hashes[] entry is not an object",
                    recoverability="permanent", section=key, file_type="jws", file_name=manifest_path.name,
                    file_path=str(manifest_path), position={"entry_index": entry_index}, context={"entry_type": type(entry).__name__}))
                continue
            content_hash = entry.get("hash")
            if not isinstance(content_hash, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", content_hash):
                run["diagnostics"].append(diagnostic(code="COLLECT_JWS_CONTENT_HASH_INVALID", stage="semantic_validation",
                    operation="validate_manifest_entry", error_type="semantic_validation", cause="manifest content hash is missing or malformed",
                    recoverability="permanent", section=key, file_type="jws", file_name=manifest_path.name,
                    file_path=str(manifest_path), position={"entry_index": entry_index}, context={"hash_value": content_hash}))
                continue
            items = entry.get("arq", [])
            if not isinstance(items, list):
                run["diagnostics"].append(diagnostic(code="COLLECT_JWS_FILE_LIST_INVALID", stage="semantic_validation",
                    operation="validate_manifest_entry", error_type="semantic_validation", cause="manifest arq field is not an array",
                    recoverability="permanent", section=key, file_type="jws", file_name=manifest_path.name,
                    file_path=str(manifest_path), position={"entry_index": entry_index}, context={"field": "arq"}))
                continue
            for file_index, item in enumerate(items):
                if not isinstance(item, dict):
                    run["diagnostics"].append(diagnostic(code="COLLECT_JWS_FILE_ENTRY_INVALID", stage="semantic_validation",
                        operation="validate_manifest_file_entry", error_type="semantic_validation",
                        cause="manifest arq[] entry is not an object", recoverability="permanent", section=key,
                        file_type="jws", file_name=manifest_path.name, file_path=str(manifest_path),
                        position={"entry_index": entry_index, "file_index": file_index},
                        context={"entry_type": type(item).__name__}))
                    continue
                raw_file_type, name = item.get("tp"), item.get("nm")
                # The official manifest identifies signature bundles as tp=vota;
                # the .vsc filename carries the concrete format.
                file_type = canonical_manifest_file_type(raw_file_type, name)
                if not isinstance(raw_file_type, str):
                    run["diagnostics"].append(diagnostic(code="COLLECT_MANIFEST_FILE_TYPE_INVALID", stage="semantic_validation",
                        operation="validate_manifest_file_entry", error_type="semantic_validation",
                        cause="manifest file type is not a string", recoverability="permanent", section=key,
                        file_type="jws", file_name=manifest_path.name, file_path=str(manifest_path),
                        position={"entry_index": entry_index, "file_index": file_index},
                        context={"file_type_value": repr(raw_file_type)}))
                listed_types.add(file_type)
                if file_type == "vota.vsc":
                    state = file_state(file_type=file_type, name=str(name) if name is not None else None,
                        section=key, status="not_applicable",
                        reason="Validation/signature sidecar is outside the analytical scope and was intentionally not downloaded.",
                        detected_format="VSC")
                    state["manifest_hash"] = content_hash
                    run["file_states"].append(state)
                    continue
                if file_type not in ("bu", "log", "rdv"):
                    run["diagnostics"].append(diagnostic(code="COLLECT_FILE_TYPE_UNSUPPORTED", stage="format_detection",
                        operation="select_manifest_file_type", error_type="format_incompatibility",
                        cause="The manifest lists a file type not supported by the collector.", recoverability="permanent",
                        section=key, file_type=str(file_type), file_name=str(name), file_path=None,
                        detected_format=str(file_type), position={"entry_index": entry_index, "file_index": file_index},
                        context={"manifest_hash": content_hash}))
                    run["file_states"].append(file_state(file_type=str(file_type), name=str(name), section=key,
                        status="not_processed", reason="Unsupported manifest file type."))
                    continue
                if not isinstance(name, str) or Path(name).name != name or "/" in name or "\\" in name or name in {".", ".."}:
                    run["diagnostics"].append(diagnostic(code="COLLECT_MANIFEST_FILENAME_UNSAFE", stage="semantic_validation",
                        operation="validate_manifest_filename", error_type="semantic_validation", cause="manifest filename is unsafe",
                        recoverability="permanent", section=key, file_type=file_type, file_name=str(name),
                        file_path=None, position={"entry_index": entry_index, "file_index": file_index}, context={"name": name}))
                    run["file_states"].append(file_state(file_type=file_type, name=str(name), section=key, status="failed", reason="unsafe filename"))
                    continue
                url = f"{base}/{key}/{content_hash}/{quote(name, safe='-_.')}"
                relative = Path(file_type) / name
                destination = root / relative
                state = {"type": file_type, "name": name, "manifest_hash": content_hash,
                         "relative_path": str(relative), "url": url}
                progress_file_index = progress_file_positions.get((entry_index, file_index), 0)
                file_start = 10 + 85 * (progress_file_index - 1) / max(1, total_progress_files)
                file_end = 10 + 85 * progress_file_index / max(1, total_progress_files)
                emit_progress(file_start, f"Preparando {file_type.upper()}", file=name)
                existing_entry = None
                if args.incremental:
                    expected_turn_directory = folder_for_pleito(args.data_id)
                    existing_acquisition_path = section_root / expected_turn_directory / "acquisition.json"
                    try:
                        previous = json.loads(existing_acquisition_path.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError):
                        previous = {}
                    if (previous.get("section_key") == key
                            and str(previous.get("data_id")) == str(args.data_id)):
                        existing_entry = next((entry for entry in previous.get("files", [])
                                               if entry.get("type") == file_type
                                               and entry.get("name") == name
                                               and entry.get("manifest_hash") == content_hash), None)
                if existing_entry:
                    existing_path = (section_root / folder_for_pleito(args.data_id)
                                     / existing_entry.get("relative_path", "")).resolve()
                    try:
                        existing_path.relative_to(section_root.resolve())
                    except ValueError:
                        existing_path = Path()
                    expected_size = existing_entry.get("size_bytes")
                    if (existing_path.is_file()
                            and (expected_size is None or existing_path.stat().st_size == expected_size)):
                        state.update({key: value for key, value in existing_entry.items()
                                      if key in {"http", "size_bytes", "sha256"}})
                        state["reused_local"] = True
                        run["files"].append(state.copy())
                        run["file_states"].append(file_state(file_type=file_type, name=name, section=key,
                            status="downloaded", path=str(relative), sha256=state.get("sha256"),
                            size_bytes=state.get("size_bytes")))
                        emit_progress(file_end, f"Reutilizando {file_type.upper()} local", file=name)
                        continue
                try:
                    body, http_metadata = fetch(
                        url,
                        progress_callback=lambda downloaded, total, base_progress=file_start,
                        span=file_end - file_start, kind=file_type, file_name=name: emit_progress(
                            base_progress + (span * downloaded / total if total else 0),
                            f"Baixando {kind.upper()}", file=file_name,
                            bytes_downloaded=downloaded, bytes_total=total),
                    )
                    save_atomic(destination, body)
                    source_hash = hashlib.sha256(body).hexdigest()
                    state.update({"http": http_metadata, "size_bytes": len(body), "sha256": source_hash, "status": "downloaded"})
                    run["files"].append(state.copy())
                    run["file_states"].append(file_state(file_type=file_type, name=name, section=key, status="downloaded",
                        path=str(relative), sha256=source_hash, size_bytes=len(body)))
                    emit_progress(file_end, f"{file_type.upper()} baixado", file=name)
                except urllib.error.HTTPError as error:
                    kind = "not_found" if error.code == 404 else "failed"
                    run["diagnostics"].append(diagnostic(code=f"COLLECT_{file_type.upper()}_HTTP_ERROR", stage="collection",
                        operation="download_file", error_type="http", cause=f"HTTP {error.code}: {error.reason}",
                        recoverability="permanent" if error.code == 404 else "recoverable", section=key, file_type=file_type,
                        file_name=name, file_path=str(destination), exception=error, detected_format=Path(name).suffix.lstrip("."),
                        context={"http_status": error.code, "url": url, "manifest_hash": content_hash}))
                    run["file_states"].append(file_state(file_type=file_type, name=name, section=key, status=kind, reason=str(error), path=str(relative)))
                    emit_progress(file_end, f"{file_type.upper()} indisponível", file=name)
                except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as error:
                    network_blocked = isinstance(error, NetworkAccessDeniedError)
                    run["diagnostics"].append(diagnostic(code="NETWORK_ACCESS_DENIED" if network_blocked else f"COLLECT_{file_type.upper()}_NETWORK_ERROR",
                        stage="collection",
                        operation="download_file", error_type="network", cause=str(getattr(error, "reason", error)), recoverability="recoverable",
                        section=key, file_type=file_type, file_name=name, file_path=str(destination), exception=error,
                        detected_format=Path(name).suffix.lstrip("."), context={"url": url, "manifest_hash": content_hash,
                            **({"winerror": 10013} if network_blocked else {})}))
                    run["file_states"].append(file_state(file_type=file_type, name=name, section=key, status="failed", reason=str(error), path=str(relative)))
                    emit_progress(file_end, f"Falha ao baixar {file_type.upper()}", file=name)
                except DownloadIncompleteError as error:
                    partial_path = destination.with_suffix(destination.suffix + ".partial")
                    if error.body:
                        save_atomic(partial_path, error.body)
                    run["diagnostics"].append(diagnostic(code=f"COLLECT_{file_type.upper()}_DOWNLOAD_INCOMPLETE", stage="collection",
                        operation="download_file", error_type="download_incomplete", cause=str(error), recoverability="recoverable",
                        section=key, file_type=file_type, file_name=name, file_path=str(partial_path), exception=error,
                        position={"partial_bytes": len(error.body)}, detected_format=Path(name).suffix.lstrip("."),
                        context={"url": url, "manifest_hash": content_hash, "http": error.http_metadata}))
                    run["file_states"].append(file_state(file_type=file_type, name=name, section=key, status="partial",
                        reason=str(error), path=str(partial_path), size_bytes=len(error.body), sha256=hashlib.sha256(error.body).hexdigest()))
                    emit_progress(file_end, f"Download parcial de {file_type.upper()}", file=name)
                except Exception as error:
                    run["diagnostics"].append(diagnostic(code=f"COLLECT_{file_type.upper()}_DOWNLOAD_ERROR", stage="collection",
                        operation="download_file", error_type="internal", cause=str(error), recoverability="indeterminate",
                        section=key, file_type=file_type, file_name=name, file_path=str(destination), exception=error,
                        detected_format=Path(name).suffix.lstrip("."), context={"url": url, "manifest_hash": content_hash}))
                    run["file_states"].append(file_state(file_type=file_type, name=name, section=key, status="failed", reason=str(error), path=str(relative)))
                    emit_progress(file_end, f"Falha ao baixar {file_type.upper()}", file=name)

    for file_type in ("bu", "rdv", "log"):
        if file_type not in listed_types:
            run["file_states"].append(file_state(file_type=file_type, name=None, section=key, status="missing",
                reason="No file of this type was listed in the JWS manifest."))
            run["diagnostics"].append(diagnostic(code=f"COLLECT_{file_type.upper()}_NOT_LISTED", stage="collection",
                operation="discover_manifest_files", error_type="missing", cause="No corresponding file entry exists in the manifest.",
                recoverability="indeterminate", section=key, file_type=file_type, file_name=None, file_path=None,
                detected_format=None, context={"manifest_path": str(manifest_path)}))
    if "vota.vsc" not in listed_types:
        run["file_states"].append(file_state(file_type="vota.vsc", name=None, section=key, status="not_applicable",
            reason="Signature/validation sidecars are outside the analytical scope and are not collected."))
    state_path = manifest_path.with_suffix(manifest_path.suffix + ".partial") if manifest_status == "partial" else manifest_path
    run["file_states"].append(file_state(file_type="jws", name=manifest_path.name, section=key,
        status=manifest_status, path=str(state_path), sha256=hashlib.sha256(state_path.read_bytes()).hexdigest() if state_path.exists() else run.get("manifest_sha256"),
        size_bytes=state_path.stat().st_size if state_path.exists() else None))
    run["file_states"].append(file_state(file_type="signature_verification", name=manifest_path.name, section=key,
        status="not_processed", reason="No trusted key/configuration is available."))
    run["finished_at_utc"] = datetime.now(timezone.utc).isoformat()

    pleito_id = None
    detected_turn = None
    bu_entry = next((item for item in run["files"] if item.get("type") == "bu"), None)
    if bu_entry:
        try:
            from analyze_section import decode_bu
            bu_data = decode_bu(root / bu_entry["relative_path"], Path(__file__).resolve().parent.parent / "schemas/bu.asn1")
            pleito_id = bu_data.get("pleito_id")
            detected_turn = bu_data.get("turno")
        except Exception as error:
            run["diagnostics"].append(diagnostic(code="COLLECT_TURN_DETECTION_FAILED", stage="semantic_validation",
                operation="read_bu_electoral_identifier", error_type="semantic_validation", cause=str(error),
                recoverability="indeterminate", section=key, file_type="bu", file_name=bu_entry.get("name"),
                file_path=str(root / bu_entry["relative_path"]), exception=error,
                context={"data_id": args.data_id, "turn_detection_source": "BU ASN.1 header"}))
    expected_turn = {"3220": 1, "3221": 2}.get(args.data_id)
    if detected_turn is not None and expected_turn is not None and detected_turn != expected_turn:
        run["diagnostics"].append(diagnostic(code="COLLECT_DATA_TURN_MISMATCH", stage="semantic_validation",
            operation="compare_manifest_dataset_and_bu_turn", error_type="semantic_validation",
            cause="BU-derived turn differs from the requested TSE data set.", recoverability="indeterminate",
            section=key, file_type="bu", file_name=bu_entry.get("name"),
            context={"data_id": args.data_id, "expected_turn_from_dataset": expected_turn,
                     "observed_turn_from_bu": detected_turn, "pleito_id": pleito_id}))

    turn_directory = folder_for_pleito(pleito_id)
    # Keep one current acquisition per turn directly in the section tree.
    # Logs live at section root because a log can cover multiple election days.
    final_root = section_root / turn_directory
    log_root = section_root / "logs"
    final_root.mkdir(parents=True, exist_ok=True)
    for item in run["files"]:
        source = root / item["relative_path"]
        if item["type"] == "log":
            destination = log_root / item["name"]
            relative = Path(os.path.relpath(destination, final_root))
        else:
            destination = final_root / item["type"] / item["name"]
            relative = Path(item["type"]) / item["name"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.exists():
            os.replace(source, destination)
        item["relative_path"] = str(relative)
    for state in run["file_states"]:
        if state.get("status") != "partial" or not state.get("path"):
            continue
        file_type, name = state.get("type"), state.get("name")
        source = root / state["path"]
        if not source.exists():
            continue
        if file_type == "log":
            destination = log_root / source.name
            state["path"] = os.path.relpath(destination, final_root)
        elif file_type in {"bu", "rdv"}:
            destination = final_root / file_type / source.name
            state["path"] = str(Path(file_type) / source.name)
        else:
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(source, destination)
    for state in run["file_states"]:
        file_type, name = state.get("type"), state.get("name")
        if not name:
            continue
        if file_type == "jws":
            state["path"] = str(Path(name + ".partial" if state.get("status") == "partial" else name))
        elif file_type == "signature_verification":
            continue
        elif file_type == "log":
            dest = log_root / (name + ".partial" if state.get("status") == "partial" else name)
            state["path"] = os.path.relpath(dest, final_root)
        elif file_type in {"bu", "rdv"}:
            if state.get("status") == "partial":
                continue
            filename = name
            state["path"] = str(Path(file_type) / filename)
    for item in run["diagnostics"]:
        file_type, name = item.get("file_type"), item.get("file_name")
        if file_type == "jws" and name:
            item["file_path"] = str(final_root / (name + ".partial" if item.get("code", "").endswith("DOWNLOAD_INCOMPLETE") else name))
        elif file_type == "log" and name:
            item["file_path"] = str(log_root / (name + ".partial" if item.get("code", "").endswith("DOWNLOAD_INCOMPLETE") else name))
        elif file_type in {"bu", "rdv"} and name:
            item["file_path"] = str(final_root / file_type / (name + ".partial" if item.get("code", "").endswith("DOWNLOAD_INCOMPLETE") else name))
    if manifest_path.exists():
        os.replace(manifest_path, final_root / manifest_path.name)
    manifest_partial_path = manifest_path.with_suffix(manifest_path.suffix + ".partial")
    if manifest_partial_path.exists():
        os.replace(manifest_partial_path, final_root / manifest_partial_path.name)
    shutil.rmtree(root, ignore_errors=True)
    try:
        root.parent.rmdir()
    except OSError:
        pass
    run["turno"] = detected_turn
    run["pleito_id"] = pleito_id
    run["turn_directory"] = turn_directory
    run["turn_detection_source"] = "BU ASN.1 cabecalho.idEleitoral; unclassified when unavailable"
    run["acquisition_path"] = str(final_root)
    acquisition_path = final_root / "acquisition.json"
    save_atomic(acquisition_path, json.dumps(run, ensure_ascii=False, indent=2).encode())
    emit_progress(100, "Coleta da seção concluída")
    print(json.dumps({"section_key": key, "acquisition_id": acquisition_id,
                      "turno": detected_turn, "pleito_id": pleito_id,
                      "acquisition": str(acquisition_path),
                      "file_states": run["file_states"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (urllib.error.URLError, ValueError, json.JSONDecodeError) as error:
        print(f"collection failed: {error}", file=sys.stderr)
        raise SystemExit(1)
