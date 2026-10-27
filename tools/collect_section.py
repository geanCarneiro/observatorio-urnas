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

from diagnostics import diagnostic, file_state, verify_jws_signature
from election_turns import folder_for_pleito

BASE_TEMPLATE = "https://resultados.tse.jus.br/oficial/ele2026/arquivo-urna/{data_id}/dados"
USER_AGENT = "observatorio-urnas/0.1 (section-level research collection)"
MAX_BYTES = 64 * 1024 * 1024


def fetch(url: str) -> tuple[bytes, dict[str, str]]:
    last_partial = b""
    last_metadata = None
    for attempt in range(6):
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
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
                }
                last_metadata = metadata
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
                    if len(data) > MAX_BYTES:
                        raise DownloadIncompleteError(f"response exceeds {MAX_BYTES} bytes", bytes(data[:MAX_BYTES]), metadata)
                if metadata["content_length"].isdigit() and int(metadata["content_length"]) != len(data):
                    raise DownloadIncompleteError(
                        f"expected {metadata['content_length']} bytes, received {len(data)}", data, metadata
                    )
                return bytes(data), metadata
        except urllib.error.HTTPError as error:
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uf", required=True, type=lambda x: x.lower())
    parser.add_argument("--municipio", required=True)
    parser.add_argument("--zona", required=True)
    parser.add_argument("--secao", required=True)
    parser.add_argument("--data-id", default="3221", help="TSE arquivo-urna dataset ID (default: 3221)")
    parser.add_argument("--output", type=Path, default=Path("dados-brutos"))
    args = parser.parse_args()
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
    root = section_root / ".staging" / acquisition_id
    run = {"section_key": key, "data_id": args.data_id,
           "turno": {"3220": "1", "3221": "2"}.get(args.data_id), "manifest_url": manifest_url,
           "acquired_at_utc": datetime.now(timezone.utc).isoformat(), "signature_status": "unverified",
           "files": [], "file_states": [], "diagnostics": []}
    manifest_path = root / "manifest.aux.jws"
    manifest_status = "failed"
    try:
        manifest_bytes, manifest_http = fetch(manifest_url)
        save_atomic(manifest_path, manifest_bytes)
        run["manifest_http"] = manifest_http
        run["manifest_sha256"] = hashlib.sha256(manifest_bytes).hexdigest()
        manifest_status = "downloaded"
    except urllib.error.HTTPError as error:
        manifest_status = "not_found" if error.code == 404 else "failed"
        code = "COLLECT_JWS_HTTP_ERROR"
        run["diagnostics"].append(diagnostic(code=code, stage="collection", operation="fetch_manifest",
            error_type="http", cause=f"HTTP {error.code}: {error.reason}", recoverability="permanent" if error.code == 404 else "recoverable",
            section=key, file_type="jws", file_name=manifest_path.name, file_path=str(manifest_path), exception=error,
            context={"http_status": error.code, "url": manifest_url}))
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as error:
        run["diagnostics"].append(diagnostic(code="COLLECT_JWS_NETWORK_ERROR", stage="collection", operation="fetch_manifest",
            error_type="network", cause=str(getattr(error, "reason", error)), recoverability="recoverable", section=key,
            file_type="jws", file_name=manifest_path.name, file_path=str(manifest_path), exception=error,
            context={"url": manifest_url}))
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
                try:
                    body, http_metadata = fetch(url)
                    save_atomic(destination, body)
                    source_hash = hashlib.sha256(body).hexdigest()
                    state.update({"http": http_metadata, "size_bytes": len(body), "sha256": source_hash, "status": "downloaded"})
                    run["files"].append(state.copy())
                    run["file_states"].append(file_state(file_type=file_type, name=name, section=key, status="downloaded",
                        path=str(relative), sha256=source_hash, size_bytes=len(body)))
                except urllib.error.HTTPError as error:
                    kind = "not_found" if error.code == 404 else "failed"
                    run["diagnostics"].append(diagnostic(code=f"COLLECT_{file_type.upper()}_HTTP_ERROR", stage="collection",
                        operation="download_file", error_type="http", cause=f"HTTP {error.code}: {error.reason}",
                        recoverability="permanent" if error.code == 404 else "recoverable", section=key, file_type=file_type,
                        file_name=name, file_path=str(destination), exception=error, detected_format=Path(name).suffix.lstrip("."),
                        context={"http_status": error.code, "url": url, "manifest_hash": content_hash}))
                    run["file_states"].append(file_state(file_type=file_type, name=name, section=key, status=kind, reason=str(error), path=str(relative)))
                except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as error:
                    run["diagnostics"].append(diagnostic(code=f"COLLECT_{file_type.upper()}_NETWORK_ERROR", stage="collection",
                        operation="download_file", error_type="network", cause=str(getattr(error, "reason", error)), recoverability="recoverable",
                        section=key, file_type=file_type, file_name=name, file_path=str(destination), exception=error,
                        detected_format=Path(name).suffix.lstrip("."), context={"url": url, "manifest_hash": content_hash}))
                    run["file_states"].append(file_state(file_type=file_type, name=name, section=key, status="failed", reason=str(error), path=str(relative)))
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
                except Exception as error:
                    run["diagnostics"].append(diagnostic(code=f"COLLECT_{file_type.upper()}_DOWNLOAD_ERROR", stage="collection",
                        operation="download_file", error_type="internal", cause=str(error), recoverability="indeterminate",
                        section=key, file_type=file_type, file_name=name, file_path=str(destination), exception=error,
                        detected_format=Path(name).suffix.lstrip("."), context={"url": url, "manifest_hash": content_hash}))
                    run["file_states"].append(file_state(file_type=file_type, name=name, section=key, status="failed", reason=str(error), path=str(relative)))

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
    final_root = section_root / turn_directory / "runs" / acquisition_id
    log_root = section_root / "logs" / "runs" / acquisition_id
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
    print(json.dumps({"section_key": key, "acquisition_id": acquisition_id,
                      "turno": detected_turn, "pleito_id": pleito_id,
                      "acquisition": str(acquisition_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (urllib.error.URLError, ValueError, json.JSONDecodeError) as error:
        print(f"collection failed: {error}", file=sys.stderr)
        raise SystemExit(1)
