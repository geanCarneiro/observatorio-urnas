#!/usr/bin/env python3
"""Download one section's official TSE manifest, BU and logs without parsing them."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE = "https://resultados.tse.jus.br/oficial/ele2026/arquivo-urna/3221/dados"
USER_AGENT = "observatorio-urnas/0.1 (section-level research collection)"
MAX_BYTES = 64 * 1024 * 1024


def fetch(url: str) -> tuple[bytes, dict[str, str]]:
    for attempt in range(6):
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                data = response.read(MAX_BYTES + 1)
                if len(data) > MAX_BYTES:
                    raise ValueError(f"response exceeds {MAX_BYTES} bytes: {url}")
                return data, {
                    "status": str(response.status),
                    "url": response.geturl(),
                    "content_type": response.headers.get("Content-Type", ""),
                    "content_length": response.headers.get("Content-Length", ""),
                    "last_modified": response.headers.get("Last-Modified", ""),
                    "etag": response.headers.get("ETag", ""),
                }
        except urllib.error.HTTPError as error:
            if error.code not in (429, 503) or attempt == 5:
                raise
            retry_after = error.headers.get("Retry-After", "")
            delay = int(retry_after) if retry_after.isdigit() else min(60, 2**attempt)
            time.sleep(delay)
    raise AssertionError("unreachable")


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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uf", required=True, type=lambda x: x.lower())
    parser.add_argument("--municipio", required=True)
    parser.add_argument("--zona", required=True)
    parser.add_argument("--secao", required=True)
    parser.add_argument("--output", type=Path, default=Path("dados-brutos"))
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z]{2}", args.uf):
        parser.error("UF must be a two-letter code")
    municipality = identifier(args.municipio, 5, "municipio")
    zone = identifier(args.zona, 4, "zona")
    section = identifier(args.secao, 4, "secao")
    key = f"{args.uf}/{municipality}/{zone}/{section}"
    stem = f"p003221-{args.uf}-m{municipality}-z{zone}-s{section}-aux.jws"
    manifest_url = f"{BASE}/{key}/{stem}"
    acquisition_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    root = args.output / key / "runs" / acquisition_id
    run = {"section_key": key, "manifest_url": manifest_url,
            "acquired_at_utc": datetime.now(timezone.utc).isoformat(),
           "signature_status": "unverified", "files": []}
    manifest_bytes, manifest_http = fetch(manifest_url)
    header, payload = decode_unverified_jws(manifest_bytes)
    save_atomic(root / "manifest.aux.jws", manifest_bytes)
    run["manifest_http"] = manifest_http
    run["manifest_sha256"] = hashlib.sha256(manifest_bytes).hexdigest()
    run["jws_header_unverified"] = header
    run["manifest_payload_unverified"] = payload

    hashes = payload.get("hashes")
    if not isinstance(hashes, list):
        raise ValueError("manifest payload does not contain hashes[]")
    for entry in hashes:
        content_hash = entry.get("hash")
        if not isinstance(content_hash, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", content_hash):
            raise ValueError("manifest contains an invalid content hash")
        for item in entry.get("arq", []):
            file_type, name = item.get("tp"), item.get("nm")
            if file_type not in ("bu", "log"):
                continue
            if not isinstance(name, str) or Path(name).name != name:
                raise ValueError("manifest contains an unsafe filename")
            url = f"{BASE}/{key}/{content_hash}/{name}"
            body, metadata = fetch(url)
            relative = Path(content_hash) / name
            save_atomic(root / relative, body)
            run["files"].append({"type": file_type, "name": name,
                                 "manifest_hash": content_hash, "relative_path": str(relative),
                                 "url": url, "http": metadata, "size_bytes": len(body),
                                 "sha256": hashlib.sha256(body).hexdigest()})

    run["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    save_atomic(root / "collection.json", json.dumps(run, ensure_ascii=False, indent=2).encode())
    print(json.dumps({"section_key": key, "signature_status": run["signature_status"],
                      "manifest_sha256": run["manifest_sha256"],
                      "downloaded": [{"type": f["type"], "name": f["name"],
                                      "size_bytes": f["size_bytes"], "sha256": f["sha256"]}
                                     for f in run["files"]],
                      "acquisition_id": acquisition_id,
                      "report": str(root / "collection.json")}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (urllib.error.URLError, ValueError, json.JSONDecodeError) as error:
        print(f"collection failed: {error}", file=sys.stderr)
        raise SystemExit(1)
