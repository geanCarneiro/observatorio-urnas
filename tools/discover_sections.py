#!/usr/bin/env python3
"""Discover official TSE section inventory and apply cumulative territory filters."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError

from collect_section import DownloadIncompleteError, decode_unverified_jws, fetch, save_atomic
from diagnostics import diagnostic


BASE_URL = "https://resultados.tse.jus.br/oficial/ele2026/arquivo-urna/{config_id}/config/{uf}/{uf}-p00{config_id}-cs.jws"
TERRITORIES = (
    "ac al ap am ba ce df es zz go ma mt ms mg pr pb pa pe pi rj rn rs ro rr sc se sp to"
).split()
MAX_LIMIT = 5000


def normalize_id(value: str | None, width: int, label: str) -> str | None:
    if value is None or value == "":
        return None
    if not re.fullmatch(rf"\d{{1,{width}}}", value):
        raise ValueError(f"{label} must be 1..{width} digits")
    return value.zfill(width)


def section_records(payload: dict, requested_uf: str) -> list[dict]:
    """Decode the documented hierarchy without assigning meaning to da/ha."""
    areas = payload.get("abr")
    if not isinstance(areas, list):
        raise ValueError("catalog payload field abr must be an array")
    records = []
    matching_areas = [area for area in areas if isinstance(area, dict) and str(area.get("cd", "")).lower() == requested_uf]
    if len(matching_areas) != 1:
        raise ValueError(f"expected one abr entry for {requested_uf}, found {len(matching_areas)}")
    municipalities = matching_areas[0].get("mu")
    if not isinstance(municipalities, list):
        raise ValueError("catalog UF entry field mu must be an array")
    for municipality in municipalities:
        if not isinstance(municipality, dict) or not isinstance(municipality.get("cd"), str):
            continue
        zones = municipality.get("zon")
        if not isinstance(zones, list):
            continue
        for zone in zones:
            if not isinstance(zone, dict) or not isinstance(zone.get("cd"), str):
                continue
            sections = zone.get("sec")
            if not isinstance(sections, list):
                continue
            for section in sections:
                if not isinstance(section, dict) or not isinstance(section.get("ns"), str):
                    continue
                records.append({
                    "uf": requested_uf,
                    "municipio": municipality["cd"].zfill(5),
                    "municipio_nome": municipality.get("nm"),
                    "zona": zone["cd"].zfill(4),
                    "secao": section["ns"].zfill(4),
                    "catalog_date_raw": section.get("da"),
                    "catalog_time_raw": section.get("ha"),
                })
    return records


def filter_sections(records: list[dict], *, uf: str | None = None, municipio: str | None = None,
                    zona: str | None = None, secao: str | None = None) -> list[dict]:
    filters = {"uf": uf, "municipio": municipio, "zona": zona, "secao": secao}
    return [record for record in records if all(value is None or record[key] == value
                                                for key, value in filters.items())]


def classify_discovery_error(error: Exception) -> tuple[str, str]:
    if isinstance(error, HTTPError):
        recoverability = "recoverable" if error.code in (408, 425, 429, 500, 502, 503, 504) else "permanent"
        return "http", recoverability
    if isinstance(error, (URLError, TimeoutError, ConnectionError, OSError)):
        return "network", "recoverable"
    if isinstance(error, (UnicodeDecodeError, json.JSONDecodeError)):
        return "decoding", "permanent"
    if isinstance(error, ValueError):
        return "semantic_validation", "permanent"
    return "internal", "indeterminate"


def discover(*, config_id: str, output: Path, uf: str | None = None, municipio: str | None = None,
             zona: str | None = None, secao: str | None = None, offset: int = 0,
             limit: int = 100, ufs: list[str] | None = None) -> dict:
    if not re.fullmatch(r"\d{4}", config_id):
        raise ValueError("config-id must contain exactly four digits")
    if offset < 0:
        raise ValueError("offset must be zero or greater")
    if not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_LIMIT}")
    requested_ufs = [uf] if uf else (ufs or TERRITORIES)
    if any(state not in TERRITORIES for state in requested_ufs):
        raise ValueError("UF must be one of the supported TSE territorial codes")

    output.mkdir(parents=True, exist_ok=True)
    catalog_dir = output / "catalogs"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    diagnostics = []
    catalog_sources = []
    records = []
    acquired_at = datetime.now(timezone.utc).isoformat()

    for state in requested_ufs:
        url = BASE_URL.format(config_id=config_id, uf=state)
        url += "?" + urlencode({"nocache": str(time.time_ns() // 1_000_000)})
        file_name = f"{state}-p00{config_id}-cs.jws"
        file_path = catalog_dir / file_name
        try:
            raw, http_metadata = fetch(url)
        except DownloadIncompleteError as error:
            partial_path = file_path.with_suffix(file_path.suffix + ".partial")
            if error.body:
                save_atomic(partial_path, error.body)
            diagnostics.append(diagnostic(
                code="SECTION_CATALOG_DOWNLOAD_INCOMPLETE", stage="collection", operation="download_section_catalog",
                error_type="download_incomplete", cause=str(error), recoverability="recoverable",
                section=state, file_type="jws", file_name=file_name, file_path=str(partial_path), exception=error,
                detected_format="JWS compact", position={"partial_bytes": len(error.body)},
                context={"url": url, "config_id": config_id, "http": error.http_metadata},
            ))
            continue
        except Exception as error:
            error_type, recoverability = classify_discovery_error(error)
            operation = "fetch_section_catalog"
            stage = "collection"
            diagnostics.append(diagnostic(
                code="SECTION_CATALOG_FETCH_FAILED", stage=stage, operation=operation,
                error_type=error_type, cause=str(error), recoverability=recoverability,
                section=state, file_type="jws", file_name=file_name, file_path=str(file_path), exception=error,
                detected_format="JWS compact", context={"url": url, "config_id": config_id},
            ))
            continue

        save_atomic(file_path, raw)
        raw_sha256 = hashlib.sha256(raw).hexdigest()
        source = {"uf": state, "url": url, "path": str(file_path), "sha256": raw_sha256,
                  "size_bytes": len(raw), "http": http_metadata, "signature_status": "not_verified"}
        catalog_sources.append(source)
        try:
            header, payload = decode_unverified_jws(raw)
            if not isinstance(payload, dict):
                raise ValueError("catalog JWS payload must be an object")
        except Exception as error:
            diagnostics.append(diagnostic(
                code="SECTION_CATALOG_JWS_PARSE_FAILED", stage="parsing", operation="decode_compact_jws",
                error_type="parsing", cause=str(error), recoverability="permanent", section=state,
                file_type="jws", file_name=file_name, file_path=str(file_path), exception=error,
                detected_format="JWS compact", context={"url": url, "sha256": raw_sha256},
            ))
            continue
        source["jws_header_unverified"] = header
        source["catalog_metadata"] = {key: payload.get(key) for key in ("dg", "hg", "idg", "f", "cdp")}
        try:
            if payload.get("cdp") not in (None, config_id):
                raise ValueError(f"catalog cdp {payload.get('cdp')!r} does not match requested {config_id}")
            records.extend(section_records(payload, state))
        except Exception as error:
            diagnostics.append(diagnostic(
                code="SECTION_CATALOG_SCHEMA_INVALID", stage="semantic_validation",
                operation="validate_section_catalog", error_type="semantic_validation", cause=str(error),
                recoverability="permanent", section=state, file_type="jws", file_name=file_name,
                file_path=str(file_path), exception=error, detected_format="JWS compact",
                context={"url": url, "sha256": raw_sha256, "config_id": config_id},
            ))

    records.sort(key=lambda item: (item["uf"], item["municipio"], item["zona"], item["secao"]))
    filters = {"uf": uf, "municipio": municipio, "zona": zona, "secao": secao}
    matching = filter_sections(records, **filters)
    page = matching[offset:offset + limit]
    page_bytes = json.dumps(page, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    report_path = output / "section-inventory.json"
    report = {
        "report_type": "official_section_inventory",
        "report_schema_version": 1,
        "config_id": config_id,
        "acquired_at_utc": acquired_at,
        "signature_status": "not_verified",
        "filters": filters,
        "catalog_sources": catalog_sources,
        "catalog_section_count_before_filters": len(records),
        "matching_section_count": len(matching),
        "selection": {"offset": offset, "limit": limit, "selected_count": len(page),
                      "next_offset": offset + len(page) if offset + len(page) < len(matching) else None,
                      "sections_sha256": hashlib.sha256(page_bytes).hexdigest()},
        "sections": page,
        "report_path": str(report_path),
        "diagnostics": diagnostics,
        "status": "complete" if not diagnostics else "partial" if catalog_sources else "failed",
        "integrity": {"raw_catalogs_preserved": True, "catalog_hashes": "sha256",
                      "cryptographic_signatures_verified": False},
    }
    save_atomic(report_path, json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8") + b"\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-id", default="3220", help="archive-urna ID used by cs.jws (default: 3220)")
    parser.add_argument("--uf", type=str.lower, choices=TERRITORIES,
                        help="limit to one UF; blank discovers all territory catalogs")
    parser.add_argument("--municipio", help="TSE municipality code; may be combined with other filters")
    parser.add_argument("--zona", help="electoral zone, retaining leading zeroes")
    parser.add_argument("--secao", help="electoral section, retaining leading zeroes")
    parser.add_argument("--offset", type=int, default=0, help="stable sorted page offset")
    parser.add_argument("--limit", type=int, default=100, help=f"maximum selected sections (1..{MAX_LIMIT})")
    parser.add_argument("--output", type=Path, default=Path("inventario"))
    args = parser.parse_args()
    try:
        municipality = normalize_id(args.municipio, 5, "municipio")
        zone = normalize_id(args.zona, 4, "zona")
        section = normalize_id(args.secao, 4, "secao")
        report = discover(config_id=args.config_id, output=args.output, uf=args.uf, municipio=municipality,
                          zona=zone, secao=section, offset=args.offset, limit=args.limit)
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps({key: report[key] for key in (
        "report_type", "config_id", "filters", "catalog_section_count_before_filters",
        "matching_section_count", "selection", "status", "report_path", "diagnostics")}, ensure_ascii=False, indent=2))
    return 0 if report["status"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
