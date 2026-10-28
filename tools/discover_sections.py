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
from section_aggregation import aggregation_counts, link_aggregations


BASE_URL = "https://resultados.tse.jus.br/oficial/ele2026/arquivo-urna/{config_id}/config/{uf}/{uf}-p00{config_id}-cs.jws"
TERRITORIES = (
    "ac al ap am ba ce df es zz go ma mt ms mg pr pb pa pe pi rj rn rs ro rr sc se sp to"
).split()


def normalize_filter_values(value: str | list[str] | tuple[str, ...] | None, width: int,
                            label: str, *, lowercase: bool = False) -> list[str] | None:
    """Parse a comma-separated filter while preserving zero-padded identifiers."""
    if value is None or value == "":
        return None
    parts = value.split(",") if isinstance(value, str) else list(value)
    normalized = []
    for part in parts:
        token = str(part).strip()
        if not token:
            raise ValueError(f"{label} list contains an empty value")
        if lowercase:
            token = token.lower()
        if not re.fullmatch(rf"[a-z]{{2}}" if lowercase else rf"\d{{1,{width}}}", token):
            expected = "two letters" if lowercase else f"1..{width} digits"
            raise ValueError(f"{label} values must contain {expected}: {token!r}")
        if not lowercase:
            token = token.zfill(width)
        if token not in normalized:
            normalized.append(token)
    return normalized


def validate_filter_hierarchy(uf: list[str] | None, municipio: list[str] | None,
                              zona: list[str] | None, secao: list[str] | None) -> None:
    if municipio and not uf:
        raise ValueError("municipio filter requires at least one parent UF filter")
    if zona and not municipio:
        raise ValueError("zona filter requires at least one parent municipio filter")
    if secao and not zona:
        raise ValueError("secao filter requires at least one parent zona filter")


def validate_filter_membership(records: list[dict], uf: list[str] | None,
                               municipio: list[str] | None, zona: list[str] | None,
                               secao: list[str] | None) -> None:
    """Require each requested child identifier to exist under its selected parents."""
    scope = records
    for key, values, label in (("uf", uf, "UF"), ("municipio", municipio, "municipio"),
                               ("zona", zona, "zona"), ("secao", secao, "secao")):
        if not values:
            continue
        available = {record[key] for record in scope}
        missing = [value for value in values if value not in available]
        if missing:
            raise ValueError(f"{label} value(s) not found under the selected parent filters: {', '.join(missing)}")
        selected = set(values)
        scope = [record for record in scope if record[key] in selected]


def _report_filter(values: list[str] | None):
    if not values:
        return None
    return values[0] if len(values) == 1 else values


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
                    "nsp": section.get("nsp"),
                    "nsa": section.get("nsa"),
                })
    return records


def filter_sections(records: list[dict], *, uf: str | list[str] | None = None,
                    municipio: str | list[str] | None = None, zona: str | list[str] | None = None,
                    secao: str | list[str] | None = None) -> list[dict]:
    filters = {"uf": uf, "municipio": municipio, "zona": zona, "secao": secao}
    return [record for record in records if all(value is None or record[key] in (set(value) if isinstance(value, (list, tuple)) else {value})
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


def discover(*, config_id: str, output: Path, uf: str | list[str] | None = None,
             municipio: str | list[str] | None = None, zona: str | list[str] | None = None,
             secao: str | list[str] | None = None, offset: int = 0,
             limit: int | None = None, ufs: list[str] | None = None) -> dict:
    if not re.fullmatch(r"\d{4}", config_id):
        raise ValueError("config-id must contain exactly four digits")
    if offset < 0:
        raise ValueError("offset must be zero or greater")
    if limit is not None and limit < 1:
        raise ValueError("limit must be positive when provided")
    uf_values = normalize_filter_values(uf, 2, "uf", lowercase=True)
    municipality_values = normalize_filter_values(municipio, 5, "municipio")
    zone_values = normalize_filter_values(zona, 4, "zona")
    section_values = normalize_filter_values(secao, 4, "secao")
    validate_filter_hierarchy(uf_values, municipality_values, zone_values, section_values)
    requested_ufs = uf_values or (normalize_filter_values(ufs, 2, "uf", lowercase=True) if ufs else TERRITORIES)
    if any(state not in TERRITORIES for state in requested_ufs):
        raise ValueError("UF must be one of the supported TSE territorial codes")

    output.mkdir(parents=True, exist_ok=True)
    catalog_dir = output / "catalogs"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    diagnostics = []
    catalog_sources = []
    records = []
    successfully_parsed_catalogs = set()
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
            successfully_parsed_catalogs.add(state)
        except Exception as error:
            diagnostics.append(diagnostic(
                code="SECTION_CATALOG_SCHEMA_INVALID", stage="semantic_validation",
                operation="validate_section_catalog", error_type="semantic_validation", cause=str(error),
                recoverability="permanent", section=state, file_type="jws", file_name=file_name,
                file_path=str(file_path), exception=error, detected_format="JWS compact",
                context={"url": url, "sha256": raw_sha256, "config_id": config_id},
            ))

    records, aggregation_diagnostics = link_aggregations(records)
    diagnostics.extend(aggregation_diagnostics)
    records.sort(key=lambda item: (item["uf"], item["municipio"], item["zona"], item["secao"]))
    child_filters_selected = any((municipality_values, zone_values, section_values))
    if child_filters_selected and len(successfully_parsed_catalogs) != len(requested_ufs):
        raise ValueError("cannot validate child filters because one or more selected UF catalogs are unavailable or invalid")
    if len(successfully_parsed_catalogs) == len(requested_ufs):
        validate_filter_membership(records, uf_values, municipality_values, zone_values, section_values)
    filter_values = {"uf": uf_values, "municipio": municipality_values,
                     "zona": zone_values, "secao": section_values}
    matching = filter_sections(records, **filter_values)
    filters = {key: _report_filter(values) for key, values in filter_values.items()}
    page = matching[offset:] if limit is None else matching[offset:offset + limit]
    matching_counts = aggregation_counts(matching)
    selected_counts = aggregation_counts(page)
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
        "section_counts": {**matching_counts,
            "selected_registered_sections": selected_counts["registered_sections"],
            "selected_aggregated_sections": selected_counts["aggregated_sections"],
            "selected_aggregation_principal_sections": selected_counts["aggregation_principal_sections"],
            "selected_principal_collectable_sections": selected_counts["principal_collectable_sections"],
            "effectively_collected_sections": None,
            "real_failures": None},
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
    parser.add_argument("--uf", help="comma-separated UFs; blank discovers all territory catalogs")
    parser.add_argument("--municipio", help="comma-separated TSE municipality codes")
    parser.add_argument("--zona", help="comma-separated zones, retaining leading zeroes")
    parser.add_argument("--secao", help="comma-separated sections, retaining leading zeroes")
    parser.add_argument("--offset", type=int, default=0, help="stable sorted page offset")
    parser.add_argument("--limit", type=int, default=None,
                        help="maximum selected sections (omit to select all matching sections)")
    parser.add_argument("--output", type=Path, default=Path("inventario"))
    args = parser.parse_args()
    try:
        report = discover(config_id=args.config_id, output=args.output, uf=args.uf, municipio=args.municipio,
                          zona=args.zona, secao=args.secao, offset=args.offset, limit=args.limit)
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps({key: report[key] for key in (
        "report_type", "config_id", "filters", "catalog_section_count_before_filters",
        "matching_section_count", "selection", "status", "report_path", "diagnostics")}, ensure_ascii=False, indent=2))
    return 0 if report["status"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
