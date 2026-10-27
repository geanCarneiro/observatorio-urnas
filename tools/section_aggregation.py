"""Interpret nsp/nsa section relationships from official TSE catalogs."""

from __future__ import annotations

import re
from collections import defaultdict

from diagnostics import diagnostic


def section_key(record: dict) -> str:
    return "/".join((record["uf"], record["municipio"], record["zona"], record["secao"]))


def _section_number(value: object) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    raw = str(value)
    return raw.zfill(4) if re.fullmatch(r"\d{1,4}", raw) else None


def _related_key(record: dict, section_number: str) -> str:
    return "/".join((record["uf"], record["municipio"], record["zona"], section_number))


def _aggregation_diagnostic(code: str, section: str, cause: str, context: dict) -> dict:
    return diagnostic(code=code, stage="semantic_validation", operation="validate_section_aggregation",
        error_type="semantic_validation", cause=cause, recoverability="indeterminate",
        section=section, file_type="section_catalog", file_name=None, file_path=None,
        detected_format="JWS/catalog JSON", context=context)


def link_aggregations(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """Return enriched records and diagnostics while preserving catalog nsp/nsa values."""
    enriched = [dict(record) for record in records]
    diagnostics = []
    invalid_keys: set[str] = set()
    by_key: dict[str, dict] = {}
    duplicate_keys: set[str] = set()
    for record in enriched:
        key = section_key(record)
        if key in by_key:
            duplicate_keys.add(key)
            diagnostics.append(_aggregation_diagnostic("SECTION_AGGREGATION_DUPLICATE_SECTION", key,
                "The same section identity occurs more than once in the catalog.", {"section_key": key}))
            invalid_keys.add(key)
        else:
            by_key[key] = record

    nsp_by_key: dict[str, str | None] = {}
    nsa_by_key: dict[str, list[str]] = {}
    parent_keys: dict[str, set[str]] = defaultdict(set)
    child_keys: dict[str, set[str]] = defaultdict(set)
    graph: dict[str, set[str]] = defaultdict(set)

    for record in enriched:
        key = section_key(record)
        raw_nsp = record.get("nsp")
        raw_nsa = record.get("nsa")
        nsp_present = raw_nsp is not None and raw_nsp != ""
        nsp = _section_number(raw_nsp) if nsp_present else None
        if nsp_present and nsp is None:
            diagnostics.append(_aggregation_diagnostic("SECTION_AGGREGATION_NSP_INVALID", key,
                "nsp does not contain a one-to-four digit principal section number.", {"nsp": raw_nsp}))
            invalid_keys.add(key)
        nsp_by_key[key] = nsp

        if raw_nsa is None:
            raw_nsa = []
        if not isinstance(raw_nsa, list):
            diagnostics.append(_aggregation_diagnostic("SECTION_AGGREGATION_NSA_INVALID", key,
                "nsa must be an array of aggregated section numbers.", {"nsa": raw_nsa}))
            invalid_keys.add(key)
            raw_nsa = []
        normalized_nsa = []
        for index, raw_child in enumerate(raw_nsa):
            child = _section_number(raw_child)
            if child is None:
                diagnostics.append(_aggregation_diagnostic("SECTION_AGGREGATION_NSA_ENTRY_INVALID", key,
                    "An nsa entry does not contain a one-to-four digit section number.",
                    {"nsa_index": index, "nsa_entry": raw_child}))
                invalid_keys.add(key)
                continue
            if child in normalized_nsa:
                diagnostics.append(_aggregation_diagnostic("SECTION_AGGREGATION_DUPLICATE_REFERENCE", key,
                    "nsa lists the same aggregated section more than once.", {"nsa_index": index, "section": child}))
                invalid_keys.add(key)
            normalized_nsa.append(child)
            child_key = _related_key(record, child)
            parent_keys[child_key].add(key)
            child_keys[key].add(child_key)
            graph[child_key].add(key)
        nsa_by_key[key] = normalized_nsa

        if nsp is not None:
            parent_key = _related_key(record, nsp)
            parent_keys[key].add(parent_key)
            child_keys[parent_key].add(key)
            graph[key].add(parent_key)
            if parent_key == key:
                diagnostics.append(_aggregation_diagnostic("SECTION_AGGREGATION_SELF_REFERENCE", key,
                    "A section cannot be aggregated into itself.", {"nsp": raw_nsp}))
                invalid_keys.add(key)

    for child_key, parent_number in sorted(nsp_by_key.items()):
        record = by_key[child_key]
        raw_nsp = record.get("nsp")
        if raw_nsp is None or raw_nsp == "" or parent_number is None:
            continue
        parent_key = _related_key(record, parent_number)
        parent = by_key.get(parent_key)
        if parent is None:
            diagnostics.append(_aggregation_diagnostic("SECTION_AGGREGATION_PRINCIPAL_MISSING", child_key,
                "nsp references a principal section absent from the catalog.",
                {"nsp": raw_nsp, "principal_section_key": parent_key}))
            invalid_keys.add(child_key)
            continue
        if record["secao"] not in nsa_by_key.get(parent_key, []):
            diagnostics.append(_aggregation_diagnostic("SECTION_AGGREGATION_LINK_MISMATCH", child_key,
                "The child's nsp reference is not reciprocated by the principal section's nsa list.",
                {"principal_section_key": parent_key, "child_section_key": child_key,
                 "principal_nsa": parent.get("nsa")}))
            invalid_keys.update((child_key, parent_key))

    for principal_key, child_numbers in sorted(nsa_by_key.items()):
        principal = by_key[principal_key]
        for child_number in child_numbers:
            child_key = _related_key(principal, child_number)
            child = by_key.get(child_key)
            if child is None:
                # Already diagnosed above; this branch adds the precise nsa origin.
                diagnostics.append(_aggregation_diagnostic("SECTION_AGGREGATION_CHILD_MISSING", principal_key,
                    "nsa references an aggregated section absent from the catalog.",
                    {"nsa_entry": child_number, "missing_section_key": child_key}))
                invalid_keys.add(principal_key)
                continue
            if nsp_by_key.get(child_key) != principal["secao"]:
                diagnostics.append(_aggregation_diagnostic("SECTION_AGGREGATION_LINK_MISMATCH", principal_key,
                    "The principal's nsa entry is not reciprocated by the child's nsp field.",
                    {"principal_section_key": principal_key, "child_section_key": child_key,
                     "child_nsp": child.get("nsp")}))
                invalid_keys.update((principal_key, child_key))

    # Detect directed cycles in child -> principal relationships.
    colors: dict[str, int] = {}
    stack: list[str] = []
    reported_cycles: set[tuple[str, ...]] = set()

    def visit(node: str) -> None:
        colors[node] = 1
        stack.append(node)
        for parent in sorted(graph.get(node, ())):
            if parent not in by_key:
                continue
            if colors.get(parent, 0) == 0:
                visit(parent)
            elif colors.get(parent) == 1:
                start = stack.index(parent)
                cycle = tuple(stack[start:] + [parent])
                canonical = min(tuple(cycle[i:-1] + cycle[:i] + (cycle[i],)) for i in range(len(cycle) - 1))
                if canonical not in reported_cycles:
                    reported_cycles.add(canonical)
                    diagnostics.append(_aggregation_diagnostic("SECTION_AGGREGATION_CYCLE", node,
                        "Section aggregation relationships contain a cycle.", {"cycle": list(cycle)}))
                    invalid_keys.update(cycle)
        stack.pop()
        colors[node] = 2

    for key in sorted(by_key):
        if colors.get(key, 0) == 0:
            visit(key)

    for record in enriched:
        key = section_key(record)
        parents = sorted(parent_keys.get(key, ()))
        children = sorted(child_keys.get(key, ()))
        raw_nsp = record.get("nsp")
        raw_nsa = record.get("nsa")
        nsp_declared = raw_nsp is not None and raw_nsp != ""
        nsa_declared = isinstance(raw_nsa, list) and bool(raw_nsa)
        is_child = bool(parents or nsp_declared)
        is_principal = bool(children or nsa_declared)
        role = "aggregated" if is_child else "principal" if is_principal else "independent"
        if is_child and is_principal:
            diagnostics.append(_aggregation_diagnostic("SECTION_AGGREGATION_ROLE_CONFLICT", key,
                "A section is both aggregated and principal in the catalog relationships.",
                {"principal_section_keys": parents, "aggregated_section_keys": children}))
            invalid_keys.add(key)
        record.update({
            "principal_section_key": parents[0] if len(parents) == 1 else None,
            "principal_section_keys": parents,
            "aggregated_section_keys": children,
            "aggregation_role": role,
            "aggregation_validation_status": "inconsistent" if key in invalid_keys else "valid" if parents or children or nsp_declared or nsa_declared else "not_applicable",
            "collectable": role != "aggregated",
            "collection_status": "aggregated" if role == "aggregated" else "pending",
        })
        if len(parents) > 1:
            diagnostics.append(_aggregation_diagnostic("SECTION_AGGREGATION_MULTIPLE_PRINCIPALS", key,
                "A section is linked to more than one principal section.", {"principal_section_keys": parents}))
            record["aggregation_validation_status"] = "inconsistent"
            record["collectable"] = False
            record["collection_status"] = "aggregated"

    return enriched, diagnostics


def is_aggregated(record: dict) -> bool:
    return record.get("aggregation_role") == "aggregated" or bool(record.get("nsp"))


def aggregation_counts(records: list[dict]) -> dict:
    aggregated = sum(is_aggregated(record) for record in records)
    principals = sum(record.get("aggregation_role") == "principal" for record in records)
    return {"registered_sections": len(records), "aggregated_sections": aggregated,
            "aggregation_principal_sections": principals,
            "principal_collectable_sections": sum(record.get("collectable", not is_aggregated(record))
                                                   for record in records)}
