#!/usr/bin/env python3
"""Deterministic section/turn consolidation helpers for bounded local runs."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import stat
import tempfile
from datetime import datetime, time, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Iterable


PROXY_RULE_VERSION = "queue-proxy-ba-matrix-a1-c14-v1"
REGULAR_CUTOFF = time(17, 0)
QUEUE_CLOSE_START = time(17, 0)
QUEUE_CLOSE_END = time(17, 30)
QUEUE_MAX_GAP = timedelta(minutes=13)
CONSOLIDATION_SCHEMA_VERSION = 1

CSV_COLUMNS = (
    "uf", "municipio", "municipio_nome", "zona", "secao", "section_key", "turno",
    "votos_dentro_proxy", "votos_fora_proxy", "votos_pos_17_sem_classificacao",
    "votos_confirmados_presidente_log", "votos_nulos_apos_suspensao_presidente_log",
    "fluxos_log_completos_duplicados_excluidos", "fluxos_log_unicos",
    "votos_presidente_rdv", "votos_presidente_bu",
    "diferenca_log_menos_rdv", "diferenca_log_menos_bu", "diferenca_rdv_menos_bu",
    "flag_anomalia_log_rdv_bu", "status_comparacao_log_rdv_bu",
    "votos_22_total", "votos_22_rdv", "votos_22_bu", "percentual_validos_22",
    "votos_22_normalizados", "percentual_normalizado_22",
    "votos_13_total", "votos_13_rdv", "votos_13_bu", "percentual_validos_13",
    "votos_13_normalizados", "percentual_normalizado_13", "total_validos_rdv",
    "total_validos_bu", "base_calculo_candidatos",
    "ultimo_voto_confirmado_raw", "ultima_confirmacao_presidente_raw", "inicio_encerramento_raw",
    "intervalo_ultimo_voto_ate_encerramento_segundos", "classificacao_proxy", "versao_regra_proxy",
    "status_processamento", "diagnosticos_processamento", "data_eleicao", "id_pleito",
    "hash_manifesto", "hash_bu", "hash_rdv", "hashes_log_json",
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _reparse_point(path: Path) -> bool:
    try:
        attrs = path.lstat().st_file_attributes
    except (AttributeError, OSError):
        return path.is_symlink()
    return bool(attrs & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def clean_data_root(data_root: Path, project_root: Path) -> int:
    """Delete children of the designated project data directory, never following reparse points."""
    project_root = project_root.resolve()
    expected = (project_root / "dados").resolve()
    data_root = data_root.absolute()
    if data_root != project_root / "dados" or data_root.resolve() != expected:
        raise ValueError(f"refusing to clean unexpected data root: {data_root}")
    if not data_root.exists():
        data_root.mkdir(parents=True, exist_ok=True)
        return 0
    if _reparse_point(data_root):
        raise ValueError(f"refusing to clean reparse-point data root: {data_root}")

    removed = 0
    for child in data_root.iterdir():
        if _reparse_point(child):
            raise ValueError(f"refusing to clean reparse point inside data root: {child}")
        if child.is_dir():
            for current, directories, files in os.walk(child, topdown=True, followlinks=False):
                current_path = Path(current)
                for name in directories + files:
                    candidate = current_path / name
                    if _reparse_point(candidate):
                        raise ValueError(f"refusing to clean reparse point inside data root: {candidate}")
            shutil.rmtree(child)
        else:
            child.unlink()
        removed += 1
    return removed


def clean_section_data(data_root: Path, project_root: Path, section: dict) -> int:
    """Remove one verified physical section tree without touching sibling sections."""
    project_root = project_root.resolve()
    expected_root = project_root / "dados"
    data_root = data_root.absolute()
    if data_root != expected_root or data_root.resolve() != expected_root:
        raise ValueError(f"refusing to clean unexpected data root: {data_root}")
    parts = [str(section.get(name, "")) for name in ("uf", "municipio", "zona", "secao")]
    if any(not value or value in {".", ".."} or "/" in value or "\\" in value for value in parts):
        raise ValueError("refusing to clean section with unsafe path components")
    target = data_root.joinpath(*parts)
    if not target.exists():
        return 0
    if _reparse_point(data_root):
        raise ValueError(f"refusing to clean reparse-point data root: {data_root}")
    current = data_root
    for component in parts:
        current = current / component
        if not current.exists():
            return 0
        if _reparse_point(current):
            raise ValueError(f"refusing to clean reparse point in section path: {current}")
    resolved_target = target.resolve()
    try:
        resolved_target.relative_to(data_root.resolve())
    except ValueError as error:
        raise ValueError(f"refusing to clean section outside data root: {target}") from error
    if not target.is_dir():
        target.unlink()
    else:
        for current_path, directories, files in os.walk(target, topdown=True, followlinks=False):
            for name in directories + files:
                candidate = Path(current_path) / name
                if _reparse_point(candidate):
                    raise ValueError(f"refusing to clean reparse point inside section: {candidate}")
        shutil.rmtree(target)
    return 1


def _cargo(entries: Iterable[dict], *, is_bu: bool) -> dict | None:
    for entry in entries:
        if is_bu:
            code = entry.get("cargo")
            if str(code) == "1" or str(code).casefold() == "presidente":
                return entry
        elif str(entry.get("cargo_code")) == "1" and entry.get("cargo_kind") == "cargoConstitucional":
            return entry
    return None


def _candidate_total(cargo: dict | None, candidate: int, *, is_bu: bool) -> int | None:
    if cargo is None:
        return None
    key = "nominal_by_number" if is_bu else "candidate_number_counts_from_nominal_votes"
    counts = cargo.get(key)
    if not isinstance(counts, dict):
        return None
    value = counts.get(str(candidate), 0)
    return int(value) if isinstance(value, (int, float)) else None


def _valid_total(cargo: dict | None, *, is_bu: bool) -> int | None:
    if cargo is None:
        return None
    if is_bu:
        total = cargo.get("qtd_votos_bu")
        categories = cargo.get("vote_type_counts", {})
        if not isinstance(total, int) or not isinstance(categories, dict):
            return None
        invalid = sum(int(value) for key, value in categories.items()
                      if str(key).casefold().startswith(("branco", "nulo")) and isinstance(value, int))
        return total - invalid
    total = cargo.get("record_count")
    white, null = cargo.get("white_votes_total"), cargo.get("null_votes_total")
    if not all(isinstance(value, int) for value in (total, white, null)):
        return None
    return total - white - null


def _pct(numerator: int | None, denominator: int | None) -> str:
    if numerator is None or denominator is None or denominator <= 0:
        return ""
    value = (Decimal(numerator) * Decimal(100) / Decimal(denominator)).quantize(
        Decimal("0.000001"), rounding=ROUND_HALF_UP)
    return format(value, "f")


def _normalized(candidate_votes: int | None, proxy_votes: int | None,
                valid_votes: int | None) -> str:
    if candidate_votes is None or proxy_votes is None or valid_votes is None or valid_votes <= 0:
        return ""
    try:
        value = (Decimal(candidate_votes) * Decimal(proxy_votes) / Decimal(valid_votes)).quantize(
            Decimal("0.000001"), rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return ""
    return format(value, "f")


def _event_datetime(event: dict) -> datetime | None:
    value = event.get("event_timestamp_raw")
    try:
        return datetime.strptime(value, "%d/%m/%Y %H:%M:%S") if isinstance(value, str) else None
    except ValueError:
        return None


def _proxy_metrics(events: list[dict], election_date: str | None) -> dict:
    day_events = [event for event in events if election_date and event.get("event_date") == election_date]
    confirmations = [event for event in day_events
                     if str(event.get("event_type", "")).startswith("voto_confirmado_")]
    presidential = [event for event in confirmations if event.get("cargo") == "presidente"]
    parsed_confirmations = [(event, _event_datetime(event)) for event in confirmations]
    parsed_confirmations = [(event, stamp) for event, stamp in parsed_confirmations if stamp is not None]
    presidential_times = [(event, _event_datetime(event)) for event in presidential]
    presidential_times = [(event, stamp) for event, stamp in presidential_times if stamp is not None]
    closures = [(event, _event_datetime(event)) for event in day_events
                if event.get("event_type") == "inicio_encerramento"]
    closures = [(event, stamp) for event, stamp in closures if stamp is not None]
    last_confirmation = max(parsed_confirmations, key=lambda item: item[1])[1] if parsed_confirmations else None
    last_presidential = max(presidential_times, key=lambda item: item[1])[1] if presidential_times else None
    closure = max(closures, key=lambda item: item[1])[1] if closures else None

    regular_count = sum(stamp.time() <= REGULAR_CUTOFF for _event, stamp in presidential_times)
    after_cut = sum(stamp.time() > REGULAR_CUTOFF for _event, stamp in presidential_times)
    gap = closure - last_confirmation if closure and last_confirmation else None
    queue_proxy = bool(closure and gap is not None
        and QUEUE_CLOSE_START <= closure.time() < QUEUE_CLOSE_END
        and timedelta(0) <= gap < QUEUE_MAX_GAP)
    if not after_cut:
        classification = "sem_votos_presidenciais_apos_17h"
        inside, outside, unclassified = regular_count, 0, 0
    elif not closure or not last_confirmation or gap is None or gap < timedelta(0):
        classification = "indeterminado_fechamento_ou_ultimo_voto"
        inside, outside, unclassified = regular_count, None, after_cut
    elif queue_proxy:
        classification = "proxy_fila"
        inside, outside, unclassified = regular_count + after_cut, 0, 0
    else:
        classification = "fora_proxy_horario_extra"
        inside, outside, unclassified = regular_count, after_cut, 0

    return {
        "votos_dentro_proxy": inside, "votos_fora_proxy": outside,
        "votos_pos_17_sem_classificacao": unclassified,
        "ultimo_voto_confirmado_raw": last_confirmation.strftime("%d/%m/%Y %H:%M:%S") if last_confirmation else "",
        "ultima_confirmacao_presidente_raw": last_presidential.strftime("%d/%m/%Y %H:%M:%S") if last_presidential else "",
        "inicio_encerramento_raw": closure.strftime("%d/%m/%Y %H:%M:%S") if closure else "",
        "intervalo_ultimo_voto_ate_encerramento_segundos": int(gap.total_seconds()) if gap is not None else "",
        "classificacao_proxy": classification,
    }


def build_summary_row(section: dict, turn: int, report: dict) -> tuple[dict, bool]:
    """Build one compact section/turn row and whether raw evidence should be retained."""
    key = "/".join(str(section[name]) for name in ("uf", "municipio", "zona", "secao"))
    if section.get("aggregation_role") == "aggregated":
        row = {column: "" for column in CSV_COLUMNS}
        row.update({"uf": str(section["uf"]).upper(), "municipio": str(section["municipio"]).zfill(5),
            "municipio_nome": section.get("municipio_nome") or "", "zona": str(section["zona"]).zfill(4),
            "secao": str(section["secao"]).zfill(4), "section_key": key, "turno": int(turn),
            "flag_anomalia_log_rdv_bu": "nao_aplicavel", "status_comparacao_log_rdv_bu": "nao_aplicavel",
            "versao_regra_proxy": PROXY_RULE_VERSION, "status_processamento": "agregada"})
        return row, False

    processing = report.get("file_processing", {})
    bu_results = [item.get("data") for item in processing.get("bus", []) if isinstance(item.get("data"), dict)]
    rdv_results = [item for item in processing.get("rdvs", []) if item.get("status") == "decoded"]
    bu = bu_results[0] if bu_results else None
    bu_cargo = _cargo((cargo for election in (bu or {}).get("elections", [])
                       for result in election.get("results", []) for cargo in result.get("cargos", [])), is_bu=True)
    rdv_cargos = [cargo for item in rdv_results for election in item.get("elections", [])
                  for cargo in election.get("cargos", [])]
    rdv_cargo = _cargo(rdv_cargos, is_bu=False)
    election_date = (bu or {}).get("election_date")

    events = report.get("relevant_events", [])
    proxy = _proxy_metrics(events, election_date)
    confirmations = suspension_null_votes = None
    for item in report.get("validation", {}).get("bu_reconciliation", {}).get("by_cargo", []):
        if str(item.get("cargo")).casefold() == "presidente":
            confirmations = item.get("explicit_confirmation_count")
            suspension_null_votes = item.get("suspension_assigned_null_vote_count")
            break
    if confirmations is None and election_date:
        confirmations = sum(event.get("event_type") == "voto_confirmado_presidente"
                            and event.get("event_date") == election_date for event in events)

    bu_total = bu_cargo.get("qtd_votos_bu") if bu_cargo else None
    rdv_total = rdv_cargo.get("record_count") if rdv_cargo else None
    diff_log_rdv = confirmations - rdv_total if confirmations is not None and rdv_total is not None else None
    diff_log_bu = confirmations - bu_total if confirmations is not None and bu_total is not None else None
    diff_rdv_bu = rdv_total - bu_total if rdv_total is not None and bu_total is not None else None
    compare_available = confirmations is not None and rdv_total is not None and bu_total is not None
    anomaly = ("indeterminado" if not compare_available else
               "true" if diff_log_rdv != 0 or diff_log_bu != 0 else "false")
    compare_status = ("indeterminado" if not compare_available else "divergencia" if anomaly == "true" else "compativel")

    valid_rdv, valid_bu = _valid_total(rdv_cargo, is_bu=False), _valid_total(bu_cargo, is_bu=True)
    candidate_totals = {}
    candidate_rdv = {}
    candidate_bu = {}
    for candidate in (22, 13):
        rdv_votes = _candidate_total(rdv_cargo, candidate, is_bu=False)
        bu_votes = _candidate_total(bu_cargo, candidate, is_bu=True)
        selected = rdv_votes if rdv_votes is not None else bu_votes
        denominator = valid_rdv if rdv_votes is not None else valid_bu
        candidate_totals[candidate] = selected
        candidate_rdv[candidate] = rdv_votes
        candidate_bu[candidate] = bu_votes
        candidate_totals[f"pct_{candidate}"] = _pct(selected, denominator)
        candidate_totals[f"normalized_{candidate}"] = _normalized(
            selected, proxy.get("votos_dentro_proxy"), denominator)
        candidate_totals[f"normalized_pct_{candidate}"] = (
            _pct(selected, denominator)
            if selected is not None and denominator and denominator > 0
            and proxy.get("votos_dentro_proxy") not in (None, 0) else "")

    source_hashes = report.get("source_files", {})
    log_hashes = source_hashes.get("log_sha256", [])
    status = "processado" if bu and rdv_cargo and confirmations is not None else "parcial"
    if not bu and not rdv_cargo and confirmations is None:
        status = "falha"
    diagnostics = report.get("diagnostics", [])
    log_accounting = report.get("log_record_accounting", {})
    row = {
        "uf": str(section["uf"]).upper(), "municipio": str(section["municipio"]).zfill(5),
        "municipio_nome": section.get("municipio_nome") or "", "zona": str(section["zona"]).zfill(4),
        "secao": str(section["secao"]).zfill(4), "section_key": key, "turno": int(turn),
        "votos_dentro_proxy": proxy["votos_dentro_proxy"], "votos_fora_proxy": proxy["votos_fora_proxy"],
        "votos_pos_17_sem_classificacao": proxy["votos_pos_17_sem_classificacao"],
        "votos_confirmados_presidente_log": confirmations,
        "votos_nulos_apos_suspensao_presidente_log": suspension_null_votes,
        "fluxos_log_completos_duplicados_excluidos": log_accounting.get("complete_duplicate_log_count"),
        "fluxos_log_unicos": log_accounting.get("unique_log_stream_count"),
        "votos_presidente_rdv": rdv_total,
        "votos_presidente_bu": bu_total, "diferenca_log_menos_rdv": diff_log_rdv,
        "diferenca_log_menos_bu": diff_log_bu, "diferenca_rdv_menos_bu": diff_rdv_bu,
        "flag_anomalia_log_rdv_bu": anomaly, "status_comparacao_log_rdv_bu": compare_status,
        "votos_22_total": candidate_totals[22], "votos_22_rdv": candidate_rdv[22],
        "votos_22_bu": candidate_bu[22], "percentual_validos_22": candidate_totals["pct_22"],
        "votos_22_normalizados": candidate_totals["normalized_22"],
        "percentual_normalizado_22": candidate_totals["normalized_pct_22"],
        "votos_13_total": candidate_totals[13], "votos_13_rdv": candidate_rdv[13],
        "votos_13_bu": candidate_bu[13], "percentual_validos_13": candidate_totals["pct_13"],
        "votos_13_normalizados": candidate_totals["normalized_13"],
        "percentual_normalizado_13": candidate_totals["normalized_pct_13"],
        "total_validos_rdv": valid_rdv, "total_validos_bu": valid_bu,
        "base_calculo_candidatos": "RDV" if rdv_cargo else "BU" if bu_cargo else "",
        "ultimo_voto_confirmado_raw": proxy["ultimo_voto_confirmado_raw"],
        "ultima_confirmacao_presidente_raw": proxy["ultima_confirmacao_presidente_raw"],
        "inicio_encerramento_raw": proxy["inicio_encerramento_raw"],
        "intervalo_ultimo_voto_ate_encerramento_segundos": proxy["intervalo_ultimo_voto_ate_encerramento_segundos"],
        "classificacao_proxy": proxy["classificacao_proxy"], "versao_regra_proxy": PROXY_RULE_VERSION,
        "status_processamento": status, "diagnosticos_processamento": len(diagnostics),
        "data_eleicao": election_date or "", "id_pleito": (bu or {}).get("pleito_id") or "",
        "hash_manifesto": source_hashes.get("manifest_sha256") or "",
        "hash_bu": source_hashes.get("bu_sha256") or "",
        "hash_rdv": json.dumps(source_hashes.get("rdv_sha256", []), ensure_ascii=False, separators=(",", ":")),
        "hashes_log_json": json.dumps(log_hashes, ensure_ascii=False, separators=(",", ":")),
    }
    evidence_required = anomaly != "false" or status != "processado" or proxy["votos_pos_17_sem_classificacao"] not in (0, None)
    return row, evidence_required


def write_batch_csv(path: Path, rows: list[dict]) -> str:
    """Atomically write a stable, sorted UTF-8 CSV and return its SHA-256."""
    path.parent.mkdir(parents=True, exist_ok=True)
    sorted_rows = sorted(rows, key=lambda row: (row["uf"], row["municipio"], row["zona"], row["secao"], row["turno"]))
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=CSV_COLUMNS, extrasaction="ignore", lineterminator="\n")
            writer.writeheader()
            writer.writerows(sorted_rows)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return _sha256_file(path)


def write_batch_manifest(path: Path, *, batch_id: str, row_count: int, csv_path: Path,
                         schema_hashes: dict, state_code: str | None = None,
                         rule_version: str = PROXY_RULE_VERSION) -> dict:
    manifest = {
        "consolidation_schema_version": CONSOLIDATION_SCHEMA_VERSION,
        "proxy_rule_version": rule_version,
        "batch_id": batch_id,
        "uf": state_code,
        "row_count": row_count,
        "csv_file": csv_path.name,
        "csv_sha256": _sha256_file(csv_path),
        "csv_columns": list(CSV_COLUMNS),
        "candidate_formula": "candidate_valid_votes * votes_inside_proxy / total_valid_votes",
        "normalized_percentage_formula": "normalized_candidate_votes / votes_inside_proxy * 100",
        "anomaly_definition": "explicit presidential log confirmations compared with presidential RDV and BU totals; indeterminate when either source is unavailable",
        "proxy_matrix_selected_range": "A1:C14",
        "proxy_definition": "all presidential confirmations through 17:00 are regular; later presidential confirmations are queue proxy only when the gap from the last explicit confirmation of any cargo to the log start-of-closure event is 0 through 12:59 and closure is between 17:00 and 17:29:59",
        "timezone_note": "Log clock values are used as recorded; their timezone has not been independently validated.",
        "schema_hashes": schema_hashes,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)
    return manifest


def preserve_section_evidence(source_root: Path, destination_root: Path, section: dict) -> Path | None:
    source = source_root.joinpath(section["uf"], section["municipio"], section["zona"], section["secao"])
    if not source.is_dir():
        return None
    destination = destination_root.joinpath(section["uf"], section["municipio"], section["zona"], section["secao"])
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, destination, dirs_exist_ok=True)
    return destination


def verify_tree_copy(source: Path, destination: Path) -> dict:
    """Verify a preserved evidence tree has exactly the same files and bytes as its source."""
    def inventory(root: Path) -> dict[str, tuple[int, str]]:
        if not root.is_dir() or _reparse_point(root):
            raise ValueError(f"evidence tree is absent or unsafe: {root}")
        result = {}
        for current, directories, files in os.walk(root, topdown=True, followlinks=False):
            current_path = Path(current)
            for name in directories:
                if _reparse_point(current_path / name):
                    raise ValueError(f"evidence tree contains a reparse point: {current_path / name}")
            for name in files:
                path = current_path / name
                if _reparse_point(path) or not path.is_file():
                    raise ValueError(f"evidence tree contains an unsafe file: {path}")
                result[path.relative_to(root).as_posix()] = (path.stat().st_size, _sha256_file(path))
        return result

    source_files = inventory(source)
    destination_files = inventory(destination)
    if source_files != destination_files:
        missing = sorted(set(source_files) - set(destination_files))
        extra = sorted(set(destination_files) - set(source_files))
        changed = sorted(path for path in set(source_files) & set(destination_files)
                         if source_files[path] != destination_files[path])
        raise ValueError(f"evidence copy differs from source: missing={len(missing)}, extra={len(extra)}, changed={len(changed)}")
    return {"verified_files": len(source_files),
            "verified_bytes": sum(size for size, _digest in source_files.values())}
