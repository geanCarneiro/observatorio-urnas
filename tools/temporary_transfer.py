#!/usr/bin/env python3
"""Read and reconcile the preserved official TSE temporary-transfer reference."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import tempfile
import urllib.request
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path


RESOURCE_ID = "4162f440-1037-4b36-9529-9544120604fa"
RESOURCE_API = "https://dadosabertos.tse.jus.br/api/3/action/resource_show?id="
BASE_DIR = Path(__file__).resolve().parents[1] / "referencias/tse/eleitorado/2026"
CANONICAL_ZIP = "transf_temporaria_secao_2026.zip"
MEMBER_CSV = "transf_temporaria_secao_2026.csv"
REQUIRED_COLUMNS = {
    "AA_ELEICAO", "NR_TURNO", "TP_TTE", "TP_ABRANGENCIA_TTE", "SG_UF_DESTINO",
    "CD_MUNICIPIO_DESTINO", "NR_ZONA_DESTINO", "NR_SECAO_DESTINO", "QT_ELEITOR",
}


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def update_reference(directory: Path = BASE_DIR, resource_id: str = RESOURCE_ID) -> dict:
    """Explicitly refresh the reference from CKAN, preserving every downloaded version."""
    api_url = RESOURCE_API + resource_id
    request = urllib.request.Request(api_url, headers={"User-Agent": "observatorio-urnas/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        api_response = json.load(response)
        api_date = response.headers.get("Date")
    if not api_response.get("success") or api_response.get("result", {}).get("id") != resource_id:
        raise ValueError("CKAN resource_show did not return the requested resource")
    resource = api_response["result"]
    url = resource.get("url")
    if not url:
        raise ValueError("CKAN resource metadata has no download URL")

    request = urllib.request.Request(url, headers={"User-Agent": "observatorio-urnas/1.0"})
    with urllib.request.urlopen(request, timeout=180) as response:
        raw = response.read()
        download = {"final_url": response.geturl(), "http_status": getattr(response, "status", None),
                    "content_type": response.headers.get("Content-Type"),
                    "content_length": response.headers.get("Content-Length"),
                    "last_modified": response.headers.get("Last-Modified"),
                    "etag": response.headers.get("ETag"), "server_date": response.headers.get("Date")}
    digest = _sha256_bytes(raw)
    directory.mkdir(parents=True, exist_ok=True)
    manifest_path = directory / "reference.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {
        "resource_id": resource_id, "versions": []}
    existing_version = next((version for version in manifest.get("versions", [])
                             if version.get("sha256") == digest and (directory / version.get("zip_file", "")).exists()), None)
    if existing_version:
        filename = existing_version["zip_file"]
        obtained_at = existing_version.get("obtained_at_utc")
    else:
        canonical_provenance = directory / f"{CANONICAL_ZIP}.provenance.json"
        canonical_digest = json.loads(canonical_provenance.read_text(encoding="utf-8")).get("sha256") if canonical_provenance.exists() else None
        if canonical_digest == digest and (directory / CANONICAL_ZIP).exists():
            filename = CANONICAL_ZIP
            obtained_at = json.loads(canonical_provenance.read_text(encoding="utf-8")).get("obtained_at_utc")
        else:
            filename = f"transf_temporaria_secao_2026-{digest[:12]}.zip"
            obtained_at = datetime.now(timezone.utc).isoformat()
            zip_path = directory / filename
            if not zip_path.exists():
                _atomic_write(zip_path, raw)
    zip_path = directory / filename
    provenance = {
        "source": "TSE CKAN", "ckan_api_url": api_url, "resource_id": resource_id,
        "resource_metadata_response_date": api_date, "resource_metadata": resource,
        "download": download, "obtained_at_utc": obtained_at,
        "path": str(zip_path), "size_bytes": len(raw), "sha256": digest,
    }
    provenance_path = Path(str(zip_path) + ".provenance.json")
    if not provenance_path.exists():
        _atomic_write(provenance_path, (json.dumps(provenance, ensure_ascii=False, indent=2) + "\n").encode())
    versions = [version for version in manifest.get("versions", []) if version.get("sha256") != digest]
    versions.append({"zip_file": filename, "sha256": digest, "size_bytes": len(raw),
                     "obtained_at_utc": provenance["obtained_at_utc"]})
    manifest.update({"resource_id": resource_id, "current_zip": filename, "versions": versions})
    _atomic_write(manifest_path, (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode())
    return provenance


class TransferReferenceIndex:
    def __init__(self, zip_path: Path, provenance: dict, expected_sha256: str):
        self.zip_path = zip_path
        self.provenance = provenance
        self.expected_sha256 = expected_sha256
        self.schema_columns: list[str] = []
        self.sections: dict[tuple[str, str, str, str, int], Counter] = defaultdict(Counter)
        self.rows_read = 0
        self.invalid_rows = 0
        self.first_row_errors: list[str] = []
        self.election_years: Counter = Counter()
        self.generation_values: Counter = Counter()
        self.turns: Counter = Counter()
        self.transfer_types: set[str] = set()
        self.scopes: set[str] = set()
        self.status = "complete"
        self._read()

    def _read(self) -> None:
        observed_hash = _sha256_path(self.zip_path)
        if observed_hash != self.expected_sha256 or observed_hash != self.provenance.get("sha256"):
            raise ValueError("preserved TSE reference ZIP does not match its SHA-256 provenance")
        try:
            archive = zipfile.ZipFile(self.zip_path)
        except (OSError, zipfile.BadZipFile) as error:
            raise ValueError(f"preserved TSE reference is not a valid ZIP: {error}") from error
        with archive:
            if MEMBER_CSV not in archive.namelist():
                raise ValueError(f"reference ZIP is missing required member {MEMBER_CSV}")
            with archive.open(MEMBER_CSV) as binary, io.TextIOWrapper(binary, encoding="latin-1", newline="") as text:
                reader = csv.DictReader(text, delimiter=";", quotechar='"')
                self.schema_columns = list(reader.fieldnames or [])
                missing = sorted(REQUIRED_COLUMNS - set(self.schema_columns))
                if missing:
                    raise ValueError(f"official CSV header lacks required columns: {missing}")
                for line_number, row in enumerate(reader, start=2):
                    self.rows_read += 1
                    try:
                        if "DT_GERACAO" in self.schema_columns and "HH_GERACAO" in self.schema_columns:
                            self.generation_values[(row.get("DT_GERACAO"), row.get("HH_GERACAO"))] += 1
                        if row["AA_ELEICAO"] != "2026":
                            continue
                        turn = int(row["NR_TURNO"])
                        amount = int(row["QT_ELEITOR"])
                        if amount < 0:
                            raise ValueError(f"sentinel or negative QT_ELEITOR={amount}")
                        municipality = row["CD_MUNICIPIO_DESTINO"].zfill(5)
                        zone = row["NR_ZONA_DESTINO"].zfill(4)
                        section = row["NR_SECAO_DESTINO"].zfill(4)
                        uf = row["SG_UF_DESTINO"].upper()
                        transfer_type = row["TP_TTE"]
                        scope = row["TP_ABRANGENCIA_TTE"]
                        if not all((uf, municipality, zone, section, transfer_type, scope)):
                            raise ValueError("blank destination or classification field")
                        self.sections[(uf, municipality, zone, section, turn)][(transfer_type, scope)] += amount
                        self.election_years[row["AA_ELEICAO"]] += 1
                        self.turns[turn] += 1
                        self.transfer_types.add(transfer_type)
                        self.scopes.add(scope)
                    except (KeyError, TypeError, ValueError) as error:
                        self.invalid_rows += 1
                        if len(self.first_row_errors) < 20:
                            self.first_row_errors.append(f"CSV line {line_number}: {error}")
        if self.invalid_rows:
            self.status = "partial"

    def section_breakdown(self, uf: str, municipality: str, zone: str, section: str, turn: int) -> list[dict]:
        key = (uf.upper(), municipality.zfill(5), zone.zfill(4), section.zfill(4), int(turn))
        counts = self.sections.get(key, Counter())
        return [{"tipo_transferencia": transfer_type, "abrangencia": scope, "quantidade_eleitores": count}
                for (transfer_type, scope), count in sorted(counts.items())]

    def federal_count(self, uf: str, municipality: str, zone: str, section: str, turn: int) -> int:
        return sum(row["quantidade_eleitores"] for row in self.section_breakdown(uf, municipality, zone, section, turn)
                   if row["abrangencia"].casefold() == "federal".casefold())

    def metadata(self) -> dict:
        repo_root = Path(__file__).resolve().parents[1]
        try:
            displayed_path = str(self.zip_path.resolve().relative_to(repo_root))
        except ValueError:
            displayed_path = str(self.zip_path)
        return {"resource_id": self.provenance["resource_id"], "resource_name": self.provenance["resource_metadata"].get("name"),
                "resource_url_discovered_via_ckan": self.provenance["resource_metadata"].get("url"),
                "zip_path": displayed_path, "zip_sha256": self.expected_sha256,
                "obtained_at_utc": self.provenance.get("obtained_at_utc"), "row_count": self.rows_read,
                "valid_data_rows": sum(self.turns.values()), "invalid_rows": self.invalid_rows,
                "data_generation_values": [{"date": date, "time_brasilia": time}
                    for (date, time), _count in sorted(self.generation_values.items())],
                "header": self.schema_columns, "turns_observed": sorted(self.turns),
                "transfer_types_observed": sorted(self.transfer_types), "scopes_observed": sorted(self.scopes),
                "status": self.status, "first_row_errors": self.first_row_errors}


@lru_cache(maxsize=4)
def _load_index_cached(zip_path_text: str, provenance_path_text: str, expected_sha: str,
                       zip_size: int, zip_mtime_ns: int) -> TransferReferenceIndex:
    del zip_size, zip_mtime_ns  # included in the cache key to invalidate after an explicit update
    zip_path, provenance_path = Path(zip_path_text), Path(provenance_path_text)
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    return TransferReferenceIndex(zip_path, provenance, expected_sha)


def load_reference(directory: Path = BASE_DIR) -> TransferReferenceIndex:
    manifest_path = directory / "reference.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        filename = manifest.get("current_zip")
        version = next((item for item in manifest.get("versions", []) if item.get("zip_file") == filename), None)
        if not filename or not version:
            raise ValueError("reference.json has no valid current ZIP version")
        zip_path = directory / filename
        expected_sha = version.get("sha256")
    else:
        zip_path = directory / CANONICAL_ZIP
        provenance_path = Path(str(zip_path) + ".provenance.json")
        if not provenance_path.exists():
            raise FileNotFoundError(f"TSE temporary-transfer reference is not installed at {zip_path}")
        expected_sha = json.loads(provenance_path.read_text(encoding="utf-8")).get("sha256")
    provenance_path = Path(str(zip_path) + ".provenance.json")
    if not zip_path.exists() or not provenance_path.exists():
        raise FileNotFoundError(f"TSE temporary-transfer ZIP/provenance is missing: {zip_path}")
    stat = zip_path.stat()
    return _load_index_cached(str(zip_path), str(provenance_path), str(expected_sha), stat.st_size, stat.st_mtime_ns)


def reference_identity(directory: Path = BASE_DIR) -> dict:
    """Return the pinned reference identity even when its ZIP cannot be parsed or opened."""
    result = {"resource_id": RESOURCE_ID, "version": None, "zip_file": None,
              "zip_sha256": None, "obtained_at_utc": None, "available": False}
    manifest_path = directory / "reference.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        result["resource_id"] = manifest.get("resource_id", RESOURCE_ID)
        filename = manifest.get("current_zip")
        version = next((item for item in manifest.get("versions", [])
                        if item.get("zip_file") == filename), None)
        if filename:
            result["zip_file"] = filename
            result["version"] = filename
        if version:
            result["zip_sha256"] = version.get("sha256")
            result["obtained_at_utc"] = version.get("obtained_at_utc")
    except (OSError, ValueError, TypeError):
        pass
    if result["zip_file"]:
        provenance_path = directory / f"{result['zip_file']}.provenance.json"
        try:
            provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
            result["resource_id"] = provenance.get("resource_id", result["resource_id"])
            result["zip_sha256"] = result["zip_sha256"] or provenance.get("sha256")
            result["obtained_at_utc"] = result["obtained_at_utc"] or provenance.get("obtained_at_utc")
            result["resource_url_discovered_via_ckan"] = provenance.get("resource_metadata", {}).get("url")
        except (OSError, ValueError, TypeError):
            pass
        result["available"] = (directory / result["zip_file"]).is_file()
    return result


def write_section_excerpts(targets: list[dict], directory: Path = BASE_DIR,
                           index: TransferReferenceIndex | None = None) -> dict[str, dict]:
    """Write original-column CSV slices only for section/turn acquisitions supplied by the caller."""
    if not targets:
        return {}
    index = index or load_reference(directory)
    if index.status != "complete":
        raise ValueError("cannot produce eligible-elector excerpts from a partially parsed reference")
    target_map: dict[tuple[str, str, str, str, int], list[dict]] = defaultdict(list)
    for target in targets:
        key_parts = str(target.get("section_key", "")).split("/")
        turn = target.get("turn")
        output_path = Path(target["output_path"])
        if len(key_parts) != 4 or turn not in {1, 2}:
            continue
        uf, municipality, zone, section = key_parts
        key = (uf.upper(), municipality.zfill(5), zone.zfill(4), section.zfill(4), int(turn))
        target_map[key].append({"output_path": output_path, "row_count": 0, "electors": 0,
                                "federal_electors": 0, "breakdown": Counter()})
    if not target_map:
        return {}

    temporary_files: list[Path] = []
    open_outputs: list[tuple[dict, object, csv.writer, Path]] = []
    try:
        for entries in target_map.values():
            for entry in entries:
                output_path = entry["output_path"]
                output_path.parent.mkdir(parents=True, exist_ok=True)
                fd, temp_name = tempfile.mkstemp(prefix=f".{output_path.name}.", dir=output_path.parent)
                temporary_path = Path(temp_name)
                temporary_files.append(temporary_path)
                stream = os.fdopen(fd, "w", encoding="utf-8-sig", newline="")
                open_outputs.append((entry, stream, None, temporary_path))

        output_lookup = {entry["output_path"]: index for index, (entry, _stream, _writer, _temp) in enumerate(open_outputs)}
        with zipfile.ZipFile(index.zip_path) as archive:
            with archive.open(MEMBER_CSV) as binary, io.TextIOWrapper(binary, encoding="latin-1", newline="") as text:
                reader = csv.DictReader(text, delimiter=";", quotechar='"')
                columns = list(reader.fieldnames or [])
                if not REQUIRED_COLUMNS.issubset(columns):
                    raise ValueError("official CSV header lacks required columns while writing section excerpts")
                for position, (entry, stream, _writer, temporary_path) in enumerate(open_outputs):
                    writer = csv.writer(stream, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL,
                                        lineterminator="\n")
                    writer.writerow(columns)
                    open_outputs[position] = (entry, stream, writer, temporary_path)
                for row in reader:
                    try:
                        if row.get("AA_ELEICAO") != "2026":
                            continue
                        turn = int(row["NR_TURNO"])
                        key = (row["SG_UF_DESTINO"].upper(), row["CD_MUNICIPIO_DESTINO"].zfill(5),
                               row["NR_ZONA_DESTINO"].zfill(4), row["NR_SECAO_DESTINO"].zfill(4), turn)
                        destinations = target_map.get(key)
                        if not destinations:
                            continue
                        amount = int(row["QT_ELEITOR"])
                        if amount < 0:
                            continue
                        breakdown_key = (row["TP_TTE"], row["TP_ABRANGENCIA_TTE"])
                        for entry in destinations:
                            output_position = output_lookup[entry["output_path"]]
                            _entry, _stream, writer, _temporary_path = open_outputs[output_position]
                            writer.writerow([row.get(column, "") for column in columns])
                            entry["row_count"] += 1
                            entry["electors"] += amount
                            entry["breakdown"][breakdown_key] += amount
                            if row["TP_ABRANGENCIA_TTE"].casefold() == "federal".casefold():
                                entry["federal_electors"] += amount
                    except (KeyError, TypeError, ValueError, AttributeError):
                        continue
        for _entry, stream, _writer, _temporary_path in open_outputs:
            stream.flush()
            os.fsync(stream.fileno())
            stream.close()
        outputs = {}
        for entry, _stream, _writer, temporary_path in open_outputs:
            output_path = entry["output_path"]
            os.replace(temporary_path, output_path)
            digest = _sha256_path(output_path)
            repo_root = Path(__file__).resolve().parents[1]
            try:
                displayed_path = str(output_path.resolve().relative_to(repo_root))
            except ValueError:
                displayed_path = str(output_path)
            outputs[str(output_path)] = {
                "path": displayed_path, "sha256": digest, "row_count": entry["row_count"],
                "registered_electors_all_scopes": entry["electors"],
                "registered_electors_federal_scope": entry["federal_electors"],
                "transfer_type_scope_breakdown": [
                    {"tipo_transferencia": kind, "abrangencia": scope, "quantidade_eleitores": count}
                    for (kind, scope), count in sorted(entry["breakdown"].items())],
                "reference_zip_sha256": index.expected_sha256,
            }
        return outputs
    finally:
        for _entry, stream, _writer, _temporary_path in open_outputs:
            if not stream.closed:
                stream.close()
        for temporary_path in temporary_files:
            if temporary_path.exists():
                temporary_path.unlink()


def presidential_only_confirmed_vote_count(session_reconstruction: dict, event_date: str) -> dict:
    sessions = [session for session in session_reconstruction.get("sessions", [])
                if session.get("habilitation_event", {}).get("event_date") == event_date]
    pres_only = []
    for session in sessions:
        confirmations = [event for event in session.get("confirmation_events", [])
                         if event.get("event_date") == event_date]
        cargos = {event.get("cargo") for event in confirmations}
        pres_confirmations = [event for event in confirmations if event.get("cargo") == "presidente"]
        if pres_confirmations and cargos == {"presidente"}:
            pres_only.append((session, len(pres_confirmations)))
    unassigned = [item for item in session_reconstruction.get("unassigned_events", [])
                  if item.get("event", {}).get("event_date") == event_date]
    blocking = sum(bool(item.get("reconciliation_relevant", True)) for item in unassigned)
    count = sum(item[1] for item in pres_only)
    return {"count": count, "presidential_only_confirmation_count": count,
            "presidential_only_session_count": len(pres_only),
            "ambiguous_session_count": sum(bool(session.get("possible_resumption")) for session, _ in pres_only),
            "completed_session_count": sum((session.get("session_end_event") or {}).get("event_date") == event_date
                                            for session in sessions),
            "incomplete_session_count": sum(session.get("status") == "incompleta_indeterminada" for session in sessions),
            "unassigned_event_count": len(unassigned),
            "reconciliation_relevant_unassigned_event_count": blocking,
            "status": "complete" if blocking == 0 else "incomplete",
            "semantics": "Each explicit presidential cargo confirmation is counted as a computed presidential vote when its reconstructed session has no confirmation for another cargo. Session-end markers and possible-resumption signals are reported separately. No elector or individual TTE record is identified."}


def reconcile_section(index: TransferReferenceIndex, section_key: str, turn: int | None,
                      log_metric: dict | None, event_date: str | None = None) -> dict:
    parts = section_key.split("/") if isinstance(section_key, str) else []
    if len(parts) != 4:
        return {"status": "dados_insuficientes", "reason": "section key must be uf/municipio/zona/secao"}
    uf, municipality, zone, section = parts
    breakdown = index.section_breakdown(uf, municipality, zone, section, turn or 0) if turn else []
    expected = index.federal_count(uf, municipality, zone, section, turn) if turn else None
    log_count = log_metric.get("count") if isinstance(log_metric, dict) else None
    log_count_valid = isinstance(log_count, int) and not isinstance(log_count, bool) and log_count >= 0
    relevant_unassigned = (log_metric.get("reconciliation_relevant_unassigned_event_count",
        log_metric.get("unassigned_event_count", 0)) if isinstance(log_metric, dict) else None)
    metric_complete = (isinstance(log_metric, dict) and log_metric.get("status") == "complete"
                       and relevant_unassigned == 0)
    if index.status != "complete":
        status, reason = "dados_insuficientes", "The reference CSV was only partially parsed."
    elif turn != 2:
        status, reason = "dados_insuficientes", "This comparison is scoped to the second election round."
    elif not metric_complete or not log_count_valid:
        status, reason = "dados_insuficientes", "No complete log metric for confirmed presidential votes is available."
    elif log_count == expected:
        status, reason = "confirmado", "The aggregate log count equals the official destination-section count for Federal temporary transfers. This confirms count compatibility only, not session-to-elector identity."
    elif log_count < expected:
        status, reason = "compatível", "The log count does not exceed the official Federal temporary-transfer count; the sources do not establish which registered voters actually voted."
    else:
        status, reason = "divergente", "The log pattern count exceeds the official Federal temporary-transfer count for this section; other explanations remain possible."
    source = index.metadata()
    return {
        "section_key": section_key, "uf": uf.upper(), "municipio": municipality.zfill(5),
        "zona": zone.zfill(4), "secao": section.zfill(4), "turno": turn,
        "status": status, "reason": reason, "log_presidential_only_confirmed_votes": log_count,
        "log_metric_detail": log_metric, "reference_federal_transfer_count": expected,
        "reference_transfer_type_scope_breakdown": breakdown,
        "reference_rows_present": bool(breakdown), "event_date": event_date,
        "individual_session_tte_join_performed": False,
        "non_inference_note": "No individual voting session is attributed to a TTE elector; only section-level counts are compared.",
        "reference": source,
    }


def write_reconciliation(rows: list[dict], json_path: Path, csv_path: Path,
                         source_note: str = "analysis_report") -> None:
    payload = {"report_type": "tse_temporary_transfer_section_reconciliation", "schema_version": 2,
               "generated_at_utc": datetime.now(timezone.utc).isoformat(), "log_metric_source": source_note,
               "scope": {"election_year": 2026, "turn": 2}, "row_count": len(rows),
               "status_counts": dict(Counter(row.get("status") for row in rows)), "sections": rows}
    _atomic_write(json_path, (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode())
    fields = ["section_key", "uf", "municipio", "zona", "secao", "turno", "tipo_transferencia",
              "abrangencia", "quantidade_eleitores", "reference_federal_transfer_count",
              "log_presidential_only_confirmed_votes", "difference_log_minus_reference", "status",
              "source_zip_sha256", "event_date"]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for report in rows:
            breakdown = report.get("reference_transfer_type_scope_breakdown") or [{
                "tipo_transferencia": "", "abrangencia": "", "quantidade_eleitores": 0}]
            for detail in breakdown:
                writer.writerow({**report, **detail,
                    "difference_log_minus_reference": (
                        report["log_presidential_only_confirmed_votes"] - report["reference_federal_transfer_count"]
                        if report.get("log_presidential_only_confirmed_votes") is not None
                        and report.get("reference_federal_transfer_count") is not None else None),
                    "source_zip_sha256": (report.get("reference") or {}).get("zip_sha256")})


def write_validation(rows: list[dict], json_path: Path, directory: Path = BASE_DIR,
                     source_note: str = "section_analysis_cargo_confirmations") -> None:
    """Write a compact audit record without bundling the reference ZIP itself."""
    identity = reference_identity(directory)
    row_reference = next((row.get("reference") for row in rows if row.get("reference")), None)
    if row_reference:
        identity.update({
            "resource_id": row_reference.get("resource_id") or identity["resource_id"],
            "version": row_reference.get("zip_path") or identity.get("version"),
            "zip_sha256": row_reference.get("zip_sha256") or identity.get("zip_sha256"),
            "obtained_at_utc": row_reference.get("obtained_at_utc") or identity.get("obtained_at_utc"),
            "resource_url_discovered_via_ckan": row_reference.get("resource_url_discovered_via_ckan")
                or identity.get("resource_url_discovered_via_ckan"),
        })
    sections = []
    differences = []
    divergences = []
    for row in rows:
        reference_count = row.get("reference_federal_transfer_count")
        log_count = row.get("log_presidential_only_confirmed_votes")
        difference = log_count - reference_count if isinstance(log_count, int) and isinstance(reference_count, int) else None
        section = {key: row.get(key) for key in (
            "section_key", "uf", "municipio", "zona", "secao", "turno", "status", "reason", "event_date",
            "log_presidential_only_confirmed_votes", "log_metric_detail", "reference_federal_transfer_count",
            "reference_transfer_type_scope_breakdown", "reference_rows_present", "reference_excerpt",
            "individual_session_tte_join_performed")}
        section["difference_log_minus_reference"] = difference
        sections.append(section)
        if difference not in (None, 0):
            differences.append({"section_key": row.get("section_key"), "difference_log_minus_reference": difference,
                                "status": row.get("status")})
        if row.get("status") == "divergente":
            divergences.append({"section_key": row.get("section_key"), "difference_log_minus_reference": difference,
                                "reason": row.get("reason")})
    payload = {
        "report_type": "tse_temporary_transfer_validation",
        "schema_version": 2,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": source_note,
        "reference": {**identity, "base_embedded_in_artifact": False},
        "sections_examined": sections,
        "section_count": len(sections),
        "status_counts": dict(Counter(row.get("status") for row in rows)),
        "differences": differences,
        "divergences": divergences,
    }
    _atomic_write(json_path, (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--update-reference", action="store_true",
                        help="explicitly query CKAN and preserve a new reference ZIP version")
    parser.add_argument("--reconcile-counts", type=Path,
                        help="JSON mapping section keys to presidential-only confirmed-vote counts; does not read logs")
    parser.add_argument("--turn", type=int, default=2)
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--output-csv", type=Path)
    parser.add_argument("--directory", type=Path, default=BASE_DIR)
    args = parser.parse_args()
    if args.update_reference:
        print(json.dumps(update_reference(args.directory), ensure_ascii=False, indent=2))
        return 0
    if args.reconcile_counts:
        counts = json.loads(args.reconcile_counts.read_text(encoding="utf-8"))
        index = load_reference(args.directory)
        rows = []
        for key, value in counts.items():
            metric = {"count": value, "status": "complete"} if isinstance(value, int) else dict(value)
            metric.setdefault("status", "complete")
            rows.append(reconcile_section(index, key, args.turn, metric, metric.get("event_date")))
        json_path = args.output_json or args.directory / "reconciliacao.json"
        csv_path = args.output_csv or args.directory / "reconciliacao.csv"
        write_reconciliation(rows, json_path, csv_path, "values_supplied_explicitly_by_user")
        print(json.dumps({"json": str(json_path), "csv": str(csv_path),
                          "status_counts": dict(Counter(row["status"] for row in rows))}, ensure_ascii=False))
        return 0
    parser.error("choose --update-reference or --reconcile-counts")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
