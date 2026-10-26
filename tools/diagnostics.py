"""Stable, JSON-safe diagnostics shared by collection and analysis stages."""

from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from pathlib import Path


ERROR_TYPES = {
    "network", "http", "download_incomplete", "integrity", "signature_cryptographic",
    "decoding", "decompression", "decryption", "format_incompatibility", "parsing",
    "semantic_validation", "reconciliation", "missing", "internal",
}
RECOVERABILITY = {"recoverable", "permanent", "indeterminate"}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def diagnostic(*, code: str, stage: str, operation: str, error_type: str, cause: str,
               recoverability: str, section: str | None, file_type: str | None = None,
               file_name: str | None = None, file_path: str | None = None,
               exception: BaseException | None = None, position: dict | None = None,
               detected_format: str | None = None, version: str | None = None,
               urn_model: str | None = None, context: dict | None = None) -> dict:
    if error_type not in ERROR_TYPES:
        raise ValueError(f"unknown diagnostic error_type: {error_type}")
    if recoverability not in RECOVERABILITY:
        raise ValueError(f"unknown recoverability: {recoverability}")
    result = {
        "code": code, "stage": stage, "file": {"type": file_type, "name": file_name, "path": file_path},
        "section": section, "operation": operation, "error_type": error_type, "cause": cause,
        "recoverability": recoverability, "position": position or {},
        "detected_format": detected_format, "version": version, "urn_model": urn_model,
        "exception": ({"type": type(exception).__name__, "message": str(exception)} if exception else None),
        "context": context or {},
    }
    return result


def summarize_diagnostics(diagnostics: list[dict], files: list[dict] | None = None) -> dict:
    dimensions = ("error_type", "detected_format", "version", "urn_model", "section")
    grouped = {}
    for dimension in dimensions:
        counts = Counter(str(item.get(dimension) or "unknown") for item in diagnostics)
        grouped[dimension] = dict(sorted(counts.items()))
    by_code = Counter(item["code"] for item in diagnostics)
    return {
        "diagnostic_count": len(diagnostics), "by_code": dict(sorted(by_code.items())),
        "by_dimension": grouped,
        "file_status_counts": dict(sorted(Counter(item.get("status", "unknown") for item in (files or [])).items())),
        "files": files or [],
    }


def consolidate_reports(reports: list[dict]) -> dict:
    diagnostics = []
    files = []
    sections = []
    for report in reports:
        diagnostics.extend(report.get("diagnostics", []))
        states = report.get("file_processing", {}).get("states", [])
        files.extend(states)
        sections.append({"section": report.get("section_key"), "overall_status": report.get("overall_status", "unknown"),
                         "validity": report.get("validity", "not_validated"),
                         "diagnostic_count": len(report.get("diagnostics", [])),
                         "file_status_counts": dict(sorted(Counter(item.get("status", "unknown") for item in states).items()))})
    return {"report_type": "consolidated_diagnostics", "report_schema_version": 1,
            "section_count": len(sections), "sections": sections,
            "diagnostics": diagnostics, "summary": summarize_diagnostics(diagnostics, files)}


def file_state(*, file_type: str, name: str | None, section: str | None, status: str,
               reason: str | None = None, path: str | None = None, detected_format: str | None = None,
               version: str | None = None, urn_model: str | None = None,
               sha256: str | None = None, size_bytes: int | None = None) -> dict:
    return {"type": file_type, "name": name, "path": path, "section": section, "status": status,
            "reason": reason, "detected_format": detected_format, "version": version,
            "urn_model": urn_model, "sha256": sha256, "size_bytes": size_bytes}


def verify_jws_signature(raw: bytes, *, section: str, file_name: str,
                         verifier=None) -> tuple[str, dict]:
    """Verification seam: production needs a configured TSE trust anchor."""
    if verifier is None:
        return "not_processed", diagnostic(code="JWS_SIGNATURE_NOT_VERIFIED", stage="integrity_validation",
            operation="verify_jws_signature", error_type="signature_cryptographic",
            cause="No trusted TSE public key or signature-verification configuration is available.",
            recoverability="indeterminate", section=section, file_type="jws", file_name=file_name,
            detected_format="JWS compact", context={"verification_status": "not_processed", "bytes": len(raw)})
    try:
        verifier(raw)
        return "verified", None
    except Exception as error:
        return "invalid", diagnostic(code="JWS_SIGNATURE_INVALID", stage="integrity_validation",
            operation="verify_jws_signature", error_type="signature_cryptographic",
            cause="Cryptographic signature verification failed.", recoverability="permanent", section=section,
            file_type="jws", file_name=file_name, exception=error, detected_format="JWS compact",
            context={"verification_status": "invalid", "bytes": len(raw)})
