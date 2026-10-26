"""Decode official TSE RDV BER files while preserving every vote record."""

from __future__ import annotations

import hashlib
from collections import Counter
from pathlib import Path

import asn1tools
from diagnostics import diagnostic


RDV_SCHEMA_SOURCE = "https://www.tse.jus.br/eleicoes/eleicoes-2026-content/arquivos/formato-arquivos-de-bu-rdv-e-assinatura-digital"
RDV_SCHEMA_BUNDLE_SHA256 = "b5ac413e81121d53de2bb1657af611f84a45fdc84388e9d9821872994bbfbafd"
RDV_SCHEMA_SHA256 = "282dcde0c7ba1f1bb128fe27443ad8d18f7490416c9546d033910a7ec557a465"

VOTE_TYPES = {
    "legenda": "party_number",
    "nominal": "candidate_number",
    "branco": "blank",
    "nulo": "null",
    "brancoAposSuspensao": "blank_after_suspension",
    "nuloAposSuspensao": "null_after_suspension",
    "nuloPorRepeticao": "null_by_repetition",
    "nuloCargoSemCandidato": "null_office_without_candidate",
    "nuloAposSuspensaoCargoSemCandidato": "null_after_suspension_office_without_candidate",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class TLV:
    def __init__(self, tag: bytes, start: int, content_start: int, end: int, children: list["TLV"]):
        self.tag, self.start, self.content_start, self.end, self.children = tag, start, content_start, end, children


class BERParseError(ValueError):
    def __init__(self, message: str, offset: int):
        super().__init__(message)
        self.offset = offset


class RDVStructureError(ValueError):
    def __init__(self, message: str, position: dict):
        super().__init__(message)
        self.position = position


def _read_length(raw: bytes, offset: int) -> tuple[int, int]:
    if offset >= len(raw):
        raise BERParseError("truncated BER length", offset)
    first = raw[offset]
    if first < 0x80:
        return first, offset + 1
    count = first & 0x7F
    if count == 0:
        raise BERParseError("indefinite BER lengths are not accepted", offset)
    if count > 4 or offset + 1 + count > len(raw):
        raise BERParseError("invalid or oversized BER length", offset)
    size = int.from_bytes(raw[offset + 1:offset + 1 + count], "big")
    if size < 0x80:
        raise BERParseError("non-minimal BER length", offset)
    return size, offset + 1 + count


def _parse_one(raw: bytes, offset: int) -> tuple[TLV, int]:
    start = offset
    if offset >= len(raw):
        raise BERParseError("truncated BER tag", offset)
    first = raw[offset]
    offset += 1
    if first & 0x1F == 0x1F:
        tag_bytes = bytearray([first])
        while True:
            if offset >= len(raw) or len(tag_bytes) >= 6:
                raise BERParseError("invalid or oversized BER tag", offset)
            octet = raw[offset]
            tag_bytes.append(octet)
            offset += 1
            if octet & 0x80 == 0:
                break
        tag = bytes(tag_bytes)
    else:
        tag = bytes([first])
    length, content_start = _read_length(raw, offset)
    end = content_start + length
    if end > len(raw):
        raise BERParseError("BER value exceeds containing bytes", start)
    children: list[TLV] = []
    if tag[0] & 0x20:
        cursor = content_start
        while cursor < end:
            child, cursor = _parse_one(raw, cursor)
            children.append(child)
        if cursor != end:
            raise BERParseError("BER constructed value has invalid child boundary", cursor)
    return TLV(tag, start, content_start, end, children), end


def _root(raw: bytes) -> TLV:
    root, end = _parse_one(raw, 0)
    if end != len(raw):
        raise ValueError("trailing bytes after RDV BER entity")
    return root


def _cargo_key(decoded: dict) -> dict:
    name, value = decoded
    return {"cargo_kind": name, "cargo_code": value}


def _json_safe(value):
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex(), "length_bytes": len(value)}
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        if len(value) == 2 and isinstance(value[0], str):
            return {"choice": value[0], "value": _json_safe(value[1])}
        return [_json_safe(item) for item in value]
    return value


def _elections_path(registry_node: TLV, registry_type: str) -> tuple[TLV, bool]:
    children = registry_node.children
    if registry_type == "EntidadeResultadoRDV":
        if len(children) != 3:
            raise ValueError("RDV result entity does not have the expected three fields")
        registry_node = children[2]
        children = registry_node.children
    if len(children) != 5:
        raise ValueError("RDV registry entity does not have the expected five fields")
    choice = children[4]
    if len(choice.tag) != 1 or choice.tag[0] not in (0xA1, 0xA2):
        raise RDVStructureError(f"unsupported RDV Eleicoes choice tag {choice.tag.hex()}", {"byte_offset": choice.start})
    return choice, choice.tag[0] == 0xA2


def decode_rdv(rdv_path: Path, collection_path: Path, schema_path: Path, bu: dict | None = None) -> dict:
    import json

    collection = json.loads(collection_path.read_text(encoding="utf-8"))
    metadata = next((f for f in collection["files"] if f.get("type") == "rdv" and
                     (f.get("name") == rdv_path.name or (collection_path.parent / f.get("relative_path", "")).resolve() == rdv_path.resolve())), None)
    if not metadata:
        raise ValueError("RDV input is not listed in acquisition manifest")
    raw = rdv_path.read_bytes()
    file_hash = hashlib.sha256(raw).hexdigest()
    if file_hash != metadata.get("sha256") or len(raw) != metadata.get("size_bytes"):
        raise ValueError("RDV bytes do not match acquisition manifest hash and size")

    spec = asn1tools.compile_files(str(schema_path), codec="ber")
    entity_raw = raw
    envelope_metadata = None
    root = _root(raw)
    decoded = None
    decoded_type = None
    errors = []
    for type_name in ("EntidadeResultadoRDV", "EntidadeRegistroDigitalVoto"):
        try:
            decoded = spec.decode(type_name, entity_raw)
            decoded_type = type_name
            break
        except Exception as error:  # decoder diagnostics are surfaced if no official variant matches
            errors.append(f"{type_name}: {error}")
    if decoded is None or decoded_type is None:
        bu_spec = asn1tools.compile_files(str(schema_path.with_name("bu.asn1")), codec="ber")
        try:
            envelope = bu_spec.decode("EntidadeEnvelopeGenerico", raw)
        except Exception as error:
            raise ValueError("unsupported RDV entity or envelope variant: " + " | ".join(errors) + f" | envelope: {error}") from error
        if envelope["tipoEnvelope"] != "envelopeRegistroDigitalVoto":
            raise ValueError(f"ASN.1 envelope type is not RDV: {envelope['tipoEnvelope']}")
        if "seguranca" in envelope:
            raise ValueError("encrypted RDV envelope is not supported; original bytes retained by collector")
        entity_raw = envelope["conteudo"]
        root = _root(entity_raw)
        envelope_metadata = {key: value for key, value in envelope.items() if key != "conteudo"}
        for type_name in ("EntidadeResultadoRDV", "EntidadeRegistroDigitalVoto"):
            try:
                decoded = spec.decode(type_name, entity_raw)
                decoded_type = type_name
                break
            except Exception as error:
                errors.append(f"envelope content {type_name}: {error}")
    if decoded is None or decoded_type is None:
        raise ValueError("unsupported RDV entity variant: " + " | ".join(errors))
    choice_node, is_sa = _elections_path(root, decoded_type)
    choice_name, election_values = decoded["rdv"]["eleicoes"] if decoded_type == "EntidadeResultadoRDV" else decoded["eleicoes"]
    registry = decoded["rdv"] if decoded_type == "EntidadeResultadoRDV" else decoded
    root_fields = decoded.get("cabecalho") if decoded_type == "EntidadeResultadoRDV" else None
    urn = decoded.get("urna") if decoded_type == "EntidadeResultadoRDV" else None
    if choice_name != ("eleicoesSA" if is_sa else "eleicoesVota"):
        raise ValueError("decoded election CHOICE disagrees with preserved BER structure")
    election_nodes = choice_node.children
    if len(election_nodes) != len(election_values):
        raise RDVStructureError("BER election count differs from decoded RDV", {"decoded_election_count": len(election_values), "ber_election_count": len(election_nodes)})

    elections = []
    all_records = 0
    for election_index, (election_value, election_node) in enumerate(zip(election_values, election_nodes), 1):
        echildren = election_node.children
        cargo_child_index = 3 if is_sa else 1
        expected_election_fields = 4 if is_sa else 2
        if len(echildren) != expected_election_fields or len(echildren[cargo_child_index].children) != len(election_value["votosCargos"]):
            raise RDVStructureError("BER cargo count differs from decoded RDV", {"election_position": election_index,
                "decoded_cargo_count": len(election_value["votosCargos"]), "ber_cargo_count": len(echildren[cargo_child_index].children) if len(echildren) > cargo_child_index else None})
        cargo_nodes = echildren[cargo_child_index].children
        cargos = []
        for cargo_index, (cargo_value, cargo_node) in enumerate(zip(election_value["votosCargos"], cargo_nodes), 1):
            cchildren = cargo_node.children
            if len(cchildren) != 3 or len(cchildren[2].children) != len(cargo_value["votos"]):
                raise RDVStructureError("BER vote count differs from decoded RDV", {"election_position": election_index,
                    "cargo_position": cargo_index, "decoded_record_count": len(cargo_value["votos"]),
                    "ber_record_count": len(cchildren[2].children) if len(cchildren) > 2 else None})
            cargo_key = _cargo_key(cargo_value["idCargo"])
            votes = []
            type_counts: Counter[str] = Counter()
            white_count = 0
            null_count = 0
            candidate_counts: Counter[str] = Counter()
            candidate_raw_digit_counts: Counter[str] = Counter()
            party_counts: Counter[str] = Counter()
            party_raw_digit_counts: Counter[str] = Counter()
            for position, (vote, node) in enumerate(zip(cargo_value["votos"], cchildren[2].children), 1):
                encoded = entity_raw[node.start:node.end]
                try:
                    roundtrip = spec.decode("Voto", encoded)
                except Exception as error:
                    raise ValueError(f"vote BER record cannot be independently decoded: {error}") from error
                if roundtrip != vote:
                    raise RDVStructureError("vote BER record differs from parent decoded value", {"election_position": election_index,
                        "cargo_position": cargo_index, "record_position": position, "byte_offset": node.start})
                vote_type = vote["tipoVoto"]
                digitacao = vote.get("digitacao")
                type_counts[vote_type] += 1
                if vote_type.startswith("branco"):
                    white_count += 1
                elif vote_type.startswith("nulo"):
                    null_count += 1
                if digitacao is not None and vote_type == "nominal":
                    candidate_counts[str(int(digitacao))] += 1
                    candidate_raw_digit_counts[str(digitacao)] += 1
                if digitacao is not None and vote_type == "legenda":
                    party_counts[str(int(digitacao))] += 1
                    party_raw_digit_counts[str(digitacao)] += 1
                votes.append({
                    "record_position_within_election_and_cargo": position,
                    "vote_type": vote_type,
                    "vote_type_code_meaning": VOTE_TYPES.get(vote_type, "unmapped_type_preserved"),
                    "digitacao_raw": digitacao,
                    "digitacao_meaning": "candidate_number" if vote_type == "nominal" else "party_number" if vote_type == "legenda" else ("present_but_semantics_unmapped" if digitacao is not None else None),
                    "record_original_ber_hex": encoded.hex(),
                    "record_original_length_bytes": len(encoded),
                })
            all_records += len(votes)
            cargos.append({
                **cargo_key,
                "quantity_choices": cargo_value["quantidadeEscolhas"],
                "record_count": len(votes),
                "vote_type_counts": dict(sorted(type_counts.items())),
                "white_votes_total": white_count,
                "null_votes_total": null_count,
                "candidate_number_counts_from_nominal_votes": dict(sorted(candidate_counts.items())),
                "candidate_number_counts_by_raw_digitacao": dict(sorted(candidate_raw_digit_counts.items())),
                "party_number_counts_from_legend_votes": dict(sorted(party_counts.items())),
                "party_number_counts_by_raw_digitacao": dict(sorted(party_raw_digit_counts.items())),
                "records": votes,
            })
        election = {"election_id": election_value["idEleicao"], "cargo_count": len(cargos), "cargos": cargos}
        if is_sa:
            election["tipoCedulaSA"] = election_value["tipoCedulaSA"]
            election["origemVotosSA"] = election_value["origemVotosSA"]
        elections.append(election)

    if bu is None:
        reconciliation = {"status": "not_run", "reason": "no decoded BU provided"}
    else:
        reconciliation = reconcile(elections, bu)
    schema_hash = sha256(schema_path)
    registry_meta = {
        "pleito": registry["pleito"], "phase": registry["fase"],
        "section_identification": registry["identificacao"],
        "load_history_codes": registry["historicoCodigosCarga"],
    }
    expected = collection["section_key"].split("/")
    rdv_section = registry["identificacao"]
    municipio_zona = rdv_section["municipioZona"]
    identity_matches = (str(municipio_zona["municipio"]) == expected[1]
                        and f'{municipio_zona["zona"]:04d}' == expected[2]
                        and f'{rdv_section["secao"]:04d}' == expected[3])
    if not identity_matches:
        raise ValueError("RDV section identity does not match acquisition section key")
    return {
        "status": "decoded", "format_variant": ("EntidadeEnvelopeGenerico->" if envelope_metadata else "") + decoded_type,
        "source": {"file_name": rdv_path.name, "relative_path": metadata.get("relative_path"),
                   "url": metadata.get("url"), "acquisition_http": metadata.get("http"),
                   "manifest_content_hash": metadata.get("manifest_hash"), "sha256": file_hash,
                   "size_bytes": len(raw), "raw_file_preserved_separately": True},
        "schema": {"source": RDV_SCHEMA_SOURCE, "bundle_sha256": RDV_SCHEMA_BUNDLE_SHA256,
                   "schema_sha256": schema_hash, "expected_schema_sha256": RDV_SCHEMA_SHA256,
                   "matches_official_copy": schema_hash == RDV_SCHEMA_SHA256},
        "entity_metadata": {"envelope": _json_safe(envelope_metadata), "registry": _json_safe(registry_meta), "result_header": _json_safe(root_fields), "urn": _json_safe(urn),
                            "elections_choice": choice_name},
        "integrity": {"sha256_matches_acquisition_manifest": True,
                      "size_matches_acquisition_manifest": True,
                      "ber_structure_valid": True, "asn1_decode": "decoded",
                      "every_vote_roundtrip_matches": True,
                      "section_identity": "matched"},
        "election_count": len(elections), "record_count": all_records,
        "ordering_note": "record_position_within_election_and_cargo reflects source sequence only; it has no chronological meaning and is not a key shared across cargos",
        "individual_record_join_policy": "RDV records are not associated with electors, enablement times, or log events",
        "elections": elections, "reconciliation_with_bu": reconciliation,
    }


def diagnose_rdv_failure(rdv_path: Path, collection: dict, error: BaseException,
                         collection_root: Path | None = None) -> dict:
    section = collection.get("section_key")
    files = collection.get("files", [])
    metadata = next((item for item in files if item.get("type") == "rdv" and
        (item.get("name") == rdv_path.name and (collection_root is None or
         (collection_root / item.get("relative_path", "")).resolve() == rdv_path.resolve()))), {})
    try:
        raw = rdv_path.read_bytes()
    except OSError:
        raw = b""
    message = str(error)
    lowered = message.lower()
    if "hash" in lowered or "integrity" in lowered:
        error_type, code, stage = "integrity", "RDV_INTEGRITY_MISMATCH", "integrity_validation"
    elif "encrypted" in lowered or "cifrad" in lowered:
        error_type, code, stage = "decryption", "RDV_ENCRYPTED_UNSUPPORTED", "decryption"
    elif "section identity" in lowered:
        error_type, code, stage = "semantic_validation", "RDV_SECTION_IDENTITY_MISMATCH", "semantic_validation"
    elif "unsupported" in lowered or "choice tag" in lowered or "not rdv" in lowered:
        error_type, code, stage = "format_incompatibility", "RDV_FORMAT_UNSUPPORTED", "format_detection"
    elif isinstance(error, BERParseError):
        error_type, code, stage = "parsing", "RDV_BER_INVALID", "ber_parsing"
    elif "trailing bytes" in lowered or "count differs" in lowered or "boundary" in lowered:
        error_type, code, stage = "parsing", "RDV_STRUCTURE_INCONSISTENT", "structure_validation"
    elif isinstance(error, (ValueError, TypeError)):
        error_type, code, stage = "decoding", "RDV_ASN1_DECODE_FAILED", "asn1_decoding"
    else:
        error_type, code, stage = "internal", "RDV_PROCESSING_FAILED", "rdv_processing"
    format_detected = "BER/ASN.1" if raw else "unknown"
    position = {"byte_offset": error.offset} if isinstance(error, BERParseError) else getattr(error, "position", {})
    diag = diagnostic(code=code, stage=stage, operation="decode_rdv", error_type=error_type,
        cause=message, recoverability="permanent" if error_type in {"format_incompatibility", "parsing", "semantic_validation"} else "indeterminate",
        section=section, file_type="rdv", file_name=rdv_path.name, file_path=str(rdv_path), exception=error,
        position=position, detected_format=format_detected, version=RDV_SCHEMA_SHA256,
        context={"size_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest() if raw else None,
                 "schema_version_or_hash": RDV_SCHEMA_SHA256, "manifest_hash": metadata.get("manifest_hash")})
    return {"status": "failed", "format_variant": None,
        "source": {"file_name": rdv_path.name, "relative_path": metadata.get("relative_path"),
                   "sha256": hashlib.sha256(raw).hexdigest() if raw else None, "size_bytes": len(raw),
                   "raw_file_preserved_separately": rdv_path.exists()},
        "partial_interpretation": {"format_detected": format_detected,
            "first_ber_tag_hex": raw[:1].hex() if raw else None,
            "bytes_read": len(raw), "complete_structural_decode": False},
        "integrity": {"sha256_matches_acquisition_manifest": bool(metadata) and hashlib.sha256(raw).hexdigest() == metadata.get("sha256"),
                      "asn1_decode": "failed"},
        "diagnostics": [diag]}


def reconcile(elections: list[dict], bu: dict) -> dict:
    """Compare only categories whose semantics are explicit and aggregate totals."""
    bu_cargos = {}
    for election in bu["elections"]:
        for result in election["results"]:
            for cargo in result["cargos"]:
                bu_cargos[(str(election["id_eleicao"]), cargo["cargo_kind"], cargo["cargo"])] = cargo
    comparisons = []
    for election in elections:
        for cargo in election["cargos"]:
            key = (str(election["election_id"]), cargo["cargo_kind"], cargo["cargo_code"])
            target = bu_cargos.get(key)
            if target is None:
                comparisons.append({"election_id": election["election_id"], **{k: cargo[k] for k in ("cargo_kind", "cargo_code")}, "status": "no_matching_bu_cargo"})
                continue
            rdv_total = cargo["record_count"]
            bu_total = target["qtd_votos_bu"]
            exact_categories = {
                "blank_including_suspension_variants": _scalar_comparison(cargo["white_votes_total"], target["vote_type_counts"].get("branco", 0)),
                "null_including_suspension_variants": _scalar_comparison(cargo["null_votes_total"], target["vote_type_counts"].get("nulo", 0)),
            }
            candidate_numbers = _compare_counter(cargo["candidate_number_counts_from_nominal_votes"], target.get("nominal_by_number", {}))
            party_numbers = _compare_counter(cargo["party_number_counts_from_legend_votes"], target.get("legend_by_number", {}))
            subcategory_divergence = any(x["divergence"] for x in exact_categories.values()) or any(x["divergence"] for x in candidate_numbers.values()) or any(x["divergence"] for x in party_numbers.values())
            status = "divergence" if rdv_total != bu_total or subcategory_divergence else "matched"
            comparisons.append({"election_id": election["election_id"], **{k: cargo[k] for k in ("cargo_kind", "cargo_code")},
                "status": status,
                "rdv_record_count": rdv_total, "bu_vote_total": bu_total,
                "divergence_rdv_minus_bu": rdv_total - bu_total,
                "exact_category_comparisons": exact_categories,
                "candidate_numbers": candidate_numbers,
                "party_numbers": party_numbers,
                "limitations": "RDV vote types that BU schema does not represent distinctly remain separate and are included only in total record count"})
    statuses = [row["status"] for row in comparisons]
    overall = "matched" if statuses and all(status == "matched" for status in statuses) else "divergence" if "divergence" in statuses else "partial"
    return {"status": overall, "by_cargo": comparisons,
            "note": "candidate and party comparisons use decoded numeric keys only; white, null, and suspension-specific RDV categories are preserved individually and not collapsed into BU categories"}


def _compare_counter(rdv: dict, bu: dict) -> dict:
    keys = sorted(set(rdv) | set(bu), key=lambda value: int(value))
    return {key: {"rdv": rdv.get(key, 0), "bu": bu.get(key, 0), "divergence": rdv.get(key, 0) - bu.get(key, 0)} for key in keys}


def _scalar_comparison(rdv: int, bu: int) -> dict:
    return {"rdv": rdv, "bu": bu, "divergence": rdv - bu}
