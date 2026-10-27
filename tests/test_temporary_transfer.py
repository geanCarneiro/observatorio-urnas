import csv
import hashlib
import io
import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from temporary_transfer import (MEMBER_CSV, TransferReferenceIndex, load_reference,
    reconcile_section, update_reference, write_reconciliation, write_section_excerpts, write_validation)
from analyze_section import presidential_only_session_metrics
from extract_section_data import extract_collection


HEADERS = ["DT_GERACAO", "HH_GERACAO", "AA_ELEICAO", "NR_TURNO", "TP_TTE", "TP_ABRANGENCIA_TTE",
    "SG_UF_ORIGEM", "CD_MUNICIPIO_ORIGEM", "NM_MUNICIPIO_ORIGEM", "NR_ZONA_ORIGEM", "NR_SECAO_ORIGEM",
    "SG_UF_DESTINO", "CD_MUNICIPIO_DESTINO", "NM_MUNICIPIO_DESTINO", "NR_ZONA_DESTINO", "NR_SECAO_DESTINO",
    "QT_ELEITOR"]


def reference_zip(rows):
    output = io.BytesIO()
    text = io.StringIO(newline="")
    writer = csv.DictWriter(text, fieldnames=HEADERS, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(MEMBER_CSV, text.getvalue().encode("latin-1"))
    return output.getvalue()


def row(section, transfer_type, scope, count, turn="2"):
    return {"DT_GERACAO": "26/10/2026", "HH_GERACAO": "06:30:08", "AA_ELEICAO": "2026",
        "NR_TURNO": turn, "TP_TTE": transfer_type, "TP_ABRANGENCIA_TTE": scope,
        "SG_UF_ORIGEM": "ES", "CD_MUNICIPIO_ORIGEM": "56251", "NM_MUNICIPIO_ORIGEM": "CARIACICA",
        "NR_ZONA_ORIGEM": "54", "NR_SECAO_ORIGEM": "1", "SG_UF_DESTINO": "ES",
        "CD_MUNICIPIO_DESTINO": "56251", "NM_MUNICIPIO_DESTINO": "CARIACICA",
        "NR_ZONA_DESTINO": "54", "NR_SECAO_DESTINO": section, "QT_ELEITOR": str(count)}


def index_from_rows(directory: Path, rows):
    raw = reference_zip(rows)
    path = directory / "reference.zip"
    path.write_bytes(raw)
    digest = hashlib.sha256(raw).hexdigest()
    provenance = {"resource_id": "fixture", "resource_metadata": {"name": "fixture"},
        "sha256": digest, "obtained_at_utc": "2026-10-27T00:00:00Z"}
    return TransferReferenceIndex(path, provenance, digest)


class TemporaryTransferTests(unittest.TestCase):
    def test_reads_official_csv_schema_and_normalizes_destination_and_category(self):
        with tempfile.TemporaryDirectory() as temporary:
            index = index_from_rows(Path(temporary), [
                row("0037", "Voto em Trânsito", "Federal", 14),
                row("0037", "Militar em Serviço", "Federal", 2),
                row("0037", "Situação de Rua", "Municipal", 3),
            ])
            self.assertIn("SG_UF_DESTINO", index.schema_columns)
            self.assertIn("TP_TTE", index.schema_columns)
            self.assertEqual(index.federal_count("es", "56251", "54", "37", 2), 16)
            self.assertEqual(index.section_breakdown("ES", "56251", "0054", "0037", 2), [
                {"tipo_transferencia": "Militar em Serviço", "abrangencia": "Federal", "quantidade_eleitores": 2},
                {"tipo_transferencia": "Situação de Rua", "abrangencia": "Municipal", "quantidade_eleitores": 3},
                {"tipo_transferencia": "Voto em Trânsito", "abrangencia": "Federal", "quantidade_eleitores": 14},
            ])
            self.assertEqual(index.status, "complete")

    def test_classifies_equal_lower_higher_and_missing_log_metrics(self):
        with tempfile.TemporaryDirectory() as temporary:
            index = index_from_rows(Path(temporary), [row("0067", "Voto em Trânsito", "Federal", 13)])
            complete = {"count": 13, "status": "complete", "ambiguous_session_count": 0,
                        "unassigned_event_count": 0}
            self.assertEqual(reconcile_section(index, "es/56251/0054/0067", 2, complete)["status"], "confirmado")
            lower = {**complete, "count": 10}
            self.assertEqual(reconcile_section(index, "es/56251/0054/0067", 2, lower)["status"], "compatível")
            higher = {**complete, "count": 14}
            self.assertEqual(reconcile_section(index, "es/56251/0054/0067", 2, higher)["status"], "divergente")
            self.assertEqual(reconcile_section(index, "es/56251/0054/0067", 2, None)["status"], "dados_insuficientes")
            self.assertFalse(reconcile_section(index, "es/56251/0054/0067", 2, complete)["individual_session_tte_join_performed"])

    def test_nonsecond_turn_and_incomplete_reference_are_insufficient(self):
        with tempfile.TemporaryDirectory() as temporary:
            index = index_from_rows(Path(temporary), [row("0037", "Voto em Trânsito", "Federal", 1)])
            metric = {"count": 1, "status": "complete"}
            self.assertEqual(reconcile_section(index, "es/56251/0054/0037", 1, metric)["status"], "dados_insuficientes")
            self.assertEqual(reconcile_section(index, "es/56251/0054/0037", 2,
                {"count": 1, "status": "incomplete"})["status"], "dados_insuficientes")

    def test_json_and_csv_exports_keep_breakdown_and_source_hash(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            index = index_from_rows(root, [row("0609", "Militar em Serviço", "Federal", 1)])
            report = reconcile_section(index, "es/56251/0054/0609", 2,
                {"count": 1, "status": "complete", "ambiguous_session_count": 0,
                 "unassigned_event_count": 0})
            json_path, csv_path = root / "reconciliation.json", root / "reconciliation.csv"
            write_reconciliation([report], json_path, csv_path)
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            with csv_path.open(encoding="utf-8-sig", newline="") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(payload["sections"][0]["status"], "confirmado")
            self.assertEqual(rows[0]["tipo_transferencia"], "Militar em Serviço")
            self.assertEqual(rows[0]["source_zip_sha256"], index.expected_sha256)

    def test_validation_artifact_is_written_for_insufficient_cross_without_redistributing_reference(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            acquisition_dir = root / "es/56251/0054/0067/turno2"
            acquisition_dir.mkdir(parents=True)
            acquisition_path = acquisition_dir / "acquisition.json"
            acquisition_path.write_text(json.dumps({"section_key": "es/56251/0054/0067", "turno": 2}))
            missing_reference = root / "missing-reference"
            missing_reference.mkdir()
            missing_reference.joinpath("reference.json").write_text(json.dumps({
                "resource_id": "fixture-id", "current_zip": "official-v2.zip",
                "versions": [{"zip_file": "official-v2.zip", "sha256": "f" * 64,
                              "obtained_at_utc": "2026-10-27T00:00:00Z"}]}))
            result = {"transferencia_temporaria": {
                "section_key": "es/56251/0054/0067", "turno": 2, "status": "dados_insuficientes",
                "reason": "reference unavailable", "log_presidential_only_computed_sessions": 13,
                "reference_federal_transfer_count": None, "reference": None}}
            with patch("extract_section_data.analyze_collection", return_value=result), \
                    patch("extract_section_data.write_validation", side_effect=lambda rows, path, source_note:
                          write_validation(rows, path, missing_reference, source_note)):
                extract_collection(acquisition_path, Path("bu.asn1"), Path("rdv.asn1"))
            validation_path = acquisition_dir / "transferencia-temporaria-validation.json"
            self.assertTrue(validation_path.exists())
            validation = json.loads(validation_path.read_text(encoding="utf-8"))
            self.assertFalse(validation["reference"]["base_embedded_in_artifact"])
            self.assertEqual(validation["reference"]["zip_sha256"], "f" * 64)
            self.assertFalse(validation["reference"]["available"])
            self.assertEqual(validation["sections_examined"][0]["section_key"], "es/56251/0054/0067")
            self.assertEqual(validation["sections_examined"][0]["status"], "dados_insuficientes")
            self.assertEqual(validation["status_counts"], {"dados_insuficientes": 1})

    def test_section_excerpt_keeps_original_columns_and_counts_every_federal_modality(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            index = index_from_rows(root, [
                row("0609", "Voto em Trânsito", "Federal", 0),
                row("0609", "Militar em Serviço", "Federal", 1),
                row("0609", "Justiça Eleitoral", "Federal", 2),
                row("0609", "Militar em Serviço", "Estadual", 10),
                row("0610", "Voto em Trânsito", "Federal", 99),
            ])
            output = root / "coletada/turno2/transferencia-temporaria-elegiveis.csv"
            result = write_section_excerpts([{
                "section_key": "es/56251/0054/0609", "turn": 2, "output_path": output}], index=index)
            with output.open(encoding="utf-8-sig", newline="") as stream:
                reader = csv.DictReader(stream, delimiter=";")
                rows = list(reader)
                fields = reader.fieldnames
            self.assertEqual(fields, HEADERS)
            self.assertEqual(len(rows), 4)
            self.assertEqual({item["NR_SECAO_DESTINO"] for item in rows}, {"0609"})
            metadata = result[str(output)]
            self.assertEqual(metadata["row_count"], 4)
            self.assertEqual(metadata["registered_electors_all_scopes"], 13)
            self.assertEqual(metadata["registered_electors_federal_scope"], 3)
            self.assertEqual(metadata["reference_zip_sha256"], index.expected_sha256)

    def test_explicit_refresh_uses_ckan_discovered_url_and_preserves_version(self):
        class Response:
            status = 200

            def __init__(self, body, url, headers=None):
                self.body, self.url = body, url
                self.headers = headers or {}

            def __enter__(self): return self
            def __exit__(self, *_): return False
            def read(self): return self.body
            def geturl(self): return self.url

        metadata = {"success": True, "result": {"id": "fixture-id", "url": "https://cdn.example/new.zip", "name": "fixture"}}
        changed_metadata = {"success": True, "result": {"id": "fixture-id", "url": "https://cdn.example/newer.zip", "name": "fixture"}}
        with tempfile.TemporaryDirectory() as temporary, patch("temporary_transfer.urllib.request.urlopen", side_effect=[
                Response(json.dumps(metadata).encode(), "https://ckan.example/api", {"Date": "Tue, 27 Oct 2026 00:00:00 GMT"}),
                Response(b"zip bytes", "https://cdn.example/redirected.zip", {"Content-Type": "application/zip"}),
                Response(json.dumps(changed_metadata).encode(), "https://ckan.example/api", {"Date": "Wed, 28 Oct 2026 00:00:00 GMT"}),
                Response(b"new zip bytes", "https://cdn.example/newer.zip", {"Content-Type": "application/zip"})]) as opener:
            original = update_reference(Path(temporary), "fixture-id")
            result = update_reference(Path(temporary), "fixture-id")
            self.assertEqual(opener.call_count, 4)
            self.assertEqual(opener.call_args_list[1].args[0].full_url, "https://cdn.example/new.zip")
            self.assertEqual(opener.call_args_list[3].args[0].full_url, "https://cdn.example/newer.zip")
            manifest = json.loads((Path(temporary) / "reference.json").read_text())
            self.assertEqual(manifest["current_zip"], Path(result["path"]).name)
            self.assertEqual(len(manifest["versions"]), 2)
            self.assertEqual(Path(original["path"]).read_bytes(), b"zip bytes")
            self.assertTrue(Path(result["path"]).exists())

    def test_only_clean_computed_presidential_sessions_count(self):
        sessions = [
            {"computed": True, "computed_event": {"event_date": "2026-10-25"},
             "confirmation_events": [{"cargo": "presidente"}], "state_events": []},
            {"computed": True, "computed_event": {"event_date": "2026-10-25"},
             "confirmation_events": [{"cargo": "presidente"}, {"cargo": "governador"}], "state_events": []},
            {"computed": True, "computed_event": {"event_date": "2026-10-25"},
             "confirmation_events": [{"cargo": "presidente"}], "state_events": [{"event_type": "falha"}]},
            {"computed": False, "computed_event": None,
             "confirmation_events": [{"cargo": "presidente"}], "state_events": []},
        ]
        result = presidential_only_session_metrics(sessions, "2026-10-25")
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["ambiguous_session_count"], 1)
        self.assertEqual(result["computed_session_count"], 3)
        self.assertEqual(result["status"], "incomplete")


if __name__ == "__main__":
    unittest.main()
