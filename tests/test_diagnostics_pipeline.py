import hashlib
import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import asn1tools

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from analyze_section import analyze_collection, log_bu_reconciliation_diagnostics, parse_cuts
from analyze_sections import analyze_sections
from collect_sections import collect_sections
from consolidate_diagnostics import consolidate_paths
from collect_section import DownloadIncompleteError, fetch
from diagnostics import summarize_diagnostics, verify_jws_signature


ROOT = Path(__file__).resolve().parents[1]


class DiagnosticPipelineTests(unittest.TestCase):
    def test_corrupt_truncated_and_unknown_files_do_not_stop_other_inputs(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            good_log = root / "good-log.zip"
            with zipfile.ZipFile(good_log, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("logd.dat", "01/01/2026 17:01:00\tx\tx\tx\tEleitor foi habilitado\nnot-a-time\tx\tx\tx\tO voto do eleitor foi computado\n")
            bad_log = root / "corrupt-log.zip"
            bad_log.write_bytes(b"not a zip archive")
            truncated_bu = root / "truncated-bu.dat"
            truncated_bu.write_bytes(b"\x30\x82\x01")
            unknown_rdv = root / "unknown-rdv.dat"
            unknown_rdv.write_bytes(b"\x30\x00")
            truncated_rdv = root / "truncated-rdv.dat"
            truncated_rdv.write_bytes(b"\x30\x82\x00")
            items = []
            for kind, path in (("log", good_log), ("log", bad_log), ("bu", truncated_bu),
                               ("rdv", unknown_rdv), ("rdv", truncated_rdv)):
                raw = path.read_bytes()
                items.append({"type": kind, "name": path.name, "relative_path": path.name,
                    "sha256": hashlib.sha256(raw).hexdigest(), "size_bytes": len(raw), "manifest_hash": "fixture"})
            collection = root / "collection.json"
            collection.write_text(json.dumps({"section_key": "es/56251/0054/0509", "files": items,
                "diagnostics": [], "file_states": []}), encoding="utf-8")

            report = analyze_collection(collection, parse_cuts(["17:00"]), ROOT / "schemas/bu.asn1", ROOT / "schemas/rdv.asn1")
            codes = {item["code"] for item in report["diagnostics"]}
            self.assertIn("LOG_ZIP_OPEN_FAILED", codes)
            self.assertIn("LOG_EVENT_TIMESTAMP_INVALID", codes)
            self.assertIn("BU_ENVELOPE_DECODE_FAILED", codes)
            self.assertIn("RDV_FORMAT_UNSUPPORTED", codes)
            self.assertIn("RDV_BER_INVALID", codes)
            self.assertEqual(len(report["file_processing"]["logs"]), 2)
            self.assertEqual(len(report["file_processing"]["bus"]), 1)
            self.assertEqual(len(report["file_processing"]["rdvs"]), 2)
            truncated_diag = next(item for item in report["diagnostics"] if item["code"] == "RDV_BER_INVALID")
            self.assertEqual(truncated_diag["position"]["byte_offset"], 1)
            self.assertEqual(report["file_processing"]["logs"][0]["events"][0]["line_number"], 1)
            self.assertEqual(report["overall_status"], "partial")
            self.assertNotEqual(report["validity"], "valid")
            summary = summarize_diagnostics(report["diagnostics"], report["file_processing"]["states"])
            self.assertGreater(summary["diagnostic_count"], 0)
            self.assertIn("es/56251/0054/0509", summary["by_dimension"]["section"])

    def test_cryptographic_signature_failure_is_structured(self):
        class InvalidSignature(Exception):
            pass

        def verifier(_raw):
            raise InvalidSignature("bad synthetic signature")

        status, item = verify_jws_signature(b"synthetic-jws", section="es/56251/0054/0509",
            file_name="manifest.aux.jws", verifier=verifier)
        self.assertEqual(status, "invalid")
        self.assertEqual(item["code"], "JWS_SIGNATURE_INVALID")
        self.assertEqual(item["error_type"], "signature_cryptographic")
        self.assertEqual(item["recoverability"], "permanent")
        self.assertEqual(item["exception"]["type"], "InvalidSignature")

    def test_log_bu_reconciliation_reports_numeric_divergences(self):
        validation = {"bu_reconciliation": {
            "by_attendance_metric": {"votos": {"log": 4, "bu": 5, "divergence_log_minus_bu": -1}},
            "by_cargo": [{"cargo": "presidente", "divergence_log_vs_bu_votes": 2}],
        }}
        items = log_bu_reconciliation_diagnostics(validation, "es/56251/0054/0509", Path("synthetic-log.zip"), "secao")
        self.assertEqual(len(items), 2)
        self.assertTrue(all(item["error_type"] == "reconciliation" for item in items))

    def test_collector_preserves_and_classifies_incomplete_download_body(self):
        class Headers(dict):
            pass

        class Response:
            status = 200
            headers = Headers({"Content-Length": "9", "Content-Type": "application/octet-stream"})
            calls = 0

            def __enter__(self): return self
            def __exit__(self, *_): return False
            def read(self, _limit):
                self.calls += 1
                return b"partial" if self.calls == 1 else b""
            def geturl(self): return "https://example.invalid/file"

        with patch("collect_section.urllib.request.urlopen", return_value=Response()):
            with self.assertRaises(DownloadIncompleteError) as caught:
                fetch("https://example.invalid/file")
        self.assertEqual(caught.exception.body, b"partial")

    def test_consolidated_report_keeps_section_and_input_failures(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            good = root / "section.json"
            good.write_text(json.dumps({"section_key": "es/56251/0054/0509", "overall_status": "partial",
                "validity": "not_validated", "diagnostics": [{"code": "RDV_BER_INVALID", "error_type": "parsing",
                    "detected_format": "BER", "version": "schema-v1", "urn_model": "secao", "section": "es/56251/0054/0509"}],
                "file_processing": {"states": [{"type": "rdv", "status": "failed", "section": "es/56251/0054/0509"},
                    {"type": "vota.vsc", "status": "not_applicable", "section": "es/56251/0054/0509"}]}}))
            broken = root / "broken.json"
            broken.write_text("{")
            report = consolidate_paths([good, broken])
            self.assertEqual(report["section_count"], 1)
            self.assertEqual(report["input_error_count"], 1)
            self.assertEqual(report["summary"]["by_dimension"]["detected_format"], {"BER": 1, "unknown": 1})
            self.assertEqual(report["summary"]["file_status_counts"], {"failed": 1, "not_applicable": 1})

    def test_batch_collection_continues_after_section_failure(self):
        class Completed:
            def __init__(self, code, stdout="", stderr=""):
                self.returncode, self.stdout, self.stderr = code, stdout, stderr

        calls = []

        def runner(command, **_kwargs):
            calls.append(command)
            if len(calls) == 1:
                return Completed(1, stderr="synthetic section failure")
            return Completed(0, stdout=json.dumps({"section_key": "es/2/0001/0002", "diagnostics": [],
                "file_states": [], "report": "collection.json"}))

        report = collect_sections([{"uf": "es", "municipio": "1", "zona": "1", "secao": "1"},
                                  {"uf": "es", "municipio": "2", "zona": "1", "secao": "2"}],
                                 Path("unused-output"), runner)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][calls[0].index("--data-id") + 1], "3221")
        self.assertEqual([section["status"] for section in report["sections"]], ["failed", "collected"])
        self.assertEqual(report["section_count"], 2)
        self.assertEqual(report["diagnostics"][0]["code"], "BATCH_SECTION_COLLECTION_FAILED")

    def test_batch_analysis_continues_after_bad_section_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            broken = root / "broken-collection.json"
            broken.write_text("not-json")
            valid = root / "valid-collection.json"
            valid.write_text(json.dumps({"section_key": "es/2/0001/0002", "files": [], "file_states": [], "diagnostics": []}))
            result = analyze_sections([broken, valid], root / "reports", [], ROOT / "schemas/bu.asn1", ROOT / "schemas/rdv.asn1")
            self.assertEqual(result["section_count"], 2)
            self.assertEqual(len(result["section_reports"]), 2)
            self.assertTrue((root / "reports/section-00001.json").exists())
            self.assertTrue((root / "reports/section-00002.json").exists())

    def test_synthetic_bu_is_processed_without_upgrading_unverified_run_to_valid(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            schema = ROOT / "schemas/bu.asn1"
            spec = asn1tools.compile_files(str(schema), codec="ber")
            identity = {"municipioZona": {"municipio": 56251, "zona": 54}, "local": 1, "secao": 509}
            urna = {"tipoUrna": "secao", "versaoVotacao": "synthetic", "correspondenciaResultado": {
                "identificacao": ("identificacaoSecaoEleitoral", identity), "carga": {"numeroInternoUrna": 1,
                    "numeroSerieFC": b"1234", "identificadorGeradorMidia": {"nome": "fixture", "serialCertificadoTPM": "fixture", "serialInstalacao": "fixture"},
                    "dataHoraCarga": "20261001T120000", "codigoCarga": "fixture"}},
                "tipoArquivo": "votacaoUE", "numeroSerieFV": b"5678"}
            bu = {"cabecalho": {"dataGeracao": "20261026T120000", "idEleitoral": ("idPleito", 304)},
                "fase": "oficial", "urna": urna, "identificacaoSecao": identity, "dataHoraEmissao": "20261026T120000",
                "dadosSecaoSA": ("dadosSecao", {"dataHoraAbertura": "20261026T090000", "dataHoraEncerramento": "20261026T170000"}),
                "qtdEleitoresCompareceram": 0, "resultadosVotacaoPorEleicao": [], "historicoCodigosCarga": []}
            content = spec.encode("EntidadeBoletimUrna", bu)
            envelope = {"cabecalho": bu["cabecalho"], "fase": "oficial",
                "identificacao": ("identificacaoSecaoEleitoral", identity), "tipoEnvelope": "envelopeBoletimUrna", "conteudo": content}
            raw = spec.encode("EntidadeEnvelopeGenerico", envelope)
            path = root / "synthetic-bu.dat"
            path.write_bytes(raw)
            collection = root / "collection.json"
            collection.write_text(json.dumps({"section_key": "es/56251/0054/0509", "files": [{"type": "bu",
                "name": path.name, "relative_path": path.name, "sha256": hashlib.sha256(raw).hexdigest(), "size_bytes": len(raw)}],
                "diagnostics": [], "file_states": []}))

            report = analyze_collection(collection, parse_cuts([]), schema, ROOT / "schemas/rdv.asn1")
            self.assertEqual(report["file_processing"]["bus"][0]["status"], "processed")
            self.assertEqual(report["file_processing"]["bus"][0]["urn_model"], "secao")
            self.assertEqual(report["overall_status"], "partial")
            self.assertNotEqual(report["validity"], "valid")
            self.assertIn("not_processed", {state["status"] for state in report["file_processing"]["states"]})


if __name__ == "__main__":
    unittest.main()
