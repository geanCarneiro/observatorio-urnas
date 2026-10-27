import hashlib
import base64
import json
import io
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import asn1tools

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from analyze_section import (analyze_collection, closure_sequence_matches_observed_order,
    log_bu_reconciliation_diagnostics, parse_cuts, process_log_file,
    reconcile_log_metrics, reconstruct_voting_sessions)
from analyze_sections import analyze_sections
from collect_sections import collect_sections
from consolidate_diagnostics import consolidate_paths
from collect_section import DownloadIncompleteError, canonical_manifest_file_type, fetch
import collect_section
from extract_section_data import extract_collection
from diagnostics import summarize_diagnostics, verify_jws_signature


ROOT = Path(__file__).resolve().parents[1]


class DiagnosticPipelineTests(unittest.TestCase):
    def test_official_vota_type_with_vsc_suffix_is_recognized_as_signature_bundle(self):
        self.assertEqual(canonical_manifest_file_type("vota", "es-p0001-s0001-vota.vsc"), "vota.vsc")
        self.assertEqual(canonical_manifest_file_type("bu", "section.bu"), "bu")

    def test_collector_marks_vsc_not_applicable_without_downloading_it(self):
        def b64url(value):
            return base64.urlsafe_b64encode(value).decode().rstrip("=")

        manifest = ".".join((
            b64url(b'{"alg":"none"}'),
            b64url(json.dumps({"hashes": [{"hash": "vsc-manifest-hash", "arq": [
                {"tp": "vota", "nm": "es-p0001-s0001-vota.vsc"}]}]}).encode()),
            "c2ln",
        )).encode()
        with tempfile.TemporaryDirectory() as temp, patch("collect_section.sys.argv", [
                "collect_section.py", "--uf", "es", "--municipio", "56251", "--zona", "0054",
                "--secao", "0509", "--output", temp]), \
             patch("collect_section.fetch", return_value=(manifest, {"status": "200"})) as fetch_mock, \
             patch("sys.stdout", new_callable=io.StringIO):
            self.assertEqual(collect_section.main(), 0)
            self.assertEqual(fetch_mock.call_count, 1)  # only aux.jws was fetched
            collection_path = next(Path(temp).rglob("acquisition.json"))
            collection = json.loads(collection_path.read_text(encoding="utf-8"))
            self.assertEqual(collection["files"], [])
            vsc_state = next(state for state in collection["file_states"] if state["type"] == "vota.vsc")
            self.assertEqual(vsc_state["status"], "not_applicable")
            self.assertEqual(vsc_state["manifest_hash"], "vsc-manifest-hash")
            self.assertFalse(any(item["code"] == "COLLECT_FILE_TYPE_UNSUPPORTED" for item in collection["diagnostics"]))
            self.assertEqual(collection["turn_directory"], "turno-desconhecido")
            self.assertNotIn("diagnostic_summary", collection)

    def test_log_activity_is_partitioned_by_event_date(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "log.zip"
            with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("logd.dat", "\n".join([
                    "04/10/2026 17:01:00\tx\tx\tx\tEleitor foi habilitado",
                    "25/10/2026 17:02:00\tx\tx\tx\tEleitor foi habilitado",
                    "25/10/2026 17:03:00\tx\tx\tx\tO voto do eleitor foi computado",
                ]))
            item = {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            result = process_log_file(path, item, "es/56251/0054/0509", parse_cuts(["17:00"]))
            self.assertEqual(result["event_counts_by_date"]["2026-10-04"]["habilitacao"], 1)
            self.assertEqual(result["event_counts_by_date"]["2026-10-25"]["habilitacao"], 1)
            self.assertEqual(result["events_by_date"]["2026-10-25"]["event_counts"]["voto_computado"], 1)
            self.assertEqual(result["activity_after_clock_cuts_by_date"]["2026-10-04"]["habilitacao_a_partir_de_17h00"], 1)
            self.assertEqual(result["events"][0]["at_or_after_clock_cuts"], ["17:00"])

    def test_nested_jez_logs_are_recursive_deduplicated_and_keep_source_equipment_identity(self):
        def zipped_log(lines, nested=()):
            output = io.BytesIO()
            with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                if lines is not None:
                    archive.writestr("logd.dat", "\n".join(lines) + "\n")
                for name, body in nested:
                    archive.writestr(name, body)
            return output.getvalue()

        nested = zipped_log([
            "25/10/2026 09:00:01\tx\tx\tx\tEleitor foi habilitado",
            "25/10/2026 09:00:02\tx\tx\tx\tVotação suspensa",
            "25/10/2026 09:00:03\tx\tx\tx\tFalha durante a votação",
        ])
        alternate_packaging = io.BytesIO()
        with zipfile.ZipFile(alternate_packaging, "w", compression=zipfile.ZIP_STORED) as archive:
            archive.writestr("logd.dat", "25/10/2026 09:00:01\tx\tx\tx\tEleitor foi habilitado\n"
                "25/10/2026 09:00:02\tx\tx\tx\tVotação suspensa\n"
                "25/10/2026 09:00:03\tx\tx\tx\tFalha durante a votação\n")
        root_raw = zipped_log([
            "25/10/2026 08:00:01\tx\tx\tx\tEleitor foi habilitado",
            "25/10/2026 08:00:02\tx\tx\tx\tVoto confirmado para [Governador]",
            "25/10/2026 08:00:03\tx\tx\tx\tO voto do eleitor foi computado",
        ], (("urna-anterior.jez", nested), ("copia-urna-anterior.jez", alternate_packaging.getvalue())))
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "urna-atual.jez"
            path.write_bytes(root_raw)
            result = process_log_file(path, {"sha256": hashlib.sha256(root_raw).hexdigest()},
                "es/56251/0054/0509", [])

        self.assertEqual(result["event_counts"]["habilitacao"], 2)
        self.assertEqual(result["event_counts"]["suspensao"], 1)
        self.assertEqual(result["event_counts"]["falha"], 1)
        self.assertEqual(result["event_counts"]["voto_computado"], 1)
        self.assertEqual(result["nested_archive_count"], 2)
        nested_source = next(item for item in result["source_files"] if item["sha256"] != hashlib.sha256(root_raw).hexdigest())
        nested_event = next(event for event in result["events"] if event["event_type"] == "suspensao")
        self.assertEqual(len(nested_event["source_aliases"]), 2)
        self.assertEqual(result["events"][-1]["event_timestamp_raw"], "25/10/2026 09:00:03")
        self.assertTrue(result["events"][-1]["equipment_identifier"].startswith("log-stream-sha256:"))

    def test_sessions_separate_habilitation_confirmation_computation_and_state_events(self):
        def event(line, kind, time):
            return {"line_number": line, "event_type": kind, "event_timestamp_raw": time,
                "event_date": "25/10/2026", "equipment_identifier": "device-hash"}

        result = reconstruct_voting_sessions([
            event(1, "voto_confirmado_governador", "25/10/2026 08:00:01"),
            event(2, "habilitacao", "25/10/2026 08:01:00"),
            event(3, "voto_confirmado_governador", "25/10/2026 08:01:10"),
            event(4, "interrupcao", "25/10/2026 08:01:15"),
            event(5, "voto_computado", "25/10/2026 08:01:20"),
            event(6, "habilitacao", "25/10/2026 08:02:00"),
            event(7, "voto_confirmado_presidente", "25/10/2026 08:02:10"),
        ])

        self.assertEqual(len(result["sessions"]), 2)
        self.assertEqual(result["sessions"][0]["status"], "computed")
        self.assertEqual(result["sessions"][0]["cargo_confirmations"]["governador"][0]["line_number"], 3)
        self.assertEqual(result["sessions"][0]["state_events"][0]["event_type"], "interrupcao")
        self.assertFalse(result["sessions"][1]["computed"])
        self.assertEqual(result["sessions"][1]["status"], "incomplete_at_end_of_log")
        self.assertEqual(result["unassigned_events"][0]["reason"], "confirmation_without_open_habilitation")

    def test_reconciliation_never_uses_habilitations_and_marks_cargo_confirmation_deltas_inconclusive(self):
        def event(line, kind):
            return {"line_number": line, "event_type": kind, "event_timestamp_raw": f"25/10/2026 08:00:{line:02d}",
                "event_date": "2026-10-25", "equipment_identifier": "device-hash"}

        events = [event(1, "habilitacao"), event(2, "habilitacao"), event(3, "habilitacao"),
                  event(4, "voto_confirmado_governador"), event(5, "voto_confirmado_presidente"),
                  event(6, "voto_computado"), event(7, "habilitacao"), event(8, "voto_computado")]
        sessions = reconstruct_voting_sessions(events)["sessions"]
        bu = {"qtd_eleitores_compareceram": 2, "elections": [{"id_eleicao": 304, "results": [{"cargos": [
            {"cargo_kind": "cargoConstitucional", "cargo": 1, "qtd_votos_bu": 2},
            {"cargo_kind": "cargoConstitucional", "cargo": 3, "qtd_votos_bu": 2},
        ]}]}]}
        rdvs = [{"elections": [{"election_id": 304, "cargos": [
            {"cargo_kind": "cargoConstitucional", "cargo_code": 1, "record_count": 2},
            {"cargo_kind": "cargoConstitucional", "cargo_code": 3, "record_count": 2},
        ]}], "reconciliation_with_bu": {"by_cargo": [
            {"election_id": 304, "cargo_kind": "cargoConstitucional", "cargo_code": 1,
             "status": "matched", "divergence_rdv_minus_bu": 0},
            {"election_id": 304, "cargo_kind": "cargoConstitucional", "cargo_code": 3,
             "status": "matched", "divergence_rdv_minus_bu": 0},
        ]}}]

        report = reconcile_log_metrics(events, sessions, bu, rdvs, "2026-10-25")
        self.assertEqual(report["status"], "matched")
        self.assertEqual(report["by_metric"]["habilitacoes"]["comparison_status"], "not_compared")
        self.assertEqual(report["by_metric"]["votos_computados_vs_bu_comparecimento"]["log_event_count"], 2)
        self.assertEqual(report["by_metric"]["votos_computados_vs_bu_comparecimento"]["status"], "matched")
        self.assertTrue(all(cargo["comparison_status"] == "inconclusive" for cargo in report["by_cargo"]))
        self.assertTrue(all(cargo["rdv_bu_reconciliation_status"] == "matched" for cargo in report["by_cargo"]))
        self.assertEqual({cargo["cargo"] for cargo in report["by_cargo"]}, {"governador", "presidente"})

    def test_unassigned_computed_event_makes_log_bu_reconciliation_inconclusive(self):
        event = {"line_number": 4, "event_type": "voto_computado", "event_timestamp_raw": "25/10/2026 08:00:04",
            "event_date": "2026-10-25", "equipment_identifier": "device-hash"}
        sessions = reconstruct_voting_sessions([event])["sessions"]
        report = reconcile_log_metrics([event], sessions, {"qtd_eleitores_compareceram": 1, "elections": []}, [], "2026-10-25")
        self.assertEqual(report["by_metric"]["votos_computados_vs_bu_comparecimento"]["log_event_count"], 1)
        self.assertEqual(report["by_metric"]["votos_computados_vs_bu_comparecimento"]["status"], "inconclusive")
        self.assertIn("could not be assigned", report["by_metric"]["votos_computados_vs_bu_comparecimento"]["reason"])

    def test_closure_order_matches_observed_log_sequence_and_allows_repeated_commands(self):
        self.assertTrue(closure_sequence_matches_observed_order([
            "comando_encerramento", "conclusao_encerramento", "inicio_encerramento"
        ]))
        self.assertTrue(closure_sequence_matches_observed_order([
            "comando_encerramento", "comando_encerramento",
            "conclusao_encerramento", "inicio_encerramento"
        ]))
        self.assertFalse(closure_sequence_matches_observed_order([
            "comando_encerramento", "inicio_encerramento", "conclusao_encerramento"
        ]))
        self.assertFalse(closure_sequence_matches_observed_order([
            "conclusao_encerramento", "inicio_encerramento"
        ]))

    def test_collection_and_extraction_are_section_turn_file_structured(self):
        schema = asn1tools.compile_files(str(ROOT / "schemas/bu.asn1"), codec="ber")
        identity = {"municipioZona": {"municipio": 56251, "zona": 54}, "local": 1, "secao": 509}
        urna = {"tipoUrna": "secao", "versaoVotacao": "synthetic", "correspondenciaResultado": {
            "identificacao": ("identificacaoSecaoEleitoral", identity), "carga": {"numeroInternoUrna": 1,
                "numeroSerieFC": b"1234", "identificadorGeradorMidia": {"nome": "fixture", "serialCertificadoTPM": "fixture", "serialInstalacao": "fixture"},
                "dataHoraCarga": "20261001T120000", "codigoCarga": "fixture"}},
            "tipoArquivo": "votacaoUE", "numeroSerieFV": b"5678"}
        bu = {"cabecalho": {"dataGeracao": "20261026T120000", "idEleitoral": ("idPleito", 3220)},
            "fase": "oficial", "urna": urna, "identificacaoSecao": identity, "dataHoraEmissao": "20261026T120000",
            "dadosSecaoSA": ("dadosSecao", {"dataHoraAbertura": "20261026T090000", "dataHoraEncerramento": "20261026T170000"}),
            "qtdEleitoresCompareceram": 0, "resultadosVotacaoPorEleicao": [], "historicoCodigosCarga": []}
        content = schema.encode("EntidadeBoletimUrna", bu)
        envelope = {"cabecalho": bu["cabecalho"], "fase": "oficial",
            "identificacao": ("identificacaoSecaoEleitoral", identity), "tipoEnvelope": "envelopeBoletimUrna", "conteudo": content}
        bu_raw = schema.encode("EntidadeEnvelopeGenerico", envelope)

        def b64url(value):
            return base64.urlsafe_b64encode(value).decode().rstrip("=")
        log_buffer = io.BytesIO()
        with zipfile.ZipFile(log_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("logd.dat", "26/10/2026 17:01:00\tx\tx\tx\tEleitor foi habilitado\n")
        log_raw = log_buffer.getvalue()
        manifest_payload = {"hashes": [
            {"hash": "bu-hash", "arq": [{"tp": "bu", "nm": "o03221es5625100540509-bu.dat"}]},
            {"hash": "log-hash", "arq": [{"tp": "log", "nm": "o03221es5625100540509-log.jez"}]},
        ]}
        manifest = ".".join((b64url(b'{"alg":"none"}'), b64url(json.dumps(manifest_payload).encode()), "c2ln")).encode()
        http = {"status": "200"}
        with tempfile.TemporaryDirectory() as temp, patch("collect_section.sys.argv", [
                "collect_section.py", "--uf", "es", "--municipio", "56251", "--zona", "0054",
                "--secao", "0509", "--data-id", "3221", "--output", temp]), \
             patch("collect_section.fetch", side_effect=[(manifest, http), (bu_raw, http), (log_raw, http)]), \
             patch("sys.stdout", new_callable=io.StringIO):
            self.assertEqual(collect_section.main(), 0)
            acquisition_path = next(Path(temp).rglob("acquisition.json"))
            acquisition = json.loads(acquisition_path.read_text(encoding="utf-8"))
            self.assertEqual(acquisition["turno"], 1)  # determined from BU idPleito, despite requested data_id 3221
            self.assertEqual(acquisition["pleito_id"], 3220)
            self.assertIn("COLLECT_DATA_TURN_MISMATCH", {d["code"] for d in acquisition["diagnostics"]})
            bu_item = next(item for item in acquisition["files"] if item["type"] == "bu")
            raw_bu = acquisition_path.parent / bu_item["relative_path"]
            self.assertEqual(raw_bu.parent.name, "bu")
            self.assertIn("turno-1", str(raw_bu))
            extract_collection(acquisition_path, ROOT / "schemas/bu.asn1", ROOT / "schemas/rdv.asn1")
            extracted = json.loads(raw_bu.with_name(raw_bu.name + ".json").read_text(encoding="utf-8"))
            self.assertEqual(extracted["turno"], 1)
            self.assertEqual(extracted["pleito_id"], 3220)
            self.assertEqual(extracted["data"]["election_date"], "2026-10-26")
            log_item = next(item for item in acquisition["files"] if item["type"] == "log")
            raw_log = acquisition_path.parent / log_item["relative_path"]
            self.assertEqual(raw_log.name, "o03221es5625100540509-log.jez")
            self.assertEqual(raw_log.parent.name, "logs")
            self.assertFalse(any(part == "runs" for part in raw_log.parts))
            self.assertFalse(any(part == "runs" for part in raw_bu.parts))
            log_extraction = json.loads(raw_log.with_name(raw_log.name + ".json").read_text(encoding="utf-8"))
            self.assertEqual(len(log_extraction["events"]), 1)
            self.assertEqual(log_extraction["events"][0]["event_timestamp_raw"], "26/10/2026 17:01:00")
            self.assertNotIn("at_or_after_clock_cuts", log_extraction["events"][0])
            self.assertFalse(any("reports" in path.parts for path in Path(temp).rglob("*")))
            self.assertFalse(any(path.name == "diagnosticos-consolidados.json" for path in Path(temp).rglob("*.json")))

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
                "file_states": [{"type": kind, "status": "downloaded"} for kind in ("bu", "rdv", "log")],
                "acquisition": "acquisition.json"}))

        report = collect_sections([{"uf": "es", "municipio": "1", "zona": "1", "secao": "1"},
                                  {"uf": "es", "municipio": "2", "zona": "1", "secao": "2"}],
                                 Path("unused-output"), runner)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][calls[0].index("--data-id") + 1], "3221")
        self.assertEqual([section["status"] for section in report["sections"]], ["failed", "collected"])
        self.assertEqual(report["section_count"], 2)
        self.assertEqual(report["diagnostics"][0]["code"], "BATCH_SECTION_COLLECTION_FAILED")
        self.assertEqual(report["section_counts"]["effectively_collected_sections"], 1)
        self.assertEqual(report["section_counts"]["real_failures"], 1)

    def test_batch_collection_skips_aggregated_sections_without_invoking_downloader(self):
        calls = []

        class Completed:
            returncode = 0
            stderr = ""
            stdout = json.dumps({"section_key": "es/56251/0054/0523", "diagnostics": [],
                "file_states": [{"type": kind, "status": "downloaded"} for kind in ("bu", "rdv", "log")],
                "acquisition": "acquisition.json"})

        def runner(command, **_kwargs):
            calls.append(command)
            return Completed()

        selected = [
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0524",
             "nsp": "0523", "aggregation_role": "aggregated", "principal_section_key": "es/56251/0054/0523"},
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0523",
             "nsp": None, "nsa": ["0524"], "aggregation_role": "principal"},
        ]
        report = collect_sections(selected, Path("unused-output"), runner, limit=2)

        self.assertEqual(len(calls), 1)
        self.assertEqual(report["sections"][0]["status"], "aggregated")
        self.assertEqual(report["sections"][0]["principal_section_key"], "es/56251/0054/0523")
        self.assertEqual(report["section_counts"]["aggregated_sections"], 1)
        self.assertEqual(report["section_counts"]["principal_collectable_sections"], 1)
        self.assertEqual(report["section_counts"]["effectively_collected_sections"], 1)

    def test_batch_collection_slices_inventory_by_offset_and_limit(self):
        calls = []

        class Completed:
            returncode = 0
            stderr = ""

            def __init__(self, section):
                self.stdout = json.dumps({"section_key": section, "diagnostics": [],
                    "file_states": [], "acquisition": "acquisition.json"})

        def runner(command, **_kwargs):
            calls.append(command)
            return Completed("synthetic-section")

        sections = [{"uf": "es", "municipio": str(number), "zona": "1", "secao": str(number)}
                    for number in range(1, 6)]
        result = collect_sections(sections, Path("unused-output"), runner, offset=2, limit=2)

        self.assertEqual(len(calls), 2)
        self.assertEqual([command[command.index("--secao") + 1] for command in calls], ["3", "4"])
        self.assertEqual(result["section_count"], 2)

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

    def test_log_bu_cross_analysis_returns_structured_result_instead_of_none(self):
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
            bu = {"cabecalho": {"dataGeracao": "20261025T120000", "idEleitoral": ("idPleito", 3221)},
                "fase": "oficial", "urna": urna, "identificacaoSecao": identity, "dataHoraEmissao": "20261025T120000",
                "dadosSecaoSA": ("dadosSecao", {"dataHoraAbertura": "20261025T090000", "dataHoraEncerramento": "20261025T170000"}),
                "qtdEleitoresCompareceram": 0, "resultadosVotacaoPorEleicao": [], "historicoCodigosCarga": []}
            content = spec.encode("EntidadeBoletimUrna", bu)
            envelope = {"cabecalho": bu["cabecalho"], "fase": "oficial",
                "identificacao": ("identificacaoSecaoEleitoral", identity), "tipoEnvelope": "envelopeBoletimUrna", "conteudo": content}
            bu_raw = spec.encode("EntidadeEnvelopeGenerico", envelope)
            bu_path = root / "synthetic-bu.dat"
            bu_path.write_bytes(bu_raw)
            log_path = root / "synthetic-log.zip"
            with zipfile.ZipFile(log_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("logd.dat", "25/10/2026 17:01:00\tx\tx\tx\tEleitor foi habilitado\n")
            files = []
            for kind, path in (("bu", bu_path), ("log", log_path)):
                raw = path.read_bytes()
                files.append({"type": kind, "name": path.name, "relative_path": path.name,
                    "sha256": hashlib.sha256(raw).hexdigest(), "size_bytes": len(raw)})
            collection = root / "collection.json"
            collection.write_text(json.dumps({"section_key": "es/56251/0054/0509", "files": files,
                "diagnostics": [], "file_states": []}), encoding="utf-8")

            class FakeTransferIndex:
                status = "complete"
                expected_sha256 = "synthetic-reference-hash"

                def metadata(self): return {"resource_id": "fixture", "zip_sha256": self.expected_sha256}
                def section_breakdown(self, *_args): return []
                def federal_count(self, *_args): return 0

            with patch("temporary_transfer.load_reference", return_value=FakeTransferIndex()):
                report = analyze_collection(collection, parse_cuts([]), schema, ROOT / "schemas/rdv.asn1")
            codes = {item["code"] for item in report["diagnostics"]}
            self.assertNotIn("CROSS_ANALYSIS_FAILED", codes, report["diagnostics"])
            self.assertIsNotNone(report["reconciliation"]["log_bu"])
            self.assertIn("event_counts_by_date", report["file_processing"]["logs"][0])
            self.assertEqual(report["transferencia_temporaria"]["status"], "dados_insuficientes")
            self.assertEqual(report["transferencia_temporaria"]["log_metric_detail"]["incomplete_session_count"], 1)


if __name__ == "__main__":
    unittest.main()
