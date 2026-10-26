import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

import asn1tools

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from rdv import decode_rdv


ROOT = Path(__file__).resolve().parents[1]


class RDVDecoderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.schema = ROOT / "schemas/rdv.asn1"
        spec = asn1tools.compile_files(str(self.schema), codec="ber")
        value = {
            "pleito": 304,
            "fase": "oficial",
            "identificacao": {"municipioZona": {"municipio": 56251, "zona": 54}, "local": 1, "secao": 509},
            "historicoCodigosCarga": ["synthetic-code"],
            "eleicoes": ("eleicoesVota", [{"idEleicao": 6258, "votosCargos": [
                {"idCargo": ("cargoConstitucional", "presidente"), "quantidadeEscolhas": 1, "votos": [
                    {"tipoVoto": "nominal", "digitacao": "013"}, {"tipoVoto": "branco"},
                    {"tipoVoto": "nulo"}, {"tipoVoto": "legenda", "digitacao": "22"},
                    {"tipoVoto": "brancoAposSuspensao"},
                ]},
            ]}]),
        }
        raw = spec.encode("EntidadeRegistroDigitalVoto", value)
        self.rdv = self.root / "synthetic-rdv.dat"
        self.rdv.write_bytes(raw)
        self.sha = hashlib.sha256(raw).hexdigest()
        self.collection = self.root / "collection.json"
        self.collection.write_text(json.dumps({"section_key": "es/56251/0054/0509", "files": [{"type": "rdv", "name": self.rdv.name,
            "relative_path": self.rdv.name, "sha256": self.sha, "size_bytes": len(raw),
            "manifest_hash": "synthetic", "url": "synthetic://fixture"}]}))
        self.bu = {"elections": [{"id_eleicao": 6258, "results": [{"cargos": [{
            "cargo_kind": "cargoConstitucional", "cargo": "presidente", "qtd_votos_bu": 5,
            "vote_type_counts": {"nominal": 1, "branco": 1, "nulo": 1, "legenda": 1},
            "nominal_by_number": {"13": 1}, "legend_by_number": {"22": 1},
        }]}]}]}

    def tearDown(self):
        self.temp.cleanup()

    def test_preserves_records_and_reconciles_without_linking(self):
        report = decode_rdv(self.rdv, self.collection, self.schema, self.bu)
        cargo = report["elections"][0]["cargos"][0]
        self.assertEqual(report["record_count"], 5)
        self.assertEqual(cargo["candidate_number_counts_from_nominal_votes"], {"13": 1})
        self.assertEqual(cargo["candidate_number_counts_by_raw_digitacao"], {"013": 1})
        self.assertEqual(cargo["party_number_counts_from_legend_votes"], {"22": 1})
        self.assertEqual(cargo["white_votes_total"], 2)
        self.assertEqual(cargo["null_votes_total"], 1)
        self.assertEqual(bytes.fromhex(cargo["records"][0]["record_original_ber_hex"])[0], 0x30)
        self.assertEqual(report["reconciliation_with_bu"]["status"], "divergence")
        self.assertIn("not associated", report["individual_record_join_policy"])
        self.assertEqual(report["ordering_note"].split(";")[0], "record_position_within_election_and_cargo reflects source sequence only")

    def test_rejects_bytes_that_do_not_match_collection_hash(self):
        self.rdv.write_bytes(self.rdv.read_bytes() + b"\x00")
        with self.assertRaisesRegex(ValueError, "do not match acquisition manifest"):
            decode_rdv(self.rdv, self.collection, self.schema, self.bu)

    def test_preserves_result_entity_metadata_and_binary_fields(self):
        spec = asn1tools.compile_files(str(self.schema), codec="ber")
        registry = spec.decode("EntidadeRegistroDigitalVoto", self.rdv.read_bytes())
        entity = {
            "cabecalho": {"dataGeracao": "20261026T120000", "idEleitoral": ("idPleito", 304)},
            "urna": {
                "tipoUrna": "secao", "versaoVotacao": "1.0",
                "correspondenciaResultado": {
                    "identificacao": ("identificacaoSecaoEleitoral", registry["identificacao"]),
                    "carga": {"numeroInternoUrna": 1, "numeroSerieFC": b"1234",
                              "identificadorGeradorMidia": {"nome": "x", "serialCertificadoTPM": "x", "serialInstalacao": "x"},
                              "dataHoraCarga": "20261001T120000", "codigoCarga": "synthetic"},
                },
                "tipoArquivo": "votacaoUE", "numeroSerieFV": b"5678",
            },
            "rdv": registry,
        }
        raw = spec.encode("EntidadeResultadoRDV", entity)
        self.rdv.write_bytes(raw)
        data = json.loads(self.collection.read_text())
        data["files"][0]["sha256"] = hashlib.sha256(raw).hexdigest()
        data["files"][0]["size_bytes"] = len(raw)
        self.collection.write_text(json.dumps(data))
        report = decode_rdv(self.rdv, self.collection, self.schema, self.bu)
        self.assertEqual(report["format_variant"], "EntidadeResultadoRDV")
        self.assertEqual(report["entity_metadata"]["urn"]["numeroSerieFV"]["bytes_hex"], b"5678".hex())
        self.assertEqual(report["integrity"]["every_vote_roundtrip_matches"], True)

    def test_decodes_official_rdv_envelope(self):
        rdv_spec = asn1tools.compile_files(str(self.schema), codec="ber")
        bu_spec = asn1tools.compile_files(str(ROOT / "schemas/bu.asn1"), codec="ber")
        inner = self.rdv.read_bytes()
        registry = rdv_spec.decode("EntidadeRegistroDigitalVoto", inner)
        urn = {
            "tipoUrna": "secao", "versaoVotacao": "1.0",
            "correspondenciaResultado": {
                "identificacao": ("identificacaoSecaoEleitoral", registry["identificacao"]),
                "carga": {"numeroInternoUrna": 1, "numeroSerieFC": b"1234",
                          "identificadorGeradorMidia": {"nome": "x", "serialCertificadoTPM": "x", "serialInstalacao": "x"},
                          "dataHoraCarga": "20261001T120000", "codigoCarga": "synthetic"},
            },
            "tipoArquivo": "votacaoUE", "numeroSerieFV": b"5678",
        }
        envelope = {
            "cabecalho": {"dataGeracao": "20261026T120000", "idEleitoral": ("idPleito", 304)},
            "fase": "oficial", "urna": urn,
            "identificacao": ("identificacaoSecaoEleitoral", registry["identificacao"]),
            "tipoEnvelope": "envelopeRegistroDigitalVoto", "conteudo": inner,
        }
        raw = bu_spec.encode("EntidadeEnvelopeGenerico", envelope)
        self.rdv.write_bytes(raw)
        data = json.loads(self.collection.read_text())
        data["files"][0]["sha256"] = hashlib.sha256(raw).hexdigest()
        data["files"][0]["size_bytes"] = len(raw)
        self.collection.write_text(json.dumps(data))
        report = decode_rdv(self.rdv, self.collection, self.schema, self.bu)
        self.assertEqual(report["format_variant"], "EntidadeEnvelopeGenerico->EntidadeRegistroDigitalVoto")
        self.assertEqual(report["entity_metadata"]["envelope"]["tipoEnvelope"], "envelopeRegistroDigitalVoto")


if __name__ == "__main__":
    unittest.main()
