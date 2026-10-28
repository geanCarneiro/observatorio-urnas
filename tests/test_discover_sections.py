import base64
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from discover_sections import (discover, filter_sections, normalize_filter_values,
    section_records, validate_filter_hierarchy)
from section_aggregation import aggregation_counts, link_aggregations


def b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def jws(payload: dict) -> bytes:
    return (b64(b'{"alg":"EdDSA","typ":"JOSE"}') + "." +
            b64(json.dumps(payload).encode()) + "." + b64(b"x" * 64)).encode()


class DiscoverSectionsTests(unittest.TestCase):
    def test_extracts_section_identifiers_and_preserves_catalog_timestamps_raw(self):
        payload = {"cdp": "3220", "abr": [{"cd": "es", "mu": [{"cd": "56251", "nm": "CARIACICA",
            "zon": [{"cd": "0054", "sec": [{"ns": "0509", "da": "04/10/2026", "ha": "20:57:17"}]}]}]}]}
        records = section_records(payload, "es")
        self.assertEqual(records, [{"uf": "es", "municipio": "56251", "municipio_nome": "CARIACICA",
            "zona": "0054", "secao": "0509", "catalog_date_raw": "04/10/2026", "catalog_time_raw": "20:57:17",
            "nsp": None, "nsa": None}])

    def test_aggregation_links_are_bidirectional_and_raw_catalog_fields_are_preserved(self):
        source = [
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0523", "nsp": None,
             "nsa": ["0622", "0524", "0621"]},
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0524", "nsp": "0523", "nsa": None},
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0621", "nsp": "0523", "nsa": None},
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0622", "nsp": "0523", "nsa": None},
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0373", "nsp": None,
             "nsa": ["0688"]},
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0688", "nsp": "0373", "nsa": None},
        ]
        records, diagnostics = link_aggregations(source)
        by_section = {record["secao"]: record for record in records}

        self.assertEqual(diagnostics, [])
        self.assertEqual(by_section["0523"]["nsa"], ["0622", "0524", "0621"])
        self.assertEqual(by_section["0523"]["aggregated_section_keys"], [
            "es/56251/0054/0524", "es/56251/0054/0621", "es/56251/0054/0622"])
        self.assertEqual(by_section["0622"]["nsp"], "0523")
        self.assertEqual(by_section["0622"]["principal_section_key"], "es/56251/0054/0523")
        self.assertEqual(by_section["0373"]["aggregation_role"], "principal")
        self.assertEqual(by_section["0688"]["collection_status"], "aggregated")
        self.assertEqual(aggregation_counts(records), {
            "registered_sections": 6, "aggregated_sections": 4,
            "aggregation_principal_sections": 2, "principal_collectable_sections": 2})

    def test_aggregation_validation_reports_missing_reciprocal_and_cyclic_links(self):
        source = [
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0010", "nsp": None,
             "nsa": ["0011", "0099"]},
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0011", "nsp": "0010",
             "nsa": ["0010"]},
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0012", "nsp": "0098", "nsa": None},
        ]
        records, diagnostics = link_aggregations(source)
        codes = {item["code"] for item in diagnostics}

        self.assertIn("SECTION_AGGREGATION_CHILD_MISSING", codes)
        self.assertIn("SECTION_AGGREGATION_PRINCIPAL_MISSING", codes)
        self.assertIn("SECTION_AGGREGATION_LINK_MISMATCH", codes)
        self.assertIn("SECTION_AGGREGATION_CYCLE", codes)
        self.assertEqual(next(x for x in records if x["secao"] == "0011")["aggregation_role"], "aggregated")

    def test_filters_are_cumulative_and_can_select_a_single_section(self):
        records = [
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0509"},
            {"uf": "es", "municipio": "56251", "zona": "0034", "secao": "0509"},
            {"uf": "sp", "municipio": "56251", "zona": "0054", "secao": "0509"},
        ]
        self.assertEqual(filter_sections(records, uf="es", municipio="56251", zona="0054", secao="0509"), [records[0]])

    def test_filter_lists_trim_spaces_preserve_zeroes_and_deduplicate(self):
        self.assertEqual(normalize_filter_values("ES, mg, es", 2, "uf", lowercase=True), ["es", "mg"])
        self.assertEqual(normalize_filter_values(" 54, 0055 ", 4, "zona"), ["0054", "0055"])
        self.assertEqual(normalize_filter_values("", 4, "secao"), None)
        with self.assertRaisesRegex(ValueError, "empty value"):
            normalize_filter_values("0509, , 0510", 4, "secao")

    def test_descendant_filters_require_every_parent_level(self):
        cases = [
            (None, ["56251"], None, None, "municipio.*UF"),
            (["es"], ["56251"], ["0054"], None, None),
            (["es"], ["56251"], None, ["0509"], "secao.*zona"),
        ]
        for uf, municipio, zona, secao, error in cases:
            if error is None:
                validate_filter_hierarchy(uf, municipio, zona, secao)
            else:
                with self.subTest(error=error), self.assertRaisesRegex(ValueError, error):
                    validate_filter_hierarchy(uf, municipio, zona, secao)

    def test_filter_lists_intersect_all_selected_parent_sets(self):
        records = [
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0509"},
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0510"},
            {"uf": "mg", "municipio": "41009", "zona": "0050", "secao": "0224"},
            {"uf": "mg", "municipio": "41009", "zona": "0034", "secao": "0509"},
        ]
        selected = filter_sections(records, uf=["es", "mg"], municipio=["56251", "41009"],
                                   zona=["0054", "0050"], secao=["0509", "0224"])
        self.assertEqual(selected, [records[0], records[2]])

    def test_discovery_preserves_catalog_bytes_and_writes_filtered_inventory_report(self):
        payload = {"cdp": "3220", "dg": "05/10/2026", "abr": [{"cd": "es", "mu": [{"cd": "56251", "nm": "CARIACICA",
            "zon": [{"cd": "0054", "sec": [{"ns": "0509", "da": "04/10/2026", "ha": "20:57:17"},
                                                   {"ns": "0510", "da": "04/10/2026", "ha": "20:57:19"}]}]}]}]}
        raw = jws(payload)
        with tempfile.TemporaryDirectory() as temp, patch("discover_sections.fetch", return_value=(raw, {"status": "200"})):
            result = discover(config_id="3220", output=Path(temp), uf="es", municipio="56251",
                              zona="0054", secao="0509", limit=10)
            self.assertEqual(result["matching_section_count"], 1)
            self.assertEqual(result["sections"][0]["secao"], "0509")
            self.assertEqual(result["section_counts"]["registered_sections"], 1)
            self.assertEqual(result["section_counts"]["principal_collectable_sections"], 1)
            self.assertIsNone(result["section_counts"]["effectively_collected_sections"])
            self.assertEqual(result["catalog_sources"][0]["signature_status"], "not_verified")
            original = Path(temp) / "catalogs/es-p003220-cs.jws"
            self.assertEqual(original.read_bytes(), raw)
            saved_report = json.loads((Path(temp) / "section-inventory.json").read_text())
            self.assertEqual(saved_report["sections"], result["sections"])

    def test_discovery_accepts_multiple_values_and_checks_child_membership(self):
        payloads = {
            "es": {"cdp": "3220", "abr": [{"cd": "es", "mu": [{"cd": "56251", "nm": "CARIACICA",
                "zon": [{"cd": "0054", "sec": [{"ns": "0509"}, {"ns": "0510"}]},
                        {"cd": "0034", "sec": [{"ns": "0509"}]}]}]}]},
            "mg": {"cdp": "3220", "abr": [{"cd": "mg", "mu": [{"cd": "41009", "nm": "EXAMPLE",
                "zon": [{"cd": "0050", "sec": [{"ns": "0224"}]}]}]}]},
        }
        def fetch_for_uf(url):
            uf = "es" if "/es/" in url else "mg"
            return jws(payloads[uf]), {"status": "200"}
        with tempfile.TemporaryDirectory() as temp, patch("discover_sections.fetch", side_effect=fetch_for_uf):
            result = discover(config_id="3220", output=Path(temp), uf="ES, mg",
                municipio=" 56251, 41009", zona="0054, 0050", secao="0509, 0224", limit=10)
        self.assertEqual(result["matching_section_count"], 2)
        self.assertEqual([row["secao"] for row in result["sections"]], ["0509", "0224"])
        self.assertEqual(result["filters"], {"uf": ["es", "mg"], "municipio": ["56251", "41009"],
            "zona": ["0054", "0050"], "secao": ["0509", "0224"]})

    def test_discovery_rejects_child_codes_absent_from_selected_parent_scope(self):
        payload = {"cdp": "3220", "abr": [{"cd": "es", "mu": [{"cd": "56251", "nm": "CARIACICA",
            "zon": [{"cd": "0054", "sec": [{"ns": "0509"}, {"ns": "0510"}]},
                    {"cd": "0034", "sec": [{"ns": "0509"}]}]}]}]}
        raw = jws(payload)
        with tempfile.TemporaryDirectory() as temp, patch("discover_sections.fetch", return_value=(raw, {"status": "200"})):
            with self.assertRaisesRegex(ValueError, "secao.*0510"):
                discover(config_id="3220", output=Path(temp), uf="es", municipio="56251",
                    zona="0034", secao="0510", limit=10)

    def test_discovery_does_not_select_jobs_when_a_parent_catalog_is_unavailable(self):
        payload = {"cdp": "3220", "abr": [{"cd": "es", "mu": [{"cd": "56251", "nm": "CARIACICA",
            "zon": [{"cd": "0054", "sec": [{"ns": "0509"}]}]}]}]}
        def fetch_for_uf(url):
            if "/mg/" in url:
                raise ConnectionError("fixture catalog unavailable")
            return jws(payload), {"status": "200"}
        with tempfile.TemporaryDirectory() as temp, patch("discover_sections.fetch", side_effect=fetch_for_uf):
            with self.assertRaisesRegex(ValueError, "cannot validate child filters"):
                discover(config_id="3220", output=Path(temp), uf="es,mg", municipio="56251",
                    zona="0054", secao="0509", limit=10)

    def test_invalid_hierarchy_is_rejected_before_fetching_catalogs(self):
        with tempfile.TemporaryDirectory() as temp, patch("discover_sections.fetch") as fetch_catalog:
            with self.assertRaisesRegex(ValueError, "municipio.*UF"):
                discover(config_id="3220", output=Path(temp), municipio="56251")
            fetch_catalog.assert_not_called()

    def test_discovery_without_limit_selects_more_than_previous_5000_ceiling(self):
        sections = [{"ns": f"{number:04d}"} for number in range(5001)]
        payload = {"cdp": "3220", "abr": [{"cd": "es", "mu": [{"cd": "56251", "nm": "CARIACICA",
            "zon": [{"cd": "0054", "sec": sections}]}]}]}
        raw = jws(payload)
        with tempfile.TemporaryDirectory() as temp, patch("discover_sections.fetch", return_value=(raw, {"status": "200"})):
            result = discover(config_id="3220", output=Path(temp), uf="es")
        self.assertEqual(result["matching_section_count"], 5001)
        self.assertEqual(result["selection"]["selected_count"], 5001)
        self.assertIsNone(result["selection"]["limit"])


if __name__ == "__main__":
    unittest.main()
