import base64
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from discover_sections import discover, filter_sections, section_records


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
            "zona": "0054", "secao": "0509", "catalog_date_raw": "04/10/2026", "catalog_time_raw": "20:57:17"}])

    def test_filters_are_cumulative_and_can_select_a_single_section(self):
        records = [
            {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0509"},
            {"uf": "es", "municipio": "56251", "zona": "0034", "secao": "0509"},
            {"uf": "sp", "municipio": "56251", "zona": "0054", "secao": "0509"},
        ]
        self.assertEqual(filter_sections(records, uf="es", municipio="56251", zona="0054", secao="0509"), [records[0]])

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
            self.assertEqual(result["catalog_sources"][0]["signature_status"], "not_verified")
            original = Path(temp) / "catalogs/es-p003220-cs.jws"
            self.assertEqual(original.read_bytes(), raw)
            saved_report = json.loads((Path(temp) / "section-inventory.json").read_text())
            self.assertEqual(saved_report["sections"], result["sections"])


if __name__ == "__main__":
    unittest.main()
