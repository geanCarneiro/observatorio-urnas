import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from update_inventory_collection_status import update_inventory_collection_status


class UpdateInventoryCollectionStatusTests(unittest.TestCase):
    def test_updates_collected_aggregated_and_failed_section_statuses(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inventory_path = root / "section-inventory.json"
            sections = [
                {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0523",
                 "nsp": None, "nsa": ["0524"], "aggregation_role": "principal", "collectable": True},
                {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0524",
                 "nsp": "0523", "nsa": None, "aggregation_role": "aggregated", "collectable": False},
                {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0525",
                 "nsp": None, "nsa": None, "aggregation_role": "independent", "collectable": True},
            ]
            inventory_path.write_text(json.dumps({"config_id": "3221", "sections": sections}), encoding="utf-8")

            collected = root / "dados/es/56251/0054/0523/turno-2"
            collected.mkdir(parents=True)
            (collected / "acquisition.json").write_text(json.dumps({
                "data_id": "3221", "section_key": "es/56251/0054/0523", "turn_directory": "turno-2",
                "acquired_at_utc": "2026-10-27T00:00:00Z",
                "file_states": [{"type": kind, "status": "downloaded"} for kind in ("bu", "rdv", "log")],
            }), encoding="utf-8")

            failed = root / "dados/es/56251/0054/0525/turno-2"
            failed.mkdir(parents=True)
            (failed / "acquisition.json").write_text(json.dumps({
                "data_id": "3221", "section_key": "es/56251/0054/0525", "turn_directory": "turno-2",
                "acquired_at_utc": "2026-10-27T00:00:00Z",
                "file_states": [{"type": kind, "status": "failed" if kind == "rdv" else "downloaded"}
                                for kind in ("bu", "rdv", "log")],
            }), encoding="utf-8")

            result = update_inventory_collection_status(inventory_path, root / "dados")
            statuses = {section["secao"]: section["collection_status"] for section in result["sections"]}
            self.assertEqual(statuses, {"0523": "collected", "0524": "aggregated", "0525": "partial"})
            self.assertEqual(result["section_counts"]["effectively_collected_sections"], 2)
            self.assertEqual(result["section_counts"]["real_failures"], 1)
            self.assertEqual(json.loads(inventory_path.read_text())["sections"], result["sections"])

    def test_section_without_acquisition_is_not_processed_not_a_collection_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inventory_path = root / "section-inventory.json"
            inventory_path.write_text(json.dumps({"config_id": "3221", "sections": [
                {"uf": "es", "municipio": "56251", "zona": "0054", "secao": "0509",
                 "nsp": None, "nsa": None, "aggregation_role": "independent"},
            ]}), encoding="utf-8")

            result = update_inventory_collection_status(inventory_path, root / "dados")
            self.assertEqual(result["sections"][0]["collection_status"], "not_processed")
            self.assertEqual(result["section_counts"]["not_processed_sections"], 1)
            self.assertEqual(result["section_counts"]["real_failures"], 0)


if __name__ == "__main__":
    unittest.main()
