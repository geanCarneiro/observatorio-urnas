import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from check_batch_report import validate_batch_report
from orchestrate_collection import _gh_json, dispatch_plan, resume_index
from plan_collection import build_plan, failed_batch_ids, retain_batches
from prepare_worker_inputs import prepare_worker_inputs
from render_worker_matrix import shard_matrix
from stage_batch_artifacts import stage_parts


class CollectionOrchestrationTests(unittest.TestCase):
    def test_gh_json_forces_get_when_query_fields_are_present(self):
        response = Mock(returncode=0, stdout='{"workflow_runs": []}', stderr="")
        with patch("orchestrate_collection.subprocess.run", return_value=response) as gh_run:
            payload = _gh_json([
                "repos/owner/repo/actions/workflows/worker.yml/runs",
                "-f", "event=workflow_dispatch", "-f", "per_page=100",
            ])

        self.assertEqual(payload, {"workflow_runs": []})
        self.assertEqual(gh_run.call_args.args[0], [
            "gh", "api", "--method", "GET",
            "repos/owner/repo/actions/workflows/worker.yml/runs",
            "-f", "event=workflow_dispatch", "-f", "per_page=100",
        ])

    def test_plan_partitions_unbounded_selection_into_bounded_worker_shards(self):
        sections = [{"uf": "es", "municipio": "56251", "zona": "0054", "secao": f"{i:04d}"}
                    for i in range(1, 250_501)]
        plan = build_plan({"2": {"config_id": "3221", "selection": {"offset": 37}, "sections": sections}})
        self.assertEqual(plan["total_sections"], 250_500)
        self.assertEqual(plan["total_batches"], 251)
        self.assertEqual([len(shard["batch_ids"]) for shard in plan["shards"]], [100, 100, 51])
        self.assertEqual(plan["batches"][0]["offset"], 0)
        self.assertEqual(plan["batches"][0]["range_start"], 37)
        self.assertEqual(plan["batches"][-1]["limit"], 500)
        self.assertEqual(len(shard_matrix(plan, 2)), 51)

    def test_resume_plan_keeps_only_batches_not_marked_successful(self):
        plan = build_plan({"1": {"sections": [{"secao": f"{n:04d}"} for n in range(230)]}},
                          batch_size=10, jobs_per_worker=5)
        success = plan["batches"][0]["batch_id"]
        failed = plan["batches"][1]["batch_id"]
        pending = plan["batches"][2]["batch_id"]
        retry_ids = failed_batch_ids(plan, {"batch_status": {success: "success", failed: "failed"}})
        self.assertEqual(retry_ids, {failed, pending} | {x["batch_id"] for x in plan["batches"][3:]})
        retried = retain_batches(plan, {failed, pending})
        self.assertEqual([item["batch_id"] for item in retried["batches"]], [failed, pending])
        self.assertTrue(retried["resumed_only_failed"])

    def test_staging_splits_large_worker_batch_into_small_downloadable_artifacts(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            sections = [{"uf": "es", "municipio": "56251", "zona": "0054", "secao": f"{n:04d}"}
                        for n in range(1, 6)]
            data_root = root / "dados"
            for section in sections:
                key = "/".join((section["uf"], section["municipio"], section["zona"], section["secao"]))
                (data_root / key).mkdir(parents=True)
                (data_root / key / "acquisition.json").write_text("{}")
            report = {"section_counts": {"real_failures": 0}, "sections": []}
            report_path = root / "batch.json"
            report_path.write_text(json.dumps(report))
            parts = stage_parts({"sections": sections}, data_root, root / "staged", offset=0,
                                limit=5, part_size=3, report_path=report_path)
            self.assertEqual(len(parts), 2)
            self.assertEqual(len(list((parts[0] / "dados").rglob("acquisition.json"))), 3)
            self.assertEqual(len(list((parts[1] / "dados").rglob("acquisition.json"))), 2)
            excerpt = json.loads((parts[0] / "section-inventory.json").read_text())
            self.assertEqual(len(excerpt["sections"]), 3)
            self.assertTrue(all(item["collection_status"] == "not_processed" for item in excerpt["sections"]))

    def test_worker_preparation_materializes_only_its_selected_inventory_slices(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            inventory_root = root / "source"
            inventory_path = inventory_root / "inventario/turno2/section-inventory.json"
            inventory_path.parent.mkdir(parents=True)
            sections = [{"uf": "es", "municipio": "56251", "zona": "0054", "secao": f"{i:04d}"}
                        for i in range(5)]
            inventory_path.write_text(json.dumps({"config_id": "3221", "sections": sections,
                                                  "selection": {"offset": 20}}))
            plan = build_plan({"2": {"config_id": "3221", "sections": sections,
                                      "selection": {"offset": 20}}}, batch_size=2, jobs_per_worker=2)
            worker = prepare_worker_inputs(plan, inventory_root, root / "worker", shard_index=0)
            self.assertEqual(len(worker["batches"]), 2)
            self.assertEqual(worker["batches"][1]["offset"], 0)
            mini_path = root / "worker" / worker["batches"][1]["inventory_path"]
            mini = json.loads(mini_path.read_text())
            self.assertEqual([section["secao"] for section in mini["sections"]], ["0002", "0003"])
            self.assertEqual(mini["selection"]["offset"], 22)

    def test_batch_status_requires_a_complete_report_without_real_failures(self):
        with tempfile.TemporaryDirectory() as temp:
            report = Path(temp) / "report.json"
            report.write_text(json.dumps({"section_counts": {"real_failures": 0}}))
            self.assertEqual(validate_batch_report(report, "success"), (True, "batch report has no real collection failures"))
            report.write_text(json.dumps({"section_counts": {"real_failures": 2}}))
            self.assertFalse(validate_batch_report(report, "success")[0])
            self.assertFalse(validate_batch_report(report, "failure")[0])

    def test_resume_reruns_only_failed_worker_jobs_and_skips_successful_or_active_runs(self):
        index = {"worker_runs": [
            {"shard_index": 0, "run_id": 1, "batch_ids": ["a"]},
            {"shard_index": 1, "run_id": 2, "batch_ids": ["b"]},
            {"shard_index": 2, "run_id": 3, "batch_ids": ["c"]},
        ]}
        statuses = {
            1: {"status": "completed", "conclusion": "success", "run_attempt": 1},
            2: {"status": "completed", "conclusion": "failure", "run_attempt": 1},
            3: {"status": "in_progress", "conclusion": None, "run_attempt": 1},
        }
        with patch("orchestrate_collection._gh_json", side_effect=lambda args: statuses[int(args[0].split("/")[-1])]), \
             patch("orchestrate_collection.subprocess.run", return_value=Mock(returncode=0, stderr="")) as gh_run:
            result = resume_index(index, repo="owner/repo")
        self.assertEqual(result["resume_result"], "failed_jobs_retried")
        gh_run.assert_called_once_with(["gh", "run", "rerun", "2", "--failed"],
                                       capture_output=True, text=True, check=False)
        self.assertEqual(index["worker_runs"][0]["resume_status"], "already_complete")
        self.assertEqual(index["worker_runs"][2]["resume_status"], "already_active")

    def test_completed_resume_does_not_trigger_new_collection(self):
        index = {"worker_runs": [{"shard_index": 0, "run_id": 42, "batch_ids": ["x"]}]}
        with patch("orchestrate_collection._gh_json", return_value={
            "status": "completed", "conclusion": "success", "run_attempt": 1
        }), patch("orchestrate_collection.subprocess.run") as gh_run:
            result = resume_index(index, repo="owner/repo")
        self.assertEqual(result["resume_result"], "already_complete_no_collection_repeated")
        gh_run.assert_not_called()

    def test_dispatch_persists_worker_run_ids_and_artifact_names(self):
        plan = {"shards": [{"shard_index": 0, "batch_ids": ["turno2-00000000-00001000"]}]}
        with tempfile.TemporaryDirectory() as temp, \
             patch("orchestrate_collection.subprocess.run", return_value=Mock(returncode=0, stderr="")) as gh_dispatch, \
             patch("orchestrate_collection._find_run", return_value={"id": 123, "status": "queued",
                                                                         "html_url": "https://example.invalid/run/123"}):
            index_path = Path(temp) / "index.json"
            index = dispatch_plan(plan, parent_run_id="456", repo="owner/repo", ref="main",
                                  workflow_file="coleta-abrangencia-worker.yml", index_path=index_path,
                                  lookup_attempts=1)
            saved = json.loads(index_path.read_text())
        self.assertEqual(index["worker_runs"][0]["run_id"], 123)
        self.assertEqual(saved["worker_runs"][0]["html_url"], "https://example.invalid/run/123")
        self.assertEqual(saved["worker_runs"][0]["artifact_names"], [
            "dados-lote-turno2-00000000-00001000-parte-1",
            "dados-lote-turno2-00000000-00001000-parte-2",
        ])
        self.assertIn("inputs[shard_index]=0", gh_dispatch.call_args.args[0])

    def test_resume_dispatches_a_planned_shard_that_never_started(self):
        index = {"plan_run_id": "456", "ref": "main", "worker_runs": []}
        plan = {"shards": [{"shard_index": 0, "batch_ids": ["turno2-00000000-00001000"]}]}
        found_run = {"id": 789, "status": "queued", "html_url": "https://example.invalid/run/789"}
        with patch("orchestrate_collection._find_run", side_effect=[None, found_run]), \
             patch("orchestrate_collection.subprocess.run", return_value=Mock(returncode=0, stderr="")) as dispatch, \
             patch("orchestrate_collection._gh_json", return_value={"status": "queued", "conclusion": None,
                                                                       "run_attempt": 1}):
            result = resume_index(index, repo="owner/repo", plan=plan, lookup_attempts=1)
        self.assertEqual(result["resume_result"], "workers_still_active_no_duplicate_dispatch")
        self.assertEqual(index["worker_runs"][0]["run_id"], 789)
        self.assertEqual(index["worker_runs"][0]["resume_status"], "already_active")
        self.assertEqual(dispatch.call_count, 1)


if __name__ == "__main__":
    unittest.main()
