import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from helpers import make_project
from rideflow.errors import DataQualityError, PipelineError, SourceError
from rideflow.monitoring import health_check
from rideflow.pipeline import run_pipeline, run_stage
from rideflow.queries import list_queries, run_query
from rideflow.silver import read_silver


class EndToEndTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.cfg = make_project(self.tmp)

    def rows(self, sql):
        conn = sqlite3.connect(self.cfg.warehouse_path)
        try:
            return conn.execute(sql).fetchall()
        finally:
            conn.close()

    def test_full_run_outputs_are_consistent(self):
        report = run_pipeline(self.cfg, "run-e2e-1")
        self.assertEqual(report["status"], "success")
        silver = read_silver(self.cfg)
        self.assertEqual(report["trips_loaded"], silver["trip_id"].nunique())
        self.assertEqual(self.rows("SELECT COUNT(*) FROM fact_trips")[0][0], report["trips_loaded"])
        total = len(self.cfg.source_path.read_text().splitlines())
        self.assertEqual(report["records_total"], total)
        self.assertEqual(report["records_rejected"] + report["records_duplicate"] + len(silver), total)
        rejects = (self.cfg.quarantine_dir / "rejects.jsonl").read_text().splitlines()
        self.assertEqual(len(rejects), report["records_rejected"] + report["records_duplicate"])
        self.assertTrue((self.cfg.reports_dir / "run_report_run-e2e-1.md").is_file())
        self.assertEqual(self.rows("SELECT status, COUNT(*) FROM pipeline_runs")[0][0], "success")
        self.assertFalse([c for c in report["checks"] if c["severity"] == "critical" and not c["passed"]])

    def test_rerun_is_idempotent(self):
        run_pipeline(self.cfg, "run-a")
        snap1 = self.rows("SELECT trip_id, status, fare_amount, driver_key FROM fact_trips ORDER BY trip_id")
        dims1 = self.rows("SELECT COUNT(*) FROM dim_driver")[0][0]
        run_pipeline(self.cfg, "run-b")
        snap2 = self.rows("SELECT trip_id, status, fare_amount, driver_key FROM fact_trips ORDER BY trip_id")
        self.assertEqual(snap1, snap2)
        self.assertEqual(dims1, self.rows("SELECT COUNT(*) FROM dim_driver")[0][0])
        self.assertEqual(self.rows("SELECT COUNT(*) FROM pipeline_runs")[0][0], 2)

    def test_incremental_new_events_flow_through(self):
        run_pipeline(self.cfg, "run-a")
        before = self.rows("SELECT COUNT(*) FROM fact_trips")[0][0]
        new = [
            {"event_id": "n-1", "event_type": "requested", "trip_id": "trip-new", "rider_id": "r9", "city_id": "dubai",
             "currency": "AED", "event_ts": "2026-01-04T08:00:00Z"},
            {"event_id": "n-2", "event_type": "completed", "trip_id": "trip-new", "rider_id": "r9", "city_id": "dubai",
             "currency": "AED", "event_ts": "2026-01-04T08:30:00Z", "driver_id": "drv-9", "fare_amount": 42.5, "distance_km": 8.0},
        ]
        with self.cfg.source_path.open("a") as fh:
            fh.write("\n".join(json.dumps(e) for e in new) + "\n")
        report = run_pipeline(self.cfg, "run-b")
        self.assertEqual(report["ingest"]["records"], 2)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM fact_trips")[0][0], before + 1)
        self.assertEqual(self.rows("SELECT fare_amount, status FROM fact_trips WHERE trip_id='trip-new'")[0], (42.5, "completed"))

    def test_quality_gate_blocks_warehouse_load(self):
        strict = make_project(Path(tempfile.mkdtemp()), max_reject_rate=0.0, max_duplicate_rate=0.0)
        with self.assertRaises(DataQualityError) as ctx:
            run_pipeline(strict, "run-strict")
        self.assertIn("silver_reject_rate", str(ctx.exception))
        conn = sqlite3.connect(strict.warehouse_path)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM fact_trips").fetchone()[0], 0)  # nothing loaded
        run = conn.execute("SELECT status, error_message FROM pipeline_runs WHERE run_id='run-strict'").fetchone()
        self.assertEqual(run[0], "failed")
        self.assertIn("silver_reject_rate", run[1])
        failing = conn.execute("SELECT passed FROM dq_results WHERE run_id='run-strict' AND check_name='silver_reject_rate'").fetchone()
        self.assertEqual(failing[0], 0)
        conn.close()

    def test_missing_source_fails_and_is_audited(self):
        self.cfg.source_path.unlink()
        with self.assertRaises(SourceError):
            run_pipeline(self.cfg, "run-nosrc")
        row = self.rows("SELECT status, error_message FROM pipeline_runs WHERE run_id='run-nosrc'")[0]
        self.assertEqual(row[0], "failed")
        self.assertIn("source file not found", row[1])

    def test_all_garbage_input_trips_not_empty_check(self):
        self.cfg.source_path.write_text("garbage\n{\n\n")
        with self.assertRaises(DataQualityError) as ctx:
            run_pipeline(self.cfg, "run-garbage")
        self.assertIn("silver_not_empty", str(ctx.exception))

    def test_quarantine_recovery_flow(self):
        """Fix the source, replay: quarantine shrinks and previously rejected trips load."""
        bad = {"event_id": "fix-1", "event_type": "requested", "trip_id": None, "rider_id": "r1", "city_id": "cairo",
               "currency": "EGP", "event_ts": "2026-01-02T00:00:00Z"}
        with self.cfg.source_path.open("a") as fh:
            fh.write(json.dumps(bad) + "\n")
        run_pipeline(self.cfg, "run-a")
        rejects = [json.loads(x) for x in (self.cfg.quarantine_dir / "rejects.jsonl").read_text().splitlines()]
        self.assertTrue(any(r["reasons"] == ["missing_trip_id"] and "fix-1" in r["raw"] for r in rejects))
        # operator repairs the record in bronze (documented recovery) and rebuilds from bronze
        for batch in sorted(self.cfg.bronze_dir.glob("batch_*.jsonl")):
            text = batch.read_text()
            if "fix-1" in text:
                batch.write_text(text.replace('"trip_id": null', '"trip_id": "trip-fixed"'))
        run_stage(self.cfg, "silver", "run-b")
        run_stage(self.cfg, "load", "run-b")
        self.assertEqual(self.rows("SELECT COUNT(*) FROM fact_trips WHERE trip_id='trip-fixed'")[0][0], 1)

    def test_invalid_run_id_and_stage_rejected(self):
        with self.assertRaises(Exception):
            run_pipeline(self.cfg, "../escape")
        with self.assertRaises(PipelineError):
            run_stage(self.cfg, "nope", "run-x")

    def test_health_and_queries(self):
        self.assertFalse(health_check(self.cfg)["healthy"])  # nothing run yet
        run_pipeline(self.cfg, "run-h")
        health = health_check(self.cfg)
        self.assertTrue(health["healthy"], health)
        names = list_queries(self.cfg)
        self.assertIn("revenue_by_city", names)
        for name in names:
            with self.subTest(name):
                columns, rows = run_query(self.cfg, name)
                self.assertTrue(columns)
                self.assertGreater(len(rows), 0)
        with self.assertRaises(PipelineError):
            run_query(self.cfg, "../../etc/passwd")
        with self.assertRaises(PipelineError):
            run_query(self.cfg, "does_not_exist")


if __name__ == "__main__":
    unittest.main()
