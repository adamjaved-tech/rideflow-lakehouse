"""Structural checks of the Airflow DAG without importing Airflow (not installed here)."""
import ast
import unittest

from helpers import REPO
from rideflow.pipeline import STAGES

SRC = (REPO / "dags" / "rideflow_dag.py").read_text()


class DagDefinitionTests(unittest.TestCase):
    def setUp(self):
        self.tree = ast.parse(SRC)

    def test_dag_declares_required_reliability_settings(self):
        for needle in ('"retries": 3', "retry_exponential_backoff", "execution_timeout", "on_failure_callback",
                       "catchup=False", "max_active_runs=1", "dagrun_timeout", 'schedule="@hourly"'):
            self.assertIn(needle, SRC)

    def test_tasks_follow_pipeline_stage_order(self):
        # tasks are generated from STAGES in order and chained with >>
        self.assertIn("for stage in STAGES", SRC)
        self.assertIn("previous >> task", SRC)
        self.assertEqual(STAGES, ("init_db", "ingest", "silver", "load", "gold_quality", "report"))

    def test_dag_uses_public_stage_api(self):
        imported = {n.module for n in ast.walk(self.tree) if isinstance(n, ast.ImportFrom)}
        self.assertIn("rideflow.pipeline", imported)


if __name__ == "__main__":
    unittest.main()
