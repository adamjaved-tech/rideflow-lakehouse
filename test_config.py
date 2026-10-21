import tempfile
import unittest
from pathlib import Path

import yaml

from helpers import REPO
from rideflow.config import load_config
from rideflow.errors import ConfigError

CFG = REPO / "config" / "pipeline.yaml"


class ConfigTests(unittest.TestCase):
    def test_loads_default_config(self):
        cfg = load_config(CFG, env={})
        self.assertEqual(cfg.source_type, "file")
        self.assertTrue(cfg.warehouse_path.is_absolute())
        self.assertEqual(cfg.bronze_dir.name, "bronze")

    def test_missing_file_raises(self):
        with self.assertRaises(ConfigError):
            load_config("/nonexistent/pipeline.yaml", env={})

    def test_env_override_wins(self):
        cfg = load_config(CFG, env={"RIDEFLOW_BATCH_SIZE": "17"})
        self.assertEqual(cfg.batch_size, 17)

    def test_invalid_env_value_raises(self):
        with self.assertRaises(ConfigError):
            load_config(CFG, env={"RIDEFLOW_BATCH_SIZE": "lots"})

    def _write(self, mutate):
        data = yaml.safe_load(CFG.read_text())
        mutate(data)
        tmp = Path(tempfile.mkdtemp())
        (tmp / "config").mkdir()
        path = tmp / "config" / "pipeline.yaml"
        path.write_text(yaml.safe_dump(data))
        return path

    def test_missing_required_key(self):
        path = self._write(lambda d: d.pop("batch_size"))
        with self.assertRaisesRegex(ConfigError, "batch_size"):
            load_config(path, env={})

    def test_unknown_key_rejected(self):
        path = self._write(lambda d: d.update(typo_key=1))
        with self.assertRaisesRegex(ConfigError, "unknown"):
            load_config(path, env={})

    def test_rate_out_of_range(self):
        path = self._write(lambda d: d.update(max_reject_rate=1.5))
        with self.assertRaises(ConfigError):
            load_config(path, env={})

    def test_kafka_requires_bootstrap(self):
        path = self._write(lambda d: d.update(source_type="kafka"))
        with self.assertRaisesRegex(ConfigError, "kafka_bootstrap"):
            load_config(path, env={})
        cfg = load_config(path, env={"RIDEFLOW_KAFKA_BOOTSTRAP": "broker:9092"})
        self.assertEqual(cfg.kafka_bootstrap, "broker:9092")

    def test_bad_as_of(self):
        path = self._write(lambda d: d.update(as_of="yesterday-ish"))
        with self.assertRaisesRegex(ConfigError, "as_of"):
            load_config(path, env={})


if __name__ == "__main__":
    unittest.main()
