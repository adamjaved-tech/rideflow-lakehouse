import json
import logging
import unittest

from rideflow.logging_setup import JsonFormatter, redact, redact_text, set_run_id


class LoggingTests(unittest.TestCase):
    def test_redacts_sensitive_keys_recursively(self):
        out = redact({"user": "a", "password": "p", "nested": {"api_token": "t", "ok": 1}, "list": [{"secret": "s"}]})
        self.assertEqual(out["password"], "***REDACTED***")
        self.assertEqual(out["nested"]["api_token"], "***REDACTED***")
        self.assertEqual(out["list"][0]["secret"], "***REDACTED***")
        self.assertEqual(out["user"], "a")

    def test_redacts_url_credentials(self):
        self.assertNotIn("hunter2", redact_text("postgres://admin:hunter2@db:5432/x"))

    def test_formatter_emits_json_with_run_id_and_no_secret(self):
        set_run_id("run-t")
        rec = logging.LogRecord("rideflow.t", logging.INFO, __file__, 1, "connecting %s", ("kafka://u:pw@h",), None)
        rec.ctx = {"stage": "x", "password": "p"}
        payload = json.loads(JsonFormatter().format(rec))
        self.assertEqual(payload["run_id"], "run-t")
        self.assertEqual(payload["stage"], "x")
        self.assertNotIn("pw@", json.dumps(payload))
        self.assertEqual(payload["password"], "***REDACTED***")


if __name__ == "__main__":
    unittest.main()
