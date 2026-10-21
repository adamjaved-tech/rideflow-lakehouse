import tempfile
import unittest
from pathlib import Path

from helpers import make_project
from rideflow.bronze import ingest, list_batches, read_checkpoint
from rideflow.errors import IngestError, SourceError
from rideflow.sources import FileReplaySource


class BronzeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.cfg = make_project(self.tmp)
        self.src = FileReplaySource(self.cfg.source_path)

    def snapshot(self):
        return {p.name: p.read_text() for p in list_batches(self.cfg.bronze_dir)}

    def test_ingest_batches_and_checkpoint(self):
        res = ingest(self.cfg, self.src)
        total = len(self.cfg.source_path.read_text().splitlines())
        self.assertEqual(res.records, total)
        self.assertEqual(read_checkpoint(self.cfg.bronze_dir), total)
        self.assertGreater(res.batches, 1)

    def test_rerun_is_noop(self):
        ingest(self.cfg, self.src)
        before = self.snapshot()
        res = ingest(self.cfg, self.src)
        self.assertEqual(res.records, 0)
        self.assertEqual(self.snapshot(), before)

    def test_incremental_picks_up_only_new_lines(self):
        ingest(self.cfg, self.src)
        first = read_checkpoint(self.cfg.bronze_dir)
        with self.cfg.source_path.open("a") as fh:
            fh.write('{"event_id": "late-1"}\n{"event_id": "late-2"}\n')
        res = ingest(self.cfg, self.src)
        self.assertEqual(res.records, 2)
        self.assertEqual(res.start_offset, first)

    def test_lost_checkpoint_replays_identically(self):
        ingest(self.cfg, self.src)
        before = self.snapshot()
        (self.cfg.bronze_dir / "_checkpoint.json").unlink()
        ingest(self.cfg, self.src)
        self.assertEqual(self.snapshot(), before)

    def test_corrupt_checkpoint_fails_loudly(self):
        ingest(self.cfg, self.src)
        (self.cfg.bronze_dir / "_checkpoint.json").write_text("{not json")
        with self.assertRaises(IngestError):
            ingest(self.cfg, self.src)

    def test_missing_source_file(self):
        with self.assertRaises(SourceError):
            ingest(self.cfg, FileReplaySource(self.tmp / "nope.jsonl"))

    def test_truncated_source_detected(self):
        ingest(self.cfg, self.src)
        self.cfg.source_path.write_text("only one line\n")
        with self.assertRaises(SourceError):
            ingest(self.cfg, self.src)


if __name__ == "__main__":
    unittest.main()
