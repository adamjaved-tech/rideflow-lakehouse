"""Bronze layer: raw, append-only micro-batches with a durable offset checkpoint.

* Batch file names encode the offset range, so re-ingesting a range rewrites the same
  file with identical content (idempotent).
* The checkpoint advances only after the batch file is durable (at-least-once, with
  idempotent writes => effectively once).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

from rideflow.errors import IngestError
from rideflow.fsutil import atomic_write_json, atomic_write_text
from rideflow.logging_setup import get_logger, log_event

log = get_logger("bronze")
CHECKPOINT = "_checkpoint.json"


@dataclass
class IngestResult:
    batches: int
    records: int
    start_offset: int
    end_offset: int


def read_checkpoint(root: Path) -> int:
    path = Path(root) / CHECKPOINT
    if not path.exists():
        return 0
    try:
        offset = int(json.loads(path.read_text(encoding="utf-8"))["offset"])
    except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise IngestError(f"bronze checkpoint is corrupt: {path}. Inspect it or delete it to re-ingest from offset 0.") from exc
    if offset < 0:
        raise IngestError(f"bronze checkpoint has a negative offset: {path}")
    return offset


def ingest(cfg, source) -> IngestResult:
    root = cfg.bronze_dir
    root.mkdir(parents=True, exist_ok=True)
    start = offset = read_checkpoint(root)
    batches = records = 0
    for next_offset, lines in source.read_batches(offset, cfg.batch_size):
        name = f"batch_{offset:010d}_{next_offset:010d}.jsonl"
        atomic_write_text(root / name, "\n".join(lines) + "\n")
        atomic_write_json(root / CHECKPOINT, {"offset": next_offset})
        source.commit(next_offset)
        batches += 1
        records += len(lines)
        offset = next_offset
        log_event(log, logging.DEBUG, "bronze batch written", file=name, records=len(lines))
    log_event(log, logging.INFO, "bronze ingest finished", batches=batches, records=records, start_offset=start, end_offset=offset)
    return IngestResult(batches=batches, records=records, start_offset=start, end_offset=offset)


def list_batches(root: Path) -> list[Path]:
    return sorted(Path(root).glob("batch_*.jsonl"))
