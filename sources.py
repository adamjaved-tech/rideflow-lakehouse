"""Event sources. A source yields (next_offset, [raw lines]) micro-batches starting at an
offset; the bronze layer owns the checkpoint, so replays are deterministic."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Iterator, Protocol

from rideflow.errors import SourceError


class EventSource(Protocol):
    def read_batches(self, start_offset: int, batch_size: int) -> Iterator[tuple[int, list[str]]]: ...

    def commit(self, next_offset: int) -> None: ...


class FileReplaySource:
    """Replays a JSON-lines file; offset = number of raw lines already consumed."""

    def __init__(self, path: Path):
        self.path = Path(path)

    def read_batches(self, start_offset: int, batch_size: int) -> Iterator[tuple[int, list[str]]]:
        if not self.path.is_file():
            raise SourceError(f"source file not found: {self.path}")
        with self.path.open(encoding="utf-8", errors="replace") as fh:
            lines = fh.read().splitlines()
        if start_offset > len(lines):
            raise SourceError(
                f"checkpoint offset {start_offset} is beyond the end of the source ({len(lines)} lines); "
                "the source was truncated or replaced. Reset the bronze checkpoint deliberately to reprocess."
            )
        for begin in range(start_offset, len(lines), batch_size):
            end = min(begin + batch_size, len(lines))
            yield end, lines[begin:end]

    def commit(self, next_offset: int) -> None:  # offsets live in the bronze checkpoint
        return None


class KafkaSource:
    """Single-partition Kafka consumer using externally managed offsets (bronze checkpoint).

    UNVERIFIED in the build environment (no broker / client library available offline);
    see README 'Verification status'. Requires `pip install -r requirements-kafka.txt`.
    """

    def __init__(self, bootstrap: str, topic: str, group: str, idle_timeout_s: float = 10.0):
        self.bootstrap = bootstrap
        self.topic = topic
        self.group = group
        self.idle_timeout_s = idle_timeout_s

    def read_batches(self, start_offset: int, batch_size: int) -> Iterator[tuple[int, list[str]]]:
        try:
            from confluent_kafka import Consumer, TopicPartition
        except ImportError as exc:  # pragma: no cover - depends on optional dependency
            raise SourceError("confluent-kafka is not installed; run: pip install -r requirements-kafka.txt") from exc

        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": self.group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        consumer.assign([TopicPartition(self.topic, 0, start_offset)])
        buffer: list[str] = []
        next_offset = start_offset
        last_message = time.monotonic()
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    if time.monotonic() - last_message > self.idle_timeout_s:
                        break
                    continue
                if msg.error():
                    raise SourceError(f"kafka error: {msg.error()}")
                last_message = time.monotonic()
                text = msg.value().decode("utf-8", errors="replace").replace("\r", " ").replace("\n", " ")
                buffer.append(text)
                next_offset = msg.offset() + 1
                if len(buffer) >= batch_size:
                    yield next_offset, buffer
                    buffer = []
            if buffer:
                yield next_offset, buffer
        finally:
            consumer.close()

    def commit(self, next_offset: int) -> None:
        return None


def build_source(cfg) -> EventSource:
    if cfg.source_type == "file":
        return FileReplaySource(cfg.source_path)
    return KafkaSource(cfg.kafka_bootstrap, cfg.kafka_topic, cfg.kafka_group)
