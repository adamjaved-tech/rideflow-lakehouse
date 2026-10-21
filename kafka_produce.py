#!/usr/bin/env python3
"""Publish a JSON-lines events file to Kafka partition 0 (for the streaming source).

UNVERIFIED in the build environment (no broker). Usage:
    RIDEFLOW_KAFKA_BOOTSTRAP=localhost:9092 python scripts/kafka_produce.py data/sample/events.jsonl
"""
import os
import sys


def main() -> int:
    try:
        from confluent_kafka import Producer
    except ImportError:
        print("confluent-kafka missing: pip install -r requirements-kafka.txt", file=sys.stderr)
        return 2
    bootstrap = os.environ.get("RIDEFLOW_KAFKA_BOOTSTRAP")
    if not bootstrap:
        print("set RIDEFLOW_KAFKA_BOOTSTRAP", file=sys.stderr)
        return 2
    topic = os.environ.get("RIDEFLOW_KAFKA_TOPIC", "rideflow.events")
    path = sys.argv[1] if len(sys.argv) > 1 else "data/sample/events.jsonl"
    producer = Producer({"bootstrap.servers": bootstrap})
    sent = 0
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            producer.produce(topic, value=line.rstrip("\n").encode("utf-8"), partition=0)
            sent += 1
            producer.poll(0)
    producer.flush(30)
    print(f"published {sent} messages to {topic}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
