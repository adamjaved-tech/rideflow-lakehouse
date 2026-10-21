"""Timestamp helpers. All pipeline timestamps are UTC, ISO-8601, second precision."""

from __future__ import annotations

from datetime import datetime, timezone

TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def parse_ts(value) -> datetime:
    """Parse an ISO-8601 string into an aware UTC datetime.

    Naive timestamps are assumed to already be UTC. Raises ValueError when the value
    is not a non-empty, parseable string.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp must be a non-empty string")
    text = value.strip()
    if text[-1] in ("Z", "z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_ts(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(TS_FORMAT)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
