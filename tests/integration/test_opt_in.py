"""Explicit network tests; configuration stays in this harness, outside the library."""

from __future__ import annotations

import os
from contextlib import closing
from itertools import islice, pairwise
from pathlib import Path

import pytest

from jetstream_lakehouse import Client, Config


def _config() -> Config:
    key_path = os.getenv("JETSTREAM_API_KEY_FILE")
    key = Path(key_path).read_text().strip() if key_path else os.getenv("JETSTREAM_API_KEY", "")
    return Config(
        endpoint=os.getenv("JETSTREAM_ENDPOINT", "https://jetstream.us-east.bsky.network"),
        api_key=key,
        max_sequence_span=int(os.getenv("JETSTREAM_MAX_SEQUENCE_SPAN", "1000")),
        request_timeout_seconds=10,
        refresh_timeout_seconds=30,
    )


@pytest.mark.skipif(
    not os.getenv("JETSTREAM_INTEGRATION_CURSOR"),
    reason="set an explicit Jetstream archive cursor to opt in",
)
def test_bounded_jetstream_archive_read():
    cursor = int(os.environ["JETSTREAM_INTEGRATION_CURSOR"])
    config = _config()
    batch = Client(config).snapshot(cursor)
    assert batch.after_seq == cursor
    assert cursor < batch.through_seq <= cursor + config.max_sequence_span, (
        "choose a cursor within the sealed archive"
    )
    assert batch.events, "choose a cursor whose archive window includes events"
    assert all(cursor < event.seq <= batch.through_seq for event in batch.events)
    assert all(a.seq < b.seq for a, b in pairwise(batch.events))


@pytest.mark.skipif(
    os.getenv("JETSTREAM_INTEGRATION_LIVE") != "1",
    reason="set JETSTREAM_INTEGRATION_LIVE=1 to opt in",
)
def test_bounded_jetstream_v2_live_contract():
    raw_cursor = os.getenv("JETSTREAM_INTEGRATION_LIVE_CURSOR")
    cursor = int(raw_cursor) if raw_cursor else None
    with closing(Client(_config()).live(cursor, timeout_seconds=20)) as stream:
        events = list(islice(stream, 3))
    assert len(events) == 3
    assert all(event.seq > (cursor or 0) for event in events)
    assert all(a.seq < b.seq for a, b in pairwise(events))
