from __future__ import annotations

import struct
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from jetstream_lakehouse import Batch, Client, Config
from jetstream_lakehouse.errors import ProtocolError


class FixtureTransport:
    def __init__(self, _config):
        self.data = (Path(__file__).parent / "native.jss").read_bytes()
        self.checksum = f"{struct.unpack_from('<Q', self.data, 4)[0]:016x}"

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def plan(self, after, before=None):
        tip = min(3 if before is None else before, 3)
        segments = (
            [
                {
                    "name": "seg_0000000000.jss",
                    "index": 0,
                    "checksum": self.checksum,
                    "minSeq": 1,
                    "maxSeq": 3,
                    "mode": "segment",
                }
            ]
            if after < tip
            else []
        )
        return {"sealedTipSeq": tip, "plannedThroughSeq": tip, "segments": segments}

    def range(self, _name, _checksum, first, last):
        return self.data[first : last + 1], len(self.data)

    def remaining(self):
        return 100


@pytest.fixture
def native(monkeypatch):
    monkeypatch.setattr("jetstream_lakehouse.reader.Transport", FixtureTransport)
    return Client(Config(max_sequence_span=10))


def test_native_official_fixture(native):
    batch = native.snapshot(0)
    assert [event.seq for event in batch.events] == [1, 2, 3]
    assert batch.events[0].record == '{"text":"hello"}'
    assert batch.events[2].operation == "delete"
    assert batch.events[2].event_payload is None
    assert (batch.after_seq, batch.through_seq) == (0, 3)


@pytest.mark.parametrize(
    "after,before,expected", [(0, 1, [1]), (1, 2, [2]), (0, 2, [1, 2]), (2, 3, [3]), (3, 3, [])]
)
def test_native_window_can_end_inside_physical_segment(native, after, before, expected):
    batch = native.snapshot(after, before)
    assert [event.seq for event in batch.events] == expected
    assert batch.through_seq == before


def test_native_raw_cbor_is_explicit_base64(monkeypatch):
    monkeypatch.setattr("jetstream_lakehouse.reader.Transport", FixtureTransport)
    import base64

    import cbor2

    events = Client(Config(include_raw_payload=True)).snapshot(0).events
    assert cbor2.loads(base64.b64decode(events[0].raw_payload_base64)) == {"text": "hello"}
    assert events[-1].raw_payload_base64 is None


def test_zero_row_filter_retains_validated_progress(monkeypatch):
    monkeypatch.setattr("jetstream_lakehouse.reader.Transport", FixtureTransport)
    batch = Client(Config(dids=("did:plc:other",))).snapshot(0)
    assert batch.events == () and batch.through_seq == 3


def test_immutable_public_values(native):
    batch = native.snapshot(0)
    with pytest.raises(FrozenInstanceError):
        batch.through_seq = 5
    with pytest.raises(FrozenInstanceError):
        batch.events[0].seq = 5


def test_paginated_http_snapshot_and_exact_filter(service):
    client = Client(Config(collections=("app.bsky.feed.post",)))
    batch = client.snapshot(0, 7)
    assert batch.through_seq == 7
    assert [event.seq for event in batch.events] == [1, 2, 3, 4, 5, 6]
    plans = [request for request in service.calls if request.url.endswith("planSnapshot")]
    assert len(plans) == 3


def test_bounded_http_snapshot_does_not_require_segment_boundary(service):
    assert [event.seq for event in Client(Config()).snapshot(0, 2).events] == [1, 2]


def test_plan_generation_changes_abort_before_decoding(service):
    calls = 0

    def mutate(plan):
        nonlocal calls
        calls += 1
        if calls > 1:
            plan["segments"][0]["checksum"] = "0123456789abcdef"

    service.plan_override = mutate
    with pytest.raises(ProtocolError, match="mixed segment generations"):
        Client(Config()).snapshot(0, 7)
    assert all(request.url.endswith("planSnapshot") for request in service.calls)


def test_plan_must_make_progress(service):
    service.plan_override = lambda plan: plan.update(plannedThroughSeq=0)
    with pytest.raises(ProtocolError, match="no progress"):
        Client(Config()).snapshot(0, 7)


def test_materialized_batch_byte_budget(service):
    with pytest.raises(ProtocolError, match="memory budget"):
        Client(Config(max_batch_bytes=1024)).snapshot(0, 7)


def test_empty_interval_never_requests_network(service):
    assert Client(Config()).snapshot(7, 7) == Batch((), 7, 7)
    assert service.calls == []


def test_deadline_includes_decoding_before_returning_candidate(service, monkeypatch):
    from jetstream_lakehouse.errors import RefreshTimeout

    clock = [0.0]
    monkeypatch.setattr("jetstream_lakehouse.transport.time.monotonic", lambda: clock[0])

    def slow_decode(*_):
        clock[0] = 2.0
        yield {"seq": 1}

    monkeypatch.setattr("jetstream_lakehouse.reader.segment_events", slow_decode)
    with pytest.raises(RefreshTimeout):
        Client(Config(refresh_timeout_seconds=1)).snapshot(0, 2)


def test_planner_cannot_claim_more_than_requested(service):
    service.plan_override = lambda plan: plan.update(sealedTipSeq=7)
    with pytest.raises(ProtocolError, match="before_seq"):
        Client(Config()).snapshot(0, 2)
