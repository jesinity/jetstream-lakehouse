from __future__ import annotations

import struct
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from jetstream_lakehouse import ArchivePlan, Batch, Client, Config
from jetstream_lakehouse.errors import ArchiveChanged, ProtocolError


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


def test_public_plan_round_trip_reads_saved_pages_without_replanning(service):
    original = Client(Config(api_key="secret-test-token"))
    plan = original.plan_snapshot(0, 7)
    serialized = plan.to_json()
    assert plan.format_version == 1
    assert (plan.after_seq, plan.through_seq) == (0, 7)
    assert "secret-test-token" not in serialized
    assert all(request.url.endswith("planSnapshot") for request in service.calls)

    service.calls.clear()
    restored = ArchivePlan.from_json(serialized)
    batch = Client(Config(api_key="rotated-token")).read_plan(restored)
    assert [event.seq for event in batch.events] == [1, 2, 3, 4, 5, 6, 7]
    assert batch.through_seq == 7
    assert all(not request.url.endswith("planSnapshot") for request in service.calls)

    service.calls.clear()
    assert Client(Config(api_key="rotated-token")).read_plan(restored) == batch
    assert all(not request.url.endswith("planSnapshot") for request in service.calls)


def test_plan_snapshot_downloads_no_segments(service):
    plan = Client(Config()).plan_snapshot(0, 7)
    assert plan.through_seq == 7
    assert len([request for request in service.calls if request.url.endswith("planSnapshot")]) == 3


def test_plan_scope_is_canonical_and_mismatch_fails_before_io(service):
    plan = Client(Config(kinds=("sync", "commit"), dids=("did:plc:b", "did:plc:a"))).plan_snapshot(
        0, 7
    )
    equivalent = Client(Config(kinds=("commit", "sync", "commit"), dids=("did:plc:a", "did:plc:b")))
    service.calls.clear()
    assert equivalent.read_plan(ArchivePlan.from_json(plan.to_json())).through_seq == 7

    service.calls.clear()
    with pytest.raises(ValueError, match="scope"):
        Client(Config(dids=("did:plc:other",))).read_plan(plan)
    assert service.calls == []


def test_plan_detects_changed_generation_without_replanning(service):
    client = Client(Config())
    plan = client.plan_snapshot(0, 7)
    service.checksum = "0123456789abcdef"
    service.calls.clear()
    with pytest.raises(ArchiveChanged):
        client.read_plan(plan)
    assert all(not request.url.endswith("planSnapshot") for request in service.calls)


def test_new_sealed_tip_does_not_expand_a_saved_window(service):
    client = Client(Config())
    plan = client.plan_snapshot(0, 3)
    service.tip = 12
    service.calls.clear()
    batch = client.read_plan(plan)
    assert [event.seq for event in batch.events] == [1, 2, 3]
    assert batch.through_seq == 3
    assert all(not request.url.endswith("planSnapshot") for request in service.calls)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda data: data[:-1],
        lambda data: data.replace('"formatVersion":1', '"formatVersion":2'),
        lambda data: data.replace('"afterSeq":0', '"afterSeq":true'),
        lambda data: _change_plan_boundary(data),
        lambda data: data.replace('"checksum":"', '"checksum":"bad'),
    ],
)
def test_saved_plan_rejects_tampering(mutation, service):
    plan = Client(Config()).plan_snapshot(0, 7)
    with pytest.raises(ProtocolError):
        ArchivePlan.from_json(mutation(plan.to_json()))


def _change_plan_boundary(data):
    import json

    payload = json.loads(data)
    payload["throughSeq"] += 1
    return json.dumps(payload)


def test_plan_read_limit_does_not_reapply_planning_span(service):
    plan = Client(Config(max_sequence_span=10)).plan_snapshot(0, 7)
    smaller_reader = Client(Config(max_sequence_span=2))
    assert smaller_reader.read_plan(plan).through_seq == 7


def test_empty_plan_does_not_make_read_requests(service):
    client = Client(Config())
    plan = client.plan_snapshot(7, 9)
    assert plan.after_seq == plan.through_seq == 7
    assert ArchivePlan.from_json(plan.to_json()).through_seq == 7
    service.calls.clear()
    assert client.read_plan(plan) == Batch((), 7, 7)
    assert service.calls == []


def test_successive_public_windows_stop_at_fixed_discovered_target(service):
    client = Client(Config(max_sequence_span=2))
    target = client.sealed_tip(0)
    service.tip = 12  # A newer archive boundary appears after discovery.
    cursor = 0
    sequences = []
    bounds = []
    while cursor < target:
        before = min(target, cursor + client.config.max_sequence_span)
        plan = client.plan_snapshot(cursor, before)
        bounds.append((cursor, plan.through_seq))
        batch = client.read_plan(plan)
        sequences.extend(event.seq for event in batch.events)
        cursor = batch.through_seq
    assert target == 7
    assert len(bounds) > 3
    assert bounds[-1][1] == target
    assert all(through <= target for _, through in bounds)
    assert sequences == list(range(1, 8))


def test_snapshot_keeps_one_deadline_across_planning_and_execution(service, monkeypatch):
    from jetstream_lakehouse.errors import RefreshTimeout

    now = [0.0]
    monkeypatch.setattr("jetstream_lakehouse.transport.time.monotonic", lambda: now[0])

    def consume_deadline(plan):
        if plan["plannedThroughSeq"] == 7:
            now[0] = 2.0

    service.plan_override = consume_deadline
    with pytest.raises(RefreshTimeout):
        Client(Config(refresh_timeout_seconds=1)).snapshot(0, 7)
    assert all(request.url.endswith("planSnapshot") for request in service.calls)


def test_event_order_is_checked_before_filtering(service, monkeypatch):
    rows = [
        {"seq": 2, "kind": "identity", "did": "did:plc:other"},
        {"seq": 1, "kind": "identity", "did": "did:plc:other"},
    ]

    def unordered(_transport, _entry, after, _through, _include_raw_payload):
        if after == 0:
            yield from rows

    monkeypatch.setattr("jetstream_lakehouse.reader.segment_events", unordered)
    with pytest.raises(ProtocolError, match="out-of-order"):
        Client(Config(dids=("did:plc:known",))).snapshot(0, 7)


def test_oversized_multibyte_saved_plan_is_rejected():
    with pytest.raises(ProtocolError, match="size limit"):
        ArchivePlan.from_json("é" * (1024 * 1024 + 1))
