from threading import Event as StopEvent

import pytest

from jetstream_lakehouse import Batch, Client, Config, Event
from jetstream_lakehouse.errors import CursorTooOld, ProtocolError


def event(seq):
    return Event(
        seq, "t", "t", "did:plc:a", "identity", None, None, None, None, None, False, None, "{}"
    )


def test_replay_pins_tip_while_archive_keeps_growing(monkeypatch):
    client = Client(Config(max_sequence_span=2))
    discoveries, windows, connections = [], [], []
    tip = [3]

    def discover(after):
        discoveries.append(after)
        return tip[0]

    def snapshot(after, before):
        windows.append((after, before))
        tip[0] = 1000  # A new segment seals while consuming the first batch.
        return Batch((event(before),), after, before)

    def live(cursor, **_):
        connections.append(cursor)
        yield event(3)  # duplicate at the inclusive seam
        yield event(4)

    monkeypatch.setattr(client._reader, "sealed_tip", discover)
    monkeypatch.setattr(client, "snapshot", snapshot)
    monkeypatch.setattr(client, "live", live)
    assert [batch.through_seq for batch in client.replay(0)] == [2, 3, 4]
    assert discoveries == [0]
    assert windows == [(0, 2), (2, 3)]
    assert connections == [3]


def test_expired_lookback_recovers_missing_archive_range(monkeypatch):
    client = Client(Config())
    tips = iter([2, 4])
    windows, connections = [], []
    monkeypatch.setattr(client._reader, "sealed_tip", lambda _: next(tips))

    def snapshot(after, before):
        windows.append((after, before))
        return Batch((), after, before)  # filtered windows still advance validated progress

    def live(cursor, **_):
        connections.append(cursor)
        if cursor == 2:
            raise CursorTooOld("expired")
        yield event(4)
        yield event(5)

    monkeypatch.setattr(client, "snapshot", snapshot)
    monkeypatch.setattr(client, "live", live)
    assert [batch.through_seq for batch in client.replay(0)] == [2, 4, 5]
    assert windows == [(0, 2), (2, 4)] and connections == [2, 4]


def test_expired_cursor_without_archive_progress_fails_without_spinning(monkeypatch):
    client = Client(Config())
    monkeypatch.setattr(client._reader, "sealed_tip", lambda _: 2)

    def live(*_, **__):
        raise CursorTooOld("expired")

    monkeypatch.setattr(client, "live", live)
    with pytest.raises(CursorTooOld, match="cannot yet bridge"):
        next(client.replay(2))


def test_replay_close_releases_nested_live_generator(monkeypatch):
    client = Client(Config())
    closed = []
    monkeypatch.setattr(client._reader, "sealed_tip", lambda _: 0)

    def live(*_, **__):
        try:
            yield event(1)
            yield event(2)
        finally:
            closed.append(True)

    monkeypatch.setattr(client, "live", live)
    stream = client.replay(0)
    assert next(stream).through_seq == 1
    stream.close()
    assert closed == [True]


def test_changed_pinned_range_does_not_yield_candidate(monkeypatch):
    client = Client(Config())
    monkeypatch.setattr(client._reader, "sealed_tip", lambda _: 2)
    monkeypatch.setattr(client, "snapshot", lambda after, before: Batch((), after, 1))
    with pytest.raises(ProtocolError, match="Pinned archive range"):
        next(client.replay(0))


def test_cancelled_replay_never_discovers_archive(monkeypatch):
    client = Client(Config())
    monkeypatch.setattr(client._reader, "sealed_tip", lambda _: pytest.fail("unexpected request"))
    stop = StopEvent()
    stop.set()
    assert list(client.replay(0, stop=stop)) == []


def test_consumer_can_restart_from_saved_cursor_after_failed_delivery(monkeypatch):
    client = Client(Config())
    monkeypatch.setattr(client._reader, "sealed_tip", lambda _: 2)
    monkeypatch.setattr(
        client, "snapshot", lambda after, before: Batch((event(1), event(2)), after, before)
    )
    saved = 0
    stream = client.replay(saved)
    first = next(stream)
    stream.close()  # destination failed, so saved is unchanged
    restarted = client.replay(saved)
    assert next(restarted) == first
    restarted.close()
