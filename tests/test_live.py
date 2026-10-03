from __future__ import annotations

import json
from threading import Event as StopEvent
from urllib.parse import parse_qs, urlsplit

import pytest
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosedError, InvalidStatus, WebSocketException
from websockets.frames import Close
from websockets.http11 import Response

from jetstream_lakehouse import Client, Config
from jetstream_lakehouse.errors import (
    CursorTooOld,
    JetstreamError,
    LiveStreamError,
    ProtocolError,
    StreamTimeout,
)
from jetstream_lakehouse.live import decode_message


def frame(seq=1, kind="identity", **fields):
    payload = {
        "$type": f"network.bsky.jetstream.subscribeEvents#{kind}",
        "seq": seq,
        "did": "did:plc:abc",
        "time": "2026-01-01T00:00:00.000000Z",
        "witnessedAt": "2026-01-01T00:00:00.000000Z",
    }
    if kind != "commit":
        payload[kind] = {"did": "did:plc:abc", "seq": 123}
    payload.update(fields)
    return json.dumps({"$type": "message", "payload": payload})


class Socket:
    def __init__(self, items):
        self.items = iter(items)
        self.closed = False
        self.reads = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def recv(self, timeout):
        self.reads += 1
        item = next(self.items, WebSocketException("connection closed"))
        if isinstance(item, BaseException):
            raise item
        return item


@pytest.fixture
def sockets(monkeypatch):
    calls = []
    pending = []

    def connect(url, **options):
        calls.append((url, options))
        item = pending.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    monkeypatch.setattr("jetstream_lakehouse.live.connect", connect)
    monkeypatch.setattr("jetstream_lakehouse.live.time.sleep", lambda _: None)
    return pending, calls


def rejected(status, code="Busy"):
    return InvalidStatus(
        Response(
            status,
            "Test",
            Headers(),
            body=json.dumps({"error": code, "message": "secret must not escape"}).encode(),
        )
    )


def test_start_from_current_tip_and_apply_backpressure(sockets):
    pending, calls = sockets
    socket = Socket([frame(1), frame(2)])
    pending.append(socket)
    stream = Client(Config()).live()
    assert calls == []
    assert next(stream).seq == 1
    assert socket.reads == 1
    assert "cursor" not in parse_qs(urlsplit(calls[0][0]).query)
    assert calls[0][1]["max_queue"] == 1
    assert calls[0][1]["proxy"] is None
    stream.close()
    assert socket.closed


def test_duplicate_and_backward_sequences_are_suppressed(sockets):
    sockets[0].append(Socket([frame(2), frame(2), frame(1), frame(3)]))
    stream = Client(Config()).live(0)
    assert [next(stream).seq, next(stream).seq] == [2, 3]
    stream.close()


@pytest.mark.parametrize(
    "failure",
    [ConnectionRefusedError(), TimeoutError(), WebSocketException(), rejected(503), rejected(429)],
)
def test_transient_connect_failure_retries(sockets, failure):
    pending, calls = sockets
    pending.extend([failure, Socket([frame(2)])])
    stream = Client(Config()).live(1)
    assert next(stream).seq == 2
    assert len(calls) == 2
    stream.close()


def test_reconnect_uses_last_yielded_cursor(sockets):
    pending, calls = sockets
    pending.extend([Socket([frame(5), OSError()]), Socket([frame(5), frame(6)])])
    stream = Client(Config()).live(0)
    assert [next(stream).seq, next(stream).seq] == [5, 6]
    assert parse_qs(urlsplit(calls[1][0]).query)["cursor"] == ["5"]
    stream.close()


def test_retry_budget_is_finite(sockets):
    pending, calls = sockets
    pending.extend([OSError(), OSError(), OSError()])
    with pytest.raises(JetstreamError, match="retry budget"):
        next(Client(Config(max_retries=2)).live())
    assert len(calls) == 3


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_permanent_rejections_do_not_retry_or_expose_body(sockets, status):
    sockets[0].append(rejected(status))
    with pytest.raises(JetstreamError) as error:
        next(Client(Config()).live())
    assert "secret" not in str(error.value)
    assert len(sockets[1]) == 1


def test_expired_cursor_is_available_to_replay(sockets):
    sockets[0].append(rejected(400, "CursorTooOld"))
    with pytest.raises(CursorTooOld):
        next(Client(Config()).live(1))


def test_slow_consumer_frame_reconnects_without_skipping(sockets):
    pending, calls = sockets
    pending.extend(
        [
            Socket([frame(1), '{"$type":"error","error":"ConsumerTooSlow"}']),
            Socket([frame(1), frame(2)]),
        ]
    )
    stream = Client(Config()).live(0)
    assert [next(stream).seq, next(stream).seq] == [1, 2]
    assert parse_qs(urlsplit(calls[-1][0]).query)["cursor"] == ["1"]
    stream.close()


def test_oversized_event_fails_instead_of_reconnect_loop(sockets):
    sockets[0].append(Socket([ConnectionClosedError(None, Close(1009, "too big"))]))
    with pytest.raises(ProtocolError, match="max_batch_bytes"):
        next(Client(Config()).live(0))
    assert len(sockets[1]) == 1


def test_idle_receive_honors_deadline(monkeypatch, sockets):
    clock = [0.0]
    monkeypatch.setattr("jetstream_lakehouse.live.time.monotonic", lambda: clock[0])
    socket = Socket([])

    def recv(timeout):
        clock[0] += timeout
        raise TimeoutError

    socket.recv = recv
    sockets[0].append(socket)
    with pytest.raises(StreamTimeout):
        next(Client(Config()).live(timeout_seconds=0.5))
    assert clock[0] == 0.5 and socket.closed


def test_deadline_includes_reconnect_backoff(monkeypatch, sockets):
    clock = [0.0]
    monkeypatch.setattr("jetstream_lakehouse.live.time.monotonic", lambda: clock[0])
    monkeypatch.setattr(
        "jetstream_lakehouse.live.time.sleep", lambda delay: clock.__setitem__(0, clock[0] + delay)
    )
    sockets[0].append(OSError())
    with pytest.raises(StreamTimeout):
        next(Client(Config()).live(timeout_seconds=0.1))
    assert len(sockets[1]) == 1


def test_stop_cancels_idle_connection(sockets):
    stop = StopEvent()
    socket = Socket([])

    def recv(timeout):
        stop.set()
        raise TimeoutError

    socket.recv = recv
    sockets[0].append(socket)
    assert list(Client(Config()).live(stop=stop)) == []
    assert socket.closed


def test_already_cancelled_stream_does_not_connect(sockets):
    stop = StopEvent()
    stop.set()
    assert list(Client(Config()).live(stop=stop)) == []
    assert not sockets[1]


@pytest.mark.parametrize("timeout", [0, -1, True, float("nan"), float("inf"), "30"])
def test_invalid_stream_deadlines(timeout):
    with pytest.raises(ValueError):
        Client(Config()).live(timeout_seconds=timeout)


@pytest.mark.parametrize(
    "message",
    [
        "null",
        "[]",
        '{"$type":"message","payload":{"$type":3}}',
        frame(0),
        frame(1, time="2026-01-01"),
        frame(1, identity=None),
        frame(1, kind="commit", operation=[]),
        frame(
            1,
            kind="commit",
            operation="create",
            collection="app.bsky.feed.post",
            rkey="a",
            rev="rev",
            record=None,
        ),
    ],
)
def test_malformed_frames_fail_closed(message):
    with pytest.raises(ProtocolError):
        decode_message(message)


def test_info_and_terminal_frames():
    assert (
        decode_message(
            '{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#info","name":"OutdatedCursor"}}'
        )
        is None
    )
    with pytest.raises(LiveStreamError) as error:
        decode_message('{"$type":"error","error":"UnknownServerError","message":"private"}')
    assert error.value.code == "UnknownError" and "private" not in str(error.value)


def test_archive_and_live_models_match_for_all_kinds(service):
    archive = Client(Config()).snapshot(0, 7).events
    assert len(archive) == 7  # create, update, delete, identity, account, sync, resync
    for event in archive:
        fields = {"did": event.did, "time": event.event_time, "witnessedAt": event.witnessed_at}
        if event.kind == "commit":
            fields.update(
                operation=event.operation,
                collection=event.collection,
                rkey=event.rkey,
                rev=event.rev,
                isResync=event.is_resync,
            )
            if event.record is not None:
                fields.update(record=json.loads(event.record), cid=event.cid)
        else:
            fields[event.kind] = json.loads(event.event_payload)
        live = decode_message(frame(event.seq, event.kind, **fields))
        assert live == event
