"""Canonical Jetstream v2 JSON decoding, bounded buffering, and reconnects."""

from __future__ import annotations

import json
import math
import re
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from threading import Event as StopEvent
from urllib.parse import urlencode, urlsplit, urlunsplit

from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException
from websockets.sync.client import connect

from .config import MAX_SEQ, Config
from .errors import CursorTooOld, JetstreamError, LiveStreamError, ProtocolError, StreamTimeout
from .models import Event, json_text

PREFIX = "network.bsky.jetstream.subscribeEvents#"
TRANSIENT_STATUS = {408, 429, 500, 502, 503, 504}
_TIMESTAMP_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})"
)


def _timestamp(value: object) -> str:
    if not isinstance(value, str) or not _TIMESTAMP_RE.fullmatch(value):
        raise ProtocolError("Invalid Jetstream v2 timestamp")
    try:
        return (
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            .astimezone(timezone.utc)
            .isoformat()
        )
    except (ValueError, OverflowError):
        raise ProtocolError("Invalid Jetstream v2 timestamp") from None


def _text(payload: dict, name: str, *, required: bool = False) -> str | None:
    value = payload.get(name)
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value:
        raise ProtocolError(f"Invalid Jetstream {name}")
    return value


def _reject_constant(_value: str) -> None:
    raise ValueError("Non-finite JSON number")


def decode_message(frame: str | bytes) -> Event | None:
    """Validate and normalize one Jetstream v2 JSON frame.

    Parameters
    ----------
    frame : str or bytes
        JSON text or encoded JSON bytes received from the WebSocket.

    Returns
    -------
    Event or None
        Normalized event with UTC timestamps and compact JSON payloads, or
        None for an informational message.

    Raises
    ------
    ProtocolError
        JSON, envelope, event fields, or values are unsupported or malformed.
    LiveStreamError
        The frame is a server error, represented by its sanitized error code.
    """
    try:
        envelope = json.loads(frame, parse_constant=_reject_constant)
    except (ValueError, UnicodeDecodeError, RecursionError):
        raise ProtocolError("Malformed Jetstream v2 JSON frame") from None
    if not isinstance(envelope, dict):
        raise ProtocolError("Jetstream v2 frame must be an object")
    if envelope.get("$type") == "error":
        code = envelope.get("error")
        raise LiveStreamError(code if isinstance(code, str) else "UnknownError")
    if envelope.get("$type") != "message" or not isinstance(envelope.get("payload"), dict):
        raise ProtocolError("Unsupported Jetstream v2 frame type")
    payload = envelope["payload"]
    msg_type = payload.get("$type")
    if not isinstance(msg_type, str) or not msg_type.startswith(PREFIX):
        raise ProtocolError("Invalid Jetstream v2 message type")
    kind = msg_type[len(PREFIX) :]
    if kind == "info":
        _text(payload, "name", required=True)
        return None
    if kind not in {"commit", "identity", "account", "sync"}:
        raise ProtocolError("Unknown Jetstream v2 event kind")
    seq = payload.get("seq")
    did = _text(payload, "did", required=True)
    if type(seq) is not int or not 1 <= seq <= MAX_SEQ or not did.startswith("did:"):
        raise ProtocolError("Invalid Jetstream v2 event identity")
    event_time = _timestamp(payload.get("time"))
    witnessed = _timestamp(payload.get("witnessedAt", payload.get("time")))
    operation = collection = rkey = cid = rev = None
    record = None
    if kind == "commit":
        operation = payload.get("operation")
        if not isinstance(operation, str) or operation not in {"create", "update", "delete"}:
            raise ProtocolError("Invalid Jetstream commit operation")
        collection = _text(payload, "collection", required=True)
        rkey = _text(payload, "rkey", required=True)
        rev = _text(payload, "rev", required=True)
        if operation == "delete":
            if payload.get("record") is not None or payload.get("cid") is not None:
                raise ProtocolError("Delete event must not contain a record or CID")
            event_payload = None
        else:
            record = payload.get("record")
            if not isinstance(record, dict):
                raise ProtocolError("Jetstream commit record must be a JSON object")
            cid = _text(payload, "cid")
            event_payload = record
    else:
        event_payload = payload.get(kind)
        if not isinstance(event_payload, dict):
            raise ProtocolError("Missing Jetstream marker payload")
        rev = _text(event_payload, "rev")
    is_resync = payload.get("isResync", False)
    if type(is_resync) is not bool:
        raise ProtocolError("Jetstream resync marker must be a boolean")
    try:
        return Event(
            seq,
            event_time,
            witnessed,
            did,
            kind,
            operation,
            collection,
            rkey,
            cid,
            rev,
            is_resync,
            json_text(record),
            json_text(event_payload),
        )
    except (ValueError, RecursionError):
        raise ProtocolError("Unsupported Jetstream JSON value") from None


def websocket_url(config: Config, cursor: int | None) -> str:
    """Build the v2 subscription URL from explicit source settings.

    Parameters
    ----------
    config : Config
        Validated HTTPS origin and subscription filters.
    cursor : int or None
        Wire sequence cursor, or None to omit the cursor parameter.

    Returns
    -------
    str
        WSS endpoint with URL-encoded cursor and repeated filter parameters.
        The API key is not included in the URL.
    """
    parts = urlsplit(config.endpoint)
    params: list[tuple[str, str]] = []
    if cursor is not None:
        params.append(("cursor", str(cursor)))
    params.extend(("kinds", k) for k in config.kinds)
    params.extend(("dids", d) for d in config.dids)
    params.extend(("collections", c) for c in config.collections)
    return urlunsplit(
        ("wss", parts.netloc, "/xrpc/network.bsky.jetstream.subscribeEvents", urlencode(params), "")
    )


def validate_stream_options(stop: StopEvent | None, timeout_seconds: float | None) -> None:
    """Check optional cancellation and lifetime controls for live iteration.

    Parameters
    ----------
    stop : threading.Event or None
        Cancellation event, or None to disable caller-triggered cancellation.
    timeout_seconds : float or None
        Positive finite duration, or None for no overall deadline.

    Raises
    ------
    ValueError
        The cancellation object or timeout has an invalid type or value.
    """
    if stop is not None and not isinstance(stop, StopEvent):
        raise ValueError("stop must be a threading.Event")
    if timeout_seconds is not None and (
        type(timeout_seconds) not in (int, float)
        or not math.isfinite(timeout_seconds)
        or timeout_seconds <= 0
    ):
        raise ValueError("timeout_seconds must be a positive finite number")


def _remaining(deadline: float | None) -> float:
    if deadline is None:
        return math.inf
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise StreamTimeout("Live stream time budget exhausted")
    return remaining


def iter_live(
    config: Config,
    cursor: int | None,
    *,
    stop: StopEvent | None = None,
    timeout_seconds: float | None = None,
    reconnect_delay: float = 1.0,
) -> Iterator[Event]:
    """Read live events and reconnect from the last yielded sequence.

    Parameters
    ----------
    config : Config
        Validated connection settings, filters, retry limits, and frame-size cap.
    cursor : int or None
        Validated exclusive lower sequence bound, or None for the current tip.
    stop : threading.Event, optional
        Set to stop receiving or interrupt retry waits.
    timeout_seconds : float, optional
        Overall lifetime starting at first iteration, including reconnects.
    reconnect_delay : float, optional
        Initial retry delay in seconds; default 1. Exponential backoff is capped
        at 20 seconds and the remaining stream lifetime.

    Yields
    ------
    Event
        Validated event with a sequence greater than the cursor and any event
        already yielded by this iterator.

    Raises
    ------
    ValueError
        Cancellation or timeout options are invalid.
    StreamTimeout
        The overall stream lifetime expires.
    CursorTooOld
        The server rejects the cursor during connection establishment.
    LiveStreamError
        A terminal error other than the retryable ``ConsumerTooSlow`` arrives.
    ProtocolError
        An event is malformed or exceeds the configured size cap.
    JetstreamError
        Connection access is denied or consecutive failures exhaust retries.

    Notes
    -----
    Cursor persistence remains caller-owned. Close the generator after stopping
    consumption early so its active WebSocket is released.
    """
    validate_stream_options(stop, timeout_seconds)
    deadline = time.monotonic() + timeout_seconds if timeout_seconds is not None else None
    last_seen = cursor
    failures = 0
    while stop is None or not stop.is_set():
        try:
            with connect(
                websocket_url(config, last_seen),
                subprotocols=["xrpc.v1.json"],
                compression=None,
                proxy=None,
                open_timeout=min(config.request_timeout_seconds, _remaining(deadline)),
                close_timeout=1,
                max_size=config.max_batch_bytes,
                max_queue=1,
            ) as ws:
                while stop is None or not stop.is_set():
                    try:
                        frame = ws.recv(timeout=min(0.25, _remaining(deadline)))
                    except TimeoutError:
                        continue  # Poll cancellation and the absolute deadline, including while idle.
                    event = decode_message(frame)
                    if event is not None and (last_seen is None or event.seq > last_seen):
                        last_seen = event.seq
                        failures = 0
                        yield event
                return
        except InvalidStatus as exc:
            response = exc.response
            try:
                error = json.loads(response.body).get("error")
            except (ValueError, AttributeError, TypeError):
                error = None
            if isinstance(error, str) and error in {"CursorTooOld", "OutdatedCursor"}:
                raise CursorTooOld(
                    "Jetstream live lookback expired; replay archive from saved cursor"
                ) from None
            if response.status_code not in TRANSIENT_STATUS:
                raise JetstreamError("Jetstream live connection was rejected") from None
        except LiveStreamError as exc:
            if exc.code != "ConsumerTooSlow":
                raise
        except ConnectionClosed as exc:
            if any(close is not None and close.code == 1009 for close in (exc.sent, exc.rcvd)):
                raise ProtocolError(
                    "Live event exceeds max_batch_bytes; retain the saved cursor"
                ) from None
        except (OSError, WebSocketException):
            # DNS/connect/reset/handshake timeouts are transient. Never expose raw URLs or bodies.
            pass
        if stop is not None and stop.is_set():
            return
        _remaining(deadline)
        failures += 1
        if failures > config.max_retries:
            raise JetstreamError("Jetstream live failures exhausted retry budget") from None
        delay = min(reconnect_delay * 2 ** (failures - 1), 20, _remaining(deadline))
        if stop is None:
            time.sleep(delay)
        else:
            stop.wait(delay)
