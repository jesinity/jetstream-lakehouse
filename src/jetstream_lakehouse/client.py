"""Public synchronous Jetstream client."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import closing
from threading import Event as StopEvent

from .config import Config, sequence
from .errors import CursorTooOld, ProtocolError
from .live import iter_live, validate_stream_options
from .models import Batch, Event
from .reader import Reader


class Client:
    """Read Jetstream archive and live events without coupling to a destination.

    Parameters
    ----------
    config : Config
        Explicit connection settings, filters, and resource limits. Construction
        validates these settings without opening a network connection.

    Notes
    -----
    The application owns destination writes and cursor persistence. A returned
    cursor is safe to save only after the corresponding events are durable.
    """

    def __init__(self, config: Config) -> None:
        self.config = config.validate()
        self._reader = Reader(self.config)

    def snapshot(self, after_seq: int, before_seq: int | None = None) -> Batch:
        """Read one validated archive window ``(after_seq, before_seq]``.

        Parameters
        ----------
        after_seq : int
            Exclusive lower sequence bound, normally the last durable cursor.
        before_seq : int, optional
            Inclusive upper bound. Defaults to ``after_seq + max_sequence_span``,
            capped at the maximum supported sequence. An explicit bound must be
            between ``after_seq`` and that cap.

        Returns
        -------
        Batch
            Fully validated, filtered events and a candidate ``through_seq``,
            capped at the sealed archive tip. An empty batch can advance the
            cursor when filters exclude every event in the covered window.

        Raises
        ------
        ValueError
            A sequence or the requested span is invalid.
        ProtocolError
            The response is invalid or exceeds a configured resource limit.
        CursorTooOld
            The requested archive history is no longer available.
        ArchiveChanged
            A planned segment generation is no longer available.
        RefreshTimeout
            The archive read exceeds its time budget.
        JetstreamError
            Access is denied or transport retries are exhausted.

        Notes
        -----
        Persist ``through_seq`` only after handling all returned events durably.
        A cursor already at or beyond the sealed tip is returned unchanged.
        """
        return self._reader.read_batch(after_seq, before_seq)

    def live(
        self,
        cursor: int | None = None,
        *,
        stop: StopEvent | None = None,
        timeout_seconds: float | None = None,
    ) -> Iterator[Event]:
        """Follow v2 from a saved cursor, or the current tip when omitted.

        Parameters
        ----------
        cursor : int, optional
            Exclusive lower sequence bound. Omit it to start at the current tip.
            Must be nonnegative and below ``10**15``; larger wire values denote
            timestamps rather than sequence cursors.
        stop : threading.Event, optional
            Set to stop iteration. Checked during idle receives and retry waits.
        timeout_seconds : float, optional
            Positive, finite lifetime starting when iteration begins, including
            reconnects. Omit for no overall stream deadline.

        Returns
        -------
        Iterator[Event]
            Lazily connected stream of events with increasing sequences matching
            the configured subscription. Reconnects suppress already yielded events.

        Raises
        ------
        ValueError
            The cursor, cancellation event, or timeout is invalid.
        CursorTooOld
            The server rejects a cursor outside its live lookback.
        StreamTimeout
            The overall deadline expires during iteration.
        LiveStreamError
            The server sends a terminal error frame.
        ProtocolError
            An event is malformed or exceeds ``max_batch_bytes``.
        JetstreamError
            Connection access is denied or retries are exhausted.

        Notes
        -----
        Save each event's sequence only after making that event durable. Close
        the iterator when breaking out of a consumer loop to release the socket.
        """
        if cursor is not None:
            sequence(cursor, "cursor")
            if cursor >= 10**15:
                raise ValueError(
                    "live cursor must be below 10**15; higher values are wire timestamps"
                )
        validate_stream_options(stop, timeout_seconds)
        return iter_live(self.config, cursor, stop=stop, timeout_seconds=timeout_seconds)

    def replay(self, after_seq: int, *, stop: StopEvent | None = None) -> Iterator[Batch]:
        """Backfill to a fixed sealed tip, then follow live with sequence deduplication.

        Parameters
        ----------
        after_seq : int
            Exclusive lower bound, normally the caller's last durable cursor.
        stop : threading.Event, optional
            Set to end iteration. Checked between archive batches and during
            live receives and retry waits.

        Yields
        ------
        Batch
            Bounded archive batches followed by single-event live batches.
            Each ``through_seq`` is a candidate cursor for caller persistence.

        Raises
        ------
        ValueError
            The starting sequence or cancellation event is invalid.
        CursorTooOld
            Required history is unavailable, or the archive cannot yet bridge
            a cursor rejected by the live endpoint.
        JetstreamError
            An archive or live read fails, including protocol, generation,
            timeout, and terminal stream errors from those readers.

        Notes
        -----
        Finish handling each batch and save its cursor after destination
        durability before requesting the next batch. An expired live lookback
        re-enters archive replay at the last yielded boundary. Each backfill
        cycle pins one sealed tip so new archive data cannot move its target.
        """
        sequence(after_seq, "after_seq")
        validate_stream_options(stop, None)
        cursor = after_seq
        recovering = False
        while stop is None or not stop.is_set():
            # Discover once per backfill cycle. Newly sealed segments after this boundary
            # belong to the server's cold replay at live cutover, not a moving client target.
            sealed_tip = self._reader.sealed_tip(cursor)
            if recovering and sealed_tip <= cursor:
                raise CursorTooOld("Sealed archive cannot yet bridge the expired live cursor")
            while cursor < sealed_tip:
                if stop is not None and stop.is_set():
                    return
                before = min(sealed_tip, cursor + self.config.max_sequence_span)
                batch = self.snapshot(cursor, before)
                if batch.through_seq != before:
                    raise ProtocolError("Pinned archive range changed during replay")
                yield batch
                cursor = batch.through_seq
            if stop is not None and stop.is_set():
                return
            try:
                with closing(self.live(cursor, stop=stop)) as stream:
                    for event in stream:
                        if event.seq > cursor:
                            yield Batch((event,), cursor, event.seq)
                            cursor = event.seq
                return
            except CursorTooOld:
                recovering = True
