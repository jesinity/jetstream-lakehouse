"""Stable, destination-neutral values returned by the Jetstream client."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any


def json_text(value: Any) -> str | None:
    """Serialize a value consistently across archive and live sources.

    Parameters
    ----------
    value : Any
        JSON-compatible value, or None for an absent payload.

    Returns
    -------
    str or None
        Compact JSON with sorted keys and unescaped Unicode, or None when the
        input is None.

    Raises
    ------
    TypeError
        The value contains an object unsupported by JSON serialization.
    ValueError
        The value contains non-finite numbers or a circular reference.
    """
    if value is None:
        return None
    return json.dumps(
        value, separators=(",", ":"), ensure_ascii=False, sort_keys=True, allow_nan=False
    )


@dataclass(frozen=True)
class Event:
    """Represent one immutable event with the same fields in both source modes.

    Attributes
    ----------
    seq : int
        Jetstream sequence number; a candidate cursor after durable handling.
    event_time : str
        Event timestamp normalized to an ISO 8601 UTC string.
    witnessed_at : str
        Jetstream observation timestamp as an ISO 8601 UTC string.
    did : str
        Decentralized identifier of the event's repository.
    kind : str
        One of ``commit``, ``identity``, ``account``, or ``sync``.
    operation : str or None
        ``create``, ``update``, or ``delete`` for commits; None for markers.
    collection : str or None
        Record collection for commits; otherwise None.
    rkey : str or None
        Record key for commits; otherwise None.
    cid : str or None
        Record content identifier when available; None for deletes and markers.
    rev : str or None
        Repository revision when supplied by the event.
    is_resync : bool
        Whether the event is marked as a resynchronization event.
    record : str or None
        Compact JSON record for create/update commits; otherwise None.
    event_payload : str or None
        Compact JSON record or marker payload, normally None for deletes.
    raw_payload_base64 : str or None
        Original archive DAG-CBOR payload encoded as base64 when requested.
        Always None for live events; defaults to None.

    Notes
    -----
    Client readers validate events before returning them. Direct construction
    stores supplied values without protocol validation.
    """

    seq: int
    event_time: str
    witnessed_at: str
    did: str
    kind: str
    operation: str | None
    collection: str | None
    rkey: str | None
    cid: str | None
    rev: str | None
    is_resync: bool
    record: str | None
    event_payload: str | None
    raw_payload_base64: str | None = None

    @classmethod
    def from_archive(cls, row: dict[str, Any]) -> Event:
        """Build an event from a decoded archive row without modifying that row.

        Parameters
        ----------
        row : dict[str, Any]
            Decoder-validated event fields. The optional ``raw_payload`` key
            contains base64 text and is renamed to ``raw_payload_base64``.

        Returns
        -------
        Event
            Event containing the supplied values, without additional decoding.

        Raises
        ------
        TypeError
            Required fields are missing or unexpected fields are present.
        """
        values = dict(row)
        values["raw_payload_base64"] = values.pop("raw_payload", None)
        return cls(**values)

    def as_dict(self) -> dict[str, Any]:
        """Copy event fields into a dictionary for downstream consumers.

        Returns
        -------
        dict[str, Any]
            A new dictionary containing every field. JSON payloads remain
            strings and absent values remain None.
        """
        return asdict(self)


@dataclass(frozen=True)
class Batch:
    """Represent a source window ``(after_seq, through_seq]`` and its events.

    Attributes
    ----------
    events : tuple[Event, ...]
        Matching events in increasing sequence order. May be empty even when
        the covered window advances, because filters can exclude all events.
    after_seq : int
        Exclusive lower bound of the covered window.
    through_seq : int
        Inclusive upper bound and candidate cursor for the next read.

    Notes
    -----
    Client-produced batches are fully validated. Direct construction does not
    validate the window or events. Persist ``through_seq`` only after all events
    are durable; producing a batch does not write to a destination or checkpoint.
    """

    events: tuple[Event, ...]
    after_seq: int
    through_seq: int
