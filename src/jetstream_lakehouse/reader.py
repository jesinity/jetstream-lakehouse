"""Validate a complete bounded plan and materialize one filtered source batch."""

from __future__ import annotations

import json
import re
from typing import Any

from .archive import segment_events
from .config import MAX_SEQ, Config, sequence
from .errors import ProtocolError
from .models import Batch, Event
from .transport import Transport

MAX_PLAN_BYTES = 2 * 1024 * 1024
MAX_PAGES = 100
_SEGMENT_NAME_RE = re.compile(r"seg_[0-9a-z]+\.jss")
_CHECKSUM_RE = re.compile(r"[0-9a-f]{16}")


class ArchiveValidator:
    """Validate archive planner metadata before any segment bytes are read."""

    @staticmethod
    def sequence(value: Any, name: str) -> int:
        """Validate a sequence-like integer received from the archive planner.

        Parameters
        ----------
        value : Any
            Expected integer in ``[0, 2**63 - 1]``; booleans are rejected.
        name : str
            Field name used in the sanitized error message.

        Returns
        -------
        int
            The unchanged value.

        Raises
        ------
        ProtocolError
            The server value has an invalid type or range.
        """
        if type(value) is not int or not 0 <= value <= MAX_SEQ:
            raise ProtocolError(f"Invalid {name}")
        return value

    @classmethod
    def validate_segments(cls, segments: Any) -> None:
        """Check the structure and ordering of planned segment descriptors.

        Parameters
        ----------
        segments : Any
            Expected list of segment or block descriptors with names,
            checksums, sequence bounds, and ordered segment/block indices.

        Raises
        ------
        ProtocolError
            A descriptor is malformed, unsupported, or incorrectly ordered.

        Notes
        -----
        No archive bytes are fetched. Physical segment bounds may extend
        beyond the requested snapshot window; decoding clips that window.
        """
        if not isinstance(segments, list):
            raise ProtocolError("Invalid snapshot segments")
        last_index = -1
        for item in segments:
            if not isinstance(item, dict) or item.get("mode") not in {"segment", "blocks"}:
                raise ProtocolError("Unsupported snapshot segment descriptor")
            if not _SEGMENT_NAME_RE.fullmatch(str(item.get("name", ""))):
                raise ProtocolError("Invalid snapshot segment name")
            if not _CHECKSUM_RE.fullmatch(str(item.get("checksum", ""))):
                raise ProtocolError("Invalid snapshot segment checksum")
            index = cls.sequence(item.get("index"), "segment index")
            low = cls.sequence(item.get("minSeq"), "minSeq")
            high = cls.sequence(item.get("maxSeq"), "maxSeq")
            if index <= last_index or low > high:
                raise ProtocolError("Invalid or unordered snapshot segment bounds")
            last_index = index
            if item["mode"] == "blocks":
                ranges = item.get("blocks")
                if not isinstance(ranges, list) or not ranges:
                    raise ProtocolError("Missing planned block ranges")
                last = -1
                for block in ranges:
                    if not isinstance(block, dict):
                        raise ProtocolError("Invalid planned block range")
                    first = cls.sequence(block.get("first"), "first block")
                    end = cls.sequence(block.get("last"), "last block")
                    if first <= last or first > end:
                        raise ProtocolError("Overlapping or invalid planned block ranges")
                    last = end


class Reader:
    """Plan and validate bounded archive reads.

    Parameters
    ----------
    config : Config
        Source settings, validated at construction without network access.
    """

    def __init__(self, config: Config) -> None:
        self.config = config.validate()

    def sealed_tip(self, after: int) -> int:
        """Discover a fixed backfill boundary without advancing the caller's cursor.

        Parameters
        ----------
        after : int
            Exclusive lower sequence bound supplied to the snapshot planner.

        Returns
        -------
        int
            Sealed archive tip reported by the planner, possibly below ``after``.
            No event rows are downloaded.

        Raises
        ------
        ValueError
            The starting cursor is invalid.
        ProtocolError
            Planner metadata or progress is inconsistent.
        JetstreamError
            Planning fails, including cursor expiry or a refresh timeout.
        """
        sequence(after, "after_seq")
        with Transport(_transport_config(self.config)) as transport:
            page = transport.plan(after)
            tip = ArchiveValidator.sequence(page.get("sealedTipSeq"), "sealedTipSeq")
            through = ArchiveValidator.sequence(page.get("plannedThroughSeq"), "plannedThroughSeq")
            ArchiveValidator.validate_segments(page.get("segments"))
            if through > tip or (tip > after >= through):
                raise ProtocolError("Invalid archive discovery progress")
            transport.remaining()
            return tip

    def read_batch(self, after: int, through: int | None = None) -> Batch:
        """Materialize a filtered archive window after validating its full plan.

        Parameters
        ----------
        after : int
            Exclusive lower sequence bound.
        through : int, optional
            Inclusive upper bound, no more than ``max_sequence_span`` beyond
            ``after``. Defaults to that span, capped at the maximum sequence.

        Returns
        -------
        Batch
            Ordered matching events and a candidate cursor capped at the sealed
            tip. A cursor beyond the tip remains unchanged.

        Raises
        ------
        ValueError
            Input bounds are invalid or exceed the configured span.
        ProtocolError
            Planning, decoding, ordering, or a resource-limit check fails.
        JetstreamError
            A request fails, including cursor expiry, changed archive generation,
            or an exhausted refresh budget.

        Notes
        -----
        No partial batch is returned on failure. An empty filtered batch may
        still advance the candidate cursor; the caller owns persistence.
        """
        sequence(after, "after_seq")
        if through is None:
            through = min(MAX_SEQ, after + self.config.max_sequence_span)
        sequence(through, "before_seq")
        if not after <= through <= min(MAX_SEQ, after + self.config.max_sequence_span):
            raise ValueError("through cursor exceeds configured sequence span")
        if through == after:
            return Batch((), after, after)
        pages: list[dict[str, Any]] = []
        covered = after
        pinned_tip: int | None = None
        cycle_end: int | None = None
        segment_generations: dict[str, str] = {}
        with Transport(_transport_config(self.config)) as transport:
            for _ in range(MAX_PAGES):
                page = transport.plan(covered, cycle_end if cycle_end is not None else through)
                tip = ArchiveValidator.sequence(page.get("sealedTipSeq"), "sealedTipSeq")
                page_through = ArchiveValidator.sequence(
                    page.get("plannedThroughSeq"), "plannedThroughSeq"
                )
                if tip > through:
                    raise ProtocolError("Snapshot planner exceeded before_seq")
                ArchiveValidator.validate_segments(page.get("segments"))
                if pinned_tip is None:
                    pinned_tip = tip
                    if tip < after:
                        # A cursor at or beyond today's sealed tip is ready for the live stream.
                        if page_through != tip or page["segments"]:
                            raise ProtocolError("Snapshot planner returned inconsistent bounds")
                        return Batch((), after, after)
                    cycle_end = min(through, tip)
                if tip != pinned_tip or page_through < covered or page_through > cycle_end:
                    raise ProtocolError("Snapshot planner returned inconsistent bounds")
                for entry in page["segments"]:
                    # Physical segments can extend beyond beforeSeq; selected rows are
                    # clipped to the validated page window during decoding.
                    previous_checksum = segment_generations.setdefault(
                        entry["name"], entry["checksum"]
                    )
                    if previous_checksum != entry["checksum"]:
                        raise ProtocolError("Snapshot plan mixed segment generations")
                pages.append(
                    {"after": covered, "through": page_through, "segments": page["segments"]}
                )
                if len(json.dumps(pages, separators=(",", ":"))) > MAX_PLAN_BYTES:
                    raise ProtocolError(
                        "Snapshot plan exceeds memory limit; reduce max_sequence_span"
                    )
                if page_through == cycle_end:
                    break
                if page_through == covered:
                    raise ProtocolError("Snapshot planner made no progress")
                covered = page_through
            else:
                raise ProtocolError("Snapshot planning exceeded page limit")
            assert pinned_tip is not None and cycle_end is not None
            candidate = cycle_end
            if candidate < after or pages[-1]["through"] != candidate:
                raise ProtocolError("Snapshot plan does not cover requested batch")
            rows: list[Event] = []
            size = 0
            previous = after
            for page in pages:
                for entry in page["segments"]:
                    for row in segment_events(
                        transport,
                        entry,
                        page["after"],
                        page["through"],
                        self.config.include_raw_payload,
                    ):
                        transport.remaining()
                        if row["seq"] <= previous:
                            raise ProtocolError("Overlapping or out-of-order archive events")
                        previous = row["seq"]
                        if self.config.matches(row):
                            event = Event.from_archive(row)
                            size += len(json.dumps(row, separators=(",", ":")).encode())
                            if size > self.config.max_batch_bytes:
                                raise ProtocolError(
                                    "Batch exceeds memory budget; reduce max_sequence_span"
                                )
                            rows.append(event)
            transport.remaining()
        return Batch(tuple(rows), after, candidate)


def _transport_config(config: Config) -> Any:
    """Small protocol adapter keeps transport independent of public config naming."""
    from types import SimpleNamespace

    return SimpleNamespace(
        endpoint=config.endpoint,
        api_key=config.api_key,
        collections=config.collections,
        kinds=config.kinds,
        dids=config.dids,
        max_events_per_refresh=config.max_sequence_span,
        request_timeout_seconds=config.request_timeout_seconds,
        refresh_timeout_seconds=config.refresh_timeout_seconds,
        max_retries=config.max_retries,
    )
