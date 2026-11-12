"""Portable, validated archive plans that can be persisted between processes."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from .config import MAX_SEQ
from .errors import ProtocolError

FORMAT_VERSION = 1
MAX_PLAN_BYTES = 2 * 1024 * 1024
MAX_PAGES = 100
MAX_SEQUENCE_SPAN = 1_000_000
_FINGERPRINT_RE = re.compile(r"sha256-v1:[0-9a-f]{64}")
_SEGMENT_RE = re.compile(r"seg_[0-9a-z]+\.jss")
_CHECKSUM_RE = re.compile(r"[0-9a-f]{16}")


def scope_fingerprint(config: Any) -> str:
    """Return a deterministic hash of the normalized source scope."""
    from urllib.parse import urlsplit

    parsed = urlsplit(config.endpoint)
    hostname = parsed.hostname.encode("idna").decode("ascii").lower()
    if ":" in hostname:
        hostname = f"[{hostname}]"
    port = parsed.port
    endpoint = f"https://{hostname}" + (f":{port}" if port not in (None, 443) else "")
    scope = {
        "endpoint": endpoint,
        "kinds": sorted(set(config.kinds)),
        "collections": sorted(set(config.collections)),
        "dids": sorted(set(config.dids)),
        "includeRawPayload": config.include_raw_payload,
    }
    encoded = json.dumps(scope, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "sha256-v1:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _sequence(value: Any, name: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_SEQ:
        raise ProtocolError(f"Invalid saved archive plan {name}")
    return value


def _validate_segments(segments: Any) -> None:
    if type(segments) is not list:
        raise ProtocolError("Invalid saved archive plan segments")
    previous_index = -1
    for item in segments:
        if type(item) is not dict or item.get("mode") not in ("segment", "blocks"):
            raise ProtocolError("Invalid saved archive plan segment")
        expected = {"name", "index", "checksum", "minSeq", "maxSeq", "mode"}
        if item["mode"] == "blocks":
            expected.add("blocks")
        if set(item) != expected:
            raise ProtocolError("Invalid saved archive plan segment fields")
        if type(item["name"]) is not str or not _SEGMENT_RE.fullmatch(item["name"]):
            raise ProtocolError("Invalid saved archive plan segment name")
        if type(item["checksum"]) is not str or not _CHECKSUM_RE.fullmatch(item["checksum"]):
            raise ProtocolError("Invalid saved archive plan checksum")
        index = _sequence(item["index"], "segment index")
        low = _sequence(item["minSeq"], "segment minSeq")
        high = _sequence(item["maxSeq"], "segment maxSeq")
        if index <= previous_index or low > high:
            raise ProtocolError("Invalid saved archive plan segment bounds")
        previous_index = index
        if item["mode"] == "blocks":
            blocks = item["blocks"]
            if type(blocks) is not list or not blocks:
                raise ProtocolError("Invalid saved archive plan block ranges")
            previous_last = -1
            for block in blocks:
                if type(block) is not dict or set(block) != {"first", "last"}:
                    raise ProtocolError("Invalid saved archive plan block range")
                first = _sequence(block["first"], "first block")
                last = _sequence(block["last"], "last block")
                if first <= previous_last or first > last:
                    raise ProtocolError("Invalid saved archive plan block range")
                previous_last = last


def _validate_payload(payload: Any) -> tuple[int, int, str, str]:
    if type(payload) is not dict or set(payload) != {
        "formatVersion", "scopeFingerprint", "afterSeq", "throughSeq", "pages"
    }:
        raise ProtocolError("Invalid saved archive plan fields")
    if type(payload["formatVersion"]) is not int or payload["formatVersion"] != FORMAT_VERSION:
        raise ProtocolError("Unsupported archive plan format version")
    fingerprint = payload["scopeFingerprint"]
    if type(fingerprint) is not str or not _FINGERPRINT_RE.fullmatch(fingerprint):
        raise ProtocolError("Invalid saved archive plan scope fingerprint")
    after = _sequence(payload["afterSeq"], "afterSeq")
    through = _sequence(payload["throughSeq"], "throughSeq")
    if through < after or through - after > MAX_SEQUENCE_SPAN:
        raise ProtocolError("Invalid saved archive plan sequence bounds")
    pages = payload["pages"]
    if type(pages) is not list or len(pages) > MAX_PAGES:
        raise ProtocolError("Invalid saved archive plan pages")
    if after == through:
        if pages:
            raise ProtocolError("Empty saved archive plan cannot contain pages")
    else:
        if not pages:
            raise ProtocolError("Saved archive plan does not cover its window")
        cursor = after
        generations: dict[str, str] = {}
        for page in pages:
            if type(page) is not dict or set(page) != {"afterSeq", "throughSeq", "segments"}:
                raise ProtocolError("Invalid saved archive plan page")
            page_after = _sequence(page["afterSeq"], "page afterSeq")
            page_through = _sequence(page["throughSeq"], "page throughSeq")
            if page_after != cursor or page_through <= page_after or page_through > through:
                raise ProtocolError("Saved archive plan pages have gaps or overlaps")
            _validate_segments(page["segments"])
            for segment in page["segments"]:
                old = generations.setdefault(segment["name"], segment["checksum"])
                if old != segment["checksum"]:
                    raise ProtocolError("Saved archive plan mixes segment generations")
            cursor = page_through
        if cursor != through:
            raise ProtocolError("Saved archive plan does not cover its window")
    return after, through, fingerprint, _canonical(pages)


class ArchivePlan:
    """A deeply immutable, source-scoped description of an archive window.

    Construct plans with :meth:`Client.plan_snapshot` or restore them with
    :meth:`from_json`; the serialized format version is 1.
    """

    __slots__ = ("_after_seq", "_locked", "_pages_json", "_scope_fingerprint", "_through_seq")

    def __init__(self, *_: Any, **__: Any) -> None:
        raise TypeError("ArchivePlan values are created by Client.plan_snapshot or from_json")

    @classmethod
    def _create(cls, after: int, through: int, fingerprint: str, pages: list[dict[str, Any]]) -> ArchivePlan:
        payload = {
            "formatVersion": FORMAT_VERSION,
            "scopeFingerprint": fingerprint,
            "afterSeq": after,
            "throughSeq": through,
            "pages": pages,
        }
        checked_after, checked_through, checked_fingerprint, pages_json = _validate_payload(payload)
        instance = object.__new__(cls)
        object.__setattr__(instance, "_after_seq", checked_after)
        object.__setattr__(instance, "_through_seq", checked_through)
        object.__setattr__(instance, "_scope_fingerprint", checked_fingerprint)
        object.__setattr__(instance, "_pages_json", pages_json)
        object.__setattr__(instance, "_locked", True)
        return instance

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_locked", False):
            raise AttributeError("ArchivePlan is immutable")
        object.__setattr__(self, name, value)

    @property
    def after_seq(self) -> int:
        """Exclusive starting sequence of the planned window."""
        return self._after_seq

    @property
    def through_seq(self) -> int:
        """Inclusive ending sequence of the planned window."""
        return self._through_seq

    @property
    def format_version(self) -> int:
        """Serialization format version, currently 1."""
        return FORMAT_VERSION

    def to_json(self) -> str:
        """Serialize this plan as deterministic UTF-8 JSON text."""
        payload = {
            "formatVersion": FORMAT_VERSION,
            "scopeFingerprint": self._scope_fingerprint,
            "afterSeq": self._after_seq,
            "throughSeq": self._through_seq,
            "pages": json.loads(self._pages_json),
        }
        value = _canonical(payload)
        try:
            size = len(value.encode("utf-8"))
        except UnicodeEncodeError:
            raise ProtocolError("Saved archive plan is not valid UTF-8 text") from None
        if size > MAX_PLAN_BYTES:
            raise ProtocolError("Saved archive plan exceeds size limit")
        return value

    @classmethod
    def from_json(cls, value: str) -> ArchivePlan:
        """Restore and fully validate a version 1 plan before any network use.

        Parameters
        ----------
        value : str
            JSON text previously returned by :meth:`to_json`.

        Raises
        ------
        ProtocolError
            The JSON is oversized, malformed, or contains unsupported data.
        """
        if type(value) is not str:
            raise ProtocolError("Saved archive plan must be JSON text")
        try:
            size = len(value.encode("utf-8"))
        except UnicodeEncodeError:
            raise ProtocolError("Saved archive plan is not valid UTF-8 text") from None
        if size > MAX_PLAN_BYTES:
            raise ProtocolError("Saved archive plan exceeds size limit")
        try:
            payload = json.loads(value, object_pairs_hook=_unique_object)
        except (ValueError, RecursionError):
            raise ProtocolError("Invalid saved archive plan JSON") from None
        after, through, fingerprint, pages_json = _validate_payload(payload)
        instance = object.__new__(cls)
        object.__setattr__(instance, "_after_seq", after)
        object.__setattr__(instance, "_through_seq", through)
        object.__setattr__(instance, "_scope_fingerprint", fingerprint)
        object.__setattr__(instance, "_pages_json", pages_json)
        object.__setattr__(instance, "_locked", True)
        return instance

    def _pages(self) -> list[dict[str, Any]]:
        """Return a fresh internal copy of the immutable page descriptors."""
        return json.loads(self._pages_json)
