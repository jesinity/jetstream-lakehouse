"""Validated Jetstream source settings, supplied explicitly by the caller."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from ipaddress import ip_address
from urllib.parse import urlsplit

KINDS = ("commit", "identity", "account", "sync")
MAX_SEQ = (1 << 63) - 1
_HOST_LABEL_RE = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?")
_DID_RE = re.compile(r"did:[a-z0-9]+:[A-Za-z0-9._:%-]+")
_COLLECTION_RE = re.compile(
    r"[A-Za-z][A-Za-z0-9-]*(?:\.[A-Za-z][A-Za-z0-9-]*)+\.(?:[A-Za-z][A-Za-z0-9]*|\*)"
)


def sequence(value: int, name: str) -> int:
    """Validate an application-supplied sequence cursor.

    Parameters
    ----------
    value : int
        Cursor between zero and ``2**63 - 1``, inclusive. Booleans are rejected.
    name : str
        Parameter name to include in a validation error.

    Returns
    -------
    int
        The unchanged cursor.

    Raises
    ------
    ValueError
        The value is not an integer in the supported range.
    """
    if type(value) is not int or not 0 <= value <= MAX_SEQ:
        raise ValueError(f"{name} must be an integer between 0 and {MAX_SEQ}")
    return value


def _integer(value: int, name: str, low: int, high: int) -> None:
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f"{name} must be an integer between {low} and {high}")


@dataclass(frozen=True)
class Config:
    """Configure an immutable Jetstream source with explicit application settings.

    Parameters
    ----------
    endpoint : str, optional
        HTTPS origin without a path or trailing slash. Defaults to
        ``https://jetstream.us-east.bsky.network``.
    api_key : str, optional
        Raw ASCII bearer token for archive requests; empty by default. Excluded
        from the configuration's repr. Public live connections do not use it.
    collections : tuple[str, ...], optional
        Up to 100 collection names or trailing ``.*`` patterns. Empty means
        all collections. Applies to commits only and requires the commit kind.
    kinds : tuple[str, ...], optional
        One to four event kinds, from ``commit``, ``identity``, ``account``, and
        ``sync``. Defaults to all four.
    dids : tuple[str, ...], optional
        Up to 10,000 decentralized identifiers. Empty means all DIDs.
    max_sequence_span : int, optional
        Maximum sequence span per snapshot, from 1 to 1,000,000; default 10,000.
        This bounds sequence distance, not the number of matching events.
    max_batch_bytes : int, optional
        Serialized archive-row budget and maximum live-message size, from
        1 KiB to 1 GiB; default 64 MiB. Not a total process-memory limit.
    request_timeout_seconds : int, optional
        Per-request timeout and live connection-opening limit, from 1 to 120
        seconds; default 20.
    refresh_timeout_seconds : int, optional
        Overall archive-read budget, from 1 to 900 seconds; default 120.
    max_retries : int, optional
        Retry count from 0 to 8; default 3. Applies per archive request and to
        consecutive live failures, resetting after a live event is yielded.
    include_raw_payload : bool, optional
        Include base64-encoded archive DAG-CBOR payloads; default False.
        Live events do not provide raw archive payloads.

    Raises
    ------
    ValueError
        A setting has an invalid type, value, or filter combination.

    Notes
    -----
    Construction validates settings and copies filter lists to tuples without
    network access. Environment loading and secret storage belong to the caller.
    """

    endpoint: str = "https://jetstream.us-east.bsky.network"
    api_key: str = field(default="", repr=False)
    collections: tuple[str, ...] = ()
    kinds: tuple[str, ...] = KINDS
    dids: tuple[str, ...] = ()
    max_sequence_span: int = 10000
    max_batch_bytes: int = 64 * 1024 * 1024
    request_timeout_seconds: int = 20
    refresh_timeout_seconds: int = 120
    max_retries: int = 3
    include_raw_payload: bool = False

    def __post_init__(self) -> None:
        # Copy application-provided lists so later mutation cannot alter a reader's filters.
        for name in ("collections", "kinds", "dids"):
            values = getattr(self, name)
            if not isinstance(values, (list, tuple)) or any(not isinstance(v, str) for v in values):
                raise ValueError(f"{name} must be a sequence of strings")
            object.__setattr__(self, name, tuple(values))
        self.validate()

    def validate(self) -> Config:
        """Check configuration values without performing network requests.

        Returns
        -------
        Config
            This same configuration instance, suitable for method chaining.

        Raises
        ------
        ValueError
            A setting violates its type, range, or compatibility constraints.
        """
        if not isinstance(self.endpoint, str) or any(
            ord(c) <= 32 or ord(c) == 127 for c in self.endpoint
        ):
            raise ValueError("endpoint must be an HTTPS origin")
        try:
            u = urlsplit(self.endpoint)
            port = u.port  # Accessing the property validates syntax and the port range.
            hostname = u.hostname
            if (
                u.scheme != "https"
                or not hostname
                or u.username is not None
                or u.password is not None
                or u.path
                or u.query
                or u.fragment
                or port == 0
            ):
                raise ValueError
            try:
                ip_address(hostname)
            except ValueError:
                host = hostname.encode("idna").decode("ascii")
                if len(host) > 253 or any(
                    not _HOST_LABEL_RE.fullmatch(label)
                    for label in host.split(".")
                ):
                    raise ValueError
        except (ValueError, UnicodeError):
            raise ValueError(
                "endpoint must be an HTTPS origin with a valid host and port"
            ) from None
        if not isinstance(self.api_key, str) or any(
            ord(c) <= 32 or ord(c) >= 127 for c in self.api_key
        ):
            raise ValueError("api_key must be a raw ASCII bearer token")
        _integer(self.max_sequence_span, "max_sequence_span", 1, 1_000_000)
        _integer(self.max_batch_bytes, "max_batch_bytes", 1024, 1024 * 1024 * 1024)
        _integer(self.request_timeout_seconds, "request_timeout_seconds", 1, 120)
        _integer(self.refresh_timeout_seconds, "refresh_timeout_seconds", 1, 900)
        _integer(self.max_retries, "max_retries", 0, 8)
        if type(self.include_raw_payload) is not bool:
            raise ValueError("include_raw_payload must be a boolean")
        if not 1 <= len(self.kinds) <= 4 or any(k not in KINDS for k in self.kinds):
            raise ValueError("kinds must contain 1–4 values from commit, identity, account, sync")
        if len(self.dids) > 10000 or any(
            not _DID_RE.fullmatch(d) for d in self.dids
        ):
            raise ValueError("dids must contain at most 10000 valid DIDs")
        if len(self.collections) > 100 or any(
            not _COLLECTION_RE.fullmatch(c) for c in self.collections
        ):
            raise ValueError("collections must contain at most 100 valid collection patterns")
        if self.collections and "commit" not in self.kinds:
            raise ValueError("collection filters require commit in kinds")
        return self

    def matches(self, event: Mapping[str, object]) -> bool:
        """Check a decoded event against the configured filters.

        Parameters
        ----------
        event : Mapping[str, object]
            Decoded row containing ``kind`` and ``did``, plus ``collection``
            when collection filtering applies to a commit.

        Returns
        -------
        bool
            Whether the kind, DID, and applicable collection filters all match.
            Collection filters do not exclude non-commit marker events.

        Raises
        ------
        KeyError
            A required event key is absent when evaluated.
        """
        if event["kind"] not in self.kinds or (self.dids and event["did"] not in self.dids):
            return False
        if event["kind"] != "commit" or not self.collections:
            return True
        collection = event.get("collection")
        return any(
            collection == c
            or (c.endswith(".*") and isinstance(collection, str) and collection.startswith(c[:-1]))
            for c in self.collections
        )
