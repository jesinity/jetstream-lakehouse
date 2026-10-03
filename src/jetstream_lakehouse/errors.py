"""Sanitized, actionable failures: never include response bodies or credentials."""


class JetstreamError(RuntimeError):
    """Base exception for failed Jetstream archive or live reads.

    Notes
    -----
    A failed refresh must not commit its candidate offset.
    """


class ProtocolError(JetstreamError):
    """An unsupported or corrupt server response was encountered.

    Notes
    -----
    Raised when a response violates the supported archive or live protocol.
    """


class CursorTooOld(JetstreamError):
    """The requested archive range or live cursor is no longer available.

    Notes
    -----
    The client does not skip data or silently reset an expired cursor.
    """


class ArchiveChanged(JetstreamError):
    """The planned archive generation is unavailable for replay.

    Notes
    -----
    Retrying cannot safely replace the recorded plan with a new archive generation.
    """


class RefreshTimeout(JetstreamError):
    """The bounded request or refresh time budget was exhausted.

    Notes
    -----
    Retain the previous checkpoint and retry in a later refresh.
    """


class StreamTimeout(JetstreamError):
    """The caller's live-stream deadline expired; resume from the saved cursor."""


class LiveStreamError(JetstreamError):
    """Represent a v2 server error using a sanitized machine-readable name.

    Parameters
    ----------
    code : str
        Server error code. Recognized codes are ``ConsumerTooSlow``,
        ``CursorTooOld``, and ``UnknownZstdDictionary``; all others are replaced
        with ``UnknownError``.

    Attributes
    ----------
    code : str
        Sanitized error code. The live iterator retries ``ConsumerTooSlow``
        within its retry budget and propagates other error frames.
    """

    def __init__(self, code: str) -> None:
        self.code = (
            code
            if code in {"ConsumerTooSlow", "CursorTooOld", "UnknownZstdDictionary"}
            else "UnknownError"
        )
        super().__init__(f"Jetstream live stream returned {self.code}")
