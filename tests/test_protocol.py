from __future__ import annotations

import pytest
from conftest import response

from jetstream_lakehouse.archive import decode_payload, segment_events
from jetstream_lakehouse.config import Config
from jetstream_lakehouse.errors import ArchiveChanged, ProtocolError, RefreshTimeout
from jetstream_lakehouse.reader import _transport_config
from jetstream_lakehouse.transport import Transport


def test_exact_ranges_pin_etag_and_content_range(service):
    with Transport(_transport_config(Config())) as transport:
        rows = list(
            segment_events(
                transport,
                {"name": "seg_0000000000.jss", "checksum": service.checksum, "mode": "segment"},
                0,
                service.tip,
                False,
            )
        )
    assert rows
    ranges = [call for call in service.calls if "Range" in call.headers]
    assert (
        len(ranges) >= 3
    )  # header, footer, plus frames intersecting the requested sequence window
    assert all(call.headers["If-Match"] == f'"{service.checksum}"' for call in ranges)


def test_stale_archive_generation_keeps_checkpoint_candidate_unavailable(service):
    service.failures = [response(412)]
    with Transport(_transport_config(Config())) as transport, pytest.raises(ArchiveChanged):
        transport.range("seg_0000000000.jss", service.checksum, 0, 255)


def test_deadline_fails_before_request(service):
    with Transport(_transport_config(Config())) as transport:
        transport.deadline = 0
        with pytest.raises(RefreshTimeout):
            transport.plan(0, 1)
    assert service.calls == []


@pytest.mark.parametrize("payload", [b"", b"\xff", b"not-cbor"])
def test_malformed_payload_is_rejected(payload):
    with pytest.raises(ProtocolError):
        decode_payload(payload)


def test_selected_block_ranges_do_not_fetch_whole_segment(service):
    entry = {
        "name": "seg_0000000000.jss",
        "checksum": service.checksum,
        "mode": "blocks",
        "blocks": [{"first": 1, "last": 1}],
    }
    with Transport(_transport_config(Config())) as transport:
        rows = list(segment_events(transport, entry, 0, service.tip, False))
    assert rows
    assert len(service.calls) == 3
    assert service.calls[-1].headers["Range"].startswith("bytes=")


def test_expired_archive_cursor_is_not_silently_reset(service):
    from jetstream_lakehouse.errors import CursorTooOld

    service.failures = [response(400, b'{"error":"CursorTooOld"}')]
    with Transport(_transport_config(Config())) as transport, pytest.raises(CursorTooOld):
        transport.plan(1, 2)
