import pytest

from jetstream_lakehouse import Client, Config


@pytest.mark.parametrize(
    "values",
    [
        {"max_retries": 1.5},
        {"max_retries": True},
        {"max_retries": -1},
        {"max_sequence_span": 1.2},
        {"max_sequence_span": False},
        {"request_timeout_seconds": 0},
        {"refresh_timeout_seconds": 901},
        {"include_raw_payload": "false"},
        {"max_batch_bytes": 1},
        {"endpoint": "https://example.com:invalid"},
        {"endpoint": "https://example.com:70000"},
        {"endpoint": "https://example.com:0"},
        {"endpoint": "http://example.com"},
        {"endpoint": "https://user:secret@example.com"},
        {"endpoint": "https://exa mple.com"},
        {"endpoint": "https://example.com/path"},
        {"endpoint": "https://example.com\n"},
        {"api_key": "secret\n"},
        {"kinds": ("unknown",)},
        {"kinds": ()},
        {"collections": ("app.bsky.feed.post",), "kinds": ("identity",)},
        {"collections": ("app.bsky.feed.post",) * 101},
        {"dids": ("did:plc:a",) * 10001},
        {"kinds": ("commit",) * 5},
        {"collections": "app.bsky.feed.post"},
    ],
)
def test_invalid_settings_rejected_at_construction(values):
    with pytest.raises(ValueError):
        Config(**values)


@pytest.mark.parametrize("cursor", [True, 1.5, -1, "1"])
def test_invalid_cursor_rejected_before_network(cursor):
    client = Client(Config())
    with pytest.raises(ValueError):
        client.snapshot(cursor)
    with pytest.raises(ValueError):
        client.snapshot(0, cursor)
    with pytest.raises(ValueError):
        client.live(cursor)
    with pytest.raises(ValueError):
        next(client.replay(cursor))


def test_wire_timestamp_cannot_be_mistaken_for_sequence():
    with pytest.raises(ValueError, match="timestamps"):
        Client(Config()).live(10**15)


def test_configuration_explicit_and_filters_immutable(monkeypatch):
    monkeypatch.setenv("JETSTREAM_ENDPOINT", "https://ignored.example")
    with pytest.raises(TypeError):
        Client()
    collections = ["app.bsky.feed.post"]
    config = Config(collections=collections, api_key="do-not-print-me")
    collections.clear()
    assert config.collections == ("app.bsky.feed.post",)
    assert config.endpoint == "https://jetstream.us-east.bsky.network"
    assert "do-not-print-me" not in repr(config)


def test_collection_filters_preserve_markers():
    config = Config(collections=("app.bsky.feed.post",))
    assert config.matches({"kind": "account", "did": "did:plc:a"})
    assert not config.matches(
        {"kind": "commit", "did": "did:plc:a", "collection": "app.bsky.feed.like"}
    )
