# Jetstream Lakehouse

`jetstream-lakehouse` is a Python 3.10+ destination-neutral client for Jetstream v2 archive snapshots and its canonical live WebSocket. It contains no sink, checkpoint store, Spark, Databricks, Kafka, or Snowflake code. Downstream consumers own delivery and durable cursor storage.

The archive decoder is adapted from the Apache-2.0 licensed Databricks Community Connector Bluesky source. See [NOTICE](NOTICE) for attribution. It verifies archive generations, exact byte ranges, xxh3 metadata checksums, Zstandard checksums and sizes, CBOR structure, and sequence ordering. Exact collection/DID/kind matching happens after decoding.

## Install

After the first PyPI release, install with:

```sh
pip install jetstream-lakehouse
```

For development from a checkout, use `uv` for the environment, lock, and build:

```sh
uv sync --locked
uv build
```

See [RELEASING.md](RELEASING.md) for the TestPyPI and PyPI release process.

This package is a library and does not load configuration from environment variables or provide a command-line entry point. Pass settings to `Config` directly. Keep credentials in your application's secret store; `api_key` is optional and applies only to Jetstream archive requests.

## Public API

```python
from jetstream_lakehouse import Client, Config
from contextlib import closing

config = Config(
    endpoint="https://jetstream.us-east.bsky.network",
    api_key="",  # optionally pass an archive API key from your application's secret store
    collections=("app.bsky.feed.post",),
    kinds=("commit", "identity", "account", "sync"),
    max_sequence_span=10_000,
)
client = Client(config)

# One bounded sealed-archive range: events in (after_seq, through_seq].
batch = client.snapshot(after_seq=123456)
for event in batch.events:
    process(event)
print(batch.through_seq)  # candidate cursor, including valid zero-event windows

# Explicit cursor resumes history; omit cursor only to follow from the current live tip.
with closing(client.live(cursor=123456)) as stream:
    for event in stream:
        process(event)

# Backfill bounded archive batches, then continue as one-event live batches.
with closing(client.replay(after_seq=123456)) as stream:
    for batch in stream:
        write_to_sink(batch.events)
        await_durability()
        save_cursor(batch.through_seq)
```

`Event` is immutable and uses the same fields for archive and live input: `seq`, `event_time`, `witnessed_at`, `did`, `kind`, `operation`, `collection`, `rkey`, `cid`, `rev`, `is_resync`, `record`, `event_payload`, and optional `raw_payload_base64`. JSON values are compact JSON strings. Missing values are `None`. Raw archive CBOR is base64 text and is not available on live events. Create, update, delete, identity, account, sync, and resync markers remain individual events; this client does not fold them into current-record state.

`event_payload` contains the record for creates/updates, the original upstream marker object for identity/account/sync, and `None` for deletes. It has the same shape in both modes. JSON keys are sorted and timestamps are normalized to UTC. A sync's `rev` is extracted from its marker object.

Archive ranges are exclusive/inclusive: `(after_seq, through_seq]`. A returned `through_seq` means the complete range was validated, including when filters matched no events. It is only a candidate cursor; the caller persists it after its own destination confirms durability. If a write fails, resume from the caller's last saved cursor. This provides at-least-once delivery to the caller, not exactly-once persistence in an external system.

`snapshot` requires an explicit `after_seq` and reads at most `max_sequence_span`. A window can start and end inside a physical segment. `replay` pins one sealed tip for the whole backfill, then connects live at that boundary. Segments sealed during backfill arrive through the server's cold replay. If live lookback expires, replay discovers a new archive boundary from its last yielded cursor; if the archive cannot bridge the gap yet, it raises `CursorTooOld` without skipping ahead.

`live(cursor=None)` starts at the current live tip. Numeric cursors are sequences; live values at or above `10**15` are rejected because the server would interpret them as timestamps. Transient connection errors, timeouts, HTTP 429/5xx, and `ConsumerTooSlow` reconnect from the last yielded sequence, suppressing repeats. `max_retries` bounds consecutive failures and resets after forward event progress. Restart after a destination failure with the application's last saved cursor.

Live uses one buffered WebSocket frame and a `max_batch_bytes` message limit. Calls yield one event at a time to apply backpressure. Use `closing(...)` when a loop may exit early. Pass a `threading.Event` as `stop` to interrupt idle reads and retry waits; replay checks it between archive batches too. An active connection attempt is bounded by `request_timeout_seconds`. `client.live(timeout_seconds=20)` imposes a total deadline across connections, idle reads, and backoff, raising `StreamTimeout` when it expires; socket shutdown can take up to one additional second.

## Filters and limits

Set `collections`, `dids`, and `kinds` on `Config`. Kinds default to all supported kinds. Collection filters constrain commits; DID-level marker events are retained unless kinds or DID filters exclude them. Providing collections while excluding `commit` is rejected, as required by the upstream API. Limits are 4 kind entries, 100 collection patterns, and 10,000 DIDs. Archive planning may over-select blocks, so filters are applied exactly after decoding. Live uses the canonical v2 repeated filter parameters.

`Config` also exposes `max_sequence_span`, `max_batch_bytes`, `request_timeout_seconds`, `refresh_timeout_seconds`, `max_retries`, and `include_raw_payload`. Invalid options and non-HTTPS origins are rejected before network calls. HTTP requests disable ambient netrc credentials, refuse redirects, cap response sizes, and pin selected bytes to ETags. Archive corruption, changed generations, malformed plans, expired cursors, and exhausted deadlines raise typed exceptions from `jetstream_lakehouse.errors`; no successful candidate cursor is returned for an incomplete archive window. The live stream uses `/xrpc/network.bsky.jetstream.subscribeEvents` with the `xrpc.v1.json` subprotocol, not the legacy endpoint.

Catch `ProtocolError` for invalid data or size limits, `ArchiveChanged` for a changed segment generation, and `CursorTooOld` for unavailable replay history. `RefreshTimeout` covers archive work; `StreamTimeout` covers an optional live deadline. `LiveStreamError.code` identifies terminal v2 errors without exposing server messages. These all inherit from `JetstreamError`, which also covers denied access and exhausted retries. Live connections disable automatic environment proxy discovery.

## Tests and downstream use

`uv run ruff check .`, `uv run pytest`, and `uv build` run without service credentials (installing/building dependencies may require PyPI access). The ordinary test suite performs no external network requests. See [tests/FIXTURES.md](tests/FIXTURES.md) for the independent Go-writer fixture's provenance. Synthetic events cover all supported kinds, including deletes and resyncs; malformed archives and HTTP ranges exercise checksum and generation validation.

The public live contract test reads three events with a 20-second deadline and requires no key or cursor:

```sh
JETSTREAM_INTEGRATION_LIVE=1 uv run pytest tests/integration -k live
```

The archive contract test requires a cursor inside sealed history and a key where the endpoint requires authentication. Put the raw token in a local file with mode `0600`, for example `.secrets/jetstream_api_key`; `.secrets/` is ignored by Git. Pass only the path on the command line:

```sh
JETSTREAM_API_KEY_FILE="$PWD/.secrets/jetstream_api_key" \
JETSTREAM_INTEGRATION_CURSOR=123456 \
uv run pytest tests/integration -k archive
```

The archive test caps the sequence span at 1,000 by default and the read deadline at 30 seconds. It requires actual events, so an empty or future range cannot pass as a verified archive read. `JETSTREAM_ENDPOINT` overrides the test endpoint; `JETSTREAM_INTEGRATION_LIVE_CURSOR` optionally tests explicit live resume. These variables belong only to the test harness; the library continues to require explicit `Config` objects. Both network tests are skipped in ordinary CI.

For further content diversity, public post datasets such as [two-million-bluesky-posts](https://huggingface.co/datasets/alpindale/two-million-bluesky-posts) can supply record JSON. They don't contain Jetstream's archive framing, generation checks, full marker stream, or cursor semantics, so use the native fixture and protocol tests for those guarantees. The [public Jetstream service](https://bsky.network/docs/jetstream-sdk/) provides real v2 frames for the live contract check.

A Lakeflow adapter or a Zerobus producer can consume the public models and own its own delivery contract and checkpoint. Those integrations belong in their downstream repositories/packages; this library does not import or depend on either.

Protocol reference: [Jetstream v2 documentation](https://github.com/bluesky-social/jetstream/blob/main/docs/README.md#52-the-v2-stream-networkbskyjetstreamsubscribeevents).
