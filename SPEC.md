# Jetstream Lakehouse: implementation specification

Status: implementation brief for Luna; no application code has been written in this repository.

## Goal and scope

Build an open-source Python package, `jetstream-lakehouse`, whose main product is a **destination-independent Jetstream v2 reader**. It must read bounded, pinned archive snapshots and continue into the canonical v2 live WebSocket stream, yielding one consistent event model. Kafka and Databricks Zerobus are separate sink adapters; neither belongs in the reader's imports or public event model. This is not a copy of the Lakeflow Connect interface. Use `uv` for environment management, locking, running checks, and building. Target Python 3.10+.

The first release has `run-once` for a bounded sealed snapshot and `follow` for snapshot-to-live replay. A caller may also start at the current live tip explicitly. The default must never silently discard history. Publish to an ordinary Kafka broker/topic directly through a producer app, or to Zerobus through its official SDK; Jetstream itself does **not** speak the Kafka protocol. Snowflake/Snowpipe Streaming is a later sink adapter.

## Authoritative references

- Jetstream v2 archive protocol and planner: https://github.com/bluesky-social/jetstream/blob/main/docs/README.md
- Existing verified Python implementation: `../lakeflow-connect-bluesky/src/databricks/labs/community_connector/sources/bluesky/` (especially `archive.py`, `transport.py`, `options.py`, `bluesky.py`, `errors.py`). Reuse behavior and tests, but remove Databricks Lakeflow/PySpark coupling. Preserve Apache-2.0 attribution and the existing `NOTICE` when adapting code.
- Existing independent archive fixture and tests: `../lakeflow-connect-bluesky/tests/unit/sources/bluesky/`; `fixtures/native.jss` was produced by the official Go segment writer and is more valuable than a simulator-only test.
- Zerobus setup and Python SDK: https://docs.databricks.com/aws/en/ingestion/zerobus-ingest
- Zerobus durability acknowledgments: https://docs.databricks.com/aws/en/ingestion/zerobus-message-blocking
- Canonical Jetstream v2 WebSocket framing/filtering/cutover: https://github.com/bluesky-social/jetstream/blob/main/docs/README.md#52-the-v2-stream-networkbskyjetstreamsubscribeevents
- Zerobus Kafka-compatible producer API (Beta; distinct from an ordinary Kafka broker): https://docs.databricks.com/aws/en/ingestion/zerobus-kafka

Read the current vendor documentation and installed SDK signatures before implementation, because these APIs can change.

## Package boundary

Suggested modules (names may change if the public API remains clear):

| Module | Responsibility |
| --- | --- |
| `config.py` | Validate Jetstream settings, sink-specific settings, secrets, and a stable source-scope fingerprint. |
| `transport.py` | Authenticated `planSnapshot` and generation-pinned, exact HTTP ranges with bounded retries and deadlines. |
| `archive.py` | Strict `jss0`/v1 metadata and block decoder; no sink imports. |
| `reader.py` | Plan one `(after, through]` window, validate all pages, read/filter selected events in sequence order, and yield a bounded batch with its candidate end cursor. |
| `live.py` | Canonical v2 WebSocket event decoding, reconnect/replay, and archive-to-live cutover behind the same event model. |
| `checkpoint.py` | Persist the last **durably delivered** source cursor and source/destination scope atomically. |
| `sinks/base.py` | Small sink protocol: `write_batch(rows)` returns successfully only after all rows are durable. |
| `sinks/kafka.py` | Ordinary Kafka producer adapter with acks, delivery-error handling, configurable topic and key, and JSON event mapping. |
| `sinks/zerobus.py` | Official Zerobus SDK adapter and JSON row mapping. Import this optional dependency only when selected. |
| `runner.py`, `cli.py` | Orchestrate source batches, destination acknowledgment, checkpoint commit, `run-once`, and live `follow`. |

Keep core imports usable without PySpark, a Kafka client, or the Zerobus SDK. Make sink dependencies optional extras. Do not depend on the Databricks community connector package at runtime. Do not make the `atproto` Python SDK the archive decoder: the current connector has stricter generation/checksum checks and handles resync kind 7. If code is copied from the connector, update imports and tests deliberately rather than broad refactoring.

## Source contract

1. Require an explicit starting cursor when there is no checkpoint. Cursor 0 means the whole archive; never silently start there or silently jump to the current tip. The cursor is exclusive. Cap each cycle by a configured maximum **sequence span** and by a byte/time budget; a filtered batch can have fewer rows than that span.
2. Support optional `collections`, `dids`, and `kinds` (`commit`, `identity`, `account`, `sync`). Default to all kinds. A collection filter constrains commits while DID-level markers can still be returned when kinds are not narrowed. Apply exact filters **after decoding**, because planner block selection may have false positives. Keep create/update/delete, identity, account, sync, and resync markers as separate source events; do not silently fold them away.
3. Plan with `network.bsky.jetstream.planSnapshot`, pin the `sealedTipSeq` and each segment's checksum, paginate until the covered cursor reaches the chosen tip, and reject missing progress, discontinuous pages, inconsistent bounds, or oversized plans. A plan and its segment generation must remain fixed for the cycle; never replan only a failed range and mix generations. An `ArchiveChanged`/expired cursor aborts the cycle and preserves the checkpoint.
4. Read only the header, footer, and selected compressed blocks with exact HTTP byte ranges, `If-Match`, ETag, and Content-Range checks. Retain the existing decoder's xxh3 metadata verification, Zstandard checksum/size limits, sequence ordering, CBOR handling, and kind 7 resync behavior. Reject malformed/unsupported data; never skip it to make progress.
5. Return the candidate end cursor even for a valid zero-row filtered window, but commit it only after the entire source window is validated. For a nonempty window, row order must be strictly increasing by sequence; gaps in the global sequence are valid.
6. Retain a bounded memory budget. Materializing one bounded batch is acceptable for the first release. Fail clearly and retain the checkpoint if it exceeds the budget; document how to reduce the configured span. Do not fetch an entire large segment to read a small window.
7. `follow` must use `/xrpc/network.bsky.jetstream.subscribeEvents` with the canonical v2 JSON framing and filter parameters, not the legacy `/subscribe` wire. Decode all event kinds into the same model as archive rows. Make compression optional initially; an uncompressed connection is acceptable. After reaching a pinned sealed tip, connect at the documented replay cursor, drop duplicate sequences at the seam, and only commit source progress after destination acknowledgments. On disconnection, reconnect from the last durably committed cursor; if the live lookback floor has passed, replay the missing range through the sealed archive before resuming live. Test this seam and recovery explicitly. Never advance a cursor just because a WebSocket frame was received.

The source event model should retain `seq`, `event_time`, `witnessed_at`, `did`, `kind`, `operation`, `collection`, `rkey`, `cid`, `rev`, `is_resync`, `record`, and `event_payload`. Raw payload is optional and explicitly represented as base64 if exposed in JSON. Preserve the distinction between a decoded JSON payload string and raw CBOR bytes.

## Sink contracts

For an **ordinary Kafka broker**, use a maintained Python producer client, configure `acks=all` and appropriate idempotent producer settings where supported, send JSON values, and use `did` as the default message key to preserve per-DID order within a topic. Wait for and inspect delivery results for every event in a source batch before committing its cursor; a queued send or a bare `flush()` call without checking errors is insufficient. Bound the number of outstanding messages, surface broker/schema/size failures, and retain the prior checkpoint on partial failure. Document that Kafka producer idempotence does not make the Jetstream-to-Kafka pipeline exactly once across a crash. A Kafka topic/broker is separate infrastructure; provide a local broker-based smoke example, but ordinary CI should use a fake producer.

For **Zerobus**, use the official `databricks-zerobus-ingest-sdk` sync Python API: `ZerobusSdk`, `TableProperties`, `create_stream`, `ingest_record_offset`, and `wait_for_offset` (or `flush` if appropriate to the SDK version). Ingest the batch in source order, retain the final returned Zerobus offset, and block once per chunk until that offset is **durable**. An offset returned by `ingest_record_offset` alone is not an acknowledgment. Always close the stream. If enqueueing, acknowledgment, validation, or closing fails, do not commit the source cursor. Acknowledgment means durable, not immediately queryable in Delta.

Zerobus also exposes a **Beta Kafka-compatible producer endpoint**. It is write-only, maps topic names to Unity Catalog tables, ignores Kafka record keys, and is not a general-purpose Kafka broker or a Kafka consumer service. It could be an additional transport for the Zerobus adapter, but do not conflate it with the ordinary Kafka sink. For a new Databricks-targeted client, use the official Zerobus SDK first unless benchmarking shows a reason to switch.

Provide a checked-in `examples/create_table.sql` with an exact JSON-compatible table contract. Recommended columns: `source_endpoint STRING`, `seq BIGINT`, `event_time STRING`, `witnessed_at STRING`, `did STRING`, `kind STRING`, `operation STRING`, `collection STRING`, `rkey STRING`, `cid STRING`, `rev STRING`, `is_resync BOOLEAN`, `record STRING`, `event_payload STRING`, and optional `raw_payload_base64 STRING`. Encode timestamps as UTC ISO-8601 strings in the first release and show casts in a sample SQL query. Make the JSON keys match the table columns exactly; use `NULL` for absent optional fields. Check Zerobus JSON type support against current docs. If an event or JSON batch exceeds the SDK's message limit, fail that cycle without moving the checkpoint or split at a safe acknowledged boundary while preserving replay semantics.

The end-to-end delivery guarantee is **at least once** for either sink. A crash can occur after a successful destination acknowledgment but before the local checkpoint commit, so replay can create duplicate `(source_endpoint, seq)` events. Include these fields in Kafka values and Zerobus rows; document consumer-side deduplication. Do not claim exactly-once delivery or assume a Delta primary key prevents duplicates. Do not checkpoint solely on process exit or stream creation.

## Checkpoint and configuration

For the prototype, use a single-writer local SQLite file or another atomic, durable store; document that it must live on persistent storage and is not shared coordination for multiple producers. Store a version, source-scope fingerprint, destination identity (sink type plus topic or table), and last committed sequence. Never reuse a Zerobus checkpoint for Kafka or one Kafka topic for another. Reject a different endpoint/filter/start/payload/destination scope against an existing checkpoint; changing only credentials or operational timeouts should not invalidate it. Enforce monotonic cursor updates and avoid exposing credentials in the fingerprint, logs, errors, or saved file. The unit of recovery is one fully acknowledged batch, not an individual row.

Read secrets from environment variables or an explicitly documented local secret provider; never put them in `pyproject.toml`, committed settings, CLI arguments, logs, or a tracked `.env`. Provide `.env.example` with names and fake placeholders only, and `.gitignore` for `.env`, `.secrets.toml`, checkpoints, `.venv`, caches, and build artifacts. Suggested settings are `JETSTREAM_ENDPOINT`, `JETSTREAM_API_KEY`, `JETSTREAM_STARTING_CURSOR`, `JETSTREAM_COLLECTIONS`, `JETSTREAM_KINDS`, `JETSTREAM_DIDS`, `SINK_TYPE`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `ZEROBUS_SERVER_ENDPOINT`, `DATABRICKS_WORKSPACE_URL`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `ZEROBUS_TABLE_NAME`, and `CHECKPOINT_PATH`. Keep source and sink credentials separate. A Databricks personal access token is not the Zerobus OAuth service-principal client secret; document the exact supported authentication mode rather than silently accepting a PAT.

Validate endpoints, destination name, bounds, and required settings before making network calls. Missing credentials should produce actionable errors without printing secret values. The CLI should support a source-only diagnostic path that can verify a bounded archive read without opening a sink or advancing a production checkpoint.

## Tests and acceptance criteria

- Port the independent `native.jss` fixture test and meaningful malformed archive/range tests from the connector. Test filters, pagination, zero-row windows, segment changes, cursor expiry, max-size/deadline failures, and strict sequence order. Keep source tests network-free by using the existing HTTP simulator.
- With fake Kafka and Zerobus sinks, prove the order `read complete → enqueue → durability acknowledgment → checkpoint commit`. Inject failure before and during acknowledgment, and after acknowledgment but before checkpoint save; prove the previous checkpoint survives and replay is possible. Verify an empty filtered batch advances only after complete source validation.
- Test canonical v2 live message decoding, archive-to-live duplicate suppression, live reconnect, below-lookback fallback to archive, and destination backpressure. No network should be needed for these tests.
- Test row mapping for all event kinds, null fields, UTC timestamps, raw payload option, and over-limit records. Test scope mismatch and single-writer checkpoint behavior.
- Add an opt-in Jetstream integration test using a bounded recent cursor and user-provided credentials; never require it in ordinary CI. Include separate opt-in Kafka and Zerobus smoke tests that write only to explicitly configured test destinations and verify a second run resumes. Do not let tests write to an arbitrary default topic or table.
- CI on GitHub runs `uv sync --locked`, lint/type checks where configured, unit tests, and `uv build`. Secret-backed tests remain manual or an explicitly configured protected workflow.
- README includes installation, exact environment variables, Kafka topic setup, sample Databricks DDL/grants, one-shot and follow commands, dry run, sample SQL and deduplication, recovery behavior, live lookback/cutover behavior, costs/limits, and how to implement another sink. Add Apache-2.0 license and NOTICE attribution.

The first release is done when a fresh checkout can run all offline checks with `uv`, a user can read a bounded Jetstream window **without any sink installed**, a configured Kafka topic can receive live events, and a configured Zerobus test table can receive an archive window. Both destinations must resume from their own last acknowledged cursor and recover safely from a simulated sink failure. Do not publish to PyPI or deploy a scheduled producer as part of this brief.

## Recommended implementation order

1. Package skeleton, attribution, uv lockfile, and offline archive-reader tests/fixtures.
2. Standalone reader/config/checkpoint API and failure tests, then canonical v2 live reader and cutover tests.
3. Ordinary Kafka sink and fake-producer acknowledgment tests.
4. Zerobus sink adapter, exact table DDL, fake-stream acknowledgment tests.
5. CLI and documentation; run offline CI checks.
6. Opt-in live Jetstream, Kafka, and Zerobus smoke verification when test endpoints and credentials are available.

Keep work in this repository on a branch without a `codex` prefix. Leave the existing Lakeflow connector and Databricks community connector PR untouched until this package is reviewed separately.
