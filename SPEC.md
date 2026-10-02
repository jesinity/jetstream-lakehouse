# Jetstream Lakehouse: implementation specification

Status: implementation brief for Luna; no application code has been written in this repository.

## Goal and scope

Build an open-source Python package, `jetstream-lakehouse`, whose main product is a **destination-independent Jetstream v2 reader**. It must read bounded, pinned archive snapshots and continue into the canonical v2 live WebSocket stream, yielding one consistent event model. Its **first sink targets Databricks Zerobus Ingest through the Kafka-compatible producer API**, using a Kafka producer client rather than the Zerobus SDK. The reader must not import sink dependencies or expose sink-specific types. This is not a copy of the Lakeflow Connect interface. Use `uv` for environment management, locking, running checks, and building. Target Python 3.10+.

The first release has `run-once` for a bounded sealed snapshot and `follow` for snapshot-to-live replay. A caller may also start at the current live tip explicitly. The default must never silently discard history. In the first sink, the Kafka topic name is the full Unity Catalog table name, and the Zerobus endpoint persists JSON record values into that table. Jetstream itself does **not** speak the Kafka protocol; this Python producer bridges the two. Ordinary Kafka, the native Zerobus SDK, and Snowflake/Snowpipe Streaming are possible later adapters, not first-release requirements.

## Authoritative references

- Jetstream v2 archive protocol and planner: https://github.com/bluesky-social/jetstream/blob/main/docs/README.md
- Existing verified Python implementation: `../lakeflow-connect-bluesky/src/databricks/labs/community_connector/sources/bluesky/` (especially `archive.py`, `transport.py`, `options.py`, `bluesky.py`, `errors.py`). Reuse behavior and tests, but remove Databricks Lakeflow/PySpark coupling. Preserve Apache-2.0 attribution and the existing `NOTICE` when adapting code.
- Existing independent archive fixture and tests: `../lakeflow-connect-bluesky/tests/unit/sources/bluesky/`; `fixtures/native.jss` was produced by the official Go segment writer and is more valuable than a simulator-only test.
- Zerobus table and service-principal setup: https://docs.databricks.com/aws/en/ingestion/zerobus-ingest
- Canonical Jetstream v2 WebSocket framing/filtering/cutover: https://github.com/bluesky-social/jetstream/blob/main/docs/README.md#52-the-v2-stream-networkbskyjetstreamsubscribeevents
- Zerobus Kafka-compatible producer API (Beta; distinct from an ordinary Kafka broker): https://docs.databricks.com/aws/en/ingestion/zerobus-kafka

Read the current vendor documentation and installed Kafka producer client signatures before implementation, because these APIs can change. The Zerobus Kafka-compatible API is Beta and must be enabled/available in the target workspace.

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
| `sinks/zerobus_kafka.py` | Kafka producer adapter configured specifically for Zerobus OAuth, the table-named topic, durable delivery acknowledgments, and JSON event mapping. |
| `runner.py`, `cli.py` | Orchestrate source batches, destination acknowledgment, checkpoint commit, `run-once`, and live `follow`. |

Keep core imports usable without PySpark or a Kafka client. Make the Zerobus Kafka sink an optional extra. Do not depend on the Databricks community connector package at runtime. Do not make the `atproto` Python SDK the archive decoder: the current connector has stricter generation/checksum checks and handles resync kind 7. If code is copied from the connector, update imports and tests deliberately rather than broad refactoring.

## Source contract

1. Require an explicit starting cursor when there is no checkpoint. Cursor 0 means the whole archive; never silently start there or silently jump to the current tip. The cursor is exclusive. Cap each cycle by a configured maximum **sequence span** and by a byte/time budget; a filtered batch can have fewer rows than that span.
2. Support optional `collections`, `dids`, and `kinds` (`commit`, `identity`, `account`, `sync`). Default to all kinds. A collection filter constrains commits while DID-level markers can still be returned when kinds are not narrowed. Apply exact filters **after decoding**, because planner block selection may have false positives. Keep create/update/delete, identity, account, sync, and resync markers as separate source events; do not silently fold them away.
3. Plan with `network.bsky.jetstream.planSnapshot`, pin the `sealedTipSeq` and each segment's checksum, paginate until the covered cursor reaches the chosen tip, and reject missing progress, discontinuous pages, inconsistent bounds, or oversized plans. A plan and its segment generation must remain fixed for the cycle; never replan only a failed range and mix generations. An `ArchiveChanged`/expired cursor aborts the cycle and preserves the checkpoint.
4. Read only the header, footer, and selected compressed blocks with exact HTTP byte ranges, `If-Match`, ETag, and Content-Range checks. Retain the existing decoder's xxh3 metadata verification, Zstandard checksum/size limits, sequence ordering, CBOR handling, and kind 7 resync behavior. Reject malformed/unsupported data; never skip it to make progress.
5. Return the candidate end cursor even for a valid zero-row filtered window, but commit it only after the entire source window is validated. For a nonempty window, row order must be strictly increasing by sequence; gaps in the global sequence are valid.
6. Retain a bounded memory budget. Materializing one bounded batch is acceptable for the first release. Fail clearly and retain the checkpoint if it exceeds the budget; document how to reduce the configured span. Do not fetch an entire large segment to read a small window.
7. `follow` must use `/xrpc/network.bsky.jetstream.subscribeEvents` with the canonical v2 JSON framing and filter parameters, not the legacy `/subscribe` wire. Decode all event kinds into the same model as archive rows. Make compression optional initially; an uncompressed connection is acceptable. After reaching a pinned sealed tip, connect at the documented replay cursor, drop duplicate sequences at the seam, and only commit source progress after destination acknowledgments. On disconnection, reconnect from the last durably committed cursor; if the live lookback floor has passed, replay the missing range through the sealed archive before resuming live. Test this seam and recovery explicitly. Never advance a cursor just because a WebSocket frame was received.

The source event model should retain `seq`, `event_time`, `witnessed_at`, `did`, `kind`, `operation`, `collection`, `rkey`, `cid`, `rev`, `is_resync`, `record`, and `event_payload`. Raw payload is optional and explicitly represented as base64 if exposed in JSON. Preserve the distinction between a decoded JSON payload string and raw CBOR bytes.

## Zerobus Kafka sink contract

Use the [Zerobus Kafka-compatible producer API](https://docs.databricks.com/aws/en/ingestion/zerobus-kafka) as the **only first-release sink**. Connect to `<workspace-id>.zerobus.<region>.cloud.databricks.com:9092` with `SASL_SSL` and `OAUTHBEARER`. Obtain a short-lived, table-scoped Databricks OAuth token from service-principal client credentials using the documented `zerobusDirectWriteApi` resource and `authorization_details`; refresh it in the Kafka client's token-provider callback on reconnect. Do not use a PAT or a static token. The service principal needs `USE CATALOG`, `USE SCHEMA`, and `MODIFY, SELECT` on the target table.

Set the topic to exactly `catalog.schema.table`. Send an uncompressed UTF-8 JSON object as each record value, matching the existing table schema. Configure `acks=all`. Zerobus ignores Kafka record keys, headers, client timestamps, and partition assignments; identity and sequence must therefore be in the JSON value. It is a single-partition, **write-only** endpoint, not an ordinary Kafka broker. Do not add consumer, admin, transactional, or topic-creation calls. Validate the table name and endpoint before connecting.

Use a maintained Python Kafka producer client whose OAuth callback and per-message acknowledgment behavior work with this endpoint. A `send()` future only means queued; `flush()` alone is not enough unless every delivery result is also checked. Wait for and inspect successful durable Produce responses for **every** event in a source batch before committing its Jetstream cursor. Bound outstanding sends and fail the batch on any authentication, schema, quota, size, or delivery error. Preserve the previous checkpoint on partial failure; replay may produce duplicates. Keep a long-lived producer during `follow` rather than reconnecting for every batch.

Provide a checked-in `examples/create_table.sql` with an exact JSON-compatible table contract. Recommended columns: `source_endpoint STRING`, `seq BIGINT`, `event_time STRING`, `witnessed_at STRING`, `did STRING`, `kind STRING`, `operation STRING`, `collection STRING`, `rkey STRING`, `cid STRING`, `rev STRING`, `is_resync BOOLEAN`, `record STRING`, `event_payload STRING`, and optional `raw_payload_base64 STRING`. Encode timestamps as UTC ISO-8601 strings in the first release and show casts in a sample SQL query. Make the JSON keys match the table columns exactly; use `NULL` for absent optional fields. Check Zerobus JSON type support and current record-size limits against its Kafka API docs. Reject an over-limit record without advancing the checkpoint.

The end-to-end delivery guarantee is **at least once**. A crash can occur after a successful Zerobus Produce acknowledgment but before the local checkpoint commit, so replay can create duplicate `(source_endpoint, seq)` rows. Include these fields in every JSON value and document query-side deduplication. Do not claim exactly-once delivery or assume a Delta primary key prevents duplicates. Do not checkpoint solely on process exit or producer creation.

## Checkpoint and configuration

For the prototype, use a single-writer local SQLite file or another atomic, durable store; document that it must live on persistent storage and is not shared coordination for multiple producers. Store a version, source-scope fingerprint, Zerobus endpoint/table identity, and last committed sequence. Never reuse a checkpoint for a different target table or endpoint. Reject a different endpoint/filter/start/payload/destination scope against an existing checkpoint; changing only credentials or operational timeouts should not invalidate it. Enforce monotonic cursor updates and avoid exposing credentials in the fingerprint, logs, errors, or saved file. The unit of recovery is one fully acknowledged batch, not an individual row.

Read secrets from environment variables or an explicitly documented local secret provider; never put them in `pyproject.toml`, committed settings, CLI arguments, logs, or a tracked `.env`. Provide `.env.example` with names and fake placeholders only, and `.gitignore` for `.env`, `.secrets.toml`, checkpoints, `.venv`, caches, and build artifacts. Suggested settings are `JETSTREAM_ENDPOINT`, `JETSTREAM_API_KEY`, `JETSTREAM_STARTING_CURSOR`, `JETSTREAM_COLLECTIONS`, `JETSTREAM_KINDS`, `JETSTREAM_DIDS`, `DATABRICKS_WORKSPACE_URL`, `DATABRICKS_WORKSPACE_ID`, `ZEROBUS_KAFKA_BOOTSTRAP_SERVERS`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `ZEROBUS_TABLE_NAME`, and `CHECKPOINT_PATH`. Keep source and sink credentials separate. A Databricks personal access token is not the Zerobus OAuth service-principal client secret; document the exact supported authentication mode rather than silently accepting a PAT.

Validate endpoints, destination name, bounds, and required settings before making network calls. Missing credentials should produce actionable errors without printing secret values. The CLI should support a source-only diagnostic path that can verify a bounded archive read without opening a sink or advancing a production checkpoint.

## Tests and acceptance criteria

- Port the independent `native.jss` fixture test and meaningful malformed archive/range tests from the connector. Test filters, pagination, zero-row windows, segment changes, cursor expiry, max-size/deadline failures, and strict sequence order. Keep source tests network-free by using the existing HTTP simulator.
- With a fake Kafka producer configured as a Zerobus endpoint, prove the order `read batch complete → enqueue → inspect every durable Produce acknowledgment → checkpoint commit`. Inject failure before and during acknowledgment, and after acknowledgment but before checkpoint save; prove the previous checkpoint survives and replay is possible. Verify an empty filtered batch advances only after complete source validation.
- Test the token-provider callback, token refresh on reconnect, OAuth failure handling, table-named topic, `acks=all`, no compression, ignored key assumptions, and delivery errors. Do not make authenticated calls in ordinary CI.
- Test canonical v2 live message decoding, archive-to-live duplicate suppression, live reconnect, below-lookback fallback to archive, and destination backpressure. No network should be needed for these tests.
- Test row mapping for all event kinds, null fields, UTC timestamps, raw payload option, and over-limit records. Test scope mismatch and single-writer checkpoint behavior.
- Add an opt-in Jetstream integration test using a bounded recent cursor and user-provided credentials; never require it in ordinary CI. Include an opt-in Zerobus Kafka smoke test that writes only to an explicitly configured test table, verifies the rows through Databricks SQL, and verifies a second run resumes. Do not let tests write to an arbitrary default table.
- CI on GitHub runs `uv sync --locked`, lint/type checks where configured, unit tests, and `uv build`. Secret-backed tests remain manual or an explicitly configured protected workflow.
- README includes installation, exact environment variables, Zerobus Beta enablement/region checks, sample Databricks DDL/grants, one-shot and follow commands, source-only diagnostic, sample SQL and deduplication, recovery behavior, live lookback/cutover behavior, costs/limits, and how to implement another sink. Explain that the table is the Kafka topic and that Zerobus does not expose Kafka consumer APIs. Add Apache-2.0 license and NOTICE attribution.

The first release is done when a fresh checkout can run all offline checks with `uv`, a user can read a bounded Jetstream window **without any sink installed**, and a configured Zerobus test table can receive archive and live events through its Kafka-compatible endpoint. The producer must resume from its last acknowledged cursor and recover safely from a simulated delivery failure. Do not publish to PyPI or deploy a scheduled producer as part of this brief.

## Recommended implementation order

1. Package skeleton, attribution, uv lockfile, and offline archive-reader tests/fixtures.
2. Standalone reader/config/checkpoint API and failure tests, then canonical v2 live reader and cutover tests.
3. Zerobus Kafka-compatible producer adapter, OAuth token provider, table DDL, and fake-producer acknowledgment tests.
4. CLI and documentation; run offline CI checks.
5. Opt-in live Jetstream and Zerobus Kafka smoke verification when the Beta endpoint, test table, and service-principal credentials are available.

Keep work in this repository on a branch without a `codex` prefix. Leave the existing Lakeflow connector and Databricks community connector PR untouched until this package is reviewed separately.
