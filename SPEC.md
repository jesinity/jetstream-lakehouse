# Jetstream Lakehouse: implementation specification

Status: implementation brief for Luna; no application code has been written in this repository.

## Goal and scope

Build an open-source Python package, `jetstream-lakehouse`, that reads bounded, pinned snapshots from the Bluesky Jetstream v2 sealed archive and writes those events to a Databricks Unity Catalog Delta table through the official Zerobus Ingest Python SDK. This is a reusable source library with a thin sink boundary, not a copy of the Lakeflow Connect interface. Use `uv` for environment management, locking, running checks, and building. Target Python 3.10+.

The first release has two execution modes: one bounded `run-once` cycle, and `follow` that repeats the same safe cycle after a configurable polling delay. `follow` polls the **sealed** archive; it is not the Jetstream live WebSocket and must not be advertised as sub-second or gap-free live cutover. Snowflake/Snowpipe Streaming and Jetstream WebSocket cutover are later adapters/features, not part of this implementation.

## Authoritative references

- Jetstream v2 archive protocol and planner: https://github.com/bluesky-social/jetstream/blob/main/docs/README.md
- Existing verified Python implementation: `../lakeflow-connect-bluesky/src/databricks/labs/community_connector/sources/bluesky/` (especially `archive.py`, `transport.py`, `options.py`, `bluesky.py`, `errors.py`). Reuse behavior and tests, but remove Databricks Lakeflow/PySpark coupling. Preserve Apache-2.0 attribution and the existing `NOTICE` when adapting code.
- Existing independent archive fixture and tests: `../lakeflow-connect-bluesky/tests/unit/sources/bluesky/`; `fixtures/native.jss` was produced by the official Go segment writer and is more valuable than a simulator-only test.
- Zerobus setup and Python SDK: https://docs.databricks.com/aws/en/ingestion/zerobus-ingest
- Zerobus durability acknowledgments: https://docs.databricks.com/aws/en/ingestion/zerobus-message-blocking

Read the current vendor documentation and installed SDK signatures before implementation, because these APIs can change.

## Package boundary

Suggested modules (names may change if the public API remains clear):

| Module | Responsibility |
| --- | --- |
| `config.py` | Validate Jetstream settings, Zerobus settings, secrets, and a stable source-scope fingerprint. |
| `transport.py` | Authenticated `planSnapshot` and generation-pinned, exact HTTP ranges with bounded retries and deadlines. |
| `archive.py` | Strict `jss0`/v1 metadata and block decoder; no sink imports. |
| `reader.py` | Plan one `(after, through]` window, validate all pages, read/filter selected events in sequence order, return a bounded batch and its candidate end cursor. |
| `checkpoint.py` | Persist the last **durably delivered** source cursor and source-scope fingerprint atomically. |
| `sinks/base.py` | Small sink protocol: `write_batch(rows)` returns successfully only after all rows are durable. |
| `sinks/zerobus.py` | Official Zerobus SDK adapter and JSON row mapping. Import this optional dependency only when selected. |
| `runner.py`, `cli.py` | Orchestrate reading, sink acknowledgment, checkpoint commit, `run-once`, and `follow`. |

Keep core imports usable without PySpark or the Zerobus SDK. Do not depend on the Databricks community connector package at runtime. Do not make the `atproto` Python SDK the decoder: the current connector has stricter generation/checksum checks and handles resync kind 7. If code is copied from the connector, update imports and tests deliberately rather than broad refactoring.

## Source contract

1. Require an explicit starting cursor when there is no checkpoint. Cursor 0 means the whole archive; never silently start there or silently jump to the current tip. The cursor is exclusive. Cap each cycle by a configured maximum **sequence span** and by a byte/time budget; a filtered batch can have fewer rows than that span.
2. Support optional `collections`, `dids`, and `kinds` (`commit`, `identity`, `account`, `sync`). Default to all kinds. A collection filter constrains commits while DID-level markers can still be returned when kinds are not narrowed. Apply exact filters **after decoding**, because planner block selection may have false positives. Keep create/update/delete, identity, account, sync, and resync markers as separate source events; do not silently fold them away.
3. Plan with `network.bsky.jetstream.planSnapshot`, pin the `sealedTipSeq` and each segment's checksum, paginate until the covered cursor reaches the chosen tip, and reject missing progress, discontinuous pages, inconsistent bounds, or oversized plans. A plan and its segment generation must remain fixed for the cycle; never replan only a failed range and mix generations. An `ArchiveChanged`/expired cursor aborts the cycle and preserves the checkpoint.
4. Read only the header, footer, and selected compressed blocks with exact HTTP byte ranges, `If-Match`, ETag, and Content-Range checks. Retain the existing decoder's xxh3 metadata verification, Zstandard checksum/size limits, sequence ordering, CBOR handling, and kind 7 resync behavior. Reject malformed/unsupported data; never skip it to make progress.
5. Return the candidate end cursor even for a valid zero-row filtered window, but commit it only after the entire source window is validated. For a nonempty window, row order must be strictly increasing by sequence; gaps in the global sequence are valid.
6. Retain a bounded memory budget. Materializing one bounded batch is acceptable for the first release. Fail clearly and retain the checkpoint if it exceeds the budget; document how to reduce the configured span. Do not fetch an entire large segment to read a small window.

The source event model should retain `seq`, `event_time`, `witnessed_at`, `did`, `kind`, `operation`, `collection`, `rkey`, `cid`, `rev`, `is_resync`, `record`, and `event_payload`. Raw payload is optional and explicitly represented as base64 if exposed in JSON. Preserve the distinction between a decoded JSON payload string and raw CBOR bytes.

## Sink and table contract

Use the official `databricks-zerobus-ingest-sdk` sync Python API for the first adapter: `ZerobusSdk`, `TableProperties`, `create_stream`, `ingest_record_offset`, and `wait_for_offset` (or `flush` if appropriate to the SDK version). Ingest the batch in source order, retain the final returned Zerobus offset, and block once per chunk until that offset is **durable**. An offset returned by `ingest_record_offset` alone is not an acknowledgment. Always close the stream. If enqueueing, acknowledgment, validation, or closing fails, do not commit the source cursor. Acknowledgment means durable, not immediately queryable in Delta.

Provide a checked-in `examples/create_table.sql` with an exact JSON-compatible table contract. Recommended columns: `source_endpoint STRING`, `seq BIGINT`, `event_time STRING`, `witnessed_at STRING`, `did STRING`, `kind STRING`, `operation STRING`, `collection STRING`, `rkey STRING`, `cid STRING`, `rev STRING`, `is_resync BOOLEAN`, `record STRING`, `event_payload STRING`, and optional `raw_payload_base64 STRING`. Encode timestamps as UTC ISO-8601 strings in the first release and show casts in a sample SQL query. Make the JSON keys match the table columns exactly; use `NULL` for absent optional fields. Check Zerobus JSON type support against current docs. If an event or JSON batch exceeds the SDK's message limit, fail that cycle without moving the checkpoint or split at a safe acknowledged boundary while preserving replay semantics.

The delivery guarantee is **at least once**. A crash can occur after a successful Zerobus acknowledgment but before the local checkpoint commit, so replay can create duplicate `(source_endpoint, seq)` rows. Include these columns and document a deduplicating query/view. Do not claim exactly-once delivery or assume a Delta primary key prevents duplicates. Do not checkpoint solely on process exit or stream creation.

## Checkpoint and configuration

For the prototype, use a single-writer local SQLite file or another atomic, durable store; document that it must live on persistent storage and is not shared coordination for multiple producers. Store a version, source-scope fingerprint, and last committed sequence. Reject a different endpoint/filter/start/payload scope against an existing checkpoint; changing only credentials or operational timeouts should not invalidate it. Enforce monotonic cursor updates and avoid exposing credentials in the fingerprint, logs, errors, or saved file. The unit of recovery is one fully acknowledged batch, not an individual row.

Read secrets from environment variables or an explicitly documented local secret provider; never put them in `pyproject.toml`, committed settings, CLI arguments, logs, or a tracked `.env`. Provide `.env.example` with names and fake placeholders only, and `.gitignore` for `.env`, `.secrets.toml`, checkpoints, `.venv`, caches, and build artifacts. Suggested settings are `JETSTREAM_ENDPOINT`, `JETSTREAM_API_KEY`, `JETSTREAM_STARTING_CURSOR`, `JETSTREAM_COLLECTIONS`, `JETSTREAM_KINDS`, `JETSTREAM_DIDS`, `ZEROBUS_SERVER_ENDPOINT`, `DATABRICKS_WORKSPACE_URL`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `ZEROBUS_TABLE_NAME`, and `CHECKPOINT_PATH`. Keep source and sink credentials separate. A Databricks personal access token is not the Zerobus OAuth service-principal client secret; document the exact supported authentication mode rather than silently accepting a PAT.

Validate endpoints, table name, bounds, and required settings before making network calls. Missing credentials should produce actionable errors without printing secret values. The CLI should support a dry source-only diagnostic path that can verify a bounded archive read without opening a Zerobus stream or advancing a production checkpoint.

## Tests and acceptance criteria

- Port the independent `native.jss` fixture test and meaningful malformed archive/range tests from the connector. Test filters, pagination, zero-row windows, segment changes, cursor expiry, max-size/deadline failures, and strict sequence order. Keep source tests network-free by using the existing HTTP simulator.
- With a fake sink/stream, prove the order `read complete → enqueue → durability acknowledgment → checkpoint commit`. Inject failure before and during acknowledgment, and after acknowledgment but before checkpoint save; prove the previous checkpoint survives and replay is possible. Verify an empty filtered batch advances only after complete source validation.
- Test row mapping for all event kinds, null fields, UTC timestamps, raw payload option, and over-limit records. Test scope mismatch and single-writer checkpoint behavior.
- Add an opt-in live integration test using a bounded recent cursor and user-provided credentials; never require it in ordinary CI. Include a separate opt-in Zerobus smoke test that creates no tables, writes to an explicitly configured test table, waits for durability, queries the result, and verifies a second run resumes. Do not let tests write to an arbitrary default table.
- CI on GitHub runs `uv sync --locked`, lint/type checks where configured, unit tests, and `uv build`. Secret-backed tests remain manual or an explicitly configured protected workflow.
- README includes installation, exact environment variables, sample DDL/grants, one-shot and follow commands, dry run, sample SQL and deduplication, recovery behavior, known sealed-archive lag, costs/limits, and the next-stage WebSocket cutover design. Add Apache-2.0 license and NOTICE attribution.

The first release is done when a fresh checkout can run all offline checks with `uv`, a user with a configured test Unity Catalog table and service principal can ingest one bounded archive window, observe the rows, rerun without moving backward, and recover safely from a simulated sink failure. Do not publish to PyPI or deploy a scheduled producer as part of this brief.

## Recommended implementation order

1. Package skeleton, attribution, uv lockfile, and offline archive-reader tests/fixtures.
2. Standalone reader/config/checkpoint API and failure tests.
3. Zerobus sink adapter, exact table DDL, fake-stream acknowledgment tests.
4. CLI and documentation; run offline CI checks.
5. Opt-in live source and Zerobus smoke verification when test credentials/table are available.

Keep work in this repository on a branch without a `codex` prefix. Leave the existing Lakeflow connector and Databricks community connector PR untouched until this package is reviewed separately.
