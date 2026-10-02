# Generic Python Jetstream v2 client: implementation specification

Status: implementation brief for Luna. This repository currently contains a specification only. The first deliverable is a reusable **Jetstream client library**, with no Databricks, Kafka, Snowflake, or Spark dependency.

## Architecture decision

Build one generic source library in this repository. Keep the existing Lakeflow connector in `../lakeflow-connect-bluesky` and build the Zerobus producer as a separate downstream application/package. Both should consume the same public Jetstream API once the library is tested and versioned. Do not implement either adapter here, and do not alter the existing Lakeflow connector or Databricks community-connector PR as part of this task. The current repository name, `jetstream-lakehouse`, does not dictate the Python import or distribution name; choose a generic name after checking availability and document that choice before publishing.

The library must support three source modes with one event model:

1. `snapshot`: bounded replay of a sealed-archive `(after_seq, before_seq]` window.
2. `live`: canonical Jetstream v2 WebSocket events from an explicitly chosen cursor or the current live tip.
3. `replay`: sealed-archive backfill followed by a seamless v2 live cutover, with duplicate suppression and recovery from an expired live lookback window.

Use Python 3.10+ and `uv` for environment management, locking, running checks, and building. The package should be useful to any Python consumer, including a CLI, Kafka producer, Lakeflow connector, or Snowflake adapter.

## Authoritative references

- Jetstream v2 archive, live wire, filters, and cutover: https://github.com/bluesky-social/jetstream/blob/main/docs/README.md
- Official Go client behavior and delivery model: https://github.com/bluesky-social/jetstream/blob/main/doc.go
- Existing verified Python decoder and transport: `../lakeflow-connect-bluesky/src/databricks/labs/community_connector/sources/bluesky/` (`archive.py`, `transport.py`, `options.py`, `bluesky.py`, `errors.py`). Reuse protocol behavior, but remove Lakeflow/PySpark coupling. Preserve Apache-2.0 and `NOTICE` attribution when adapting code.
- Independent native archive fixture and tests: `../lakeflow-connect-bluesky/tests/unit/sources/bluesky/fixtures/native.jss` and adjacent tests. The fixture was produced by Jetstream's official Go segment writer.

Read current upstream specifications and code before implementing wire details. The package must not import the Lakeflow connector or `atproto` Python SDK at runtime. The existing decoder has strict archive generation/checksum checks and handles resync kind 7; do not lose those properties while extracting it.

## Public API and ownership

Expose a small, documented API with an immutable event type and a bounded batch type, for example `Event` and `Batch(events, after_seq, through_seq)`. `through_seq` is the **candidate** cursor covered by a fully validated batch, even when filters yield zero events. The library never treats a returned batch as delivered. It neither writes sink data nor persists a destination checkpoint. The caller must acknowledge its sink first and then save `through_seq`; on failure it can recreate the reader from its last saved cursor. Sequences increase but are not necessarily contiguous. Document exclusive/inclusive cursor semantics.

The public event model must retain `seq`, `event_time`, `witnessed_at`, `did`, `kind`, `operation`, `collection`, `rkey`, `cid`, `rev`, `is_resync`, `record`, and `event_payload`. Make raw CBOR optional and explicitly distinguish bytes/base64 from decoded JSON. Keep create/update/delete, identity, account, sync, and resync markers inline; the library does not fold them into current record state or silently discard them. Represent absent fields consistently across archive and live paths.

Support `collections`, `dids`, and `kinds` (`commit`, `identity`, `account`, `sync`). Default to all kinds. Collection filters constrain commits; DID-level markers remain available unless the caller explicitly narrows kinds. Apply exact filters after archive decoding because planner block selection may have false positives. Validate all options and endpoints before network calls. Never log bearer keys or use ambient credentials unexpectedly.

Suggested internal boundary: `config.py`, `errors.py`, `transport.py`, `archive.py`, `live.py`, `client.py`, and `models.py`. Names may change if the public API stays simple. Keep the strict binary decoder private. No sink modules, table DDL, OAuth code, Databricks CLI, or local SQLite checkpoint are in this library.

## Archive correctness

1. Require an explicit `after_seq` for historical replay. Cursor 0 means the whole archive; do not silently start there or jump to the present. Cap each snapshot batch by a configured sequence span, byte budget, and deadline.
2. Plan with `network.bsky.jetstream.planSnapshot`. Pin `sealedTipSeq` and segment checksums, paginate to the planned cursor, and reject missing progress, inconsistent bounds, discontinuous pages, or oversized plans. Keep one fixed plan/generation while reading it. On `ArchiveChanged`, expired cursor, or malformed data, fail without issuing a successful candidate cursor.
3. Read only selected headers, footers, and compressed blocks through exact byte ranges. Retain `If-Match`, ETag, Content-Range, xxh3 metadata, Zstandard checksum/size, sequence-order, CBOR, and kind 7 resync checks. Reject corruption rather than skipping it. Do not download a whole large segment for a small window.
4. Emit a zero-row batch when a valid filtered range was completely scanned so a caller can persist progress after its own checks. Never emit progress for an incomplete or unvalidated range.

## Live and cutover correctness

Use `/xrpc/network.bsky.jetstream.subscribeEvents` and its canonical v2 framing/filter parameters, not the legacy `/subscribe` wire. Decode all supported kinds into the same event model as archive reads. An uncompressed connection is acceptable initially; optional dictionary compression can follow. Handle heartbeats, control/error frames, and reconnects according to current upstream documentation.

For `replay`, finish the pinned sealed range and then subscribe from the documented cursor. Suppress duplicate sequences across the seam and on reconnect. If a saved cursor falls below the live lookback floor, replay the missing sealed-archive range before following live again; never silently clamp forward and lose events. Let the caller resume from its **last durably saved** cursor, not the last frame received in memory. Bound in-memory queues and expose backpressure/cancellation so a slow sink cannot grow memory without limit. The library's guarantee is at-least-once delivery to its caller, not exactly-once persistence in an external system.

## Packaging, docs, and tests

- Use `uv` and a checked-in lockfile. Keep the installable package free of PySpark, Databricks, Kafka, and Snowflake dependencies. Include Apache-2.0 license and NOTICE attribution. Add `.gitignore` for `.venv`, build/cache files, local secrets, and test output.
- Port the independent `native.jss` fixture test and meaningful malformed archive/range tests from the connector. Cover filters, pagination, zero-row windows, changed generations, expired cursors, byte/time limits, and sequence order. Use the existing HTTP simulator where appropriate, but do not rely only on simulator-generated archives.
- Test canonical v2 live framing, all event kinds, archive/live event equivalence, cutover duplicates, reconnect, below-lookback recovery, cancellation, and bounded backpressure with local fakes. No service credentials are required for ordinary tests.
- Include an opt-in bounded live contract test against a real Jetstream endpoint, using user-provided credentials where required. It must not run in normal CI or assume a particular private key exists.
- Document installation, `snapshot`, `live`, and `replay` examples, event/cursor semantics, filter behavior, archive limits, error types, security, and at-least-once recovery. Include a short consumer example showing `for batch in client: write_to_sink(batch.events); await_durability(); save_cursor(batch.through_seq)` without importing a sink library.
- CI runs `uv sync --locked`, lint/type checks where configured, unit tests, and `uv build`. Do not publish to PyPI during this task.

The library is ready for downstream integration when a fresh checkout passes offline checks, the native archive fixture decodes correctly, a real bounded Jetstream snapshot can be read on an opt-in run, and the public API supports both a Lakeflow refresh and a Zerobus producer without source-code changes.

## Separate follow-on integrations

**Zerobus bridge (separate application/package):** consume the library's batches/events and produce JSON to Databricks Zerobus's [Kafka-compatible ingestion endpoint](https://docs.databricks.com/aws/en/ingestion/zerobus-kafka). That Beta endpoint is write-only; its topic is the fully qualified Unity Catalog table name, and it requires `SASL_SSL`, `OAUTHBEARER` with a refreshable table-scoped service-principal token, uncompressed JSON values, and `acks=all`. It ignores Kafka keys, headers, and timestamps, so `source_endpoint` and `seq` belong in each JSON value. Check **all** durable Produce results before saving the Jetstream cursor. The bridge owns OAuth, table DDL, delivery retries, and its persistent checkpoint. A crash after destination acknowledgment but before checkpoint save can replay events, so downstream rows require `(source_endpoint, seq)` deduplication. No bridge code is in this task.

**Lakeflow connector (existing separate repository):** consume the library's bounded snapshot API while keeping the Lakeflow schema, option parsing, and offset/partition contract in the connector. Lakeflow/Spark, not the source library, decides when a completed batch and its candidate cursor are committed after Delta writes. Check upstream packaging and dependency rules before replacing the current embedded decoder; do not break the open PR merely to force this reuse. No connector code is changed in this task.

Recommended order: finish and version the generic library; then build the Zerobus bridge; then assess whether/how the Lakeflow connector can depend on the library. Keep implementation work on a branch without a `codex` prefix.
