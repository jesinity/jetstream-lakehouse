# Changelog

## 0.2.0 - Unreleased

- Add `Client.sealed_tip`, `Client.plan_snapshot`, and `Client.read_plan` for
  bounded archive planning and exact replay across processes.
- Add immutable public `ArchivePlan` values with deterministic, validated JSON
  format version 1 and source-scope binding.
- Preserve `snapshot`'s shared planning and reading deadline and existing
  `live`/`replay` behavior.
