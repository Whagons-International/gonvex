# 0022: Rulings from the wave-1 reviews (A01 part 1, K01, K04, K05)

Date: 2026-11-11. Status: decided (lead).
Inputs: PR #337 (A01 part 1), #339 (K01), #338 (K04) and its note 0021, #340 (K05) and its note 0020.

## 1. Who owns what: storage (K01) and module host (K04)

| Duty | Owner |
|---|---|
| Every SQL semantic: single-statement admission, ATTACH/PRAGMA/VACUUM/transaction-control refusal, `$N` binding, read-only Queries, declared-type decoding, REGEXP, 100-parameter handling, the aborted-transaction rule, the commit refusal after a caught SQL error, savepoints, the deferred-FK check, row counters | **K01** (`kernel/storage/`) |
| **The one catalog cache** and its invalidation (`invalidateSchema()`) | **K01**. K04's `schemaChanged()`, K02 migrations and the A01 transform call it. R04 builds on it. |
| The `ctx` surface per kind, capability gating, determinism (pinned clock, seeded random, entropy), the host-call error wrapping (`host call failed: …`), result caps, budgets, generations and `system.reload` | **K04** (`kernel/host/`) |

There is one SQL engine in the tree. K04 drops its S2 stand-in and calls K01.

## 2. K01 deviations (accepted, platform-forced)

| # | Deviation | Ruling |
|---|---|---|
| a | REGEXP is compiled to GLOB/`length`/`substr` and supports a subset of patterns; others fail with a clear error | Accepted. Whagons uses one pattern (`^PR-[0-9]+$`). A01's drift CI gains a check that lists REGEXP patterns found in the app's module sources and fails on one K01 cannot compile. |
| b | No decltype: columns the projection inference leaves open decode as expressions | Accepted. The `storage-sql` scenario and K08 scenarios catch any visible difference. |
| c | The FK check is table-level until K03 reports written rows; it can flag a violation that predates the invocation | Accepted until K03 lands. **K03 must wire `noteRows`** so the check is row-level before G1. Staging data has 0 FK violations (P03, S1). |
| d | Integers beyond ±2^53 are inexact | Same as Rust's module boundary (JS numbers). Accepted. |
| e | A statement mixing `$1` and `?1` gets both columns named `$1` | Accepted. No Whagons statement does this. |
| f | `existsDataRows` with 300 probes takes 4 statements against Rust's 2 | Accepted (100-parameter limit). |

## 3. K04 findings (note 0021)

- §1: the interface is superseded by §1 above. K04 adapts to K01, not the other way round.
- §2: K04's `done_when` becomes "runtime-contract vectors pass; a real Reducer runs from the app bundle; the
  Query dispatch path passes the vectors". "A real Whagons Query" moves to K08, because every Whagons Query
  is a runtime-executed plan.
- §3: module globals are lexical bindings. K19 builds the Worker version with K04's prelude.
- §4: an export with no handler fails at dispatch, as in Rust.
- §5 (R25, closes 0016 G0-11):
  - a: callers map a CPU-limit reset to Rust's deadline error for the command that caused it and quarantine
    the command id. Accepted.
  - b: the tenant-object script sets **`limits.cpu_ms = 5000`**. K02 chunks migrations so no chunk exceeds
    2 s of CPU on the platform. Revisit at V04.
  - c: a pure `for(;;){}` is not stopped per call in stage 1. Sockets live in gateways, so the blast radius
    is the tenant's in-flight calls for up to 5 s. Accepted.
  - d: added to S5's scope: can a Worker Loader isolate make host calls into a tenant that holds its
    transaction?
  - e: the conformance case for a pure-loop deadline asserts the error class and text, not the timing.
    Platform-forced.

## 4. K05 findings (note 0020)

| # | Finding | Ruling |
|---|---|---|
| 1 | A hibernatable socket whose client never sent a frame sees the 10 s auth-deadline close (1008) about 20 s after connecting, because the edge closes TCP about 10 s after the close frame | Accepted deviation. Code and reason are right, and the SDK always speaks on open. |
| 2 | Rust refuses single frames over 16 MiB on member sockets; the kernel cannot see frames and accepts messages up to the platform cap (0018) | Accepted. The kernel is more permissive, and no client relies on the refusal. Conformance asserts 16 MiB against Rust and 32 MiB − 1 KiB against the kernel, each where it applies. |
| 3 | Rust closes an oversize public-transport frame with 1002, not the 1009 in A.1.9 | Code is the contract: **1002**. `CONTRACT.md` A.1.9 is corrected. |
| 4 | Workers RPC caps a serialized argument at 32 MiB | Every tenant-to-gateway delivery batch stays under it; K10 splits. The gateway closes 1011 rather than send a frame of 32 MiB − 1 KiB or more. |
| 5 | The tenant object writes a KV entry when a gateway gets its first socket of the tenant | Allowed. It is routing state that must survive the tenant object's eviction, not ephemeral data. One write per gateway and tenant, never per frame. |

## 5. A01 part 1

- The interface (`SchemaTransform`, `TransformError`, `addedObjects()`, the T5 capture SQL) is what K02 and K03
  build on.
- `addedObjects()` is the only exclusion list for M02, P03 export and the checksum tools. The P03 export
  back to a Rust file must also drop every added object (scratch, queue and partition tables), so a restored
  file equals Rust's schema. That is part of A01 part 2's acceptance.
- The T3 generated columns on `recordStorage_roles` are visible to `SELECT *`. Part 2 must show that no
  Whagons module SQL and no runtime record-view code reads `recordStorage_roles` with `*`.
- Part 2 measures cold wake and memory with the 1,152 added objects, against S1's 997 ms median.

## 6. Harness

`thin-sdk-parity` flakes between two Rust backends (`opens[0].cursor.revision` r5 vs r6; 3 of 11 runs on
the base fixture, K04). It is a harness bug: the scenario must not depend on a timer-driven revision. K05
owns the fix as a separate small PR to `cloudflare/conformance/`. No assertion is loosened beyond making
that one value timing-independent.
