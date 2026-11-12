# Cloudflare assembly proof ledger

Snapshot: 2026-11-12, after PR404 merge/publication and PR400 source-image merge3d92e133. Owner: integration lead.
Raw append-only coordination ledger: `/mnt/data/work/gonvex-cloudflare-lead/verification-ledger.md`.
This table records evidence domains; it does not replace any original `done_when` or gate.

| Gate / artifact | Actual state | Owner and next evidence |
|---|---|---|
| Source-image lane / PR404 | Merged `ca1ae8a0`; exact-source archives, layers and development registry publication verified. No serving deployment. | Lead: validate/publish the next K22 exact-source images. Final builder must include final full-cloudflare source. |
| K22 / PR400 | Published `3d92e1339b2d65dbc47ac2f5713f56bd9e57d738` adds the export lane to checkpoint `1c86e629`; serving code is unchanged, runtime `18afb02a5cccde933c571f7448a930ebafe574a0`. Author 869 workerd /4 Node /49 conformance-unit tests, typecheck and generators pass; 15 existing skips. Bundle1,678,656 bytes /401,105 gzip; cycles1→1. Acceptance UNMET. | K22 implementation owner continues. NEW independent1c86 recheck clears bounded R1/0070 paths:191 workerd /53 Node tests, cycles and SQLite generator pass. Pinned COMMENT5877478894 gives no acceptance approval; prior review history remains. |
| Local comparison | Full38 **FAILED**,34 PASS /4 FAIL in526s:31 two-backend PASS plus3 kernel-only PASS. Raw receipt-upgrade, session-handshake and session-frames fail. Public-limits setup503 left kernel assertions unexecuted. Isolated unchanged public-limits later passed separately. | K22: preserve campaign/raw failures; fix or prove actual platform behavior. Unchanged39th reducer-errors scenario remains locally blocked under0057, not waived. |
| Quiet fault / recovery | Author unchanged-SDK controlled registration-fault probe passed12 digests: real error, single durable write, same-subject receipt, intent retirement/revision liveness; two exact processing-fact cleanup cycles and later writes. | Fresh verifier checks source/evidence boundaries; this does not prove all lifecycle or deployed faults. |
| Owner lifecycle /0069–71 | Finite UNKNOWN guard exists; token prototype, terminal drain, stranded durable OWNER cleanup and indefinite churn availability remain unproved. | K22: bounded prototype and actual reentrancy/restart/late-call/ambiguous-outcome tests; lead records decisions; new exact-artifact review after implementation. |
| Private platform / K22 | No assembled Worker/Container deployment or deployed config IDs. CPU enforcement, durable causal marker, caller quarantine/mapping, unaffected clients, real SDK/deploy and full package matrix remain open. | K22 prepares exact-source image campaign and bounded `gonvex-cf-dev-*` proof; lead verifies archives/publishes. No local synthetic timeout pass. |
| Additional package domains | Enabled K13 files differ from baseline no-object-store configuration; K02/K11/K12/K13/K17/K18/K20/K21 assembly gaps remain explicit in K22's catalog. | K22: real handler/config-aligned scenarios, matching Rust enabled-storage domain, actual Container/DuckDB and all missing scenarios. No test adapter substitutes for deployment proof. |
| X01 / PR397 | `6b2ec526` source recheck clear within stated scope; original acceptance UNMET. | Assembled runtime first, then V03 26 tenants /1,000 users while scale-out/in actually occurs, hot-tenant/quiet-neighbor and storm/fault evidence. Evaluate X02 honestly. |
| V01–V06 / capacity / security | Not passed. No assembled deployed capacity curves, safe user count or fleet elasticity claim. | Lead coordinates after assembly/review. Measure backend and generator bottlenecks, durable-confirmation/receiver tails, correctness, SQL/CPU/isolate-memory/bandwidth/cost, quotas and shared Control/auth/limiter pressure. |
| Uptime / tracing / M05 | Canonical plan exists; no external monitor/status/DNS/live alert activation or uptime achievement. Original five M05 paging definitions still require original source. | Lead prepares portable code/config and real dev drill evidence. Missing heartbeat is unknown;99.9% is owner-proposed, not SLA. No purchases or unsolicited notifications. |
| R01–R06 / migration | Original dependencies/G3b and actual-duration14-day/30-day soak/shadow/backup/rollback gates remain. No live staging switch or production load/write. | Baseline and candidate work follow written dependencies; prepare a reviewable staging candidate only when evidence supports it. |

Separate reference observation: both Rust and runtime18afb accept an invitation sharing Bob's earlier
`board.add` receipt key while preserving its foreign receipt and prior admin role. The actual authorized
principal probe is `/mnt/data/work/gonvex-k22/invitation-collision-2/`; the first protected-data404 probe is
retained. This is an observed reference quirk, not a unilateral K22 path-validation fix. Outside-gate
behavior is preserved. Any hardening needs a separate both-implementation decision covering stored-path
validation and Control claim/error/retry semantics; R1's exact-path read-only migration-gate exception is
separate.
