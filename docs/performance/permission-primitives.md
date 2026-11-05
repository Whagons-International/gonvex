# Experimental local indexed permission backend

This is a verified synthetic backend stage, not a replacement for v2 or an
approved Whagons/public API. Base: `e5812dfbcf27eca83ed0c224f40e954df91b4170`.
The reopen fix `e12c86565e80064034dbd406ae4807c304c053cc` remains included.

## What exists

The default-off `permission-primitives` Cargo feature enables private runtime
modules and a store feature. No Replica/WebSocket selector, automatic app
migration, SDK method, protocol field or provisioning change enables it.

The store owns an independent explicit local schema, normalized grant/row facts,
source-completion manifests and group edges. Different sources preserve alternate
supports. Indexed recursive SQL deduplicates group paths; effective support counts
count distinct grant sources, not paths. Final-batch group cycles reject and
poison the transaction. Explicit scoped roles lower to groups and atom grants;
a tenant role, team role and row-scoped role have different identities. No role
label automatically grants app permissions.

Runtime selection actually executes SQL over these normalized relations. Slot
joins and `UNION`/`INTERSECT`/`EXCEPT` perform admission without a full-row Rust
predicate loop. Results are distinct, binary-ordered string keys only. An
explicit finite named universe bounds subtraction. Empty conjunctions and unknown
operators reject; empty disjunctions deny. No global deny precedence is inferred.

The query adapter checks the actual connection path, authoritative active Member,
route/project/tenant, full canonical scope, physical database instance,
required index generation, epoch, exact revision, complete source receipts,
compiled policy identity and supported key-only projection in one transaction.
Incomplete, stale, failed, copied or future-generation indexes reject. The
physical UUID is bound to Unix device/inode; copying epoch/UUID does not create
another admitted instance. Installation is restricted to the canonical directory
`/mnt/data/work/gonvex-permission-fixtures`, requires the explicit local flag, and
is never invoked by ordinary provisioning. This fixture implementation is
intentionally local/Unix-specific.

The exact-set digest covers sorted, deduplicated length-prefixed atom bytes and
full project/database/tenant/epoch/table/collection/policy/projection/semantics
identity, including user-specific atoms. Canonical bytes remain the equality
authority; role names and digest equality alone do not authorize sharing.

`apply_and_diff` saves true pre-state keys in a SQLite TEMP relation before any
batch writes, applies all replacements, seals readiness, and computes both SQL
set differences. It deliberately queries the full finite scope: no claim of small
candidate work. Supported normalized facts, source receipts and readiness commit
or roll back together. Readiness is stamped after change-feed finalization and
before COMMIT, with a feature-gated store hook. Failed mutation helpers prevent a
successful commit.

The event-local encoding helper keys reuse by exact set, generation/serial,
revision, full collection contract, representation and member projection. It has
no global cache or network enqueue path.

## Verification, 2026-11-05 UTC

Owned window began 05:14:48 UTC, after explicit reload release. All heavy phases
used `coach-resources` with 6G/2CPU declared, existing compile wrappers/caps and
headroom checks before each phase. Actual owned target:
`/mnt/data/work/gonvex-reopen-target`; logs and generated outputs:
`/mnt/data/work/gonvex-permission-verification`. The wrapper's approved compatibility
cwd/receipt argument was this worktree; build/cache outputs stayed on `/mnt/data`.
No dependency installation, production/staging access or application endpoint
calls occurred. Only newly created synthetic local fixture databases were opened.

Commands from `rust/`, with that `CARGO_TARGET_DIR`:

```sh
cargo check --offline -p gonvex-runtime -p gonvex-store
cargo check --offline -p gonvex-runtime -p gonvex-store --features permission-primitives --tests
cargo test --offline -p gonvex-runtime --lib --features permission-primitives permissions:: -- --nocapture
cargo test --offline -p gonvex-runtime -p gonvex-store --lib -- --test-threads=1
cargo test --offline -p gonvex-runtime -p gonvex-store --lib --features permission-primitives -- --test-threads=1
cargo clippy --offline -p gonvex-runtime -p gonvex-store --lib --features permission-primitives -- -W clippy::redundant_clone
```

Both checks pass. Default tests: runtime **396 passed / 0 failed / 8 ignored**;
store **44 / 0 / 0**. Feature-on tests: runtime **402 / 0 / 8**; store **44 / 0 / 0**.
Six new tests pass, including a real SQL fixture with 64 deterministic generated
before/after batches compared against v2 SQL and the interpreted v2 evaluator.
The fixture also probes nested AND/OR/HasAtom/subtraction, alternate direct and
multigroup grants, final-support revoke, negative flips without new positive
atoms, row+grant changes in one transaction, rollback, cycles, incomplete
backfill, real readiness failure modes, revoked Members, scope/policy/projection
mismatches and copied-file identity. There were no observed mismatches in these
synthetic cases. They do not prove production equivalence.

Verification found and fixed a missing required-generation binding guard.
Follow-up cleanup removed new auto-deref lint warnings. Clippy passes with
pre-existing warnings outside the new implementation.

Before push, the current CLI was built and dashboard/template codegen rerun with
their original project IDs. This refreshed existing module manifests/bundles and
agent API artifact hashes. Source-file snapshots and function schemas are
unchanged. Template bundle changes are region path comments; dashboard also
refreshes existing SDK `dueWhen` validation already present in the pinned source.
These generated-only updates are isolated from the backend implementation.
`pnpm --filter @gonvex/docs typecheck` passes and regenerates docs metadata.
Cached dependencies were reused read-only with pnpm automatic dependency repair
disabled; outputs/caches were redirected to `/mnt/data`. No i18n generator exists
in the relevant package scripts/workflows.

Normal Cargo dependency trees with the feature off/on are identical, dependency
declarations match the base, and the new normal permission-module graph is
acyclic. Formatting and whitespace checks pass for changed implementation files.
Whole-workspace `cargo fmt --all -- --check` still fails in five untouched files:
`runtime/src/open_reasons.rs`, `runtime/src/replica.rs`,
`runtime/src/snapshot_cache.rs`, `store/examples/reader_warm_bench.rs`, and
`store/tests/reader_open_single_flight.rs` (under `rust/crates/`). Every file's
bytes equal the pinned base, and extracted base copies independently fail
rustfmt. Proofs/logs are retained in `baseline-format-proof.json` and
`baseline-fmt-*.log` in the output directory. These are isolated baseline
formatting failures, not permission test failures.

## Remaining acceptance gates

Live Replica/window/feed activation remains disabled. It requires exact app
source adapters, dependency-complete recipient/candidate selection, stable
projection/order/budget/window refill, and a commit/admission barrier that
invalidates old sharing on revoke/expiry before subsequent enqueue. The encoding
helper alone does not supply that barrier. Normalized permission-only writes do
not yet publish live subscription events.

There is no automatic Whagons port. Its PERSONAL rule must remain personal grants
OR (ordinary grants EXCEPT personal workspace rows), with workspace AND spot
conditions and overlapping ordinary grants inside that subtraction. Parent null
FK/absence/hidden-row distinctions, predicate replacement, existential via links,
correlated tuples, depth 1–4 spot ancestry, strict-true app flags versus v2 runtime
truthiness, SQL/JSON null and scalar/collation behavior remain unsupported adapters
until proven exact. Generic group DAG reachability is not an unbounded spot
adapter. Action permissions remain separate from read visibility.

No authorized full local production-v2 dataset has been verified. The hard
production-copy differential gate remains pending; partial or synthetic files
cannot substitute, and no remote production fetch is authorized. Write
amplification, disk/index bytes, memory, query CPU, reload/sign-in budgets and
large permission-change costs remain unmeasured here. This local verification
and push do not authorize integration, architecture acceptance or full-app use.
