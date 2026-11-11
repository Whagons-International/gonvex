# Kernel visibility (K09)

Visibility masks v2 in the tenant object, ported from the Rust runtime (`rust/crates/runtime/src/visibility*.rs`,
`principal_cache.rs`, `permission_bits.rs`; design and contract in `docs/visibility-masks.md`). It covers the three
forms of a plan, which must agree on every row and principal: compiled SQL, the interpreter, and class programs. It
also covers the parent ACL indexes, the principal cache and the permission bits.

```ts
import { VisibilityRegistry, createQueryVisibility, parsePlan, resolveWithParents, readClock,
  compilePredicate, rowMatches, memberVisibility, rowValue, EvalMemo } from "./index.ts";

const registry = new VisibilityRegistry();                 // one per tenant object
registry.setPermissionGrants(manifest.permissionGrants);   // optional manifest projection

// Inside one K01 invocation (one snapshot): StorageSession satisfies VisibilitySql.
const plans = new Map(Object.entries(manifest.visibility).map(([t, p]) => [t, parsePlan(p)]));
const acl = registry.acl.ensure(session, plans, [plan]);   // parent/derived indexes at this snapshot
const clock = readClock(session);
const resolved = resolveWithParents(session, tenantSession, plan, clock.revision, plans, acl,
  undefined, { cache: registry.principals, clock });       // SQL only on a cache miss

const params: unknown[] = [];
const where = compilePredicate(plan, resolved, "r", params);          // snapshots and windows
rowMatches(plan, resolved, row);                                      // the reference evaluator
const handle = memberVisibility(plan, resolved, "tenant");            // class program; release() when done
if (handle.kind === "compiled") handle.member.visibleMemo(memo, changeRow);

registry.apply({ epoch, revision, changes });   // delivery pushes each committed transaction (K10)
registry.permissions?.sync(session);            // before a Reducer's first ctx.permissions.has (K04 port)
const port = createQueryVisibility(registry);   // K08's QueryVisibility (kernel/query/visibility.ts)
```

| File | What | Rust |
| --- | --- | --- |
| `plan.ts` | Manifest shape (serde defaults), validation with Rust's texts, `validateManifest`, role pruning, active sets, ACL and index columns, dependency columns, principal anchors | `visibility.rs` |
| `mask.ts` | The interpreter (`rowMatches`), the reference semantics | `visibility_mask.rs` |
| `sql.ts` | `compilePredicate`, `compileSet` | `visibility.rs` `compile_*` |
| `class.ts` | Compiled programs, shared classes, residuals, stable ids, `EvalMemo`, `ClassScope.candidates` | `visibility_class.rs` |
| `principal.ts` | Resolution (member row, principal, grant and derived sets), `resolveWithParents`, `recomputeDerived`, grant contributions, the `*ChangeConcerns` routing | `visibility.rs`, `visibility_snapshot.rs` |
| `acl-index.ts` | `ParentAclIndex`, `GrantIndex`, readiness | `visibility_index.rs` |
| `registry.ts` | `ParentAclRegistry`: load, projection, catch-up, push, poison | `visibility.rs` `ParentAclRegistry` |
| `feed.ts` | The clock and paged reads of `_gonvex_sync_changes` | `change_feed.rs` (as read here) |
| `principal-cache.ts` | `PrincipalCache`: per (plan sets, account), change-log probe, capture classes | `principal_cache.rs` |
| `permission-bits.ts` | `permissionGrants` projection as per-member bitsets with reference counts | `permission_bits.rs` |
| `query-port.ts` | K08's port over manifest JSON, `maskChangeRelevant` | `live_query.rs` `mask_visibility_change_relevant` |
| `tenant.ts` | `VisibilityRegistry`: what one tenant object keeps | |

## Invariant V1

`rowMatches`, the class programs and every `*ChangeConcerns` function are pure, synchronous functions of one row and
one resolved principal (plus the tenant-shared indexes already in memory). They take no SQL handle. Only
`principal.ts` resolution, `registry.ts` loads and catch-up, `principal-cache.ts` probes and `permission-bits.ts` sync
take a `VisibilitySql`. A write never makes a connection run visibility SQL. Index maintenance is one read per
index per wake (catch-up), or none at all when delivery pushes the transactions it has already read.

## How it adapts to the Durable Object

- **Synchronous.** Durable Object SQL is synchronous and the object runs one event at a time. So resolution, index
  loads and probes are plain function calls inside the caller's K01 invocation. Rust's single-flight locks and its
  applier tasks are not needed.
- **Index completeness.** Rust subscribes each index's applier to the in-process feed before loading it. Here a load
  reads the clock and the rows in one synchronous step, so no commit can fall between them. `ensure` brings every
  index it returns to the snapshot's clock, in one of two ways. Delivery can push the transactions it already read
  (`registry.apply`), or `ensure` reads the log for the index's own table (`gonvex_sync_changes_table` index,
  1,000-row pages). An epoch change, a log pruned past the index, a gap in pushed revisions or a changed projection
  poisons the index, and the next `ensure` reloads it.
- **Class lifetimes.** Rust frees a class when its last handle drops. Here `MemberVisibility.release()` (or
  `HandleCache.clear()`) releases a binding, and a class leaves its scope when its last member is released.
- **Capture classes.** The principal cache caches only resolutions whose input tables are captured under their own
  name. Kernel capture triggers are plain SQL (A01 T5), so an entity table is one whose three
  `_gonvex_sync_<table>_*` triggers write `_gonvex_sync_changes`. A record view counts when its storage's triggers
  stage `_gonvex_record_pending`. Rust looks for the `gonvex_capture(` call instead. Without `PRAGMA
  schema_version`, the cached classes are dropped by `invalidateSchema()`, which the caller makes alongside K01's.
- **Permission bits.** K04's `PermissionIndex` port (`has` → `boolean | "rebuilding"`). The tenant object calls
  `sync(session)` inside the Reducer's transaction before the first `has`. This replaces Rust's wait until the bits
  cover the transaction's revision. A rolled-back Reducer never flips a bit.

## Differences from Rust

1. **Numbers.** A parsed JSON number does not keep integer/float identity. `1` equals `1.0` here, as it does in
   SQLite. Serde says they differ, so Rust's interpreter (not its SQL) treats them as different. An integral float's
   token text is `1`, not `1.0`. Visibility tokens are ids (TEXT), and the Whagons plans compare no REAL column.
2. **Fingerprints and stable class ids** are SHA-256 over canonical JSON (object keys in code-point order), not
   Rust's serde byte order. Neither is on the wire: fingerprints are server-side resume anchors, and the class id in a
   batch header is informational (`delivery.rs` `class_id`). Stable ids are 16 hex characters.
3. **Shared parent resolutions** (Rust `shared_parent_resolution`, a memory optimization that deduplicates identical
   parent resolutions across subscriptions) are not ported. The principal cache already shares the SQL part. R03/R04
   own heap per object.

## Verification

`pnpm test` in `cloudflare/` runs these in workerd against Durable Object SQLite:

- **`test/corpus.test.ts`: the `done_when`.** All four tests of `visibility_corpus.rs` with the same 8 plans, 12
  principals (three of them principal changes), 216 tasks and 68 notes. The SQL compiler runs on Durable Object
  SQLite through K01's kernel SQL, and its result must equal the interpreter and the compiled program (value, raw JSON
  and memo) on every row: 96 SQL cases and 68,736 row checks. Also covered: irregular rows, class sharing and moves,
  and the soundness of `candidates`. Deliberately broken versions (a member atom that is always true; `IS NOT TRUE`
  replaced by `IS FALSE`) fail it.
- `test/masks.test.ts`: every `mask_tests` case of `visibility.rs` (role pruning, grants, grant joins, principal
  routing, derived sets F1–F3, `inheritsAny` F4, predicate override F5, inherited SQL), the `visibility_index.rs`
  tests, the token test, validation texts and manifest inheritance checks, and the `permission_bits.rs` unit test.
- `test/tenant.test.ts`: the corpus principals rebuilt from permission rows and resolved with SQL. All three forms
  agree on them. Indexes are caught up from K03's change log after data and permission writes, and the push path
  poisons on a gap or a foreign epoch. The principal cache records hits, probed hits and invalidations. Also covered:
  the ephemeral developer principal, capture classes, permission bits equal to SQL through 9 commits and a rollback,
  and K08's port.
- `test/whagons.test.ts`: the 17 real Whagons plans (`rust/crates/runtime/src/perf_fixture/whagons_module.json`)
  validate, all compile without fallback, and agree in all three forms over 24 seeded random principals: 189,408
  checks. The largest SQL predicate binds 48 parameters (limit 100).
- Conformance `visibility-masks` (`cloudflare/conformance/scenarios/`): masks v2 through Replica Collections for
  three Members. It passes Rust against Rust. It runs against the kernel once K10 delivers replicas.

## Numbers

Local workerd, `test/tenant.test.ts`: the first resolution of a task child (`taskNotes`) with its `tasks` parent is
16 statements and 393 rows read. That covers the member row, six sets for each of the two plans, and the parent
index load of 216 tasks. A later open of the same plan in a snapshot is 1 statement and 1 row read (the clock), from
the principal cache. Row checks after resolution read nothing. The package is not yet imported by `src/index.ts`, so
the Worker bundle is unchanged. K07, K08 and K10 wire it in.
