# Local Replica snapshot and permission source bindings: source stage

Work in progress after pushed head `68987788ca04738cb0c2470e448aa39dad796886`.
The source checkpoint is `bd8720dadea70d1a2c00a632cb547b9053e06eff`. SDK/CLI
builds and five focused declaration/extraction tests passed in the granted window.
Rust verification is incomplete because the original check was cancelled during
severe host filesystem I/O contention, before any compiler diagnostics. Prior
stage Rust tests do not cover these edits. No integration or serving activation.

## Snapshot slice

`replica_permission.rs` is a feature-gated child of `replica.rs`, with access to
its existing window collector and snapshot/ready builders. Explicit private
registration pins a real loaded collection, module generation/artifact, normalized
binding and full source DDL. The canonical projection identity covers the entire
collection definition and DDL: types, filters, projected columns, ordering and
budgets. Physical path/instance, exact index generation, source receipts,
authoritative active Member and same-snapshot clock remain mandatory.

An explicit owned-fixture store operation installs source-row dirty guards.
Registration and every read verify exact guard SQL. INSERT/UPDATE/DELETE clears
row-source receipts and readiness atomically, including writes bypassing the
normal change-feed writer. DDL/guard changes reject. All row sources must be
replaced and sealed before publication. No ordinary provisioning installs these
guards. Normalized grant/group facts remain authoritative; this does not infer
Whagons source mappings or authorize an application-managed opaque mask.

The actual source projection joins the indexed distinct admitted key relation
before collection filters, deterministic order/key tie-break and maxRows+1.
SQLx streams projected JSON through the existing byte/row collector, canonical
hashes and message builders. No full-list Rust authorization loop. SQL-key type
validation currently scans the source key relation: this is real work and must
be included in later query-cost results. No speed or capacity claim.

The initial slice requires one non-null TEXT primary key, default binary
collation, projected key/order fields, an explicit ASC/DESC direction and no
JSON order field, BLOB declaration or explicit collation. Existing JSON/BOOLEAN
projection and typed filter binds use v2's row encoder and scalar bind rules.
Unsupported cases reject; source declarations cannot supply SQL fragments.

`tests/permission_replica.rs` prepares exact v2 `open_replica_fresh` comparison:
real collection, normalized SQL, full projected rows, hashes, messages, row/byte
windows, ASC/DESC, NULL order, ties, SQL NULL/JSON null, BOOLEAN/JSON projection,
filter NULL, exclusion, same-TX revoke+row insert, boundary refill, raw dirty
write rejection and rollback. This is synthetic source-prepared coverage only.

The result deliberately has no subscription, cannot resume, and is unreachable
from wire handlers. Current v2 defaults remain unchanged. Repeated cold SQL reads
recompute a bounded window including its replacement boundary; incremental live
boundary refill is not implemented. Legacy message metadata in this comparison
is not primitive sharing/resume authority.

## Commit and admission lease barrier required before delivery

The existing encoder helper does not implement this barrier. A future private
per-database publication coordinator must own a monotonically versioned immutable
admitted-set object and recipient lease directory. Its authority includes physical
instance/epoch, full manifest/provider/dependency/policy/projection identities,
canonical exact grants AND admitted rows, index/snapshot generation, revision,
principal-specific residual identity and a validity deadline. Args, filters,
window/order/budgets and representation are additional delivery-contract keys.
Digest equality alone never authorizes reuse.

1. Writers hold the database's publication write lease before final validation
   and COMMIT. Source facts, normalized facts, dependencies, receipts, serial,
   revision and pending recipient invalidations are one SQLite transaction.
   Failed validation/rollback publishes no new authority or message.
2. Keep that lease across COMMIT and synchronous installation of the committed
   authority/version. Invalidate old recipient leases and purge/cancel pending
   frames that depend on revoked authority BEFORE releasing the write lease.
   A coordinator crash/uncertain commit poisons the directory; reconstruct from
   durable state before permitting primitive enqueue. No awaitable network send
   occurs inside the SQLite writer; only bounded in-process publication.
3. A snapshot preparation is read-only and starts AFTER admission. It retains
   its exact read identity. Enqueue acquires a publication read lease, validates
   current immutable authority plus principal/collection/expiry, then atomically
   attaches that version to the recipient's bounded queue. If revision advanced,
   reconcile from a complete ordered delta or reject/reprepare. Validation and
   queue insertion cannot be separated by a writer publication.
4. The actual socket dequeue must also validate the attached lease/version (or
   participate in the same publication barrier). Otherwise a frame queued before
   revoke can be emitted afterward. Cancel or reject stale frames; envelopes,
   ACKs/backpressure and client windows remain recipient-owned. Already delivered
   bytes cannot be recalled; tests must define commit/dequeue ordering precisely.
5. Expiry is a versioned transactional invalidation with deadline checked both
   at enqueue and dequeue against a monotonic validity lease. If an expiry event
   is delayed or unknown, reject after the deadline. No unobserved wall-clock
   permission predicate or cached frame may outlive its authority.
6. Feed gaps, policy/module swap, failed index/backfill, replaced database,
   membership revocation and admission reset poison leases before future enqueue.
   An exact same-set cache hit does not bypass admission or readiness checks.

This requires explicit race/property tests around commit/rollback, queue/dequeue,
revoke during preparation, multiple writers/events, expiry, disconnect and
backpressure. None of this is enabled by the cold fixture snapshot entry.

## Complete candidate index plan

The current normalized diff still captures the whole finite scope before and
after. Replace it only after the source compiler emits a dependency certificate
for every supported leaf/operator. Required indexed relations, prefixed by full
scope and generation, are:

- source occurrence (logical binding + base/join alias tuple of typed row keys +
  projection occurrence + derivation version) -> grant/group/row fact support;
- source table/key/column -> old and new occurrence tuples/subjects/row keys;
- subject <-> groups, group <-> parent groups, role scope <-> assignments, and
  effective subject atom <-> supporting occurrences (deduplicated paths);
- slot+atom -> row keys, row key -> slot/atom occurrences, universe -> row keys;
- parent/via key -> referring children, plus bounded depth/alias occurrence;
- policy leaf -> rows/subjects/global inputs, including HasAtom, negative-only,
  no-positive-hit and finite-universe complement branches;
- collection -> exact source/filter/order/projection columns and boundary index.

Before mutation, capture affected old occurrences and candidate admission in TEMP
relations under the writer. Expand OLD and NEW joins/targets, changed support
zero/nonzero transitions, group reach and parent/via reverse links to a fixed
point with finite checked bounds. Include directly written rows independently
of authorization changes. Preserve alternative supports and memberships; a path
removal is not a final atom revoke. Apply all batch edits, compute NEW occurrences,
then evaluate the COMPLETE candidate key/subject union with indexed SQL against
true before/after state. Negative additions can remove rows; negative removals
can add rows without any positive atom addition.

For a HasAtom or global/policy/default/universe input, the certificate can require
the whole named universe/all affected principals. Do that work honestly; never
prune only to positive hits. Unknown dependency, unbounded expansion, unsupported
coercion, missing reverse image or gap rejects primitive publication until rebuild.
Large real permission changes can correctly touch many keys.

Order/projection-only writes still produce updates/movement. A bounded affected
window queries an indexed replacement after its exact (order,key) boundary and
recomputes row/byte limits; candidate membership differences alone cannot prove
window completeness. Compare candidate+refill results with the existing full-
scope SQL oracle before replacing it. Measure evaluated candidates, actual source
rows/joins, affected principals, SQL work, write amplification and index bytes;
report full-scope fallbacks explicitly rather than calling them constant work.

## Approval and remaining limits

Gabriel approved primitive API and Whagons branch implementation after the earlier
survey. That authorizes source work; it does not prove adapter correctness or
activate serving. Module SDK/compiler ownership is separate from `packages/client`
(hydration child). Whagons worker owns its existing isolated app checkout. Typed
source declarations, extraction, backend normalization and provider completeness
must agree; opaque app-maintained personal/ordinary slots are not an accepted
replacement for the actual Whagons permission program.

An approved private 28-dump local export is now available per parent. This worker
has not opened/scanned/restored it. Same-kit conversion/preflight and the complete
private copy oracle remain pending separate grants. No production/staging reads, schema changes, endpoint calls or
deployment are authorized. Query/write/disk/memory costs and full-app acceptance
remain unmeasured.


## Verification and artifact provenance

Owned LAB reservation: 07:00:03–07:16:55 UTC, 2026-11-05, with original hard end
07:30:03. Every heavy phase used headroom and coach resource admission; build
outputs stayed under `/mnt/data`. The original single Cargo check spent 13m21s
with about four CPU seconds and no output/diagnostics while host I/O was severely
pressured. Parent requested cancellation at 07:15:16. Only the owned unit was
stopped; it became inactive, all known owned child PIDs exited, and only the
owned lock was removed after ownership/inode checks. Wrapper exit was **143**;
direct Cargo exit was not captured because cancellation terminated the payload.
This is **INCOMPLETE/environment IO blocked**, neither a Rust source pass nor fail.
No second Cargo/read job, cap change or performance run was started.

The SDK and CLI TypeScript builds pass. Five focused JS tests pass: immutable
source declaration/export, callback/accessor/cycle rejection, exact old/new
source dependency certificate, unsupported/coercing/opaque input rejection, and
artifact extraction/authority/real collection validation. Initial failures were
missing cached SDK dependency link and an absolute test entrypoint; both were
corrected. The first SDK command bootstrapped pnpm through Corepack in the owned
cache; later phases disabled Corepack network and reused project dependencies
read-only. No project dependency install occurred.

Paired artifact receipt:
`/mnt/data/work/gonvex-permission-stage3/artifacts/pairing.json`.
New local SDK version **0.5.2-permission-stage3.202611050700**, tarball SHA256
`3d45a19d0934e30921bde295f5807d7f6f557f134ac792ba3a7be408897d3c44`.
The CLI is an audited external executable tree, not an installable CLI package.
Parent approved explicit invocation of
`node /mnt/data/work/gonvex-permission-stage3/artifacts/cli-dist/index.js`, while
keeping the app's freshly installed staging.31 CLI identity unchanged. No alias,
silent pnpm reroute or registry/version claim. Cached CLI dependencies originate
from the integration checkout's existing CLI dependency tree and are read-only.

Both artifacts were **built from dirty fingerprint**
`91067d45d9e480292a94e479e64477785547d33c8ae7ab5956e974e75017139a`;
matching checkpoint `bd8720da` was frozen **afterward**. Every SDK/CLI TypeScript
input blob was then verified against the receipt. No exact-commit-at-build claim.
The initial source patch is preserved privately, SHA256
`6f41eccff9cc62657177c557b863bc60eccfbf605ae17c5ba136a33f86ebbfaf`.

App source audit currently has 20 owner-policy table candidates, **19 tables with
40 actual Replica paths**; one candidate has no Replica path. Twenty paths lack
orderBy and are unsupported by this first cold adapter. Policy/source declaration
coverage is not serving/window acceptance. The next adapter gap fixtures and
exact collection shape matrix are maintained by the app worker. Only one exported
`permissionPrimitives({...})` program is extracted per module; unsupported task
source staging is a plain typed literal, not a second helper call.

Full feature-off/on Rust tests, real source-to-facts/window/message oracle runs,
full SDK/CLI regressions, required generators and cycle checks remain pending.
The checkpoint has not been pushed. Write amplification and query/disk/memory
costs were not measured; source ingestion currently rebuilds full logical batches,
and scopes conservatively reject stale coverage. Content-only writes must not
change the canonical Member identity atom set. No live feed, network selector,
production-data processing or staging schema migration was enabled.
