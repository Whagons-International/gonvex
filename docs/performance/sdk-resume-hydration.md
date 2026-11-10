# SDK resume hydration ownership

This staging follow-up starts at `512e61186d7c4abda40ceca329136e10761f6307`.
It carries source commits `2743e0a0` and `728b857e` as separate, patch-identical
commits `76a7fb2e` and `cd31d5e6`. Existing staging persistence-barrier tests and
all newer base behavior are retained. No runtime, app feature, package version,
wire contract or registry publication changes are included.

The original shipping observation found 41 cursorless warm opens whose
same-scope checkpoints were already stored and became available after
hydration. This ordering fix waits for the current durable `replicaReady`
before constructing a collection resume request or adding it to an open batch.
Per-subscription waiters share activation and collapse duplicate sends. Failed
or obsolete hydration cannot fall back to a cursorless open.

After hydration, construction and batch flush both recheck current scope,
auth-frame identity, socket/connection generation, subscription object/id,
verification generation, listeners, opening state and per-attempt ownership.
Close, final unsubscribe and replacement activation discard pending ownership.
An old waiter or a reentrant batch flush cannot clear or send its successor.
A successful activation marks only its own readiness promise hydrated; an
already-resolved promise and repeated setter calls still produce one open.
Auth-deferred batches release unsent markers so the existing preserved
subscription path can resume them.

Only collection opens wait for row hydration. Control RPCs, journal admission
and outbox bootstrap retain the progress needed to complete startup. Existing
capabilities, retry protocol and collection limits stay unchanged. Pending
hydration work belongs to its current subscription/attempt and is discarded
with that ownership.

## Verification on staging

The tested SDK source is `cd31d5e6a2090469fc5aadc7bf1084f6a661a7e2`.
Later edits to this document do not change its runtime source. All checks ran
inside the owned 6 GiB / 2 CPU aggregate with memory/IO and /mnt headroom gates.
Build output, caches, fixtures, browser profiles and reports live under
`/mnt/data/work/gonvex-staging-followups-20261105`.

- All eight package builds/types and 1,085 client tests pass. This includes
  deterministic deferred-storage tests for all 41 checkpoints, empty/populated
  windows, duplicate sends, stale scopes/auth/sockets, cancellation, replacement
  waiters, rejected hydration, listener return and outbox/control progress.
- A 257-open batch crossing two frames tests reentrant auth/attempt replacement:
  the old second frame neither sends nor clears a newly queued attempt.
- OPFS types and 19 engine checks pass.
- Actual Chromium IndexedDB storage, rollback, browser crash and upgrade recovery:
  53 tests pass. Three additional native browser intent/query/control ownership
  checks pass, retaining their stale-source and transaction-abort controls.
- Compiled-source oracles pass: 12 watermark/readiness schedules and three source
  controls, five intent schedules and two controls, 27 query schedules and nine
  controls, 38 Control Query watch schedules and 12 controls. Every deliberate
  stale-source mutation must fail its specified assertions; no control is waived.
- Both configured projects' codegen and agent API output were regenerated and
  are unchanged. Docs metadata/typecheck passes. Runtime import cycles remain
  the same two pre-existing edges, with none added.

The private canonical source is an exact archive, not another checkout. The
native receipt's read-only `git rev-parse` uses the existing owned checkout's
GIT_DIR only after asserting that its HEAD equals that archive pin. Private
browser launchers add `--disable-gpu` and retain their original finally-close;
these safety overlays and their hashes are recorded outside the branch.
Initial metadata/PATH setup failures are recorded separately. The metadata-hook
per-step log was overwritten before archival; its incomplete receipt, observed
error note and tool transcript remain. Corrected runs reached every assertion.

## Shipping composition stays separately pinned

The historical SQLite.1 app still uses the approved composed candidate at
`/mnt/data/work/gonvex-sdk-composition/candidate-2`, with entry SHA256
`60b2673135e0ffd245b2219fb8a18cd2f6add3ac98ccbfa6af9cb6f5ae20eb0a` and 128-member
payload map `392387f207e602ecf09bc5c3dff4fce9fd7a385bf62a5239676cdb4bbe71ae33`.
Its authorization freeze is `2c784cc9`; that records the actual hydration core
`728b857e` and composition `c2d283b7`. The shipped explicit defer/release patch
is a separate dependency closure. This source PR neither replaces that artifact
with a plain library build nor applies a PostgreSQL or optional starter overlay.

The corrected 15cb oracle and frozen shipping composition separately passed
1,056 matched-principal/authority/window/cursor/full-row comparisons against the
staging OOM candidate, including cached opens and actual share/revoke. That is
not a claim that this plain SDK build equals the composed shipping artifact.
The original full Mac gate remains FAILED and immutable. No new full1k series,
capacity, full Whagons/starter acceptance or permission-SLO pass is claimed.
