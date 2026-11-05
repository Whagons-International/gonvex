# SDK resume hydration fence: core verified

Branch `perf/sdk-resume-hydration` starts at exact integration
`2b7e2ff9f6950ea73a4659c284c9204262314574`, in the existing owned checkout.
Reviewed runtime branch `perf/reload-delta` at `387ce467` is preserved. No runtime,
app, wire contract, dependency, package version or built SDK artifact changes.
The core source and tests passed the explicitly granted verification window
on 2026-11-05 UTC, after heap's explicit release. Shipping package composition
and observer recapture still require a separate grant; core proof is not a
shipping fix claim.

The [shipping observation](shipping-sqlite1-capture.md) and Step 19 establish
that all 41 missing warm cursors had same-scope stored checkpoints before the
open and became available after hydration (37 empty / 4 populated). That
observation authorizes this ordering fix; this branch has no new capture or
whole-app CPU/capacity result.

`sendReplicaOpen` now waits on the current durable `replicaReady` before
constructing any resume request or entering the open batch. Per-subscription
waiters share that activation and collapse duplicate calls. A successful
activation marks only its own promise hydrated. After the wait, current
publication/outbox/socket/scope ownership, auth-frame identity, subscription
object/id/verification generation, connection, listeners and opening state
must still permit the open. Replaced waiters cannot remove their successors.
Close, final unsubscribe and replacement hydration discard pending ownership;
failed or obsolete activations cannot fall back to cursorless opens.

Batches construct requests only at flush, rechecking current hydration and
owner per frame. An auth-deferred batch releases opening/socket markers so
the existing preserved-subscription auth completion can resume it. The gate
covers collection opens only: control queries/RPCs, journal admission and
outbox bootstrap are not placed behind row hydration or behind a wait that
needs their progress. Existing authentication admission rules are unchanged.

## Deterministic core verification

`packages/client/src/sdk-resume-hydration.test.ts` controls real in-memory
storage loading with explicit promises, then observes send/construction and
flush. Fake timers drive only the actual batch timer; no elapsed-time sleeps
or polling determine the ordering. Executed cases:

- Forty-one stored checkpoints, alternating empty/nonempty, restored before
  all opens; single and batched modes; duplicate sends collapse and one scope
  load occurs. Every emitted request must carry the stored cursor/digest.
- Auth frame, socket generation, scope, disconnect, unsubscribe, replacement
  subscription, verification cancellation and close while hydration waits.
- Ownership changes after hydration but before batch flush; no request may
  even be constructed for a stale owner.
- A retained subscription with no listeners, later listener return, auth
  batch deferral/release with preserved subscriptions, replacement waiter,
  rejected hydration, queued batch followed by scope failure, and a stale
  promise completing after a newer readiness promise is installed.
- Outbox journal admission completes while durable row activation is held.

The existing preserved-batch regression is adjusted to establish a connected,
hydrated transport explicitly before invoking its private send methods.

The exact commands, direct exits and test counts from the completed core
window are recorded below. No test ran during heap's hold. No source fix was
needed after executing the prepared regressions.

## Shipping package composition is a separate acceptance step

Integration base `2b7e2ff9` contains **no subscription defer/release API**.
Its existing pending batches and retained-listener ownership are fenced here.
The actually shipped SQLite.1 package has an additional patch on which the app
relies for explicit defer/release. Passing these base tests cannot establish
that patched package's behavior, and replacing it with a plain base build
would lose required behavior.

Only after base test proof, prepare a separately identified isolated package
that composes the existing shipping patch with this fence. Preserve its
existing API and deferral semantics; record baseline package/patch/source
hashes, exact composition diff, resulting artifact hash and source provenance.
Check defer/release before and during hydration, nested/retained subscriptions,
release after scope replacement or failure, cancellation, and auth-preserved
batch handling. Failed/stale owners must leave no pending map or deferred
request that a later release can resurrect. Do not infer new API authorization
from the missing base API or silently ignore the shipping overlay.

A later explicitly admitted observer recapture of that composed package must
show all 41 proven stored-checkpoint paths opening with cursors, matching
path/args/scope/generation and preserving the shipping collection set. Until
then, no claim that the actual shipping cursor issue is fixed. No packaging,
SDK installation into the app, endpoint, browser, staging/production access or
application feature change was performed by this source-preparation step.

## Source follow-up before the verification grant

The initial `replicaReady` is a resolved placeholder. Its initial hydrated
marker now refers to that same placeholder, avoiding an unmarked fulfilled
promise in the field initialization. Construction always replaces it through
`setReplicaReady` with the unauthorized initial scope activation before the
client is exposed. The sole assignment enabling authoritative scope is in
`activateReplicaDirective`, immediately followed synchronously by
`setReplicaReady(activateScope(...))`. All later readiness assignments use
that setter. Its fulfillment callback is registered before open waiters;
reusing the same promise is a no-op. A prepared regression covers opening
immediately after installing an already-resolved promise, repeated setter
calls, exactly one construction/send and zero remaining waiters.

Additional prepared tests exercise the public-control RPC send bypass while
both auth and row hydration are held, plus 257 opens split across two frames.
A first-frame transport callback changes auth ownership: the old second
frame must not construct/send, and must release its unsent opening marker.
If that callback instead queues a replacement attempt, the old flush must
neither send it nor clear its pending/opening ownership. Per-attempt owners
are captured when the flush begins and rechecked against the current weak-map
owner per frame. This small source correction addresses that discovered
unsent-marker case; its prepared regressions subsequently passed in the
explicit core window below. No build/test/packaging or new lab claim occurred
during heap's reservation.

## Completed core window: 2026-11-05 06:46–06:50 UTC

Verified source is exactly `728b857e456d6d7a9f299883fdf1ba17c8e594b9`
(including its parent `2743e0a0`). No source correction was needed during this
window. The task branch remains separate from integration. Runtime branch
`perf/reload-delta` is unchanged at `387ce467`.

All heavy phases passed build-headroom immediately beforehand and ran under
new admitted resource units with unchanged existing policy/compile caps.
Typecheck/tests/generators requested 5 GiB, 2 CPUs and explicit 200% CPU quota;
the AST graph check requested 2 GiB/2 CPUs. Cached dependencies were reused
read-only through ignored node_modules links. There was no dependency install,
implicit pnpm repair or package build. New Vitest caches/config/logs and the
client incremental typecheck file are under
`/mnt/data/work/gonvex-sdk-resume-hydration`. Docs generator cache/output
uses the same child's existing `/mnt/data/work/gonvex-reload-delta/docs` links.
Resource wrapper metadata cwd remains the owned `/home/gabriel` checkout.

Commands below ran from `packages/client`, directly invoking existing tools
to avoid pnpm's dependency repair. `CONFIG` denotes the committed
[test configuration](evidence/sdk-hydration-vitest.config.mjs.txt).

```sh
node node_modules/typescript/bin/tsc -p tsconfig.json --noEmit --incremental \
  --tsBuildInfoFile /mnt/data/work/gonvex-sdk-resume-hydration/client.tsbuildinfo
node node_modules/vitest/vitest.mjs run --config "$CONFIG" \
  src/sdk-resume-hydration.test.ts src/sdk-resume.test.ts
node node_modules/vitest/vitest.mjs run --config "$CONFIG" \
  src/auth-generation.test.ts src/sdk-lifecycle.test.ts \
  src/sdk-delta-lifecycle.test.ts src/sdk-persistence-barrier.test.ts \
  src/cached-startup.test.ts src/external-auth.test.ts \
  src/control-watch-ownership.test.ts src/control-watch-close-order.test.ts \
  src/outbox-recovery.test.ts src/outbox-admission.test.ts \
  src/outbox-settlement.test.ts src/local-replica-hydration.test.ts src/index.test.ts
```

- Client typecheck: direct exit **0**, [output](evidence/sdk-hydration-typecheck-1.log.txt).
- Focused tests: direct exit **0**, **40 passed** (26 new hydration cases,
  14 existing resume tests), [output](evidence/sdk-hydration-focused-1.log.txt).
- Existing lifecycle run: direct exit **1**. **264 tests passed across 12
  loaded files**, but the persistence-barrier suite failed **before loading**
  because the source adapter's `@gonvex/client` dependency link was absent in
  this isolated checkout. This was not a passing full invocation; preserve
  the [initial failure](evidence/sdk-hydration-lifecycle-1.log.txt).
- Resolver-only repair: add an exact test alias `@gonvex/client` to this
  branch's `packages/client/src/index.ts`, so the source SQLite adapter tests
  this actual SDK rather than a cached shipping artifact. No source,
  dependency store, lockfile or package artifact was changed. Rerun only the
  affected `src/sdk-persistence-barrier.test.ts` with that config: direct exit
  **0**, **99 passed**, [output](evidence/sdk-hydration-persistence-2.log.txt).

Total verified: **403 distinct tests across 15 files**, with the initial
suite-resolution failure retained and explained. The lifecycle selection
covers actual auth scope changes, preserved/reconnected subscriptions,
publication cancellation, durability barriers on memory/legacy/indexed and
SQLite adapters, outbox admission/recovery/settlement and local hydration.
The new suite proves the initial-resolved-promise path, 41 checkpoint resumes,
public-control send bypass, journal progress, between-frame retirement and
reentrant replacement ownership. These are controlled unit/lifecycle tests,
not a shipping package or browser capture.

Before push, from repository root:

```sh
pnpm --config.verify-deps-before-run=false --filter @gonvex/docs typecheck
```

Docs metadata/route generators and typecheck passed with direct exit **0**;
no tracked generated file changed. See [output](evidence/sdk-hydration-generators.log.txt).
No Gonvex schema, agent API or i18n generator inputs changed. Client package
build/packaging is intentionally deferred to its separate composition grant.
Whitespace checks passed.

A cached TypeScript 6 AST parser compared the internal workspace runtime
imports/exports and literal dynamic imports reachable from client `index.ts`
against exact base `2b7e2ff9`, excluding type-only edges. The graph is identical:
39 modules / 66 edges. It contains one existing `local-runtime` cycle between
`portable.ts` and `read-view.ts` in both revisions: **no new runtime cycle**,
not a cycle-free graph. [Graph and base comparison](evidence/sdk-hydration-import-cycles.json),
[reproducible checker](evidence/sdk-hydration-import-cycles.cjs.txt).
External dependency graphs are outside this workspace-edge check; the patch
adds no runtime imports.

Claim was 06:46:29.868 UTC, hard end 07:06:29.868. Every owned unit exited and
no matching units remained before exact ownership assertion and lock removal
at **06:50:09.086 UTC**. [Release record](evidence/sdk-hydration-core-release.json).
No jobs remain, and no additional heavy work follows this release. No
shipping patch composition, browser, endpoint, app change, staging/production
access, CI work or integration merge occurred. Core acceptance is complete;
preserved shipping defer/release composition and observer recapture remain
separate, unperformed acceptance steps.


The separately granted shipping composition has now passed 40 actual-artifact
tests; see [composition report and later capture plan](sdk-shipping-composition.md).
That does not change the core source result above or establish an observed
shipping cursor/CPU win. The actual app installation/observer remains pending.
