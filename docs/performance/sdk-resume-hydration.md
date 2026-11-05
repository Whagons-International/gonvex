# SDK resume hydration fence: source preparation

Branch `perf/sdk-resume-hydration` starts at exact integration
`2b7e2ff9f6950ea73a4659c284c9204262314574`, in the existing owned checkout.
Reviewed runtime branch `perf/reload-delta` at `387ce467` is preserved. No runtime,
app, wire contract, dependency, package version or built SDK artifact changes.
This source patch and its tests are **uncompiled/unexecuted** pending an explicit
lab grant after heap's release (its extended deadline alone is not a release).

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

## Deterministic verification prepared, not yet run

`packages/client/src/sdk-resume-hydration.test.ts` controls real in-memory
storage loading with explicit promises, then observes send/construction and
flush. Fake timers drive only the actual batch timer; no elapsed-time sleeps
or polling determine the ordering. Prepared cases:

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

After an explicit grant, use admitted `/mnt/data` targets/caches and headroom
checks to run the client typecheck and focused Vitest hydration/resume tests.
Then run relevant existing auth-generation, SDK lifecycle/delta lifecycle,
persistence-barrier, control/outbox/cached-startup and storage hydration tests
as indicated by failures/ownership changes. Required docs generators and an
exact-base runtime import-cycle comparison precede pushing SDK source.
No test command has been executed during heap's hold. Source whitespace
checks pass; there is no passing test claim yet.

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
