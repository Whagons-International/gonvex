# Prepared shipping hydration composition (not executed)

Core source is tested at `728b857e`, evidence pushed as `14d3e6f8` on
`perf/sdk-resume-hydration`. This directory is source-only preparation for a
separate package build/test/observer grant. No composed package exists, no
artifact test was run and no shipping behavior improvement is claimed.

`shipping-source-provenance.json` inventories the read-only installed
`@gonvex/client@0.5.2-sqlite.1` from app `37ca8278b`, its full package file
hashes, package metadata and exact existing patch. Required baseline pins:

- `dist/index.js`: `4f9ce8be47c21742b49d760ebf0c695da13ab4c2f814d6df5a7d843a416fa2b4`
- `package.json`: `2dcf61a5a6afc97a4ba1036671e9525e88440d9f9b675232dfebd95f50a44d8e`
- Existing app patch: `06786316f912c262f7638b8f3c82cff02a8787f71eabcafca84b4087a3333334`

## Future composition strategy

1. Under the separate grant, create a complete owned `/mnt/data` copy of this
   exact installed package, including its exports, declarations, dependency
   versions and all existing patched behavior. Verify every inventoried hash
   before changing it. Do not build a plain main SDK and replace shipping.
2. AST-select only the tested core's three methods (`sendReplicaOpen`,
   `flushReplicaOpens`, `setReplicaReady`) and three private fields
   (`hydratedReplicaReady`, `replicaOpenHydrations`, `replicaOpenOwners`).
   Transpile those members for the shipping JS target during the **future**
   build phase. Add only the two approved lifecycle cleanups: close clears
   the hydration map; final unsubscribe removes its entry. Do not wholesale
   replace close, auth, subscription or generated-local-runtime methods.
3. Byte/hash-compare every other member and package file against baseline.
   In particular preserve existing `subscribeReplicaTransport` defer logic,
   `releaseDeferredReplicaOpens`, its declaration/option, auth mechanisms,
   unrestricted visibility patch and bootstrap behavior. No new feature API.
   Record any extra ownership correction separately with the test exposing
   its need. Avoid stale source-map provenance: regenerate/remove the changed
   file's map with an explicit inventory delta rather than silently reuse it.
4. Produce an exact unified composition diff and sidecar manifest including
   baseline package/patch/core hashes, tooling/source revisions, per-member
   preservation checks, every changed file/hash and final package tree hash.
   No external package publication or app installation. The local artifact's
   version alone is not provenance: preserve the hash-pinned sidecar.
5. Run `shipping-artifact.check.mjs` explicitly against the isolated result,
   then the adapted 41-checkpoint/ownership suite with imports resolving to
   **that artifact**, not `packages/client/src`. Preserve the published
   SQLite.1 dependency graph; do not silently substitute base workspace SDKs.
   Later compare same-package baseline and fixed observer captures using the
   actual shipping module and unchanged collection set. Confirm all 41 proven
   same-scope checkpoints resume and record any real collection-set drift.

The prepared Node artifact check requires `GONVEX_COMPOSED_SDK_ROOT`, rejects
an absent or non-isolated root, validates the sidecar and changed JS hash,
and imports only that installed package's `dist` files. It checks actual
shipping defer/release before and after hydration, empty and populated
windows, cancellation, repeated release and scope/close retirement. Its
entry name intentionally is not ordinary Vitest/default test discovery.
Run later as:

```sh
GONVEX_COMPOSED_SDK_ROOT=/mnt/data/work/<owned-package> \
  node --test verification/sdk-hydration/shipping-artifact.check.mjs
```

Sidecar required by that prepared test:
`composition-provenance.json` at package root with
`baselineClientJsSha256`, `existingPatchSha256`, `coreSourceCommit`, and
`composedClientJsSha256`. The final record must also retain the full tree
inventory and protected-member verification above; this test does not
replace that provenance audit. No skip/fallback path counts as a pass.

## Known source-only deferred-owner question

The existing overlay queues `{ref,args,onMessage,metadataOnly,cancelled,release}`
before creating a subscription. Its release method checks only `cancelled`,
clears the deferral predicate and invokes `subscribeReplicaTransport` in the
then-current scope. A core-method transplant does not give those older
intents a captured scope owner. Therefore failure/rotation can potentially
resurrect old-scope args as a new subscription; the prepared cross-scope test
is intended to detect that, not asserted passing.

An ownership correction may be necessary within the approved fence scope.
It must distinguish legitimate initial-login deferrals (which have not yet
received an authoritative scope) from intents owned by a retired admitted
identity. Preserve both current-owner release progress and cancellation;
never treat a newer scope as authorization for an older member's deferred
args. Test initial auth activation, repeated auth preserving subscriptions,
real identity rotation, close and failed hydration before choosing the
minimal guard. Do not silently remove all deferred reads or alter the app's
first-screen behavior to satisfy the tests. This question and artifact proof
remain open for the next explicit composition grant.

## Authorized two-phase owner implementation, source only

`deferred-owner-overlay.mjs` now implements the approved correction as a
pure, non-executed transformation of the pinned shipping JS **after** the
core hydration member transplant. Importing it does not read/write package
files, transpile code or launch a process. It requires exact, unique pinned
text anchors and returns the proposed JS plus per-edit before/after hashes
and the complete changed-member list. It has not been imported/executed;
there is no resulting file/package hash to claim yet.

The private deferred-entry state is:

- `owner === undefined`: provisional, queued before authoritative admission.
  First valid directive admission binds it once to `{scope, activation}`;
  initial login's scope reset preserves provisional entries.
- An owner object is admitted and immutable. Scope reset changes the session
  activation symbol and cancels/removes old admitted entries immediately;
  an old admitted entry never becomes provisional again, even if the scope
  string later returns to its original value.
- Public release keeps its original global predicate-clearing behavior and
  marks queued entries released. Provisional released entries wait for first
  admission. Admitted released entries wait for that owner's durable promise
  before invoking the real shipping transport. No control RPC/journal waits.
- The original cancellation closure stays attached to the same entry before
  and after release. A per-entry task token prevents replacement/cancellation
  from publishing; changed `replicaReady` is rechecked after the wait, and
  failure removes the intent permanently. Existing core checks still own
  per-subscription/batch sends.
- A private weak owner map follows a launched deferred subscription. Ordinary
  same-scope reconnect keeps its admitted activation and retained listeners;
  automatic resume after identity/scope retirement cannot reuse its old args.
  A new explicit current-owner subscription is a new intent and can join the
  existing shared transport, rather than an old callback reauthorizing itself.
- Close cancels all pending entries. No option, public method signature,
  wire field, auth policy or app first-screen behavior is changed. The extra
  fifth transport argument is private implementation plumbing only.

Changed existing members for the owner overlay are exactly
`subscribeReplicaTransport`, `releaseDeferredReplicaOpens`,
`activateReplicaDirective`, `sendReplicaOpen`, `resetReplicaScopeState` and
`close`; new private helpers/map are enumerated by the exported
`changedMembers`. This is in addition to the already reviewed core transplant
and its two cleanup insertions. All other shipped auth, admission, defer
predicate, generated-local-runtime and visibility code must remain byte/hash
identical. Keep declaration/option compatibility; record all index JS/map
file changes and per-member hashes at actual composition time. The source
provenance inventory remains the immutable input baseline, not an output
manifest.

Artifact cases now include both initial-login release orders (release before
or after first admission), cancellation after release but before hydration,
real same-scope reconnect/retained listeners, retired identity, equal-string
new admission, and permanent cancellation after hydration failure. They use
the actual composed package's original defer/release APIs and private owner
state, never base `src`. None has run. Core 403 tests do not cover this overlay;
separate build/test grant and artifact proof are still mandatory before any
shipping observer/app installation. No external package publication or
integration merge is authorized by this source-preparation commit.
