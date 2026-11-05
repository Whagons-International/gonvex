# Experimental shipping SQLite.1 hydration composition

The isolated patched shipping SDK artifact passed **40 artifact tests**.
No app install, browser/server/auth endpoint, observer capture, registry
publication, integration merge, PR, CI or customer data was used. This is
module/lifecycle correctness evidence, not proof that the 41 actual shipping
cursorless opens have disappeared or any sign-in CPU/latency improvement.

## Frozen identity and provenance

- Experimental identity: `gonvex-sqlite1-hydration-experimental-candidate-2`.
- Frozen package root: `/mnt/data/work/gonvex-sdk-composition/candidate-2`.
- Composition source: `c2d283b75db12cc4f5bd35cffc97fa42a03adfb8`.
- Tested core source remains `728b857e456d6d7a9f299883fdf1ba17c8e594b9`;
  reviewed core evidence `14d3e6f8` is preserved without source changes.
- Baseline is the **installed patched shipping** `@gonvex/client@0.5.2-sqlite.1`
  from app `37ca8278b`, JS SHA256
  `4f9ce8be47c21742b49d760ebf0c695da13ab4c2f814d6df5a7d843a416fa2b4`,
  existing app patch SHA256
  `06786316f912c262f7638b8f3c82cff02a8787f71eabcafca84b4087a3333334`.
- Output `dist/index.js` SHA256:
  `60b2673135e0ffd245b2219fb8a18cd2f6add3ac98ccbfa6af9cb6f5ae20eb0a`.
- Payload filename/hash-map SHA256:
  `392387f207e602ecf09bc5c3dff4fce9fd7a385bf62a5239676cdb4bbe71ae33`.
  This is the hash of the inventoried published payload file map, not a
  tarball hash; it excludes the provenance sidecar and dependency link.
  The public package name/version is retained for compatibility; it is not
  sufficient to identify this experimental artifact.

[Freeze record](evidence/sdk-shipping-frozen-artifact.json) records sidecar,
core, patch and transform hashes. [Full provenance](evidence/sdk-shipping-provenance.json)
includes every baseline/output file hash, every changed/unchanged/added class
member hash and per-edit overlay hashes. Source inventory is retained in
`verification/sdk-hydration/shipping-source-provenance.json`.

## Composition and preservation audit

`verification/sdk-hydration/compose-shipping.mjs` verifies all 129 input
package file hashes and the existing app patch, reads exact committed core
source, and refuses an existing/non-isolated output directory. Cached
TypeScript 6.0.3 transpiles only the three approved hydration methods and three
private fields. Two lifecycle cleanup insertions and the separately approved
two-phase deferred-owner overlay are then applied to the shipping JS. It does
not build a plain main SDK or replace other shipped functionality.

Only `dist/index.js` changes; its now-stale `dist/index.js.map` and mapping
footer are explicitly removed. The remaining **127 published payload files**
are byte-identical, including package metadata, exports and public declarations.
All nine changed existing members are enumerated, with 312 original members
byte-identical and nine private members added. Constructor/auth credential,
RPC, outbox, generated-local-runtime and unrestricted-visibility patch code
outside those allowed methods remains unchanged. The existing members changed:

- `close`, `unsubscribeReplicaListener`: pending ownership cleanup.
- `sendReplicaOpen`, `flushReplicaOpens`, `setReplicaReady`: reviewed hydration
  fence plus deferred-owner check at send.
- `subscribeReplicaTransport`, `releaseDeferredReplicaOpens`: cancellation,
  release and private owner propagation while retaining the shipping API.
- `activateReplicaDirective`, `resetReplicaScopeState`: first admission binds
  provisional intents; retired admitted generations are purged.

The artifact uses the shipping package's original cached dependency graph
through a read-only dependency link, not base workspace SDK dependencies.
No install or pnpm repair occurred. No declaration/public option, wire field,
auth policy, permission primitive or app first-screen behavior changed.
Public `deferReplicaOpen` / `releaseDeferredReplicaOpens` remain present.
[Post-test audit](evidence/sdk-shipping-audit.json) verified all output and
baseline files again after execution.

All 31 import/export declarations in the changed JS are identical to shipping;
all other payload JS files remain identical. Workspace AST graph comparison
with exact core base `2b7e2ff9` also has unchanged runtime edges (39 modules,
66 edges, the same pre-existing local-runtime portable/read-view cycle).
[Graph evidence](evidence/sdk-shipping-import-cycles.json). This is no added
cycle, not a claim that the existing graph is cycle-free.

## Ownership contract and discovered regression

Provisional deferred intents survive pre-admission scope setup and release,
then bind once at first valid authoritative admission. Admitted owners use an
immutable scope plus session activation, never revert to provisional, and
are cancelled at scope retirement even if the scope string later repeats.
Release waits for the matching durable hydration before launching transport.
Original unsubscribe handles remain effective before and after release.
A weak owner guard follows launched deferred subscriptions: ordinary
same-scope reconnect/retained listeners continue; automatic reopens cannot
reuse retired-identity args. A fresh explicit current-owner subscription is
a new intent, not authorization for an old callback to renew itself.

The first candidate passed the initial 12 overlay tests and 26 transplanted
core cases. An added reentrant cached-snapshot observer unsubscribed before
`subscribeReplicaTransport` returned its cancellation handle. Candidate 1
sent two opens rather than one: **12 passed / 1 failed, direct exit 1**.
[Preserved failure](evidence/sdk-shipping-reentrant-before.log.txt).
The narrow overlay correction invokes the returned unsubscribe when the
intent was synchronously cancelled or its owner retired during publication.
Core source is unchanged. A second case also exercises scope retirement from
that callback. Candidate 2 passes both without dropping its sibling reads.

## Direct verification results

Every heavy phase used build-headroom on its actual `/mnt/data` output and a
new coach-resources unit, with existing compile/policy caps unchanged.
Composition/Node tests requested 3 GiB / 2 CPUs; artifact Vitest 4 GiB / 2;
docs generators 5 GiB / 2; audit 2 GiB / 2. Each used explicit 200% CPU quota.
Logs/caches/configs/output remain under `/mnt/data/work/gonvex-sdk-composition`
(except the same child's existing docs caches under `/mnt/data`).

Core composition command uses the cached compiler path through
`GONVEX_TYPESCRIPT_COMPILER` and a new output argument:

```sh
node verification/sdk-hydration/compose-shipping.mjs /mnt/data/work/gonvex-sdk-composition/candidate-2
GONVEX_COMPOSED_SDK_ROOT=/mnt/data/work/gonvex-sdk-composition/candidate-2 \
  node --test --test-timeout=15000 verification/sdk-hydration/shipping-artifact.check.mjs
```

The final Node invocation passed **14/14**, no skips, direct exit 0:
actual defer/release before/after hydration, provisional first admission in
both release orders, cancellation before/after release, failure, close,
retired/equal-string activation, same-scope reconnect/retained listeners and
the two reentrant regressions. [Output](evidence/sdk-shipping-test2.log.txt).

`prepare-artifact-core-tests.mjs` reads the exact core test at `728b857e`,
rewrites its three SDK imports to the frozen artifact's `dist` files, and
refuses unexpected imports. The emitted test provenance records both test
hashes and `sourceFallback:false`. Running its isolated Vitest config passed
**26/26**, direct exit 0: 41 checkpointed opens in single/batch modes,
auth/socket/scope/unsubscribe/cancel/close, resolved/latest hydration,
pending-map replacement, between-frame retirement/reentrant replacement,
control RPC and journal progress. [Output](evidence/sdk-shipping-artifact-core2.log.txt).
These executions import the real composed payload; the earlier 403 source
core checks are not counted as overlay tests. Existing published helper
sourcemaps point to omitted TS sources and produce warnings in Vitest; they
are preserved baseline files, not unresolved imports or failed tests.

Docs metadata generators/route generation/typecheck passed direct exit 0
via `pnpm --config.verify-deps-before-run=false --filter @gonvex/docs typecheck`;
no tracked generated output changed. [Output](evidence/sdk-shipping-generators.log.txt).
Inventory/import audit passed direct exit 0. No core change justified rerunning
403 core suites, and none was repeated. Other generator inputs are unchanged.

The lab was atomically claimed 08:21:23.398 UTC, cleanup margin 08:43:23.398,
hard end 08:46:23.398. All owned units exited; exact owner assertions preceded
lock removal at **08:30:00.506 UTC**. [Release record](evidence/sdk-shipping-release.json).
No heavy work continued after release.

## Reviewable later observer plan — not executed

A separate explicit grant must authorize capture resources, the package's
isolated app installation and local fixture/auth endpoints. Before starting:

1. Verify this artifact's freeze record and all payload hashes, and the
   baseline SDK/patch/app/module pins from the shipping capture report.
   Keep baseline and fixed packages immutable in distinct owned copies.
2. Use the same parent-approved immutable runtime binary/source and synthetic
   DB reset source for both phases. Clone whole DB directories including WAL
   and SHM; never modify shared baseline data. Reuse ordinary verified
   synthetic fixture authentication, with no policy bypass or customer data.
3. Use the shipping app `37ca8278b` and its matching generated module; change
   only the isolated package payload/explicit artifact identity. Preserve all
   original patches, defer/release calls, navigation and collection set. Do
   not touch the first-screen application bug. Verify the served package hash
   and clear only owned bundler caches so an old prebundle cannot masquerade
   as the fixed SDK. The existing unchanged React patch remains in place.
4. Warm both phases from equivalent browser/IndexedDB checkpoint state with
   the same origin, path/args, auth identity, scope/generation, cursor/digest
   and DB/module epochs. Do not clear durable windows to make the fixed phase
   trivially cold, suppress opens, delay/reopen the app artificially or copy
   another identity's checkpoint. Close any prior owned browser before reset.
5. Observe resume-state reads, hydration completion, defer/release lifecycle
   and actual emitted frames. Match each of the 41 proven stored-checkpoint
   paths (37 empty / 4 populated), not just aggregate counts. Required proof:
   requests are constructed after current hydration and include the exact
   stored cursor/digest; no stale owner/frame or missing required collection.
   Record any true collection-set drift instead of silently reducing scope.
6. Exercise first-login/provisional release plus same-scope reload/reconnect,
   and a separate controlled owner retirement. Baseline cold-first visits can
   legitimately omit cursors. The hypothesis concerns the 41 warm checkpointed
   paths, not forcing every first visit to resume or ignoring needHashes for
   genuinely stale/expired/revoked history.
7. If a browser is granted on arch3, use `--disable-gpu` and close it in
   `finally`; resource-admit it, preserve sanitized metadata only, and stop
   every owned app/Vite/forwarder/container before asserting/releasing its
   own lock. No endpoint/browser is started by this document.

Parent chooses the paired runtime and schedules the actual observer. Capture
should preserve source/artifact/module/frame hashes and report installed-mode
and path coverage before any whole-app CPU claim. Backend/permission integration
is separate; this task neither merged nor exercised those pending branches.
