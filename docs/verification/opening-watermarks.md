# Deferred watermark decisions

An accepted reducer can receive its watermark before a reopened subscription
has caught up. `handleReplicaWatermark` records live opening subscriptions as
blockers. `drainOpeningReplicaWatermark` can pass that deferred revision to
persistence only after the current subscriptions finish opening and each retained
blocker has a sufficient ready certificate.

[`verify-opening-watermarks.mjs`](../../verification/typescript/verify-opening-watermarks.mjs)
parses the actual `drainOpeningReplicaWatermark` method in
[`index.ts`](../../packages/client/src/index.ts). It translates the owner, socket,
connection and blocker conditions to SMT, rather than checking a separately
written canonical predicate. Unsupported control flow is rejected. Structural
checks bind the live-opening aggregate, blocker iteration, removed-subscription
skip, certificate read and final call to `persistReplicaWatermark` to that
translation. They also require `resetReplicaScopeState` to clear every iterated
subscription's old `readyCursor` unconditionally.

Eleven solver laws establish the following decisions for an arbitrary blocker:

- Changed scope or socket activation, a closed client, a disconnected socket or
  an opening live subscription prevents draining.
- A current live blocker requires a ready certificate, the captured epoch when
  present, and a ready revision at least as high as the deferred revision.
- A sufficient current blocker permits progress under the other prerequisites.
  A removed or unsubscribed blocker does not prevent progress.

The source loop applies the same tested condition to every blocker and returns
on the first insufficient one. The proof concerns the synchronous admission to
persistence. A separate check below covers publication after that call yields.

## Ownership after storage completion

`persistReplicaWatermark` previously advanced the shared processed revision after
awaiting storage, without checking whether authentication had changed. Four SDK
regressions hold an actual adapter's completion after its write commits, rotate
to another tenant and accept a reducer at revision 4. The old tenant's watermark
1000 incorrectly allowed that reducer to succeed while the replacement tenant's
durable cursor was still 3. The baseline fails this assertion with memory, both
IndexedDB adapters and native Node SQLite.

The method now captures replica scope, socket generation and receipt activation
before storage. It persists the captured scope and checks all three identities,
closed state and connection state after completion. A stale completion cannot
advance the processed revision, complete pending calls or start receipt
retirement. Four additional schedules leave and return to the same scope while
the write is held. The old write remains durable in that scope, but its old
completion cannot certify the returning activation's subscription readiness.

[`watermark-publication.mjs`](../../verification/typescript/watermark-publication.mjs)
translates the actual post-await guard. Its closed AST check binds owner capture,
the scoped awaited storage call and every subsequent publication effect to that
guard. It checks fresh receipt activation allocation on scope reset. Seven laws
cover changed scope, socket or activation, closure, disconnection, current-owner
progress and return to the same scope. Six changed guards must produce solver
counterexamples. The existing required policy step invokes this check and retains
its nested receipt.

The required runtime workflow also compiles three source controls through Vite
without editing the checkout. Removing the whole post-await guard must expose
the four early successes. Removing only the activation comparison must expose
the four stale publications on return to the same scope. Every failure must
reach its named assertion; a setup error cannot satisfy the control. This runner
uses deterministic transport, fake IndexedDB and native in-memory Node SQLite.
The separate backend/browser compositions retain their own storage boundaries.

The same audit found a second completion race in `acceptReplicaReady`. After a
window write, the old operation could restore its ready cursor onto a subscription
object reused by the replacement tenant. A deferred watermark then retained the
old database epoch and never drained, even after the replacement data and cursor
were durable. Four more baseline schedules reproduce this stalled reducer.
The method now rechecks captured subscription ID, verification identity, socket
generation and owner scope before publishing the certificate. Its closed AST
bridge checks those captures, the sole persistence await, the synchronous early
return and the final guarded certificate publication. This is structural
correspondence, not an additional solver theorem. The third compiled source
control removes that guard and must reproduce the four stalled completions.
The runtime runner requires all twelve positive schedules before checking the
three controls.

Nine changed production guards yield solver counterexamples: missing scope,
activation, close, connection, readiness, epoch or revision checks; denying all
blockers; and retaining removed blockers. The extractor also rejects the exact
old source behavior that retains the readiness certificate across scope reset,
conditional clearing, changed opening predicates and changed persistence order.
Sixteen extractor regressions check accepted and refused source changes. The
mandatory policy workflow runs both extraction tests and solver checks and
retains their source hashes, generated queries and receipt.

The source bug and its execution evidence are documented in
[composed-sdk.md](composed-sdk.md#fixed-account-tenant-rotation-and-readiness-certificates).
All four storage-adapter schedules failed before clearing old readiness, and the
fixed-account cross-tenant schedule reached real receipt, watermark and ready
frames before stalling on the previous source. The fixed SDK completes both.
These failures connect the certificate reset check to an observed lifecycle
violation; refusing a source shape alone is not a runtime counterexample.

The model uses integer revisions and uninterpreted primitive owner, epoch and
reference identities. It assumes validated finite safe revisions, nonnegative
listener counts, ordinary typed fields and equality, no reflective or aliased
writes or reentrant getters, and no synchronous owner change before entering
persistence. The post-await proof additionally checks the publication guard's
placement and captured identities, assuming ordinary private fields, fresh
Symbols and synchronous built-in operations. It does not establish blocker
capture, snapshot integrity, durable storage behavior, reentrant observers, all
asynchronous publication paths or the complete recovery protocol. Those remain
separate obligations in the eight-guarantee register.
