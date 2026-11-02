# Ownership across asynchronous work

The owner of an operation is the outbox scope and its activation identity at
invocation. Changing away and back to the same scope creates another activation;
scope-string equality alone does not authorize an old callback. Closing the
client invalidates publication too.

An operation may finish a write to its original owner's journal after that owner
leaves. This does not authorize publishing its result into the current replica,
changing current optimistic bookkeeping or waking delivery for a new owner.
Journal completion and live publication are separate decisions.

## Reproduced retry boundary

`intent-ownership.test.ts` drives public `retryIntent`, `setAuth`, `listIntents`
and replica selectors with an injected KV journal. It pauses the retry's write
completion after the old owner's pending record is stored, changes account or
tenant, then releases completion. Before the fix, the old owner's optimistic
row appears in the newly activated replica. A third baseline case returns
successful retry completion after the client closes.

The correction captures scope and generation before the first await. It checks
that owner after recovery readiness, local-lane admission, journal lookup and
retry persistence, before optimistic publication, and before reporting success
or waking delivery. It retains the old journal's ID, key and arguments. The
tests include account change, tenant change, close, changing away and back to
the same account, and an unchanged-owner positive control. Denying every retry
would fail the positive control.

## Reproduced discard boundary

Another schedule pauses deletion completion for Alice's pending intent after
the journal row has been deleted. Bob has a different journal row with the same
intent key, which is valid because intent namespaces are owner-specific. The
client switches to Bob and restores his prediction while Alice's discard is
still awaiting completion. Before the correction, that old completion removes
Bob's prediction even though Bob's durable journal entry remains intact.

`discardIntent` now captures the invoking activation before its first await
and checks it before journal selection and after deletion, before touching the
current optimistic maps or replica. The unchanged-owner control still removes
Alice's prediction. The rotated-owner case preserves Bob's prediction and his
journal. Invalidation returns false for the live operation; it does not roll
back an already completed write or deletion in the original owner's journal.

These are controlled SDK schedules over a KV fixture, not deployed incidents
or real-browser/native-SQLite executions. They establish selected failure
boundaries and cannot close AUTH-1 by themselves.

## Reproduced restore boundary

Restoration previously checked its activation once after loading the journal.
It could then await deletion of an already accepted entry and continue projecting
the old journal without checking the activation again. Both local-execution
receipt cleanup and ordinary committed-entry cleanup reproduce the same result:
Bob's restored prediction is replaced with Alice's after Alice's deletion
completion is released. Alice's remaining pending intent stays in her journal.

Restoration now checks the captured activation before starting, after storage
readiness/synchronization, after journal reads, after each awaited cleanup, when
queued rebase work actually enters its lane, and after rebase. It checks each
projection iteration and checks again before scheduling follow-up work because
projection observers can synchronously change authentication or close the client.
The current-owner controls still restore Alice's pending prediction after cleanup.

Four restore schedules cover the two cleanup paths with and without owner
rotation. These are cache/session fixtures with a trusted stored identity, not
backend authentication-admission evidence.

## Reproduced accepted-settlement deletion

`settlement-ownership.test.ts` covers direct delivery, queued single calls and
`reducer.callMany`. Alice's accepted result crosses committed revision 8 and the
replica watermark before the fixture pauses completion of her committed journal
write. Bob then activates a separate journal entry with the same command key,
a different numeric entry ID and his own restored prediction.

All three original paths deleted Bob's durable entry instead of Alice's. The
success helper looked up a command key in the current owner's map after awaiting
Alice's commit persistence. This was actual fixture-store deletion, not only an
incorrect return value or prediction.

Retirement now uses the captured originating entry ID. Volatile map cleanup also
requires the captured activation to remain current and the mapped entry ID to
match. An accepted old-owner result can finish retirement in its own journal
without clearing Bob's map or prediction. A superseded direct call rejects with
`superseded` after retirement rather than returning a successful result to the
new owner. Direct admission captures its activation before recovery readiness;
background drain admission and settlement retain the same activation token.

The tests include unchanged-owner acceptance controls. Error schedules pause
persisted `update_required`, `unauthenticated` and transient failures before
completion. Old-owner failures may update the originating journal, but cannot
close the new session for an app update, reauthenticate it or schedule its retry
timer. Direct error settlement uses the captured entry ID too. Current-owner
controls still request the update or reauthentication when appropriate. The new
owner's failed intent stays failed in these error fixtures to prevent a new
legitimate delivery from obscuring the old completion's effects.

`owner-retirement.mjs` also translates the actual restricted retirement helper
into symbolic effects. Four solver laws require the originating deletion ID,
forbid stale-owner or mismatched-entry volatile cleanup, and admit valid cleanup.
Four broken helpers must violate named laws, including rerouting retirement
through the new owner's map. The translator models a fallback map lookup after
guarded map deletion. Unknown helper statements and effects fail extraction.
It trusts a pure `current()` callback, built-in Map/Set behavior, non-NaN journal
IDs and the outbox implementation deleting the supplied ID. It does not prove
caller entry provenance, callback freshness or storage semantics. Its receipt
binds the client and verifier hashes; the existing ownership CI command runs it.

The error-guard negative control removes only the post-persistence update/auth
guards. All six rotated-account cases fail: three close Bob for Alice's update
error and three send a third auth request. The six unchanged-owner controls
pass. An earlier error-mutant attempt had unrelated test-source interference
and is excluded from this evidence; the retained second run checks source
stability before and after execution.

These 24 schedules use the public SDK, a controlled socket, memory replica
storage and an injected shared KV journal. They do not prove backend tenant
admission or actual IndexedDB/SQLite storage behavior. Reentrant observers,
internal local rebase awaits, multiple concurrently settling entries and failed
commit persistence still require their own schedules.

## Source-linked ownership decision

`ownership-policy.ts` is the actual predicate consumed by retry, discard,
restoration and outbox admission/settlement.
`verification/typescript/verify-ownership.mjs` parses that production source
with the pinned TypeScript API and accepts only one typed, single-return
function, primitive parameters, strict equality and Boolean operations.
Unknown statements, calls, coercion, assertions and changed parameter domains
fail verification.

Scope strings are an uninterpreted equality domain. Because the accepted source
does no string operations, this preserves primitive JavaScript string equality.
Activation identities are primitive Symbols represented by another uninterpreted
equality domain. Five laws cover closed-client denial, scope and activation
mismatch denial, current-owner admission and return to the same scope with
another activation. Six well-typed broken predicates must
reach Z3 and violate a named law. Denying all work must fail admission too.

The existing required policy workflow now includes the verifier and retains its
source, solver hashes, SMT problems and negative-control results. These workflow
changes are local pending final delivery, along with the production corrections.

This proof establishes the predicate's decision. Placement after every relevant
await, closure of every publication path, storage atomicity and backend
authorization still require separate evidence. Parser, translation, solver and
primitive JavaScript semantics remain trusted. Runtime fault schedules check
selected call-site placements separately.

## Activation freshness

The original production expression `++this.outboxScopeGeneration` was extracted
from `activateOutboxScope` and executed twice starting at JavaScript's maximum
safe integer. Both allocations produced 9007199254740992. The same extracted
expression produced different values from zero. This is a precision-boundary
counterexample for the allocation rule, not a deployed occurrence or evidence
that an actual client performed that many activations.

Owner activation now uses `Symbol()` identities. A newly allocated Symbol is
distinct from every earlier one even when returning to the same scope. Holding
an old token in a pending closure retains its identity; later allocation cannot
recreate it. Tokens stay in client-local coordination state rather than persisted
journal or replica records.

`owner-generations.mjs` checks the actual client AST. It requires fresh `Symbol()`
initialization and exactly the four existing invalidation sites: constructor,
owner activation, quarantine and close. It rejects counters, reusing a token,
global-registry `Symbol.for`, missing or additional invalidation sites, known
computed access and indirect property assignment. The proof receipt records the
client source hash, allocation sites and this checker's hash.

Freshness relies on the standard, unmodified JavaScript Symbol allocator and
ordinary access to the private SDK field. The AST check is not a proof of all
JavaScript reflection, hostile monkey-patching or when every invalidation site
is reached. These trust assumptions differ from a finite numeric counter
remaining below a precision boundary. Existing return-to-owner fault schedules
continue checking that an old activation cannot publish into a new one.

## Finite asynchronous composition model

`AsyncOwnership.tla` models one captured operation through journal selection,
write completion and optional rebase completion. Owner changes, return to the
same owner under another activation, and close can occur between these phases.
An already admitted journal write may finish in the original owner's namespace
after rotation; publishing its result requires current ownership separately.

| Model action | Source correspondence |
| --- | --- |
| `ReadCompletes` | Journal lookup followed by the ownership check before mutation or cleanup |
| `WriteCompletes` | Retry/discard persistence or accepted-entry cleanup completes; check before live effects or further rebase |
| `RebaseCompletes` | A queued rebase enters its lane under a checked activation, then completion is checked before later publication |
| `Rotate` / `Close` | Scope activation/quarantine or client disposal invalidates earlier work |

The checker exhausts two owners and generation bounds 2 and 3. Correct cases
check admission ownership, publication ownership and journal namespace isolation.
Five broken policies must yield named violations: omit the read, write or
rebase guard; compare only scope; or reroute a journal write to the new owner.
A separate positive witness must reach a successful current-owner publication,
so an unreachable publication path cannot count as this model's success evidence.

This is a finite protocol model, not source refinement or a liveness proof.
Generation allocation is abstracted as fresh bounded activations. Each modeled
completion/publication segment is atomic; arbitrary user-callback reentrancy
inside that segment and multiple simultaneous operations are outside the model.
Production reentrancy and interference need separate source audits and schedules.
The model's optional rebase phase does not establish that all internal rebase
awaits are correctly fenced. The required policy workflow is configured to run
this model and retain its traces at final delivery.

## Rebase completion after close

The isolated continuation in `gonvex-ownership-resume-1029` reproduced a further
failure through public `retryIntent` and `close`. Rebase paused in the portable
executor, the client closed, then execution completed. The original rebase
checked only scope identity, which close does not replace. It admitted a new
`updateLocal` write after close and replaced the durable old prediction.
An execution rejection after close could also report a replay fallback to the
closed client's observer and persist the cleared prediction.

Rebase now captures the owner activation and replica scope before its first
storage await. It checks them after storage synchronization, journal reads,
execution and prediction persistence, and before each later entry and final
publication. Queued coordination checks the invoking activation on entry.
Deferred replay wakeups retain that activation too. Journal writes admitted
before invalidation may finish in the original namespace; completion does not
authorize another write or publication. Optimistic bookkeeping is installed
before replica publication so it does not follow application notification.

`local-ownership.test.ts` has six schedules for successful execution, failed
execution and prediction persistence, each with close and an open-client
control. Open controls must still replay or report the fallback. A persistence
write already admitted before close may finish, while a delayed execution cannot
admit a new write after close. Two additional account-change listener schedules
inspect replica state, journal visibility and coordination bookkeeping. Those
listener schedules also pass the old implementation and are compatibility
coverage, not reproduced-bug evidence.

Reverting only the two rebase methods produces two expected failures, on
successful and failed execution completing after close. The remaining six
schedules pass. The candidate passes all eight. This is an injected KV journal,
trusted cached identity and fake portable executor. It is not actual native
storage failure evidence or a proof of every local execution or peer-refresh
path. The existing solver checks still establish only the restricted ownership
predicate, allocation rules and retirement helper; they do not prove rebase
control flow.

## Local admission and peer refresh

`local-admission-ownership.test.ts` pauses portable preparation, legacy execution
or durable enqueue completion, with open-client and closed-client schedules.
Preparation already fenced close. The other two closed-client schedules returned
a successful local result after close. Late legacy execution also reached intent
insertion after close. Local admission now checks the invoking activation after
readiness, lane admission, journal lookup, storage synchronization, execution and
enqueue, and before returning a result. An enqueue already admitted before close
may finish in the original journal; it cannot publish or report success afterward.
The execution helper fences its own delayed hydration and read-view callbacks,
uses the captured executor, and checks activation before storing read metadata.

`peer-ownership.test.ts` pauses a peer journal read, changes Alice to Bob and back
to Alice, installs a newer durable prediction, then releases the old read. The
old scope-only check accepted the original Alice read in the new Alice activation
and replaced `latest` with `peer-old`. Different-account and closed-client
controls already denied publication, and the unchanged-owner control published
the peer update as expected. Peer refresh now retains an activation for each
notification pass and checks it after each await and before later notifications
or drain wakeups. A distinct peer notification received during the old pass still
gets a fresh pass. Scheduled local replay also retains its invoking activation
before entering its microtask and local lane.

These schedules exercise the public SDK with a controlled shared KV store and
trusted cached identities. They do not establish native storage behavior or all
concurrent peer-notification interleavings. Local admission and peer-refresh
fixes pass 88 focused tests, including the existing local reducer lifecycle
suite. Baseline admission has two expected failures and baseline peer refresh
has one, with their positive controls passing.

## Real tenant admission and receipt routing

`rust/crates/postgres/tests/tenant_isolation.rs` runs against PostgreSQL with a
real control-plane session token, physical tenant memberships and tenant routes.
The same session is denied tenant B while its member is revoked, even when a
stale control-plane directory row claims access. It is admitted after that
physical membership becomes active. The test then uses the same account, intent
key, reducer path and business row ID in tenants A and B. Both claims succeed in
their own transactions and each route replays its distinct receipt result.
Tenant B's business row remains in its own physical tenant namespace.

The owned PostgreSQL 16 fixture passes this expanded test. Setting
`GONVEX_TENANT_ISOLATION_REQUIRE_NATIVE=1` without a database URL produces the
expected failure rather than a skipped test. The required runtime regression
workflow now sets that flag in an explicit tenant-isolation step. This supplies
control-plane/session-to-transaction routing evidence. It does not yet exercise
HTTP/WebSocket reducer admission, the V8 module host with these admitted sessions,
revocation racing an already admitted transaction, or a database machine crash.

## Authentication allocation freshness

`auth-generation.test.ts` injects JavaScript's maximum safe integer into the old
numeric auth-fetch generation before initial and rejected-token refresh. The
fetch begins, a newer account/token is installed, then the old fetch resolves.
Both precision-boundary schedules replace the newer token with the stale one;
the two ordinary-counter controls deny the stale completion. This is a boundary
counterexample for the allocation expression, not a deployed occurrence or a
claim that a client performed that many refreshes.

Auth fetch/install invalidation now allocates fresh Symbols. Initial and rejected
fetches also call the source-linked ownership predicate before credential
installation. The allocation checker is extended to the auth initializer and all
seven installation, close, auth-error and fetch sites, with typed broken-allocation
controls. The four fixed schedules and 142 existing auth/reconnect controls pass locally.
Type-checking and all 30 then-current extraction tests pass; the source-linked
proof checks the seven auth allocation sites. Socket, snapshot-verification and reset generations have also been changed
locally to fresh Symbols, with a new direct-allocation/capture checker. Those
additional changes are pending type-checking, runtime and proof execution.

## Remaining source audit

| Path | Publication boundary | Evidence still required |
| --- | --- | --- |
| `retryIntent` | After journal update, before prediction/bookkeeping | Predicate proved and delayed-write schedules pass; composition and actual storage/platform failure tests remain |
| `discardIntent` | After asynchronous discard, before optimistic removal/rebase | Delayed deletion with reused owner-specific keys reproduced/fixed; other await placements and actual storage tests remain |
| `restoreOutbox` | Awaited acknowledgement and local rebase after its initial owner check | Both cleanup races and post-close rebase writes have regression schedules; full reentrancy, peer refresh and real storage schedules remain |
| `rebaseLocalEntriesLocked` | Storage synchronization, execution, prediction writes and final publication | Post-close execution success/failure reproduced and fixed; in-flight persistence and synchronous account-change controls pass. Broader owner rotation, peer refresh and actual storage remain |
| `runOptimisticReducer` | Enqueue completion, network result and commit persistence | Accepted retirement and delayed error schedules pass. Local execution admission fences readiness/execution/enqueue completion. Broader direct optimistic admission, owner-return and commit persistence failure remain |
| `settleOutboxBatch` | Commit/reject persistence and optimistic cleanup | Captured-ID accepted retirement and delayed error schedules pass for single/batch wire; concurrent siblings, local rebase internals and actual storage remain |
| Replica/query snapshot and delta handlers | Verification, persistence and promotion | Connect generation checks to subscription replacement, owner rotation and deferred watermark handling |
| Token refresh and auth restoration | Fetch/load completion before installing credentials/directives | Cover out-of-order refresh, sign-out, newer auth and return to the same account |
| Backend reducer/receipt routing | `TenantSession.route` and account subject at transaction/lookup | Real PostgreSQL session/membership admission and cross-tenant receipts pass. HTTP/WebSocket reducer execution and revocation races remain |

The rows above are audit targets, not claims that each contains a confirmed bug.
Backend receipt tests already exercise subject separation, but their synthetic
session leaves tenant admission outside their evidence.

## Completion for AUTH-1

Account for every await that can lead to owner-specific publication, including
work queued before it enters a coordination lane. A source proof of a validity
predicate must be accompanied by evidence that production code checks it at the
right boundary and performs no unguarded publication after a later await.
Retain source-mutant counterexamples and fault schedules for the composed path.
Separate callback ownership from backend tenant authorization and disclose
remaining platform assumptions. AUTH-1 remains incomplete while the audit rows
above lack this evidence.


## Real authenticated account rotation

`verification/backend-receipts/sdk_owner_switch.mjs` composes one SDK/socket,
real SQLite, the production WebSocket runtime, V8 and PostgreSQL across account
A->B->A. It holds A's acceptance persistence promise after the actual journal
commit and durable data barrier. B has a parked intent sharing the logical key
but owning another journal ID. A's released callback must neither acknowledge
B's prediction nor alter B's journal. The old call reports `superseded`; B's
read-only receipt lookup cannot see A's acceptance. Explicit B retry creates
one B receipt, and returning to A reconciles A without ordinary redelivery.

The source control removes only the pending-call identity check in the delayed
reducer completion. The same schedule then fails its named replacement-owner
prediction assertion. The current backend postconditions require eleven original-account receipts and
one other-account receipt in tenant A, matching twelve effects. Row visibility
is public within this fixture tenant; the oracle concerns callback/journal
ownership and receipt isolation, not hiding authorized tenant rows. The seed
write opts into shared-store refresh and is serialized before rotation. The same fixed account now also switches between separate tenant databases; see
[the tenant schedule](composed-sdk.md#fixed-account-tenant-rotation-and-readiness-certificates).
Its positive run and independent stale-completion source control pass. It also
exposed and fixed a cached readiness certificate surviving a tenant epoch change.
Broader permission changes and arbitrary asynchronous publication paths still
require their own evidence. Competing delivery uses separate journals and is
covered by the composed coordinator.


## Terminal replica activation handles

Startup recovery exposed `LocalReplica.activateScope` retaining its settled
promise. A failed read remained a permanent same-scope failure after the stored
fault was removed, and `forceReload` could return a previous completed read.
Success and failure now clear only their own activation handle. The public API
regressions check a real changed snapshot on forced reload and an old Alice
completion while Bob's read remains pending. Compiled browser controls show
that omitting handle ownership queues Bob twice, while omitting retirement keeps
the old rejection after a real IndexedDB counter repair.

These checks complement the shared startup numeric-domain fence documented in
[indexeddb-browser.md](indexeddb-browser.md#validate-startup-before-publication-and-retire-settled-activations).
The source extractor checks the publication order and terminal identity guard;
these selected schedules do not establish every asynchronous owner path.


## Invalidation callbacks and bounded activation identities

`LocalReplica` allocates its process-local generation before invalidation
callbacks, retires the previous cached handle immediately, then checks that no
listener requested another generation before queuing startup. An old outer call
therefore cannot replace the handle installed by a newer listener request.
Returning to the same owner creates a fresh activation after an intervening
request. Closed instances reject before coalescing a cached read.

The counter remains numeric so existing opaque dependency fingerprints retain
their representation. Positive safe-integer admission prevents identity reuse at
the binary64 boundary, and a failed increment invalidates every earlier admitted
identity. A new replica is required after exhaustion. The actual prefix increment
and policy have eight binary64 laws; source checks bind the cache, callback and
publication ordering. Unit baselines record the wrong final owner, and compiled
browser controls reproduce all three failures. See
[indexeddb-browser.md](indexeddb-browser.md#activation-identity-exhaustion-and-listener-reentrancy)
for the evidence and the remaining proof boundary.


## Intent snapshot refresh ownership

The public `subscribeIntents` projection had an additional stale completion path.
`refreshIntents` compared the account/tenant scope string after reading the
journal, but did not compare its activation or check close. A real shared-store
read was held after capturing its rows. Both account A->B->A and close then
allowed the old rows to reach observers. Checking only the final snapshot misses
the account-return case because a later refresh can overwrite the stale value.

`refreshIntents` now captures the fresh activation before its journal read and
calls the source-linked `ownsCurrentScope` predicate immediately before
publication. A superseded read requests a new refresh for the current open
owner. Closing during recovery readiness also stops before the journal read.
There is no await between the final ownership check and snapshot publication.

`intent-refresh-ownership.test.ts` records every observer notification and checks
account switch, tenant switch, return to the same account, close and unchanged
ownership. The account-return and close cases fail on the previous source at
the named stale-snapshot assertion. The unchanged-owner case must publish, so
denying every refresh cannot pass. The shared KV journal's durable rows remain
unchanged by this read-only operation. This fixture uses the actual SDK and
injected asynchronous storage, rather than a native browser or mobile database.

`intent-publication.mjs` checks the complete method AST, including capture,
readiness, journal read, post-await guard, replacement refresh and publication.
Six structural controls reject changed placement or effects. The existing
ownership solver proves the called scope/activation/close predicate. The AST
checks are correspondence evidence, not new solver theorems or full asynchronous
refinement. Required runtime CI also compiles two changed production guards and
requires both to reach the same stale-publication assertion.


A separate real Chromium fixture now repeats the five schedules through
`DexieReducerOutbox` and native IndexedDB. It holds the completion of the actual
adapter read. On return to Alice, a separate native journal handle discards the
old rejected intent and creates a distinct new one. Observers must never receive
the captured old record, including transient notifications before the next
refresh. The native fixture passes all five schedules and two compiled controls.
The controls omit activation equality or restore the old scope-only policy.
Removing only the explicit close predicate is redundant with close's fresh
activation allocation, so that variation is not counted as a failing control.
Required runtime CI executes the native browser fixture and retains its receipt.

## Query and Replica callback publication

The callback boundary also needs ownership checks. A first query or Replica
listener can synchronously change account, tenant or activation, or close the
client. Checking ownership only before the listener loop allowed the remaining
listeners to receive the old frame. Cached replay had the same problem between
scheduling its microtask and running it. Deferred query storage completion could
also publish after account A->B->A or close.

`capturePublicationOwner` captures scope, receipt activation, outbox activation
and socket identity. Query publication additionally requires the same registered
subscription and routing ID. Replica publication also requires its verification
generation. The SDK checks these conditions after storage completes and before
each listener. Cached replay captures its owner before scheduling and requires
the same published cache entry when it runs.

Queries now keep separate received and published messages. Patches normalize
against the received message immediately. A late subscriber can replay only a
successfully materialized message. The publication queue preserves wire order,
including a later error or non-row result that does not need an entity write.
A failed materialization requests a snapshot and publishes no result. Owner
reset clears revision state as well as cached messages, allowing the next
owner's first sequence to start independently.

`query-publication-ownership.test.ts` has 27 public callback schedules. They
record every notification, include unchanged-owner and fresh-owner progress,
hold real memory-adapter writes and inject a rejected write. The corrected
fixtures fail on the frozen previous source in 19 cases, with eight passing
controls. Nine compiled production mutations must reach 24 named failures.
Those mutations separately restore listener-loop leakage, stale cached replay,
received-cache publication, failed-write publication, scope-only ownership,
out-of-order publication and rejection of a new owner's first revision.

`query-publication.mjs` checks seven complete method ASTs and complete cache and
reset statements against `query-publication-contract.ts.txt`. Fourteen changed
guards or effect placements must be rejected. The existing solver checks the
called scope/activation/close predicate. The new checks establish structural
correspondence, not a new theorem about JavaScript scheduling or native storage.

`browser-query-publication.check.mjs` runs 54 candidate schedules in actual
Chromium across both shipping IndexedDB adapters. An independent storage handle
checks the absent row before a held write and after a failed transaction. The
failure fixture aborts a native transaction after an entity put succeeds, then
requires a repaired write to publish. Eight mutated SDK bundles repeat selected
schedules on both adapters and must expose 16 intended violations. Runtime CI
requires all 70 browser schedules, the compiled SDK controls and their receipts.
This fixture uses deterministic transport frames. Backend authentication, process
crash and mobile driver execution require their separate evidence.

## Retained Control Query watch ownership

The public Control Query watch kept an additional copy of its result and error
after the transport subscription had discarded them. Its queued notifications
and listener fanout could therefore expose the previous owner. Unsubscribing
all listeners also permanently stopped query delivery, while the retained watch
could later replay its old value to a new owner.

The watch now clears stale results and errors on scope change and on reads of
an inactive snapshot. Query and scope subscriptions restart when a listener
returns, and the last listener releases both subscriptions. Notifications check
the captured publication owner before each callback, including initial replay.
Scope observers run before the new scope fields are installed, so the clear
notification captures that installed owner in a subsequent microtask. A guard
on the scope activation rejects obsolete clear notifications.

Cached values use a separate session-scope identity. A transport disconnect
invalidates in-flight receipt reconciliation but preserves an authorized cache.
The new session-scope Symbol changes on scope reset, independently of socket
reconnection. The source check requires a fresh Symbol at initialization and
the sole reset assignment. Both account A->B->A and visibility A->B->A invalidate
an inactive cached result. Reconnecting the same owner retains the same snapshot.

`control-watch-ownership.test.ts` records public callbacks and snapshot reads in
34 schedules. The frozen previous source fails 26 and passes eight, including
fresh-owner and reconnect controls. Ten compiled source mutations must reach
18 named failures. They remove notification guards, stale snapshot checks,
subscription restart or scope notifications, capture the owner too soon, tie a
cache to its socket, or reuse a visibility activation. Twelve structural controls
check the complete watch method and the two allocation sites against
`control-watch-contract.ts.txt`. These AST checks reuse the separately verified
ownership predicate and publication helper. They do not prove a JavaScript
scheduler or backend authorization. The fixtures use deterministic frames;
control-role fixtures perform no storage writes. Runtime and policy CI require
these checks.

## Scope clears during real connection teardown

A further real WebSocket schedule exposed two missed notifications. A transport
wrapper can change account in a close listener registered before the SDK's
listener. In Node `ws`, the SDK then invalidates receipt reconciliation before
the queued scope clear runs. A second schedule changes account after the SDK's
close handler, then calls the public `connect()` method in the first watch
listener. The next watch listener can miss the clear because its notification
was tied to the previous socket. In both cases the snapshot getter was already
empty while the observer's displayed value remained the previous account's.

Scope clears now use session activation for their deferred guard and capture
the installed session owner for notification. Data and initial notifications
retain their transport checks. A reconnect can proceed during an empty-state
notification while later listeners still learn that the old account's value
was invalidated. Account and visibility changes still invalidate the session
owner before any later listener can run.

`control-watch-close-order.test.ts` uses a real Node `ws` loopback server and
actual close events. It records the external/SDK listener order and public
observer state, holds admission of the next connection until that oracle runs,
and then requires fresh-owner progress. The previous source fails both schedules;
reverse listener ordering and unchanged-owner cache retention pass. The combined
watch execution now has 38 schedules. Twelve compiled source variants must
reach 20 named failures, including separate receipt-activation and socket-owner
controls for these two native-close defects. Fourteen structural controls cover
the complete updated watch contract and session allocation sites.

`browser-control-close.check.mjs` repeats the four schedules with Chromium's
native WebSocket and a loopback server. Its socket-owner mutation must reproduce
the missed clear during a public reconnect. Chromium was observed to run the
clear between the external and SDK close callbacks, so the receipt-activation
mutation is a safe diagnostic probe there. Local beforeimage runs record that
engine difference without counting the probe as a failing control. Runtime CI
requires the four candidate schedules and the failing socket-owner control.
These are actual transport/event-loop checks with a fixture protocol server.
They do not assert backend authentication or storage durability.
