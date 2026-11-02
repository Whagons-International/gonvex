# Resource ownership audit

RES-1 covers admission capacity, native worker lifetime and snapshot wire credit.
A numeric timeout or credit helper alone cannot establish the caller's cleanup
behavior. This register separates those decisions from actual lifetime evidence.

## Native isolate permits

`module-runtime-v8/src/pool.rs::IsolatePool::acquire` obtains an owned Tokio
semaphore permit before taking an idle worker or starting a native worker.
The new lease owns both objects before awaiting startup readiness. Canceling
that await therefore runs the same lease destructor as canceling an invocation.
Healthy release puts the worker in the idle list before releasing its permit.
Unhealthy release and recycling transfer the permit into `IsolateWorker::retire`.

`isolate.rs::WorkerControl` serializes retirement and disposal with one mutex.
If disposal already completed, retirement can drop the permit immediately.
Otherwise retirement stores it until `WorkerControl::stopped` takes it once.
`worker_main` declares `WorkerExit` before its runtime and test lifetime guard,
so Rust drops those later declarations before releasing the retained permit.
These ownership transfers rely on Rust destructor order, exclusive owned permits,
Tokio's semaphore semantics and the absence of undefined behavior in dependencies.

Four native V8 pool regressions pass in the isolated checkout:

- Cancellation of both pending host work and unbounded CPU work retains capacity
  while native teardown is deliberately held, then returns capacity after release.
- CPU and asynchronous initialization deadlines dispose workers across repeated
  failed starts and allow capacity to be reacquired.
- Zero and unrepresentable startup deadlines fail explicitly.
- Canceling startup cannot create another worker while the only native teardown
  slot remains held; releasing teardown returns that permit.

These are real Linux V8 executions through `deno_core`, not a fake worker.
They test selected bounded schedules. They do not prove arbitrary native
interruption latency, scheduler fairness, poison/panic recovery or process death.
The observed test timeouts are test failure bounds, not a promised production SLA.

`PermitRetirement.tla` models capacities 1 and 2, with one extra worker identity
to expose premature capacity reuse. It distinguishes startup, active leases,
healthy idle workers, retirement and native exit before lease retirement. Three
broken policies release before disposal, leak after disposal or return capacity
twice. A successful repeated-call witness prevents an unreachable admission path
from serving as positive evidence. The model exhausts 66 distinct states at capacity 1 and 580 at capacity 2.
All three broken policies violate their named laws, and the repeated-call
witness is reached. Two source-mutant native controls also fail as intended: replacing retained
permit ownership with an immediate drop returns available capacity 1 while
native teardown is held, where both canceled-call and canceled-startup tests
require 0. Each control runs in its own native test process. The model is
not source refinement. The required policy workflow runs it and retains traces.

## PostgreSQL receipt initialization

`receipt_initialization_cancellation_releases_lock_and_admission` first observes
a real backend waiting on the receipt-schema advisory lock and one active query
admission permit. It aborts that initializer task and requires admission to
return to zero. After releasing the fixture gate, another initializer must
complete, a separate connection must acquire the same advisory lock, and a real
reducer must commit. Saturating the configured admission capacity and refusing
one additional admission checks that cancellation did not release capacity twice.

The lock is transaction-owned. The schedule allows SQLx's queued rollback to
finish after the fixture gate is released; it does not claim that dropping a
future immediately cancels a statement already executing in PostgreSQL. The
test deadlines are failure bounds rather than a promised cleanup latency.
The required runtime initialization step selects this case by name alongside
the four schema/legacy regressions.

All five native initialization cases pass. A separately built production-source
mutation calls `std::mem::forget` on the initializer's admission permit. The
cancellation schedule rejects it at the named admission-leak assertion, with
one active permit instead of zero. Restoring production source passes all five
cases again and passes Clippy with warnings denied. This source mutation is
local evidence; the explicit CI case currently requires the positive schedule.

This schedule measures the application's admission semaphore. It does not prove
that server-side PostgreSQL work has already stopped when the caller's future
is dropped. Retaining or cancelling outstanding database work before reusing a
global backend-work budget needs its own correspondence and lifetime audit.

## Module-host root admission and reply ownership

The module host previously acquired its global root permit before checking the
invocation deadline. A real V8 action held the only root slot while four queued
calls supplied deadlines through the request, context or both. All four failed
to answer before the held action was released. Their expired bodies did not run,
but the outer admission wait could retain the caller and its transaction without
a deadline bound. The inner isolate-queue test could not expose this outer wait.

`InvocationContextWire::effective_deadline` now shares one extracted minimum
policy with engine-context construction. `handle_invoke` applies that deadline
to root admission, including zero/expired refusal, before acquiring a generation
lease. Requests with no invocation deadline retain their existing queue behavior.
Nested calls bypass the root slot so a parent cannot deadlock its own child.

The actual framed host/V8 regression covers those six schedules. It requires the
four finite callers to answer while the original root remains held, preserves
unbounded and nested progress, checks handler call counts and reacquires exactly
one permit. The expired callers must neither run nor retire the healthy isolate.
The 50/250ms observation windows reuse the existing native V8 queue regression's
failure bounds; they are not a promised production response SLA.

The same audit reproduced four reply/request bookkeeping defects. A duplicate
request replaced and dropped the original cancellation sender. Cancellation
also removed an ID before the old task's final cleanup, permitting premature
reuse. A cancelled host-call future retained its pending reply, and the reply
counter wrapped after exhausting `u64`. These have separate failing baselines.

Request registration now uses a vacant map entry. Cancellation takes an optional
sender but keeps the original reservation until its task queues a terminal frame
and finishes. A `PendingHostCall` destructor removes only its originating reply
on success, error or cancellation. Checked atomic allocation refuses at the
reserved maximum counter value. The last usable position still works, exhaustion
does not send a frame, and a late cancelled reply cannot reach a newer call.
These ownership arguments rely on Rust destruction and the standard library's
atomic, `Entry`, `Option`, map and oneshot semantics.

[`verify_host_deadlines.py`](../../verification/verus/verify_host_deadlines.py)
verifies the actual minimum-selection body and checked-add callback over the
full `u64` domain. Seven changed deadline policies and four changed counter
policies must produce verification failures. Ten structural source controls
check the conversion/admission and reservation/destructor bridges separately.
They are source correspondence checks, not ten additional solver theorems.
Native protocol tests check zero and maximum wire timestamp conversion. The
required runtime step selects all five admission/ledger regressions, refuses
ignored or missing execution, and retains their logs. The required policy step
retains the proof, controls and source hashes.

System clock accuracy and sampling, native timestamp conversion, Tokio scheduling
and semaphore cancellation, arbitrary dependency panics and full asynchronous
refinement remain outside these proofs. Root admission counts logical root RPCs;
the per-isolate pool separately retains its permit through native worker exit.
Neither count establishes a global bound on idle module heaps or PostgreSQL
backend processes.

## Sandbox execution reservations

`SandboxManager::run` records an active execution before waiting for its global
admission permit. The previous source cleared that record only after worker
completion. Aborting a caller during admission therefore left the workspace
permanently active. A real Tokio task-abort regression reproduces the leak while
the sole permit remains held. The normal worker-configuration failure control
clears its reservation and permits workspace destruction.

An `ExecutionOwnership` guard now captures the original workspace and execution
before admission. Dropping an unfinished run schedules mutex-protected cleanup
on its captured Tokio runtime. Cleanup changes the workspace only if its active
execution still matches that captured identity. Normal completion disarms the
guard after recording the terminal result. Shutdown may remove a workspace;
cleanup then has no target. Cleanup requires a live executor and eventual mutex
progress. It is asynchronous and has no promised wall-clock latency.

Eight native controller tests cover the held admission wait, a real Linux child
process waiting for its result, different or missing workspace/execution
identities, explicit cancellation during admission and blocked stdin, the stdin
watchdog and a successful uncancelled worker response. They require terminal
bookkeeping, eventual child exit, workspace destruction and exactly one
reacquirable permit. The child uses a fixture executable to test the actual Rust
process controller. It is not an execution of the TypeScript sandbox engine.
All eleven ordinary sandbox library tests pass. A separate required native
coordinator executes the exit-held retirement case.

`verify_sandbox_ownership.py` extracts the actual `Option<&str>` ownership helper.
Verus checks its exact string identity decision and three caller laws. Four
changed string policies must fail verification. Fifteen separate structural controls
pin the originating map targets, Drop scheduling, guard placement, cancellation
during admission and the watchdog around request and response I/O. These checks do not prove Tokio scheduling, process
kill/reap timing, arbitrary panics or a global physical subprocess bound.
Required runtime CI selects all eight controller tests and refuses ignored or
missing execution. Required policy CI retains the source proof and controls.

Explicit cancellation originally waited for an occupied admission slot. Once a
worker started, both cancellation and the watchdog waited behind its stdin
write. Three separate previous-source regressions expose these failures with a
held permit and a real child that does not consume its request. The 512 KiB
request remains within the existing code-size policy and fills the native pipe.
Each negative run aborts its owned caller, confirms child exit and clears the
fixture before reporting the named failure.

Admission now selects cancellation before acquiring capacity. A cancelled wait
returns terminal cancellation without dispatching a worker. The worker I/O
future owns its child across request writing and response collection, with the
existing cancellation and timeout select surrounding both. The unchanged
watchdog includes its existing two-second grace. The five-second test bounds
are failure bounds, not a production cleanup SLA. A successful native response
control checks that open, uncancelled work still completes with its result.

The initial task-abort and explicit-cancel fixes still returned capacity before
physical child exit. A separate native test now attaches an owning ancestor
tracer to the fixture child, holds its exit event and confirms that its memory
mappings remain present. With configured concurrency one, the previous source
started a second real child during that hold in four schedules: caller abort
while awaiting a response, caller abort during stdin, explicit cancel during
stdin and timeout during stdin. Each schedule records the overlap and releases
and reaps its owned children before reporting the named failure.

`SandboxChild` now pairs the actual child with an owned admission permit before
its first I/O await. A successful `wait` plus completed output collection is the
only normal release site. Cancellation, I/O failure and dropping the caller move
both objects into retirement. The reaper requests kill, awaits the same child
and then returns the permit. The state is taken once, so normal release and
retirement cannot both consume it. These source checks rely on Rust's ownership
and destructor semantics and Tokio's owned semaphore and process APIs.

If `wait` cannot confirm exit, the actual `retirement_requires_closure` policy
closes admission before returning its permit. A separate injected-result test
checks the real semaphore: queued acquisition wakes with an error rather than
starting another child, while a successful wait preserves ordinary admission.
This is a test of the result-handling boundary, not an observed operating-system
wait failure. Verus proves the actual closure decision and two laws. Three
changed closure policies must fail verification, alongside the existing four
identity controls. The proof now has seven obligations across its two production
policies and seven typed negative programs. Fifteen structural controls pin the
child/permit pair, the single normal release and retirement/error paths.

All four fixed exit-held schedules retain capacity, prevent the replacement
child from starting, then require replacement progress after native exit and
exact single-permit reacquisition. The mandatory Linux CI coordinator refuses a
missing fixture and cannot count an ignored or absent test as execution. A ptrace
permission or protocol failure fails the check and receives no negative-control
credit. The test uses fixture executables through the actual Rust controller,
not the TypeScript sandbox engine. Its receipt binds source and executable hashes.

The physical correspondence is for one manager on a live Tokio executor under
Linux kill, wait and tracing semantics. It does not establish a machine-wide
limit across independent managers, arbitrary kernel failures or execution after
the owning executor has shut down. The finite permit-retirement model above is
an abstract policy check, not a source refinement proof of this process guard.

## Snapshot credits and deadlines

Preparation futures must continue releasing their database resources while the
socket loop awaits an inline query. The real PostgreSQL starvation regression
in [composed-sdk.md](composed-sdk.md#snapshot-preparation-and-inline-receipt-lookup)
failed before preparation tasks were scheduled independently. Each active
`Preparation` owns its Tokio handle; dropping it requests cancellation. The
queue keeps task completions behind its existing ticket check before returning
anything to the socket publisher. Clear, removal and queue drop now have a
cleanup schedule that does not poll completion again. All eight queue tests
pass, including count/byte admission and replacement checks. This is selected
execution evidence and source correspondence, not a whole async resource proof.

`verify_snapshot_credit.py` extracts production admission and prefix-release
bodies and verifies retained frame/byte conservation. `verify_snapshot_ack.py`
checks the production current-snapshot sequence transition. The source bridge
checks call sites, but the proofs do not establish transport durability or every
subscription replacement/cancellation path by themselves.

`runtime/src/snapshot_delivery.rs` has selected caller regressions for active and
retired ACK prefixes, replayed ACKs, buffered frames, cancellation/replacement,
close, certificate release and native subscription closure. A fresh source-bound Linux runtime run passes all 18 snapshot-delivery tests,
alongside eight real V8/runtime/PostgreSQL receipt tests. This exercises the
selected caller schedules, not the complete SDK snapshot protocol.

`call-budgets.md` specifies the source-linked full-range duration policy and its
native adapter assumptions. Deadline propagation through queue waits, V8 dispatch,
cancellation and host RPC still needs its composed audit. An exhausted timeout
must not silently become an unbounded call.

RES-1 remains open until all affected lifetime paths, failures and negative
controls have current evidence and the independent guarantee audit closes them.
