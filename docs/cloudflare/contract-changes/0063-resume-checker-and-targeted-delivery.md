# 0063: Verified resume cache and targeted scope delivery

Date: 2026-11-12. Status: decided (lead). No new wire behavior or waived assertion.

## Same-socket digest-only resume

K22's new supplemental SDK deployment probe completed generation/schema/defaults, stale-artifact refusal,
receipt replay and visibility/reauth checks, with equal final database checksums, but its overall result is
still FAIL. One new probe assertion expected `client.updateRequired.contract.version`; the actual existing
protocol defines `contract: number`, and both runtimes sent `contract: 2`. Correct that new assertion only;
retain the original failed transcript and the original raw upgrade/handshake scenarios unchanged.

Source review confirms the digest checker empties its hash map on every outgoing open. The SDK's
`system.reload` handler resumes replicas, and `replicaOpenRequest` prefers the committed window's
`windowResumeState` containing cursor/digest without keys/hashes. A same-socket reopen can therefore
legitimately retain rows already observed by the checker. This is a checker-model issue to fix and test,
not a passing deployment result or permission to accept a server's digest as evidence of row contents.

The approved narrow correction may reuse a previously observed cache only when both keys/hashes are
absent and all of these are established: same physical socket and subscription id; same path and args/full
subscription descriptor; same authorized scope; complete, nontruncated committed window with no partial
snapshot staging; locally recomputed digest exactly matching the offered digest; and a cursor consistent
with the observed epoch/revision. Model the SDK's eligible watermark advancement where relevant rather
than requiring a stale cursor or inventing an unseen revision.

Never infer cached rows for fresh sockets/ids, changed paths/args/scope, reset/closed subscriptions or
unverifiable history. Nonpreserved auth and applicable revocation invalidate cache authority. Explicit
keys/hashes keep their existing precedence. If the baseline is unprovable, report the missing evidence;
do not silently skip checks, reduce checked counts or normalize a mismatch away. Continue recomputing
every subsequent ready/delta digest from actual retained rows.

Regression coverage must include altered server digests after a valid matched resume, mismatched offered
digest/cursor, changed socket/id/path/args/scope, partial snapshots/reset/close, explicit hashes, eligible
watermarks and ordinary application values. Keep the existing digest-drift mutation and all prior assertions.

## A socket is not a recipient of another socket's targeted payload

The combined single-holder `thin-sdk-parity` campaign exposed a real source failure: the Gateway's
scope predicate challenged every unaddressed socket when a normal per-socket open/query payload had no
global watermark. Repeated whole-host fences stranded initial SDK views. Retain the frozen failure.

For an unaddressed socket, a normal other-socket-only payload applies **no** instance, data or watermark
and must not itself initiate a scope barrier. Scope challenges remain for an unambiguous empty whole-holder
challenge and applicable global unscoped delivery. A global watermark needs authoritative scope even when
the instance happens to match; a new instance on unrelated targeted work does not authorize touching others.
Membership handoff and universal recovery keep their existing meanings. Preserve empty bootstrap,
captured epochs, scope/recovery acknowledgments and zero progress on challenge/unacknowledged receipts.

Use actual multi-socket Host tests for concurrent opens/live queries, new auth during a barrier, genuine
global/new-instance work and no fabricated acknowledgment for unaddressed sockets. Also cover failed
barriers with deferred initial opens: produce authorized results or a real failure/fresh retry path, rather
than leaving quiet clients stranded or adding an unbounded internal retry. Rerun unchanged thin-SDK and
named static-one-holder scenarios after the frozen campaign; final automatic-fleet proof remains required.

Stranded recovery OWNER lifetime, private-platform CPU/quarantine, full parity and real deployment gates
remain open. The separately reported actual multi-holder telemetry pass does not resolve these failures.
