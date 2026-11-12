# 0067: Initial-page provenance and split progress

Date: 2026-11-12. Status: decided (lead). Extends 0064; implementation and assembled proof remain open.

## Why terminal completion is insufficient

K10 can drain snapshot pages before terminal ready. An older page may still be in flight when the holder
processes unsubscribe or same-epoch id reuse. The terminal completion book protects semantic acceptance,
but cannot by itself prevent that intermediate page from applying stale rows. Separately, splitting can
leave ready/completion in an earlier part and a raw `replica.watermark` or envelope watermark in a later
part with no completion authority. Neither gap is closed by ordinary RPC success.

## Provenance fence

Permit `SocketWork.initialScope?: InitialRequestRef`, using 0064's server-originated reference. It is a
provenance fence only: it cannot accept or clear a pending request, complete a subscription, establish scope,
advance progress or substitute for `initialCompletions` and their explicit processing receipts.

Every data/progress page and split part belonging to that initial operation retains the same reference.
This includes referenced shared rows, opened/removed metadata and terminal work. Check the existing
server-owned subscription epoch first, then the exact active kind/id/path/generation before any instance,
data, metadata, sink or raw/envelope watermark application. Obsolete initial work applies nothing. Never
retag an old page with a newer request or a scope-challenge epoch. Preserve the prior installed successful
query when a replacement refuses, as 0064 requires.

One `initialScope` can describe only one originating operation. Do not put a winning reference on mixed
generations or unrelated normal commit operations. Separate such work/envelopes; terminal-only multi-ref
refusals can retain 0064's per-operation completion binding. Validate reference/completion consistency,
safe integer fields and full retained-byte accounting. A completion-free page is not a completed request.

## Fail before an unsupported holder applies data

An old holder may ignore an optional field and apply stale rows before a missing acknowledgment is noticed.
Use the additive thin holder entry `deliverReplicaInitial(tenantId,payload)`, or an equivalently proven
pre-application capability tied to the actual holder incarnation. Route it through the same OrderedOutlet
queue, credits and single in-flight RPC; there is no second delivery channel. The entry validates fenced
initial work before delegating the existing apply path. A missing method fails before ordinary application;
local placement requires explicit support too. Do not probe support by applying and inspecting a receipt.
Use this support boundary for initial work carrying either `initialScope` or `initialCompletions`, including
terminal-only multi-ref work whose exact per-operation references provide the fences.

Keep normal commit, membership, recovery and whole-holder scope-control work outside this initial-only
entry. Its existence is support, not terminal enqueue evidence. The existing actual sink callback and
`initialCompletions` receipt still determine completion; obsolete or missing status certifies no progress.

## Split watermark authority

Conservatively remove initial-only trailing raw `replica.watermark`, SocketWork watermark and global
watermark authority when all contributing accepted/enqueued completion authority cannot accompany the
part that emits it. A completion-free tail cannot borrow another part's acknowledgment. This also applies
when ready batches flush before later work. Normal independently scoped commit splits remain unchanged.
Any future progress-binding optimization needs its own explicit operation-bound proof.

Suppressing unsafe progress proves safety, not liveness. Required regressions hold an actual workerd
intermediate page across same-epoch unsubscribe/id reuse; cover shared rows, repeated splits, ready plus
trailing watermark, batching, refusal, auth change and unsupported-holder zero application. Preserve normal
commit behavior and prove unchanged SDK initial views plus subsequent immediate write/receipt/revision
liveness. Keep failing raw evidence. No source assertion or unit-only result closes the deployed K22 gate.
