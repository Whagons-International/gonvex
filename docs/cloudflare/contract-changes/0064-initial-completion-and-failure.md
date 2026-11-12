# 0064: Initial subscription completion and ordered failure

Date: 2026-11-12. Status: decided (lead). Internal metadata only; SDK frame shapes stay unchanged.

## Normal initial completion

Use one completion mechanism, replacing the provisional third-element plain-frame reference:

```ts
interface InitialRequestRef {
  kind: 'query' | 'replica';
  id: string;
  path: string;
  epoch: number;
  generation: number;
}
// SocketWork extension:
initialCompletions?: readonly {
  request: InitialRequestRef;
  opIndex: number;
  outcome: 'accepted' | 'refused';
}[];
```

The Session stamps generations in server-only dispatch metadata. A reference binds to the originating
physical socket, tenant/scope and exact still-pending request, not merely a reused subscription id. Publish
the monotonic counter and bounded pending book atomically with the existing private recovery generation;
restore must not reuse a generation after partial publication/hibernate. Validate safe integers/overflow.
Only semantic admission/completion changes persist, not every change frame or result hash.

The sender attaches metadata only after the actual initial operation completes or refuses. K08 can
successfully install/resume a subscription with only `query.progress` when the offered window revision
matches: that completed no-change operation may be `accepted`. Arbitrary/older progress, change updates
and intermediate snapshot progress carry no terminal authority. A refused replacement preserves the prior
successful semantic request. This addresses review N5; an older A update cannot consume pending B.

The holder validates index/kind/id/path/epoch/generation and commits semantics only after that specific
terminal operation reaches actual socket sink acceptance under the still-current scope. Batcher enqueue
alone is insufficient; flush/track ready batches before acceptance. This is server enqueue evidence, not
receiver application. Split payloads remap indices and retain a completion only with its actual operation;
account every metadata string/array/envelope before admission. Invalid indices, duplicates, stale/filtered
operations or unsupported binding cannot become successful same-id fallback or certify unsent progress.

## Completion receipt and progress

The holder returns explicit per-operation processing facts:

```ts
// ReplicaReceipt extension:
initialCompletions?: Record<string, readonly {
  generation: number;
  status: 'enqueued' | 'obsolete';
}[]>;
```

Keys are physical connection ids. Generations are unique across initial kinds within that owner. Validate
the exact offered connection/generation set, with no duplicate, foreign or extra status. `enqueued` comes
from the actual Session apply/sink callback, after any ready batch flush and current-scope check. It does
not mean receiver application. `obsolete` permits cleanup of that exact old correlation only; filtered
initial work causes no instance, data or watermark change. Unsubscribe/id reuse cannot certify unsent work.

Initial watermark/delivered authority requires the actual `accepted` outcome, an explicit `enqueued` status
and a still-current owner/epoch when the engine processes the receipt. An enqueued `refused` error is
terminal correlation evidence, not snapshot/progress authority. Missing old-holder status, invalid status
or sink failure is unsupported/failure with suppression, never ordinary-success fallback or bulk cleanup.

A shared maximum watermark cannot inherit authority from a different completion. Unless progress is
explicitly bound to each accepted operation, every completion contributing to an initial payload/socket
watermark must be accepted and enqueued; otherwise that initial progress stays zero. Preserve this rule
across mixed, split and batched work. Normal independently scoped commit acceptance remains unchanged.
Count receipt metadata as well as request metadata. Test mixed enqueued/obsolete, refused/enqueued,
missing/duplicate/foreign statuses, reverse arrival, sink/batch failure and auth change during acknowledgment.
See 0067 for the provenance-only intermediate-page fence and conservative split-watermark handling.
0070 permits partial exact processing facts only during actual recovery; ordinary completion stays full/exact.

## Ordered terminal failures

The additive marker is `ReplicaDelivery.initialFailure?: {v:1,
reason:'delivery-recovery-failed', requests:Record<connectionId,readonly InitialRequestRef[]>}`.
`ReplicaReceipt.initialFailureAck?:1` means support/terminal processing only, never a revision, scope or
recovery acknowledgment, and never proof that a client received an error.

Use the normal `OrderedOutlet.send` queue and its existing byte/recovery fences. Marker-only work routes
through an additive holder method `deliverInitialFailures(tenantId,payload)` on that **same queue**, with
one RPC in flight as before. An old holder lacks the method and fails before ordinary delivery application.
Sending an unknown marker through legacy `deliverReplica` and discovering a missing ack afterwards is
insufficient: an old holder could already mutate its instance. Ordinary delivery rejects the marker rather
than falling through. Local placement requires explicit support before invoking an old normal receiver.

Validate the whole envelope before side effects: empty shared/socket work, no watermark, membership,
recovery or scope-fence mixing. Await `Session.failReplicaInitials` on each applicable existing serial queue.
Only exact originating, still-pending references may send existing `query.error` / `replica.error`; never
retag with a later or challenge-returned epoch. Remove a current correlation only after actual successful
sink acceptance and scope recheck. Stale/replaced refs leave new requests untouched; missing connections
are explicit. Partial delivery then RPC failure is idempotent on a later fresh attempt. An ack may process
already obsolete refs; it does not mean every listed ref caused a client frame.

Neither errors nor processing acknowledgments clear recovery suppression, advance accepted/delivered
revision or watermark, or establish instance/scope proof. Capacity/network/old-holder failure stays explicit
and retains applicable correlations. No alternate channel, fake receipt, dropped pending request or internal
retry loop. Include all new marker and normal-completion metadata in holder/isolate byte credits; otherwise
this would repeat X01's corrected omitted-field accounting defect.

## Retry and quiet-socket registration

Fresh real client/commit events may retry once immediately, singleflight per holder with a bounded coalesced
owed-event allowance. Internal registration, rebuild, error output, receipt handling, diagnostics and timers
cannot mint retry credit. A one-second cooldown must not strand an immediate legitimate mutating call.

Every actual normal caller's current server-owned view is observed, including quiet writers. An internal
empty `replica.openMany` through the real existing handler may register a revalidated current view; it must
not create an opening cohort, emit data, reset opens or advance progress. Reject stale observation explicitly:
an empty response from a silently ignored view is not registration proof. Use existing server-only recovery
provenance, not a client-decoded authority field. If no catch-up/deferred work exists, finish from the already
awaited legitimate bootstrap/recovery ack without fabricating a receipt or revision advancement.

Required tests cover no-change progress, old-update/replacement refusal, scope/id reuse/unsubscribe,
split/ready batching/sink failure/partial publication, unsupported peers, complete byte accounting, and real
unchanged SDK quiet initial failure followed by immediate writer/receipt/revision liveness. Source or raw
error-frame checks alone do not satisfy this last gate.
