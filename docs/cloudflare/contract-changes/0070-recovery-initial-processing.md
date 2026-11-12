# 0070: Exact initial-processing facts during recovery

Date: 2026-11-12. Status: bounded follow-up permitted; no implementation or lifecycle acceptance claimed.
Normal 0064 completion remains exact/full. This extends processing cleanup only during actual recovery.

## Offered references and actual callbacks

Permit `ReplicaDelivery.recoveryInitials?: Record<physicalConnectionId, readonly InitialRequestRef[]>`
only on whole-holder recovery. A genuine empty scope-bootstrap SocketWork may instead carry
`recoveryInitials?: readonly InitialRequestRef[]`. It still has no rows, operations, opened/removed metadata
or watermark. Original references never acquire a returned challenge epoch or new request identity.

The sender captures immutable originating refs plus the actual Connection object and epoch when enqueuing
the barrier. Session validates those refs against current authoritative state and performs the actual
authorized rebuild. The existing `Session.send(text, InitialCompletion)` actual sink callback is the only
terminal enqueue fact. No second completion rule, replay or delivery channel is introduced.

Return facts through existing `ReplicaReceipt.initialCompletions`, only for supplied refs actually completed
or authoritatively processed. `enqueued` requires the real terminal callback; `obsolete` requires known
semantic retirement. Unknown history, omitted refs and unrebuilt replica requests remain omitted/UNKNOWN,
not inferred obsolete from absence. Use plain own-key records that native RPC can serialize safely.

## Partial exact-subset processing

Partial facts are permitted in this recovery context. Validate the entire returned subset before any cleanup:
each physical-connection key is offered, generations/statuses are valid and unique, and every fact maps to
the original offered kind/id/path/epoch. At receipt processing, require the same current Connection object,
epoch and exact still-applicable book entry. Id reuse, replacement or auth changes cannot clear newer work.

Foreign/duplicate/malformed facts invalidate all cleanup facts; missing or omitted facts remain UNKNOWN.
Clear only explicitly validated exact correlations. Never bulk-clear because recoveryAck or scopeAck exists.
Old holders ignoring the optional request return no facts and therefore certify no correlation cleanup.

These facts grant no accepted outcome, initial/commit watermark, sent/delivered revision, instance or scope
authority. Recovery/scope acknowledgment literals keep their existing independent meanings. Normal initial
payload validation stays full/exact; this partial rule cannot be used as an ordinary-success fallback.

## Budgets and proof

Charge every map, list, ref, envelope, receipt and retained copy to the same outlet budgets before retention.
Metadata cannot silently expand 0056's recovery reserve or invoke its legal-singleton exception while other
credits are live. Offer a bounded subset fitting the existing reset/control admission; omit the remainder
explicitly or fail admission honestly. No unbounded reserve, internal retry loop or lowered argument cap.

Test partial success, unknown/unrebuilt refs, duplicates/foreign facts, oversize admission, lost responses,
replacement/auth/new Connection races and older holders through native RPC. Verify repeated recovery,
bounded pending-state growth and actual SDK quiet-request/write/receipt/revision liveness. Host-barrier
suppression returns zero floor before checking whether subscriptions are installed; a processing fact never
substitutes for a real progress certificate.

The retained Engine.initials lifecycle gap stays open until actual fixed-artifact evidence proves it. A finite
guard or one passing recovery does not establish indefinite availability. Keep frozen failed evidence and
fresh source review separate; original K22/X01, OWNER, platform and capacity gates remain unchanged.
