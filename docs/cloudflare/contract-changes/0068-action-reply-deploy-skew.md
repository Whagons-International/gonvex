# 0068: Action reply correlation across deployment versions

Date: 2026-11-12. Status: decided (lead); N4 fixes, rollout and legacy compatibility remain unproved.

## New replies

Separate terminal execution from acknowledged reply settlement. K22 proposes server-generated invocation
correlation and additive `settleActionReply` returning `{v:1,status:'enqueued'|'obsolete'}`. A missing older
method fails before unsafe same-id application. New holders refuse replies without the required generation;
they do not infer ownership from the client call id alone. Preserve the existing SDK frame shapes.

`enqueued` means actual socket sink acceptance for the matching current owner/generation/scope, not queued
work or receiver application. `obsolete` requires authoritative terminal abandonment/replacement. Missing
native socket enumeration while a durable owner remains is UNKNOWN/RPC failure, never obsolete success.
An owner index is not, by itself, proof of eventual stranded-owner cleanup.

Retain complete terminal result and correlation authority in bounded private storage until honest settlement.
The proposed 64-KiB reply chunks are a representation, not a new result-size contract or isolate-fit proof.
Retry delivery idempotently without rerunning the Action. Use bounded admission, retry/backoff and explicit
undeliverable states; do not spin indefinitely, fabricate success, lose the locator before acknowledgment or
silently discard uncertain authority. Actual rollback, partial publication and cleanup must be verified.

## Existing rows cannot acquire missing history

Pre-fix live Workflow rows and old holder pendingActions can lack the new generation. Already-done old rows
may have lost the locator or complete result. Do not mint a generation, guess same-id ownership, reconstruct
a truncated/lost result or replay the Action to manufacture a completion. Preserve explicit unsupported or
undeliverable state and its evidence. A narrower legacy path requires separately demonstrated immutable
correlation; no such safe path is approved by this ruling.

This limitation is a rollout gate, not an accepted loss of required behavior. The staging candidate needs a
concrete legacy drain/migration precondition with verified evidence, and actual old/new holder/tenant,
deploy/rollback, pending reply and Workflow retry tests. Empty legacy state must be established, not assumed.
Keep staging/production unchanged. New development namespaces do not prove a later rolling upgrade safe.

## Evidence and cost

Required cases include failure before and after actual holder application, duplicate completion, large
results, hibernate/reset, scope/id reuse, physical close, missing native enumeration with retained owner,
old method/generation absence and Action execution count remaining one. Verify full result integrity and
that subsequent real SDK work remains live. Fresh independent review pins the published fixed artifact.

The author measured a healthy small completion increasing from two to nine SQLite row/index writes in
workerd. Record exact frozen source/configuration, chunk bytes and failure-path costs before attributing
capacity or billing impact. This author-reported number is not deployed cost evidence or an independent
approval. Original K12/K22, lifecycle, platform, parity and capacity gates remain unchanged.
