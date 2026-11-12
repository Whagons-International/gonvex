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

## Input representation and observed row-limit drift

The same private chunking approach may hold exact args/session/provenance inputs: atomically publish
bounded 64-KiB chunks and count/byte/digest manifest with the run, verify complete reconstruction before
claim/invocation, and atomically transfer required reply correlation before deleting old inputs. Missing,
corrupt or partial generations fail closed. Legacy inline reads do not invent a reply generation. Preserve
argument/result contents and existing caps; no new RPC, Action replay or per-frame persistence follows.

The author reports local SqlStorage accepted over 2.3 million aggregate UTF-8 column bytes, so the new
supplemental expectation of a 2-MB rejection failed. Preserve raw calls-6 evidence and record exact source,
workerd/config and per-column counts separately from physical row/page overhead. The current official
[limits page](https://developers.cloudflare.com/durable-objects/platform/limits/) lists 8 MB for SQLite
strings, blobs and rows; the brief's 2-MB assumption remains recorded as drift, not measured enforcement.
Neither a local acceptance nor this doc changes a public contract or proves the remote ceiling. Verify
chunk bounds/integrity/atomicity independently; actual K04 maxResultBytes and inbound wire limits are
separate boundaries. Correcting this new fixture assumption does not waive any original scenario.
