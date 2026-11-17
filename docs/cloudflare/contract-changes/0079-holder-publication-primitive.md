# 0079: Await the exact local holder publication

Date: 2026-11-12. Status: narrow local implementation permitted; lifecycle/activation acceptance UNMET.
Continues 0058/0059/0076/0078. This is publication evidence, never owner or terminal authority.

Permit local `SocketHost.publishRecovery(ws, session, claim): Promise<RecoveryReference>` and a readonly
`HolderRecoveryStore.published(ref, connectionId, initialSequence)` header predicate. Neither is a Gateway
RPC, root export, module host capability or client field. No outlet/placement/M05 change is implied.

Before effects, verify the actual current Session, physical socket/tag, issuer, epoch and scope against
server-owned state. Install the reentrant-publisher guard before invoking publication. Dirty only the
semantic snapshot; use existing atomic generation/attachment publication, await actual `ctx.storage.sync`,
then recheck the SAME physical scope and EXACT still-current committed published reference and manifest/
owner header. A valid retained old generation or pending header is insufficient. Reject supersession.

The readonly predicate cannot replace or weaken existing chunk/hash/restore validation, ownership indexes,
attachment-publish failure handling or generation/GC policy. Propagate sync/publication/scope failures;
preserve old-generation retention and fail-closed semantics. No ACK follows merely from queued work.

The caller must still prove that its exact pending book/intent/binding is included in THAT published
snapshot and perform actual current-view/revalidation checks. The primitive grants no owner admission,
terminal witness, quiescence, retry credit or stranded-OWNER cleanup. No per-frame call, KV counter or
per-result-hash durability is added. Lost close/finalization evidence remains UNKNOWN under0058/0069/0076.

Required native evidence: hold actual storage.sync and prove no caller continuation; wrong claim/reentry,
scope changes, supersession/pending header, attachment and sync failure; restore metadata and actual scope
clear without accepting old state. Then coherent immutable source and NEW independent review.

Author phase21 reports 73 named native/model cases and root TypeScript passing, using normal resource
gates and the unchanged installed compile wrapper. Actual local Host/KV/serializeAttachment/eviction used
synthetic 2M permissions and 31 semantic requests: full JSON 4,005,717 UTF8 bytes, attachment 1,035 bytes,
276 private chunks, largest observed text chunk16,384 bytes. Native eviction restored full metadata;
the actual Member-switch handler cleared old-scope requests/generation. Held-sync/failure cases passed.
These are author local representation/wiring results from `/mnt/data/work/gonvex-k22/owner-prototype-21/`,
not an immutable independent source approval or remote serialization/row ceiling, shared128MiB fit,
K17 module provenance, terminal ownership, SDK or N6 proof. Owner draft remains uncommitted/unactivated.
Frozen19b/final3 diagnostic evidence is separate and unchanged. All original gates remain.
