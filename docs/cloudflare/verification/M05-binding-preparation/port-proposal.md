# Proposed public evidence port — not implemented

No SDK, client, wire, server, protocol, schema, K15 or K22 change is included or authorized here.
Lead must assign ownership and review this API separately after the preparation pin.
Internal fields and receipt helpers below are provenance only; monitors must never access them.

At framework `8851a125dbd7316e6de30717bbcaf807ee2f5eb7`, public
`packages/client/src/index.ts:4780–4826` returns `Promise<T>` from `reducer`.
The normal online path uses an SDK-generated random command ID. Lines 5168–5200 consume
`committedRevision` internally and resolve the application result. The private
`readReceiptCursor` at 4401–4414 validates an existing receipt's epoch/revision. Neither is
a public durable-evidence method, and a returned task row is not a receipt.

The exact proposed SDK method is:

```ts
// PROPOSAL ONLY; FunctionReference/JsonValue/ReplicaCursor are existing protocol/SDK types.
reducerWithCommitEvidence<Args extends JsonValue, Result extends JsonValue>(
  ref: FunctionReference<Args, Result>,
  args: Args,
  options: { offline: "reject"; optimistic: []; timeoutMs: number },
): Promise<{
  result: Result;
  confirmation: {
    source: "server-durable";
    cursor: ReplicaCursor; // { epoch: string; revision: number }
  };
}>;
```

Required semantics before this proposal can become an implementation:

- Bind the actual server receipt/cursor to this exact SDK-generated command, ref, authenticated
  tenant and session generation internally. Never accept application-supplied revision fields,
  caller trace/operation IDs, guessed receipt keys or an unrelated latest cursor as evidence.
- Require online server execution, no local reducer path, optimism or queued outbox. Refuse
  absent/unsupported receipt capabilities, absent or ambiguous confirmation, invalid cursor,
  auth/session/epoch change and timeout. Preserve ambiguous writes; do not automatically retry.
  Use existing protocol/receipt semantics only. If they cannot prove this association, remain
  unsupported and return to lead with the exact gap; this proposal grants no wire extension.
- Preserve the ordinary application result separately. A caller-local operation ID stays distinct
  from SDK idempotency/command IDs and trace IDs. No credential, row content, raw tenant ID or
  receipt identifier enters monitoring export. No raw internal errors are serialized.
- The M05 adapter's current numeric `revision` port is insufficient to establish same-epoch
  application. A later owned adapter must validate the full actual cursor, compare it with a
  separate authorized client's public `localReplica.cursor()` epoch and the exact watch's
  `localReplicaState().computedRevision`, require complete,
  up-to-date server state and exact synthetic field matches, and refuse cache/epoch reset.
  This is a separate prerequisite, not proven by exposing commit metadata.

Separately, the fresh read needs a source-audited real application operation that reaches tenant
SQLite with actual auth/visibility. `tasks.get` is a bounded replica collection. The current
M05 `sdkApplication` one-shot `query` path refuses it; removing `delivery` is not a solution.
A later lead-coordinated binding may select an existing compatible app operation or implement an
audited replica-based fresh read with explicit server/SQLite evidence. A fresh client, successful
row match, HTTP 200 or cursor alone cannot attest the storage/deployment path. No such read
port is implemented here. Browser reload is another independent missing proof.

These three boundaries—actual durable command confirmation, authoritative fresh read and
second-client applied state—remain BLOCKED independently. No public API review or approval
is claimed by the offline audit.
