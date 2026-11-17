# M05 explicit server-execution option — proposal v1, NOT implemented

Source: framework b9c4b2483a5629421caf8a29c2ee867f0d8f21d5; historical application 73276b5589d363c1190a6d028413a21559226631. This proposal has no SDK, server, protocol, module or runtime edits. The existing conditional method still refuses genuine local-execution refs. The disabled M05 binding stays disabled. This does not revive the unsupported GENERAL `reducerWithCommitEvidence` contract.

## Proposed public shape and exact cutpoint

Add only `execution?: 'server'` to the options of `reducerWithTransactionEvidence`, at client/index.ts:4813. The explicit value is required for a `localExecution:1` ref. Absence keeps the existing refusal; no change to ordinary `reducer()`, LocalRuntime, query, outbox, generated refs or their offline/optimistic metadata. No caller-supplied artifact, entropy, identity, receipt key, callbacks, proof declarations or recovery switch is accepted as an option.

The following is a code-shaped proposal, not an applied patch or stub API:

```ts
// Proposed options validation at 4824, while retaining all current checks:
const explicitServer = options.execution === 'server';
if (options.execution !== undefined && !explicitServer) return refuse();
if (Object.keys(options).some(k =>
    !['offline','optimistic','timeoutMs','execution'].includes(k))) return refuse();
if (ref.localExecution !== undefined &&
    !(ref.localExecution === 1 && explicitServer)) return refuse();
// offline MUST still equal 'reject', optimistic MUST still be empty.
// Keep ref.optimistic refusal, tenant/member/public/kind/path guards unchanged.
```

The exact implementation would additionally require a private **accepted auth artifact record**, not the public `activeArtifactHash()` value. Populate it only in the accepted, latest, current tenant `auth.result` branch around 2390/2436, using that frame's actual `result.artifactHash` with bounded nonempty opaque-string validation and the exact admitted identity object. An absent/invalid value clears it; never retain the previous value on a new auth result. Capture before admission callbacks, then recheck latest auth/socket and the object identity after those callbacks. Bind hash, socket generation, accepted auth-frame ID, admission object, scope/session generations, replica activation and definition epoch. No caller assertion or later catalog may populate it. `system.reload`, auth start/error/supersession, close/error/reset, tenant/member/account/visibility change, new activation or epoch must invalidate it. A reload advertisement is insufficient to repopulate it. A later explicit auth outside this evidence call may establish a new record; this method never reconnects or reauthenticates.

For explicit server execution of a local ref, require the real installed generated local binding's nonempty artifact hash to equal the accepted auth record. Also refuse any supplied ref.artifactHash that differs. Genuine task refs contain no artifactHash or intentEntropy: do not pretend otherwise, or infer provenance from path alone. The generated local binding at this app pin advertises `f8336ddff29861c552dc03aeb9e5b0a8c7056589c709fd29d0d4e4dbd91171bc`; that historical build hash is not a deployed-artifact observation. With no accepted auth artifact or no genuine binding association, refuse before send.

Extend the existing `current()` predicate at 4832–4854 with exact accepted-artifact-record identity/hash/binding checks, and invalidate any pending evidence call on reload. Check after each await, callback, serialization, and immediately before both sends; keep the existing monotonic total deadline. Retain the existing strict current tenant/member/auth/socket/epoch/window/activation/recovery/barrier checks.

At 4862's direct payload only, the proposed explicit branch would be:

```ts
// PROPOSAL ONLY: id and entropy are generated once internally, not supplied.
{type:'reducer.call', id, path, args, idempotencyKey:id, receiptPath:path,
 artifactHash: acceptedAuthArtifact.hash, intentEntropy: fresh64HexEntropy,
 trace: existingTrace}
// NO own clientContract property in this optional per-COMMAND branch.
// Auth's configured clientContract and all server session checks remain intact.
// Do not call generic send(), call(), reducer(), runLocalReducer(), connect(),
// outbox admission, replay, requestReauthentication() or any retry helper.
```

Generate entropy with the same 32-byte cryptographic source/hex encoding used in runLocalReducer:1631–1649, distinct from the fresh command/key and trace. Fail closed if unavailable. The key equals the fresh call ID for current transaction/receipt association; it is not a trace or task entity ID. Use the unchanged direct captured OPEN socket; no queued send or ambiguous retry. For option-absent calls, retain the current conditional payload behavior and rejection boundary exactly. There is no claim that the existing option-absent API already proves exact artifact identity.

## Why omission matters, and what is actually proved

`acceptsClientArtifact` (K12 module.ts:124) accepts exact hash OR same optional command contract. Keeping both a stale hash and contract 3 can execute a newer module. Public transaction provenance has no executed-artifact field, and a receipt fence cannot repair that gap.

The current native controlled fixture exercised the original SELF WebSocket → Session decoder/dispatch → tenant RPC → actual K12 path → module host/SQLite/receipt path, with an authenticated session contract 3 across an actual v1→v2 same-contract deployment. A stale captured auth hash plus command contract 3 executed; the same stale hash with the optional command field omitted refused without a task row or receipt; the current exact hash without that field executed. `native-artifact-03-resources.log` records 1 added test plus the 8 original receipt tests PASS. This fixture used the original board module only to test general dispatch. It is NOT historical Whagons handler execution, artifact negotiation with Whagons, or full transaction-evidence acceptance.

Omission survives: decoder.ts:80–83 optional u64big is absent/undefined, Session.session.ts:529 forwards the original call without adding its stored session contract, bindings.ts:41 passes the frame through tenant RPC, reducer/object.ts:148 and tenant.ts:197 dispatch it unchanged. K12 path.ts:273 checks an optional command contract only when present; path.ts:276 with undefined therefore requires exact hash. Authentication is separate: Session.session.ts:297/400 checks the actual connected contract before every tenant frame; auth:689–697 validates its configured contract. Never remove either check.

K12 obtains the active ProjectModule inside ReducerPath.serial, validates hash against it, and runs its host under the same queue. KernelTenant.activate also holds this serial queue; installCode installs the host registry and active module together. TenantModuleHost.run leases the active generation synchronously before awaiting storage. This is source support for association through execution, not a live deployment measurement.

Rust source independently preserves omission: lib.rs:5010–5074 destructures the optional command client_contract and passes it unchanged to ExecutionAccess, not connected_client_contract. execution.rs:481–521 captures a ProjectModule (or access.module), validates the optional command contract and exact artifact against that module. The same module's generation/schema/schedulable module feed invocation:580–636. Existing modules.rs:503 implements the same OR; execution.rs:2002's original unit test explicitly asserts stale hash with None refuses. lib.rs:2544ff separately enforces connected_client_contract on tenant frames. Rust tests/build were NOT executed in this task; this is source proof only. No restore/epoch transition or production deployment race is claimed.

## Actual app semantics and blockers

The genuine generated create/rename/remove refs remain localExecution:1 and offline allowed. Ordinary reducer() at 4887 selects runLocalReducer, not a direct server-only call. That path establishes localBinding identity/artifact, scope/generations, entropy, local execution and coordinated outbox admission, then sends artifact+entropy on outbox drain. It cannot be reused for this evidence call without violating the no-optimism/no-outbox/no-retry contract.

The original server path is feasible in source: createCommands.create → createTaskIntent/createTaskWithPolicy; rename → runTaskIntent/updateTask; bulkCommands.remove → archiveTask(false). The K12 dispatcher selects those actual exports and original argument schemas; it does not replace handlers based on localExecution. The opt-in must send genuine args under the actual principal and preserve every server permission, reference, default, notification, workflow, SLA, inventory and soft-archive check. It must not add a suppression flag or turn denied egress into success. Create's supplied entityId remains separate from the operation key; creation validates it against relation IDs, allocates the task number, and applies defaults before inserting. Remove updates deletedAt/deletedBy and associated lifecycle state; it is not physical cleanup.

No genuine full app module was produced in this task: exact dependency versions are unavailable in the inspected approved pinned tools, and the task-only relative closure has an unresolved declared action target. The genuine read module compiled separately, but its full original migration/source manifest failed actual native control persistence with SQLITE_TOOBIG. Thus no real app authenticated artifact/binding match, actual handler invocation, fresh SQLite watch, positive visibility row, effect isolation result, or second authorized client applied command is claimed. These remain concrete blockers even if the option is later authorized.

## Evidence acceptance and adverse expectations

Preserve the merged TransactionEvidenceCall implementation and normal watermark/application path. Either result/frame order is allowed; require exact command/path/origin/root/actor association and full DB epoch/revision from its transaction; reject duplicate/conflicting, filtered, no-op, missing or unsubscribed evidence. Original processing/storage application must actually accept this transaction; an older matching R4 skipped after unrelated R5 stays UNKNOWN and triggers no receipt query. Require the single authenticated exact subject/key/path receipt found=true in the same epoch at fence >= command R, but return transaction R, never later fence R+1. Timeouts mean possible application/UNKNOWN, not cancellation or retry permission.

Before implementation release, add tests for: absent/invalid/old auth artifact; new auth missing hash cannot inherit it; reload/same-contract reload before/after serialization and every await; binding/ref hash mismatch; unknown execution values/extra keys; forged option auth/hash/proof/entropy; localExecution without explicit opt-in; entropy unavailable; pending auth/updateRequired; queue/reconnect/local/outbox hooks never entered; one keyed send only; all merged evidence races including stale-skip R4/R5. The current 29 transaction regressions pass unchanged, including the two corrected stale-skip schedules; future option/capture logic is not implemented or tested. Rollback removes the additive option/state capture and leaves option-absent refusal, ordinary behavior and wire/server/schema untouched. Old servers with absent artifact/capabilities refuse; no weaker fallback.

This is ready for a source/contract ruling only. There is NO authorization or implementation of the SDK option here.
