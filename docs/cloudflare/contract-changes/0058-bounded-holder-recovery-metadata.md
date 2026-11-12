# 0058: scope fencing and bounded holder recovery metadata

Date: 2026-11-12. Status: authorized design/ownership; exact-artifact proof still required.

K22 owns the narrow SocketHost persist/restore/releaseSlots/close-cleanup change and leaf recovery-store;
X01 has handed off immutable PR #397 and is not retasked to edit its Host implementation. Lead integrates
K22's patch with X01's common-sink/metrics hooks. This does not accept either implementation or waive a gate.

Retained live/control query requests, replica subscriptions and direct Action correlation can exceed the
existing 16-KiB socket attachment limit. Authorize a narrow exception to "nothing ephemeral in SQLite":
bounded holder recovery metadata in the holder's real synchronous KV/SQLite, keyed by connection/scope/epoch.
It is not a result-row cache, execution/replay queue or per-commit telemetry sink. Do not silently drop active
entries, reduce legal argument/subscription limits or move customer data into an unbounded recovery cache.

- Move oversized subscription/correlation maps out of attachments, with <=64-KiB UTF-8 chunks, versioned
  generation manifest/hash/scope/epoch and bounded active namespace/lifecycle cleanup. Chunk every legal
  large request; one giant KV/row/map is not an answer. Auth identity left in attachments still needs measured
  serialized-size proof for legal identities, not an assumption that all remaining fields fit.
- Explicit SessionHooks.recoveryChanged marks semantic request/correlation additions/removals/success/scope
  clear. Persist the latest ACCEPTED SEMANTIC request (including current args/window/filter/options/owner),
  not every result's windowRevision/resume hash and not each generic persist callback/delivered frame.
- Latest result hashes may remain memory-only. After hibernation/restart, execute the actual authorized query
  and send a valid fresh/full result where hash continuity cannot be proved. Never claim unchanged/preserved
  state from a stale hash. This loses an optimization, not the required data/visibility semantics; demonstrate
  actual SDK/protocol acceptance and unchanged conformance. Do not weaken a transcript assertion/normalizer
  to conceal a real contract mismatch; report it for ruling.
- Publish store generation/reference before attachment, keep old generation until successful attachment
  publish, and recover atomically or detect mismatches. Bound/clean orphan staged generations on failures.
  Missing/partial/corrupt/wrong-scope metadata fails closed with bounded recovery (1012), never stale restore.
  Close/abandoned-connection cleanup must not erase live newer ownership or race a current generation.
- pendingActions metadata is correlation only: no automatic re-execution/replay or fabricated success.

Scope epoch fence: stamp every known connection on each commit, including no-op/watermark-only work, and
EVERY split part. Reject mismatches BEFORE instance/data/watermark changes. Scoped missing-epoch old-sender
work is not safe to treat as current: bounded staggered 1012 is allowed with explicit unsupported/skew state,
not infinite bouncing. Undefined work with a new instance cannot certify scope; bounded reconnect is allowed.
Same-instance undefined work is ignored without watermark advance. Verify current-epoch no-subscription
receipt/watermark paths still progress. Fresh current-epoch query/replica opens establish scoped instance.
Recovery captures epoch before serialized work and rejects on scope/tenant changes; no stale recovery ack.
queryRecovery is constructed only server-side, never decoded from client data.

Test attachment boundaries/large args/counts, semantic request changes, hibernate/restore with lost hashes,
partial persistence/attachment failure, close cleanup, late callbacks and Member/tenant/reauth/deploy races.
Measure storage/CPU amplification and no per-frame writes. Original correctness/visibility/durability and
V01–V06/local/platform gates remain open until evidence exists.
