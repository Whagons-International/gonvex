# 0065: Bounded authority admission and terminal release

Date: 2026-11-12. Status: decided (lead). Explicit resource-admission adaptation; no capacity claim.

## Pending initial requests

A provisional ceiling of 4,096 unsettled initial references per physical socket is permitted with aggregate
count and estimated-byte admission. It is a new resource guard, not an existing Query contract inferred
from the tenant-wide replica waiting-open limit. It must not limit installed subscriptions, tighten argument
or frame limits, evict accepted work, or discard the prior successful subscription when a replacement is
refused. Refuse new admission honestly and with existing correlated error shapes before forwarding or
mutating the pending book. Do not invent retryability fields absent from the protocol.

K22's declared provisional aggregate envelope is 16,384 unsettled references and 32 MiB estimated semantic
pending bytes per isolate, with a sole legal large singleton when otherwise empty. These are safeguards,
not measured heap/throughput guarantees. Count full retained semantic requests and metadata, including
across sockets/objects, and release credits on actual matching terminal/obsolete lifecycle events.
Legal-frame copies, other state, completed semantic-request storage and the shared 128-MB isolate still
require V04 measurement. No fit or safe-user-count claim follows from these constants.

Boundary, batch/duplicate-id, replacement, overflow, hibernate and aggregate accounting tests are required.
4095/4096/4097 cases must preserve existing accepted requests and all argument contents. No per-frame
persistence or unbounded historical generation map is introduced merely to enforce the counter.

## Historical scope fences

Review N6 found the tenant retaining every historical physical connection epoch. Permit an additive terminal
owner release and finite count/estimated-byte authority admission. Keep active owners working at the
ceiling; refuse new authority before application invocation using an honest correlated failure. Record the
chosen ceiling/rationale and metrics as provisional safeguards, not existing Rust semantics or proof of
unlimited churn/availability. Never evict active or uncertain fences to admit another connection.

A release binds the physical owner/incarnation, tenant instance and terminal epoch. Stale/duplicate/old-owner
release cannot delete newer state. Count underlying issued calls and deferred admitted work, not a timeout
wrapper. A settled caller-side timeout/network error does not establish that remote work finished. Ambiguous
completion is UNKNOWN. Do not restore lost in-flight accounting as zero after a reset.

Tenant-side terminal reset/drain must establish quiescence before deleting a fence. Late old work cannot
recreate authority through default admission after release. Tests must reorder held dispatch/reset/recovery
with release, include late completion after timeout, holder/tenant resets, duplicate old release and old
requests arriving after release. No blind TTL, missing-enumeration inference or per-frame KV counter writes.

Finite admission can bound uncertain retention while cleanup remains unresolved. It does not close 0058's
stranded durable OWNER lifecycle gate or prove eventual cleanup. Preserve UNKNOWN/blocker evidence until
authoritative cleanup and actual fault/availability tests establish the promised behavior.
0069 permits a bounded tenant-instance authority prototype with explicit admission/drain and callback
race/reentrancy guards. It does not mark terminal release or stranded-owner availability complete.
