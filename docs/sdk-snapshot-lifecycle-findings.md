# Snapshot lifecycle findings

Tested against staging `79668483b46fd8901aa0beacaf9f6e837d2f92e6` through public GonvexClient subscriptions and WebSocket frames, with gated MemoryLocalReplicaStorage calls.

## Removed subscription blocks other feeds

While subscription B awaits a storage write, queue subscription A's first non-final snapshot page, then unsubscribe A with zero retention. The old queued handler can create a stager after A has been removed. Its entry in the client's global staging set then defers unrelated feed frames forever if no final page arrives.

The new regression fails on staging with B stuck at value 2 after receiving value 3. It covers removal and a replacement subscription with the same key. The fix checks exact subscription ownership when queued work executes and invalidates the removed owner's generation. Close, disconnect and scope reset also clear the staging set.

## Recovery continues after close

A need-hashes observer can call `client.close()` before a queued recovery microtask executes. On an unbatched connection the old recovery calls connect again. A reset awaiting storage can also notify an old observer after close and schedule recovery.

The need-hashes tests first commit a hashed window, reconnect with a real cached cursor/digest request, and require full-integrity keys/hashes on normal repair. Three baseline regressions demonstrate the extra socket or observer notification. Batched need-hashes already passes and remains a compatibility control. Recovery now checks owner, scope and transport generation after awaits and again in its microtask. Explicit public connect after close, normal recovery, automatic reconnect and activation-failure recovery remain covered.

## Validation boundary

These are deterministic native JavaScript regressions, not formal proofs of the async protocol. Tests use a fake transport and real memory storage, with no deployed occurrence, native phone behavior or performance claim. The shared client implementation serves browser and React Native callers. Expo storage and platform lifecycle behavior have separate coverage.

An additional source-review candidate remains open: an already-running delta storage write may still notify its observer after close. This patch does not claim to cancel in-flight writes or fence every SDK publication path. That trace needs a separate public regression before being reported as another reproduced bug.
