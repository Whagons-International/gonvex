# 0093: receipt-crash accepts both documented outcomes of the in-flight call (decided by lead 2026-11-18)

Raised by X03 (criterion 5, run7 receipt-crash step 7).

## Observation

"Backend killed right after a keyed call is sent" sends `board.bump` with an idempotency key and restarts the
backend at once. The scenario's own description says the replay after the restart applies exactly once
**whether or not the first attempt committed**.

The Rust reference is SIGKILLed microseconds after the send, so the call never commits before the kill. Its
effect arrives with the replay after the restart. A deployed backend's restart is a separate request (the dev
fixture abort, #482), so the call usually commits first: its effect reaches the SDK before the reconnect, and the
replay returns the stored receipt. Both are exactly-once, yet the strict per-step frame diff differed.

## Decision

A step-scoped normalizer accepts **exactly** the two documented outcomes of that call, for the SDK session's
board frames in steps 7 ("backend killed right after a keyed call is sent") and 8 ("replay after the second
restart applies once"):

- **committed after the restart** (reference shape): no board frame before the reconnect; the reconnect snapshot
  holds the counter at N (its value before the call) at the last revision seen before step 7. Step 8 brings
  exactly one transaction and one delta with the counter at N+1, plus its ready and watermark.
- **committed before the crash**: exactly that effect block (one transaction, one delta at N+1, ready and
  watermark, all at the commit revision) arrives before the reconnect; the reconnect snapshot holds N+1 at that
  revision; step 8 has no board frame.

The second shape is rewritten into the first. The effect block moves to step 8 on the reconnect's subscription,
and the snapshot is restored to N at the earlier revision. The two command-id aliases that first-appearance order
swaps are swapped back. **Anything else fails the run and is left unchanged**: two effects, a bump other than +1,
no effect at all (a lost call), effects in both steps, or a snapshot that disagrees with the early effect. The
step observations (`replay` result and final `sdkRows`) are still compared exactly and independently, so a
doubled or lost effect also fails there.

## Implementation

`scenarios/receipt-crash.mjs` `canonicalizeInFlightOutcome`, called by the runner on normalized frames through
the scenario's `canonicalize` hook. It records `trace.inFlightOutcome`, which is not compared.
`test/receipt-crash-outcome.test.mjs` covers both accepted shapes (the second compares equal to the first) and
five rejected ones. End to end through the remote restart path: reference committed-after-restart, candidate
committed-before-crash, receipt-crash PASS. Locally both backends commit after the restart: PASS.
