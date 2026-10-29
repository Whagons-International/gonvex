# IndexedDB browser guarantees

`pnpm --filter @gonvex/client test:browser-storage` runs both production IndexedDB
adapters in real Chromium on Linux. It bundles the SDK source, uses an isolated
persistent browser profile, and verifies these schedules for each adapter:

| Schedule | Required observation |
| --- | --- |
| Abort after a native entity write request succeeds | All rows and the durable checkpoint roll back together |
| Kill the owned browser during an uncommitted transaction | Reopening the same profile reads the old rows and checkpoint |
| Kill the browser after an acknowledged commit | Reopening retains committed values and deletion tombstones; stale transactions cannot restore older values |
| Receive a reducer result and watermark during a paged snapshot | Caller remains pending, intent remains durable, and checkpoint stays behind the unfinished snapshot; successful promotion allows completion |
| Abort native writes during snapshot promotion, then crash/reopen | No success response, no checkpoint advancement, and the same durable intent key, local sequence and arguments remain retryable |

The abort and mid-transaction crash fixtures witness the native IndexedDB
request's `success` event before interrupting the transaction. The crash test
keeps that transaction active with native requests until the browser is killed.
It obtains the browser PID from its own CDP session and verifies the PID's
command line identifies this fixture's temporary profile before sending
`SIGKILL`. It never targets a user's existing browser. Profile data lives in the
configured test directory and is removed at completion.

The SDK scenarios inject protocol-shaped messages through a controlled socket.
They use actual IndexedDB for the replica and outbox. All state changes go
through the normal SDK and adapter interfaces; the frame-lane promise is read
only to join processing. A restarted queue must recover the original durable
intent instead of allocating a replacement identity.

## Negative control and model connection

A separate, in-memory bundle removes the production `deferSnapshotFrame` guard.
Both adapters must then persist watermark revision 8 while the snapshot row
covered by that certificate is still absent. Arbitrary exceptions, missing
fixtures and timeouts do not satisfy this negative control. The production
bundle must keep revision 7 until the final snapshot page is persisted.

This connects real browser schedules to the saved-data-before-certificate
invariant in `ReplicaDurability.tla`. It is integration evidence rather than a
source refinement proof. Removing the guard also shows why an interleaved
watermark is not, by itself, evidence of an unsafe SDK acknowledgement. The
consumer's snapshot deferral is part of the contract.

The suite contains ten positive adapter scenarios and the broken-bundle check
for both adapters. A separate outbox scenario commits an accepted intent, kills
the browser and verifies late rejection/retry/failure cannot re-arm the same
durable record after restart. The Node runner reports 14 tests including parent
tests. The original 13-test Chromium 149 run used one CPU, 1 GiB memory and no
swap; peak charged memory was 304 MiB. CI repeats the suite and retains a JSON receipt
with completion status, source hashes, bundle hashes and browser version. A
missing declared case or failed negative control fails the run.

## Limits

These checks cover browser-process failure while the OS remains running, not
machine power loss or disk failure. They do not prove Safari, Firefox, quota
exhaustion, storage eviction, authorization admission, arbitrary cross-tab
schedules, server-native encoded snapshot delivery or native app SQLite. The
controlled wire is not a running Rust/PostgreSQL backend. These boundaries
remain in the broader guarantee register. No dedicated OPFS or SQLite WASM work
is included.

CI already installs Chromium. For a local run, install the workspace's pinned
Playwright Chromium and supply a directory on the build filesystem:

```sh
GONVEX_BROWSER_TEST_DIR=/absolute/test-profile-parent \
GONVEX_BROWSER_TEST_OUTPUT=/absolute/receipt.json \
pnpm --filter @gonvex/client test:browser-storage
```
