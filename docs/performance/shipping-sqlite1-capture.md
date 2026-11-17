# Shipping SQLite.1 collection observation (2026-11-05 UTC)

This is actual shipping-branch observation, not a current-staging capture or a
capacity measurement. App source37ca8278b, published patched0.5.2-sqlite.1 SDK,
and its own generated module ran on the unchanged7d01af5b runtime. No app/SDK
behavior fix was applied. Only served-code observation markers were added.
Current staging814415ca0 has29 frontend-file deltas plus module/SQL differences;
its live collection counts remain unverified. No frontend/module overlay was used.

Pins: module SHA256 `7676b14074cef430d1b74bcf4bc0d3414669f87d5e130e5f7fe7603cb345e353`;
SDK patch `06786316f912c262f7638b8f3c82cff02a8787f71eabcafca84b4087a3333334`;
client JS `4f9ce8be47c21742b49d760ebf0c695da13ab4c2f814d6df5a7d843a416fa2b4`;
React JS `680e3a1339281aab9388de6d5cabeeacde6b3c9a344d6d1714134ba7572f6da9`;
runtime `d8283384ed1df4ddff65c46fed0e31075a8a0978fc9ade608d8cac9d597d5f32`.

## Counts and phase boundaries

Both cold routes observed132 wire-open attempts and132 distinct path+args:
welcome/GetStarted and normal signin redirect to /workspace/all. The immutable
harness list is `bench/whagons/app-opens-shipping-37ca8278b.json`; member arguments
use `$member`. Its hash and raw phase hashes are in evidence/shipping-capture-sha256.json.
This conservative first-visit union retains the late `__local.userPresence` and
`floatingChat.listActiveMessages` opens. It has41 __local paths and91 other paths.

Warm reload:130 startup opens (89 cursor/41 cold), then131 settled opens
(90 cursor/41 cold). There are131 unique path+args in this warm observation.
Do not compare132 first-visit unique opens with the old177–178 attempt count as
though both measured uniqueness. The old full first-visit unique count is unavailable.
The old reload had130 opens,126 argument-less and40 __local;126 did not mean126 local.

Times below are seconds from each phase's observation start, including human
signin/GetStarted time. They are timeline markers, not page-load benchmarks.

| Observed route | Gate arm | Gate claim | First tasks.listWindow result | First release | Last open | Observation end | Opens before first task result |
|---|---:|---:|---:|---:|---:|---:|---:|
| Cold welcome/GetStarted |9.020|33.665 (already released)|33.841|24.020 (15s unclaimed fallback)|76.985|161.603|126|
| Cold signin redirect |22.045|23.135 (waiting)|23.320|23.808 (claimed)|37.401|175.356|125|
| Warm workspace reload |0.622 (released)|1.109 (released)|1.310|already released at startup|15.979|135.889|130|

The redirect opened125 collections before the first task window. The observed
shipping path therefore does not demonstrate31 pre-grid opens. Source-only
inspection found firstScreenCollections.ts deferBelowFirstScreen returns false
outside /workspace/. Auth ensureLocalCollections calls subscribeReplicaTransport
when activating scope, before the workspace necessarily mounts. This is a source
explanation to test against cold-route timing, not proof every early open used
that route (the earlier cold observer did not record a route per open). The final
warm hydration observer records /workspace/all for its131 opens. No first-screen
app fix is authorized here. A second concrete source path is
WorkspaceStartupTaskDemand.tsx26–29: its effect calls releaseFirstScreenCollections
whenever !onWorkspace. App.tsx242 mounts it. The registered SDK release method
unconditionally clears replicaOpenDeferral (client/dist/index.js2834–2836), so a
non-workspace visit can disable collection deferral for subsequent workspace
navigation independently of the task gate's waiting/claimed state. This mechanism
is source evidence; the earlier cold observer did not mark that specific effect.
Settling was
observed well beyond15s, preserving deferred opens and the release marker.

## Auth cause and ordinary fixture preparation

The observed unverified synthetic account hit the actual app's
email-verification-required policy. Identity generation2 was cancelled by the
policy signOut (generation3); the valid external grant arrived after cancellation
and failed the existing generation guard. This was policy cancellation, not proof
of an SDK/runtime contract mismatch. Grant observations retained names/types only:
string tokens, finite current numeric expiries, account ID, one tenant, matching
active tenant/catalog/selector. Expected control-only frames were kept distinct
from app auth. Tokens and OOB codes were never logged.

With explicit parent authorization and the policy marker confirmed, the owned
clone's load-user-0@example.invalid completed the emulator's ordinary VERIFY_EMAIL
OOB flow at05:43:43.632301 UTC. emailVerified changed false→true. No policy bypass,
credential-metadata hack, real email, shared baseline mutation or external auth was
used. See evidence/shipping-fixture-email-verification.json. Future synthetic
accounts require the same normal verification preparation. The failed-auth raw
sanitized proof remains under /mnt/data/work/gonvex-gate6-capture with committed hash.

## Durable hydration evidence; SDK fix pending independent approval

All41 cursorless warm opens had a matching durable cursor+hash record for the
same scope/window/path/args.37 held zero rows;4 were nonempty. Each early resume
read preceded IDB load completion; post-hydration the same window had a cursor.
Thus empty-window persistence does not explain these41 cold opens.

The final force-restarted observer records scope6,133 persisted windows,
IDB end1793858512712.1ms and replica hydrated1793858512762.5ms. Per-path read,
wire-open, IDB end, hydrated times, durable revision, row/hash counts and
post-hydration state are committed in evidence/shipping-hydration-41-proof.{json,csv}.
These are checkpoint metadata, not row content or credentials.

Shipping client/dist/index.js activateReplicaDirective starts activateScope and
ensureLocalCollections while only awaiting outboxJournalReady. restoreOutbox
publishes that journal barrier before replicaReady. sendReplicaOpen/flush do not
await durable hydration before synchronous windowResumeState. IndexedDB restores
empty-window records too. Proposed ordering fix: await current-scope hydration,
then recheck auth/socket/scope generation and subscription/deferral ownership before
constructing or flushing opens. Deterministic hydration/cancel/auth-rotation tests
are required. This report implements no SDK/app change; Gabriel approval is separate.

## Cleanup and limits

Capture own lock released06:03:37.314890 UTC after all owned tabs, Vite servers,
forwarders and gvcap containers stopped. A new independent integration slot began
06:04:21 UTC, ending06:24:21. No runtime merge/binary swap occurred during capture.
Raw observation chunks, isolated source, configs and sanitized failed-auth proof
remain in /mnt/data/work/gonvex-gate6-capture. This was a single synthetic member,
dev frontend and warmed backend; no latency percentile, sign-in CPU, full-app
reload performance, gate pass or current-staging parity claim is made.
