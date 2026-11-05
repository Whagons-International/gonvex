# Starter bootstrap transport candidate

Default off. This candidate adds a real authenticated WebSocket/SDK path to the
accepted SQLite starter backend. It does not activate full Whagons policies,
change an app, compose the shipping SQLite.1 candidate2/defer API overlay, or
replace the separate PG compatibility artifact.

## Provenance and admission

Branch `t3/sqlite-starter-bootstrap` descends from accepted backend
`dac237a96e2de62fc042f352c3107dfecb134dff`. It cherry-picks hydration implementations
`2743e0a0` and `728b857e` as `e20dd48e` and `c7185f65`; evidence branch `14d3e6f8`
was inspected, not merged. The backend's pinned integration/permission choices
are recorded in [starter-files.md](starter-files.md).

Both runtime feature `starter-files` and `GONVEX_STARTER_BOOTSTRAP=1` are required.
The client must explicitly opt in, inject a bounded inflater, and use an engine
with guarded atomic promotion. The server acknowledges negotiation after real
native session authentication. Unsupported authentication/providers/windows
remain ordinary opens. The original local permission index restriction remains:
this candidate's structural provider is usable only in explicitly installed
owned local fixtures. No automatic production index installation is added.

The narrow sealed `starter-shared-team-v1` proof checks the actual loaded v2
policy, source declaration/dependency closure, physical schemas, exact collection
and projection. A version string, legacy normalized receipt, declared complete
boolean, or test certificate constructor cannot authorize an offer. Member
lists, nonempty args, filters, parents, joins, negatives, unmodeled membership
status/deletion columns, truncation and native sum integrity use normal opens.
The independent full-provider compiler is not accepted through this proof.

The normalized SlotAny compiler uses an indexed finite-universe `IN` lookup.
Its former join between DISTINCT-backed views caused nested scans at 9k roots.
The SQL policy hash changes, invalidating existing local registrations; they
must be reinstalled. A VM-instruction-budget regression compares 9k allowed
roots with overlapping grants, negatives and foreign-universe rows against an
independent expected set without relying on host wall time.

## Wire v1

The existing authenticated `/ws` negotiates `replicaStarterFiles:1`. An ordinary
`replica.open`/`openMany` may include `starter:{version:1}` for a cold cache.
Usable cached cursors keep normal XOR resume. A negotiated cursor-bearing open
can switch only after a fresh authoritative clock proves history expired or the
epoch changed; its existing cursor/digest request needs no new hint. A valid
current-epoch cursor returns to normal resume before reading any file. A miss
completes a normal snapshot
and schedules a bounded low-priority singleflight file build; it never waits for
that build. The live test blocks the build permit while checking normal ready.

- `replica.starterOffer`: connection/open/auth-owned opaque offer ID, exact
  serialized loaded definition and empty args, projection/types, order/budgets,
  physical visibility scope, starter cursor, server-computed XOR and immutable
  piece descriptors. The internal canonical set/source identity stays server-side.
- `replica.starterPiece`: client requests one sequential index with that offer ID;
  the server returns bounded base64 encoded bytes. Content hash alone is never
  a retrieval capability. There is no HTTP file/hash endpoint.
- `replica.starterCommit`: client confirms receipt/cursor/digest. These values
  acknowledge integrity, never establish authority or a resume anchor.
- Final ordinary `replica.ready` carries the authoritative final cursor/digest.
  Any invalidation/cancel/expiry produces `replica.starterFallback` and at most
  one existing normal open. No second client subscription is created.

Offers, each piece, publication and final commit revalidate current native
session/account/member/route, physical database/epoch, exact canonical set,
source/policy/projection/schema authority, ready generation, permission serial
and retention. Every indexed source mutation invalidates conservatively. Same
member atoms with a row moved out of scope deny the old manifest/piece before
payload, including access using an old piece hash. Relevant changes use normal
open; this version permits only unchanged authorized content plus unrelated
revision catchup.

A fresh writer-ordered guard encloses nonblocking bounded channel enqueue.
Network sends occur after releasing database connections. Enqueue-before-revoke
linearizes an authorized read; revoke-before-enqueue denies payload. Auth
retirement drains stale queued pieces/ready. Final commit performs the actual
normal-v2 principal resolution and SQL window/digest proof, commits its read,
then revalidates under the writer before enqueue and installs the server-owned
normal XOR anchor. Neither client digest nor normalized-set fingerprint is an
anchor. These scans are included in the module benchmark.

## SDK staging and codec

`@gonvex/client@0.5.2-starter-bootstrap.0` and the correspondingly versioned
protocol are separate candidate artifacts, not a shipping SDK replacement.
An example Node opt-in is:

```ts
import { GonvexClient } from '@gonvex/client';
import { nodeStarterInflater } from '@gonvex/client/node-starter-inflater';
const client = new GonvexClient(url, {
  project, tenant, token,
  replicaStarterFiles: true,
  starterInflater: nodeStarterInflater,
});
```

The host adapter is JavaScript with a generated `StarterInflater` declaration;
it adds no Node ambient types or new dependencies to the browser SDK build.
The browser entry has no Node import. No inflater means capability off. The only
provided/tested codec is the separate Node adapter using zlib `maxOutputLength`
**during** raw inflation. This does not qualify real browser new-device loads.
Each piece uses independent level-1/window-12 raw PMD with no context takeover
and the v1 sync trailer convention. Encoded/raw hashes and the encoding identity
are checked. Input/base64, inflation output, integers, NDJSON depth, duplicate
columns/keys, projected key/types, private extra columns and individual rows are
bounded before admission; initial and final canonical XOR use existing rules.

Rows and catchup live solely in an UNCOMMITTED RAM map. All ordinary reads,
hydration, disk checkpoints and windowResumeState ignore it. Only final ready
creates one final-cursor `promoteStarterWindow` transaction. It captures immutable
scope/window/activation ownership. Retirement/cancel/disconnect aborts through
physical IndexedDB commit and prevents RAM publication. Storage without that
primitive cannot negotiate. Memory and IndexedDB engine implementations support
it; other adapters fall back. Old active cache stays usable through staging.
Crash/reload loses the stage and observes only the prior committed checkpoint.

Limits: 256 KiB encoded/raw piece and individual row, 32 MiB decoded collection,
100,000 rows, 1,024 pieces, 60-second offer, eight server offers/connection,
8 MiB server per-connection and
64 MiB server process offer/envelope reservations, one outbound channel credit,
128 registered providers and 16 queued background handles. Disk/build queue
limits retain backend defaults. SDK aggregate accounting is 64 MiB across at
most four stages, including pending codec/base64 buffers, parsed rows/key/hash
maps and catchup. It charges a conservative estimate, not a JS heap/RSS hard
limit, and rejects an offer when its declared worst-case charge does not fit.
Cancelled pending inflate retains its charge until completion. Stats report
current and peak charges; all terminal paths release them.

## Evidence and qualification

Receipts, artifact checksums, live traces, module measurements, cycles and exact
suite results are in `/mnt/data/work/gonvex-starter-bootstrap/results.json`.
`transport-rfc.md` there records the reviewed contract. The live test imports
separately compiled public SDK artifacts and uses real loopback WebSocket,
native authentication, SQLite sources and normal v2 SQL. It verifies first miss
normal ready while build is blocked, later negotiated bootstrap, unrelated-clock
catchup, subsequent normal XOR resume without base-row evaluations, retention-old cached
cursor bootstrap with the old active checkpoint preserved until final ready,
old-client
normal load, feature-disabled old-wire-server fallback, and a row-scope revoke
before piece request with no old bytes and one normal fallback. The old-server
case is a feature-disabled current runtime, not a historical binary execution.

Lifecycle tests cover UNCOMMITTED reload boundaries, independent readers,
bounded inflate, aggregate charges, auth/disconnect/unsubscribe retirement and
storage failure at final promotion. A controlled real IndexedDB transaction
(fake-indexeddb) aborts after physical writes but before commit and preserves
the prior checkpoint. Native browser persistence has not been tested here.
Writer-order tests hold the actual tenant writer through enqueue and prove a
competing revoke cannot enter earlier; current serial/account/session/member
and retired-generation negative cases also deny delivery.

The synthetic module benchmark uses 9,002 visible rows with 9,000 additional
400-byte descriptions, fresh SDK/storage per operation and a warm server/OS.
It compares actual normal cold load to offer/authority/window/format scans,
piece retrieval/decode, final normal-v2 anchor proof and guarded hydration.
Results include noisy wall ns/op, server process CPU, Rust allocation requests
(excluding SQLite C/Node), Node CPU/heap/peak RSS, logical stage charges,
compressed/raw/disk bytes and build cost (including provider registration). The debug runtime and concurrent host
prevent capacity claims. It is not the full app or 1,000-user/browser gate.

Measured module result (three paired operations; medians, unoptimized runtime):

| Measure | Normal cold client | Starter cold client |
| --- | ---: | ---: |
| Ready wall time | 498.6 ms | 2,490.9 ms |
| Rust process CPU | 61.6 ms | 1,827.8 ms |
| JSON application bytes | 4,032,973 | 146,045 |

This candidate reduces transferred application bytes and is slower in this
module measurement. It establishes no cold-load CPU/latency improvement. Normal
operations benefit from existing server caches; the starter measurement includes
fresh source/window checks and the final v2 proof on every operation. Raw rows
occupy 4,021,989 bytes, encoded pieces 103,417 bytes and
published disk files 138,193 bytes. Build plus provider registration
cost 950.2 ms. Published bytes are the logical write cost;
filesystem journal/block amplification was not measured. The peak SDK stage
charge was 52,935,464 bytes and returned to zero after
settlement. Whole Node RSS includes the SDK, committed cache and runtime, and
is not limited by that staging charge. Full allocation, RSS, codec, hydration,
authority/piece/anchor stage timings and built-in fast-profiler counters are in
`performance-summary.json`, `module-benchmark.json` and `fast-profiler.json`.
External `perf record` was denied by the resource unit's perf-event access;
no kernel/resource setting was changed.

Full Whagons coverage remains false. No real dataset was opened; the private
consumer remains pending. Full-provider integration, shipping artifact
composition, native browser decoding/persistence and app/harness activation
require separate qualification. No SDK/app shipping API preservation is implied.
