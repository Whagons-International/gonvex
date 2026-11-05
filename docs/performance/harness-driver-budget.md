# Harness driver optimization: budget qualification failed

Implementation `06a277f67d3eab7e1bf3ee63c6a886fd99227cef` completes incremental checkpoint and abort-accounting work.
No second 1000-user baseline was attempted. The original historical case remains
`INVALID_GENERATOR`; runtime capacity, final-gate SLO and 5k core claims remain unqualified.
The final frozen receiver replay fails under the actual 6-GiB/2-CPU lane, even
with no live runtime or auth work competing during its offered bursts.

## What changed and how correctness was checked

The thin driver maintains projected-row hashes and the XOR incrementally, journals
membership additions/removals, publishes only changed members, and advances
metadata-only watermarks without traversing collection rows. It suspends dirty
records until an ordered ready/watermark commits them atomically. Fresh reloads
still independently validate every stored member and digest. `needHashes` and
legacy SHA mode retain complete inventories. Canonical arguments, principal,
origin, tenant, contract, module epoch, full window membership and cursor stay bound.
Deep immutable value interning requires exact JSON equality; row IDs alone never
reuse values or authorize another identity's checkpoint. Cross-pool releases and
replacement order are covered by fresh-driver tests.

Results/progress now contain event-loop, GC and hash/checkpoint-work counters.
Real Node workers start with the TSX loader directly. Coordinated SIGTERM waits
for fsynced partial/progress acknowledgements before reaping each owned PID.
Partial results preserve error events, raw measurements, sign-in timestamps,
unfinished operations, incomplete-user counts and artifact pins.

All **57 Node tests and 5 Python tests pass**, including the approved actual SDK
through identical snapshot/page/delta/transaction/reset/needHashes histories,
24 further mutation histories, corruption/identity boundaries, and a 9k-row,
132-window protocol test. In that test 100 metadata-only watermarks perform zero
row visits, rehashes or checkpoint copies; a changed row plus a deletion commits
exactly two changed memberships, including an equal-cursor batch watermark.
Docs MDX/route generators and typecheck pass; harness runtime import cycles remain
0 → 0 against integration2b7e2ff. Only harness/docs files changed; app/schema,
protocol and SDK/framework generators have no changed inputs.

A fresh isolated synthetic runtime proof with the final driver passed cold/warm
**132/132** SDK/thin projected-row comparisons for the admin and an ordinary
principal, and fresh full-set comparisons after an actual shipping share and
old-row revoke. Actual SDK warm `__local.userPresence` opens/cursors were required.
Share/revoke convergence in this small proof was 1249/984 ms; this is correctness
evidence and no passing permission-latency claim. Normal password and verified
synthetic identities were used. No email-policy bypass or customer data was used.

The eight-worker abort-only probe ran actual `run.ts` processes, offered eight
users over a 900s ramp, and stopped after initial progress. **8/8 durable partials**
were acknowledged and merged, with pending/readiness/error accounting and SDK
pins intact. It is not an eight-user capacity/SLO result.

## CPU attribution

The original first-burst miss (7/75 late; generator p95/p99 1412/1960 ms) remains
preserved at `../historical-baseline-20261105/REPORT.md`. Its non-runtime 1.376-core
mean could not distinguish auth, generator and sampler.

The new isolated component probe repeats run.ts's existing-account sign-up
refusal followed by password sign-in. It retains the same offered 75/5s and
150/5s timestamps, using the previously normally verified accounts. Runtime
token exchange and collection opens are excluded from this component probe.
Each component has a separate actual child cgroup. The sampler reads cgroup
resources once per second; these figures do not claim to price every production
profiler/HTTP sampling configuration.

| Offered identities | Auth CPU-s | Request driver CPU-s | Resource sampler CPU-s | Completed p95/p99 ms |
| --- | ---: | ---: | ---: | ---: |
| 75/5s | 0.181853 | 0.248971 | 0.119903 | 4.40/48.52 |
| 150/5s | 0.404073 | 0.261359 | 0.124716 | 3.48/5.25 |

Neither phase missed an arrival window. Helper auth used about 0.036/0.081 cores
at these offered rates; resource sampling used about 0.024/0.025 cores. Auth and
this sampler do not explain the captured full-row receiver's CPU demand.
The separate full cold/warm/share/revoke correctness proof consumed 50.606 driver,
7.440 runtime, 5.994 auth and 1.160 sampler CPU-s including startup. Its driver
figure includes six actual SDK sessions and thin sessions, so it cannot be
attributed entirely to thin code or used as a paired speed comparison.

Captured-frame CPU profiles located repeated full integrity scans, page scans,
checkpoint publication and serialization before optimization. After the changes,
JSON receive/parse, exact immutable-value comparison, initial hash construction,
per-window membership maps and GC remain material. Exploratory 20-user/5s
replays of the same full history reduced checkpoint CPU from 4.393 to 0.195 CPU-s
and receiver CPU from 6.366 to 2.958 CPU-s. These intermediate diagnostic iterations
are retained with their profiles, not presented as a formal same-head capacity A/B.
A parse-only floor for 75 histories was 1.941 CPU-s: it does **not** prove that
all possible receiver implementations require more than two CPUs.

## Frozen full-payload burst qualification

`final-qualification.json` and `final-replay-inputs.json` pin the final measurement.
The measured runtime driver/supervisor sources match implementation `06a277f67d3eab7e1bf3ee63c6a886fd99227cef`.
Eight real Node receiver workers use independent checkpoint stores and a shared
immutable value pool within each process. Each worker first proves one actual
candidate-2 SDK reference and one thin reference against the captured SDK row
oracle. All eight reference proofs passed. Reference startup is serial; it is
outside the offered burst and not a replacement for SDK arrival qualification.
Node heaps are bounded at 384 MiB per worker to limit transient GC expansion;
the enclosing actual aggregate caps stay unchanged.

Every attempt opens all 132 actual shipping intents and receives the entire
captured cold history: 338 real frames, 8,851 tasks, 16,042 total window rows,
approximately 18.58 MB of JSON. Each receiver parses its complete wire frames,
checks page order/authority, computes its actual hashes/digest and publishes
validated rows. Only immutable payload text is cached by the frame producer;
receiver JSON parsing and state work are timed. Frame-envelope producer CPU is
reported separately (0.412/0.662 CPU-s).

All final attempts reached complete readiness and matched the full SDK projected
row oracle. Those expensive independent row comparisons run **after all eight
timed results** at a global barrier, so they cannot starve another timed worker.
The final histories perform zero full-row integrity scans after the reference
prewarm; they still parse and check every received row and maintain every window.

| Offered users | Arrival p95/p99 ms | Starts after 5s | Full ready p95/p99 ms | Receiver CPU-s | Receiver cores at offered rate | Total worker CPU-s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 75/5s | 79.13/80.26 | 0 | 4280.39/4419.92 | 12.869367 | 2.574 | 16.747658 |
| 150/5s | 86.11/187.47 | 1 | 8551.93/8749.28 | 20.766223 | 4.153 | 25.231843 |

Qualification is **false**: full readiness exceeds p95<500ms/p99<1000ms, and one
150-user start misses its five-second offered window. No pending readiness was
dropped. GC wall spans summed across workers were 5.001/4.236 seconds; maximum
observed event-loop delays were 401.87/304.87 ms. These concurrent wall spans
must not be added to CPU-s. Summed sampled worker RSS was 3,305,508,864 and
3,689,357,312 bytes. The encompassing final measurement service peaked at
4,437,061,632 bytes; OOM counters were zero. Its replay interval reports
1,588 throttled parent periods out of 2,427. Cap files record exactly
`memory.max=6442450944`, `cpu.max=200000 100000`, swap disabled.

This is an intentionally full-payload receiver stress qualification using one
captured authorized principal/history, **not** the production tenant/permission
mix. No cache is copied between identities. It has 16 resident reference sessions
before the first burst and at most 241 sessions after both bursts, not 1000.
Bursts contain thin arrivals; actual SDK references remain resident. It therefore
cannot qualify the eight SDK users' live burst lifecycle, 1000-user residency,
steady delivery, deploy refresh or complete permission behavior. Its failure is
sufficient to withhold the requested load under the declared qualification rule;
it is not a proof that every optimized architecture or the exact weighted live
mix must fail on two CPUs.

## Practical resource/architecture choice

The current full-JSON/SDK reference architecture needs measured receiver
throughput of 2.574 cores at 75/5s and 4.153 cores at 150/5s, before auth, runtime
and other generator/GC work. Total worker demand at 150/5s is about 5.046 cores
at the offered rate. Dividing by the offered interval is a throughput sizing
figure, not a prediction that a larger allocation meets tail latency. The
requested two-core **aggregate** cannot provide that measured driver envelope.
No caps were increased and no work was moved to the Mac.

The parent can choose a separately budgeted generator (including the coordinated
native Mac option) and qualify it with these frozen inputs, leaving the actual
runtime at one CPU/2 GiB pinned to 7,15. A practical first trial needs more than
the measured ~5.05 generator cores plus distinct auth/sampler headroom; it is not
a guaranteed passing allocation. Full 1000-user resident/cache memory, actual SDK
arrivals, transport delay and the same bursts must then be qualified before the
single second baseline. Alternatively, retaining two aggregate CPUs requires a
further receiver architecture change, such as an exact validated wire-value
interning decoder, plus SDK/oracle/corruption equivalence evidence. The parse-only
floor leaves theoretical room; this report makes no universal impossibility claim.
Neither option was silently selected or launched here.

## Capability and qualification matrix

| Requirement | Status/evidence |
| --- | --- |
| Harness cache/state/measurement implementation | Done; 57 Node + 5 Python checks and actual full-set SDK comparisons |
| Durable coordinated aborts | Done; all eight actual run.ts workers acknowledged fsynced partials |
| Historical shipping SQLite schema/module + 132 intents | Qualified small synthetic probes; unchanged frozen pair |
| Normal new-device load | Implemented; no starter label or private fixture substitution |
| Generator under actual 6 GiB/2 CPU | Failed full-payload burst qualification; no second 1k run |
| Realistic weighted 1k SLO, 300s steady, 1000 cached-refresh attempts | Unqualified; original baseline invalid, second run withheld |
| Actual SDK refused-open burst observation | Instrumented/tests retained; no new qualifying live 1k evidence |
| Runtime CPU per snapshot/reload/delivery and 5k projection | Unqualified; receiver CPU above is generator work only |
| Current staging SQLite physical schema/module/open set | Blocked; PG compatibility receipts do not qualify SQLite |
| Full permission primitives/provider/342-path acceptance | Blocked until real complete handoff; shipping share/revoke is narrower |
| Bilaterally negotiated starter SDK/network path | Blocked; default-off backend/RFC/injected decoder is insufficient |
| Runtime-restart-all-at-once | Separate stress case; not run and never folded into gate |

## Pins and preserved evidence

- Implementation source: `06a277f67d3eab7e1bf3ee63c6a886fd99227cef` on `perf/harness-expanded-gate`.
- Runtime source `2b7e2ff9f6950ea73a4659c284c9204262314574`; binary
  `8367959c2ec47c6480b3fa27baa119d47364ac5c37f9045f8e26b07dfc22abf8`.
- Module-host binary `a6decd429c53d3fa6df5ff6735eee45c550b42063113935eddb39918eaae4fe3`.
- Shipping app `37ca8278be4365951816d05c3312abc66df51dd4`; module
  `d39570bbfc685c71f377f30033063f3853e239b37278db7d59b7c7372bdebbd3`;
  manifest `7676b14074cef430d1b74bcf4bc0d3414669f87d5e130e5f7fe7603cb345e353`.
- Approved candidate-2 SDK JS `60b2673135e0ffd245b2219fb8a18cd2f6add3ac98ccbfa6af9cb6f5ae20eb0a`;
  all 128 payload members verified against the frozen composition; original
  patched SQLite.1 and deferred-open APIs preserved. No PG artifact installed.
- Required collection fixture `f6e151af66b445edb796ab3a2c4d28e474426789b5502ff7326829d907e65f62`.
- Final thin-client source SHA `b9d8a001c507de3e5c48540b0520c432e435d94da66d7e5a582afb0fb7e2ea55`;
  checkpoint source SHA `561829c50fe98084860e8c6c08624e397ef87f902ec2ef640a37eb70e7bb2ef3`.
- `artifact-manifest.json` SHA `f216bd4499f15dd1380f0b6f4e343d75e3d663f9df13145c4347a7a623e2f4d4` pins final source, frames,
  SDK wire oracle, old invalid report/receipts, all physical-schema evidence,
  stopped own synthetic probe database/schema hashes, tests and result files.
- `final-replay-inputs.json` SHA `7f78477b134fe8ce5a09ae8b7393bb922696bba478af65c54887e70e624b3ce2`
  preserves the exact input/source freeze. Its README copy predates the final
  documentation; every measured runtime driver/supervisor source matches the implementation.
- Evidence root: `/mnt/data/work/gonvex-harness/driver-opt-20261105`.
  Original invalid-case evidence remains untouched in the historical-baseline directory.

An initial micro replay OOMed at 6 GiB while eight SDK references prewarmed
concurrently without per-worker heap bounds. It is preserved as a failed setup,
with 199.590 CPU-s/101.463s and no qualifying result. Intermediate mixed-SDK
replays let one worker's post-timing oracle overlap another worker's timed work;
their wall-latency values are excluded from final qualification. Test development
also caught missing stream bindings in new fixtures and a wrong SDK diagnostic
field; these were corrected, and all 57 final tests pass. The first post-fix
native proof passed its oracle but its wrapper then hit an unset log-path
variable before component probes. Component/abort probes ran separately after
that wrapper fix. None of these attempts was another 1000-user case.

All native runtimes/auth helpers and replay workers are stopped. Runtime probes
used actual nested one-CPU/2-GiB caps, affinity 7,15 and isolated loopback namespaces.
The aggregate stayed at 6 GiB/2 CPUs including local helpers. No browser, Mac,
customer-data, CI, PR, deployment, privilege or other-worker-cap action occurred.
The lane lock is released at handoff; the final handoff receipt pins this report
and the final feature/documentation head.
