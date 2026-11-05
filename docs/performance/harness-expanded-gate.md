# Expanded realistic-1k harness checkpoint

This harness branch starts at integration
`2b7e2ff9f6950ea73a4659c284c9204262314574`. It changes no runtime, SDK or app
feature and integrates no permission/starter/heap candidate.

The mixed-v1 schedule remains 700 users over 900 seconds, 75 at 180–185 seconds,
75 at 450–455 seconds and 150 at 720–725 seconds, then a 300-second plateau.
Shared shard epochs, eight shards/eight SDK users, production mix/skew,
uniformly phased one reload/user/hour and actual presence-v2 negotiation remain
requirements for the intended profile. Runtime requirements remain one vCPU,
2 GiB, pinned to CPUs 7,15; generator CPU is separate. No new capacity run is
claimed by this checkpoint.

New opt-in cases and fail-closed qualification are described in
[the workload README](../../bench/whagons/README.md#expanded-harness-qualification-pending).
The deploy-refresh wave does not restart the runtime. It retains full-set
readiness, start-window latency/error attribution, late completions, pending
operations and task delivery oracles. SDK cache qualification requires the
actual loaded snapshot to contain every expected collection cursor and its
membership rows. A runtime restart storm remains a separate stress experiment.

| Required path | Present evidence | Qualification |
| --- | --- | --- |
| Mixed sign-in bursts and steady plateau | Existing schedule, shard barrier and focused qualification tests retained | Harness only; no new measured pass |
| Cached deploy refresh for every user | Fresh SDK instances retain public memory storage; fresh thin instances retain complete validated checkpoint memberships, rows, hashes and cursors | Driver implemented and SDK differential fixture tested; 1000-user measurement still pending |
| Real permission grant/revoke | Ordinary app share/revoke reducers plus fresh SDK row-set oracle; counts additions/removals and old-row removal | Implemented; new live execution pending |
| SDK refused-open lifecycle | Observes actual outgoing retries and incoming refusals/readiness, including late recovery | Observer tests pass; no synthetic needHashes exemption |
| Retry timing | Shipping SDK source has 250ms doubling to 5s; thin client adds bounded jitter | SDK itself has no jitter in inspected shipping artifact; observer does not alter it |
| Current staging app/module on SQLite | PG source codegen covers 368 tables/4338 columns; full SDK pairing quarantined | SQLite module/schema/runtime execution and current open-set capture still unqualified |
| Shipping app collection baseline | 37ca8278b fixture, 132 unique first-visit paths | Historical shipping fixture; cannot establish current staging |
| Complete permission providers | Narrow runtime permission candidate; full binder false | REG006 metadata/provider coverage unresolved |
| Starter new device | dac237a9 private backend, feature off | No negotiated client/protocol/network pipeline; mandatory blocker |
| Runtime restart all at once | Not mixed into ordinary traffic qualification | Separate stress case, not run |
| Steady delivery CPU / 5k sizing | Requires a qualified realistic-1k run and tenant-size-weighted profiler samples | No new estimate or capacity claim |

QUARANTINED: the earlier full SDK pair is frozen at `e7624725d37d0c478b0b2bde1c894e5edca3aeeb`,
SDK `0.5.2-permission-schema.202611050943`, tar SHA
`84a523a26eb50614f9fc0960948bd413d12f4b54542c51c2f9645d56c16adab7`.
Its quarantined receipt is `/mnt/data/work/gonvex-cli-schema/artifacts/pairing.json`.
It was read only, never installed or evaluated. CLI owner found its data.js/data-write.js runtime SQL semantics differ from published staging31 (SQLite json_each versus PG ANY/jsonb), despite types/codegen passing. A new explicit PG compatibility SDK must preserve the published staging31 runtime/data/contract bytes and add only the real compiled permission extension/export/version with the exact CLI pin. That new receipt is pending. No PG module was substituted into a SQLite lab. The
experimental SDK hydration composition remains unintegrated and has no matched
full-set performance claim.

No load run was started: the full gate lacks mandatory runtime/client paths,
and an old shipping binary/fixture run would not resolve those blockers. No
existing lab or other owner's worktree was changed. Preserve the historical
baseline binary and evidence. Use a fresh owned synthetic copy when a qualified
pair becomes available, verify exact runtime/SDK/schema/module/
app/fixture hashes, then run the mixed and cached wave cases before estimating
5k CPU. Do not treat this checkpoint's helper tests as live app qualification.

Validation artifacts live under `/mnt/data/work/gonvex-harness`: 52 Node / 5 Python
focused tests, TypeScript syntax check, runtime import-cycle comparison with
integration (harness scope, zero before/after), docs generators/typecheck, actual
cgroup caps and artifact inventory. No CI, PR, staging, deployment or browser
was started. Execution used an owned outer systemd unit with actual 6 GiB
memory.max and 200000/100000 cpu.max, then coach-resources inside that unit.
The first docs attempt lacked the copied root tsconfig; after restoring the
same base configuration in the owned output tree, generators and typecheck
passed. No dependency install or Cargo build was needed.


## Cached thin driver continuation

The 992 thin clients now retain complete independent checkpoint state across
fresh driver instances. Checkpoints use process-local storage; their canonical
immutable row values may be shared, while identity, authority, membership,
collection contract and cursor state never come from another user's cache.
Only actual server-authenticated scopes activate a checkpoint. Mutation frames
invalidate reusable state until a validated ready or ordered watermark certifies
a consistent row set/cursor. Deferred snapshot changes cannot publish new rows
under an old snapshot cursor. Full refresh qualification still requires all
expected windows and membership rows for every attempted user. Pruned cursors,
changed module/scope, failed opens and incomplete/corrupt pages cannot become a
cached pass by retaining a ready cursor or copying the server digest.

The differential test loads the existing shipping patched SQLite SDK directly
from its immutable installed artifact (entry SHA
`4f9ce8be47c21742b49d760ebf0c695da13ab4c2f814d6df5a7d843a416fa2b4`).
Identical synthetic protocol frames enter both clients through their public
WebSocket transport. It compares cold and fresh-instance warm opens, projected
row results, cursor/digest negotiation, needHashes keys/hashes, pruning recovery,
paged snapshots, changes and stream-addressed batches. These are client protocol
fixtures, not a live runtime/app acceptance result. No full 1000-user load,
production traffic, current app module swap or capacity claim was made.

The new read-only PG compatibility receipt is
`/mnt/data/work/gonvex-cli-schema/artifacts-pgcompat/pairing.json`, SDK
`0.5.2-permission-pgcompat.202611051025`, tar SHA
`11672e357b6bec7ad86f846cd2bf8152321f97c3184d0746b4d792835c1ad7b7`.
The old e762 full SDK remains quarantined. No SDK package was installed or
substituted. This receipt does not unblock current SQLite schema/module/runtime
qualification or complete permission provider acceptance. The reviewed starter bootstrap RFC is approved for default-off implementation on its owner branch, including bilateral capability negotiation, bounded inactive staging and fresh authority/catchup promotion. It still has no implemented, negotiated client handoff here. Those
mandatory gate blockers remain unchanged.
