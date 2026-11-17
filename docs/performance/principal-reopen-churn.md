# Gate6: principal-change reopen churn

Base: `38e600f6824a255e706d0353ac51032d628b9afa` (`perf/integrate-5k`).
Task branch: `perf/reopen-churn`. No integration merges, endpoints or full-app
load are part of this module experiment.

## Trigger

The real Whagons `spots.list` visibility declaration (app pin `7d97617cf`,
`gonvex/spots.ts`) includes a grant set with this join chain:

```
tasks.workspaceId = workspaceTeams.workspaceId
workspaceTeams.teamId = memberTeams.teamId
memberTeams.memberId = members.id
WHERE memberTeams.memberId = context.member.id
  AND members.see_all_spots = true
```

`replica_mask::mask_step` calls `principal_change_concerns` and returns
`Resync::Principal` when it returns true. On the base, its per-set dependency
helper always adds `members.id/account_id/status/role/permissions`, including
columns that this join does not read. The members occurrence has no directly
written context constraint, so it is treated as unanchored. Consequently a
**different member's permissions/role/status update** can force every subscriber
to reopen. Status really is an input to the related tenant-setting grant, but
only for the member named through the join. This establishes a trigger for the
Step 16 `needHashesPrincipal` pattern; it does not attribute every recorded
reopen to one reducer.

## Change

Keep the current context member's identity checks intact. Separate those checks
from set-specific columns. Propagate context equalities through the declared
inner equijoins, respecting aliases and `leftAlias`, so the members occurrence
can be excluded when neither row image can join to the current member.
An unrelated column is not an equality anchor. Each set is analyzed separately;
an unanchored OR alternative still causes resync.

Cache only the plan's dependency columns and equality metadata in `PlanCaches`.
No member values, query results, revisions or fingerprints enter this cache.
Public dependency tracking retains identity columns, and principal-cache
invalidation, visibility fingerprints, grants, derived sets and wire messages
are unchanged. Unknown row images or missing anchor values stay conservative.

## Verification and measurement

The focused fixture reduces the real all-spots member and tenant-setting grant
joins. Tests cover unrelated writes, the current member's identity inputs,
`see_all_spots` grants/revokes, membership inserts/deletes/reassignments, unknown
images, aliases, unanchored OR alternatives, inherited principals, ACL ancestry
moves/deletes and cache reuse across members. A SQLite oracle executes the grant
join through member-flag and team-membership transitions.

The ignored `principal_reopen_measurement` test performs 100,000 checks of a
second member's permissions update. It reports resync decisions, wall time and
`CLOCK_THREAD_CPUTIME_ID` CPU nanoseconds per check. This measures routing work
and avoided principal reopen requests; it does not include snapshot CPU or prove
the full-app capacity gate. Allocations and resident memory are not measured.

Builds and artifacts use `/mnt/data/work/gonvex-reopen-target` and
`/mnt/data/work/gonvex-reopen-evidence`. The current resource wrapper accepts only
`/home/gabriel` for its receipt/cwd argument; the coordinator verified and approved
using the real checkout for `--build-dir`, with the actual Cargo target on
`/mnt/data`, headroom checks there, and the existing compile wrapper unchanged.

### Results (2026-11-05 UTC)

Exclusive window: 03:54–04:00 UTC, after open-cost's explicit release. Every
heavy phase ran through `coach-resources`; the existing `coach-compile` wrapper
and caps were retained. No other program measurement ran inside this window.

The new `irrelevant_member_writes_do_not_reopen_spots` regression fails on the
base at `permissions` and passes with the fix. Three samples per binary, debug
`test` profile, identical 100,000-check input:

| metric | base | fixed | change |
|---|---:|---:|---:|
| principal-resync decisions / 100,000 checks (each sample) | 100,000 | 0 | all irrelevant requests avoided |
| median thread CPU ns/check | 11,438 | 2,833 | 4.04× faster; −75.2% |
| median wall ns/check | 11,594 | 3,004 | 3.86× faster |

CPU samples: base `10,325 / 11,438 / 13,241`; fixed
`3,349 / 2,833 / 2,824` ns/check. These are debug routing measurements, not
optimized runtime sign-in or snapshot timings. New cache storage is per plan;
its resident bytes and allocations were not measured.

Commands (inside the admitted unit, from `rust/`, with the target above):

```sh
cargo test -p gonvex-runtime --lib --no-run --message-format=json
# Saved the emitted test executable separately for base and fixed builds.
/mnt/data/work/gonvex-reopen-evidence/baseline-tests irrelevant_member_writes_do_not_reopen_spots --nocapture
# Expected failure on base: permissions.
/mnt/data/work/gonvex-reopen-evidence/fixed-tests principal_change_regression --nocapture
# 7 passed, 0 failed, 1 ignored (timing).
/mnt/data/work/gonvex-reopen-evidence/fixed-tests
# Complete runtime library suite: 396 passed, 0 failed, 8 ignored.
# Includes the existing SQL/interpreted/compiled visibility corpus oracle.
/mnt/data/work/gonvex-reopen-evidence/baseline-tests visibility::principal_change_regression::principal_reopen_measurement --ignored --exact --nocapture
/mnt/data/work/gonvex-reopen-evidence/fixed-tests visibility::principal_change_regression::principal_reopen_measurement --ignored --exact --nocapture
# Timing commands run three times per binary.
cargo clippy -p gonvex-runtime --lib -- -W clippy::redundant_clone
# Exit 0; existing warnings outside the changed implementation remain.
```

`rustfmt --edition 2021 --check` on both changed Rust files and
`git diff --check` pass. Raw build JSON, test logs, separate test binaries and
`measurements.json` remain under `/mnt/data/work/gonvex-reopen-evidence`.
There are no new production import edges or runtime import cycles: the new
production helpers are in the existing visibility module; the separate module
is test-only.

## Limits and integration

Unprovable joins and global permission inputs can still resync multiple users.
This is intentional conservative routing. Actual changes still take the existing
resync/refresh paths. The full-app visibility oracle and mixed burst/steady gate
belong to integration. The implementation touches `visibility.rs` principal and
dependency sections plus a new test module, avoiding snapshot construction and
ACL-index shape methods owned by the open-cost workstream.
