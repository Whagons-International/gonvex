# 0032: Rulings from the first wave-2 review (K09, K07, K08, K11, K13)

Date: 2026-11-11. Status: decided (lead).

## Note numbering on merge (0024 §5)

Four packages proposed a note numbered 0030. The merged numbering is:

| Number | Note |
|---|---|
| 0030 | K07 Reducer path |
| 0031 | K09 visibility |
| 0032 | this note |
| 0033 | K08 live queries |
| 0034 | K11 scheduler |
| 0035 | K13 files |

## K09 (note 0031): merged

- Items 1–7 accepted. **K10 must release class bindings on unsubscribe and rebind** (item 2).
  `shared_parent_resolution` (item 6) goes to R03/R04.
- **Intended behavior IB5, the orphan-row finding.** Rust never delivers a row that becomes visible when its
  parent is inserted later (an orphan note under a newly visible task), although a fresh snapshot shows it.
  The kernel **delivers it**: the client converges on exactly what a fresh Rust snapshot shows. Rust-vs-kernel
  conformance excludes that path from frame comparison. A kernel-only scenario asserts the delivery (K10).

## K07 (note 0030): merged

All 11 items accepted.
- #1: a tombstone is `result IS NULL` on a committed row, which supersedes 0009's `expired = 1`.
- #2: the receipt is read before the module runs. This differs from Rust only for a reused key with
  different arguments.
- #3: gate text `module database capability failed: tenant schema migration in progress; retry`, class
  transient.
- #9: a receipt over 2 MB fails the call cleanly. This is platform-forced, pending the platform check.
- **Follow-up (from K11's finding):** outbox and scheduled rows must carry the full `InvocationProvenance`
  (`artifactHash`, `durableHops`; `host_calls.rs:354`). Otherwise Reducer-scheduled jobs run as the system,
  and self-rescheduling chains never hit the 32-hop limit. Small PR with a hop-limit test. The K07 thread
  has ended, so the coordinator assigns it.

## K08 (note 0033): changes requested

- The engine meets the parity bar: 117 frames and 67 `windowRevision` hashes equal Rust's on `tasks.grid`.
- **Before merge:** replace `visibility-standin.ts` with K09's `createQueryVisibility(registry)` (one
  visibility implementation in the tree), fix the fixture's duplicate `savepoints` export, and renumber the
  note to 0033.
- Items 1–6 accepted:
  - patches stay deferred (0012 R19);
  - every commit is answered in order;
  - an additive `bySocket` goes into K05's `DeliveryBatch`;
  - gateways resend subscribes with `windowRevision` when the tenant-object instance changes (K10 implements
    the gateway side);
  - relevance narrowing via K09;
  - `json_each` for `IN` lists.

## K11 (note 0034): approved

- Dead outbox rows go to `_gonvex_action_dead`; `_gonvex_action_outbox` stays byte-identical to Rust.
- Accepted:
  - `gonvex_scheduled_jobs` gains `last_error` and the lease and dead indexes;
  - a lost-lease cap;
  - one outbox Action in flight per tenant;
  - attempt-guarded completion;
  - alarm-driven wakes in place of 30 s polling.
- Dead cron runs are kept **30 days**, then pruned on the alarm.
- K11's new objects are added to A01's `addedObjects()` exclusion list.
- R06's baseline: 6,153 alarm events per tenant per day (4 per minute), so the object never hibernates.

## K13 (note 0035): approved

1. **The caller of a file is the account id or the tenant Member id.** Whagons writes `ownerId: memberId`.
2. **Label owners (option b).** An owner that is neither an account nor a Member of the tenant (for example
   `calling-recording:<id>`) is a label. Such a file is readable by any caller in the tenant, as in Rust.
   IB1 holds in full for every file a principal owns. A system principal is still refused on another owner's
   private file.
3. Private-file URLs are signed with the owner bound in, and the URL shape is unchanged. **K19's
   `/dev/storage/files` returns `url: null` for private rows.**
4. No files conformance scenario yet, because the harness has no Actions on the kernel and no S3 endpoint
   for Rust. **V01** adds it.

## Housekeeping

- K02's `resume after interruption` workerd test can exceed its 5 s timeout under parallel load (5.7 s).
  K02 raises the limit in its platform follow-up PR.
- The platform follow-ups listed in each PR stay open until the credentials vault is back.
