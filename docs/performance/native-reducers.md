# Native reducers: inventory (workstream 10b, phase 1)

Status: inventory only. Gabriel changed the scope on 2026-11-03: the broad native-reducer port is
not being built, because the volume that matters stays in JS. The follow-up work is the cheaper
JS path (workstream 10e, see `module-host-bridge.md`).

## Sources

- Function counts: `gonvex/_generated/manifest.json` in the Whagons lab checkout
  (`/home/gabriel/work/whagons-sqlite-lab`, generated 2026-11-03): **992 reducers, 138 actions,
  35 one-off (`oneShot`) queries**, 807 replica collections and 10 live queries. The two kinds that
  already run natively are replica collections and live queries.
- Classification of all reducers: the coordinator's first pass over staging (~950 reducers).
  About **29 are pure data writes** (settings CRUD: `sla*`, `reminderRules`, `jobPositions`,
  `notificationSoundRules`) and about **63 are a permission check plus writes**. The rest need JS.
- Volume: `bench/whagons/traffic-aggregate.json` (`path_counts`): production client traces,
  13 accounts, 7 tenants. It is a sample and does not give complete traffic rates.
  It contains 2,421 reducer calls.

## Classification rules

- **native-eligible**: argument validation, an identity or permission check that a declared rule
  can express (active member, owner equals `member.id`, row visible under the table's declared
  `visibility`), then insert, update or delete of rows. Values come from arguments, context
  (`now`, `member.id`) or constants. The writes are fixed in number, or a single predicate
  write (`updateWhere`/`deleteWhere`).
- **borderline**: eligible after one of these DSL features: an upsert keyed by a unique tuple,
  a conditional write (`if row.field is null`), a JSON field merge, or a visibility check that
  uses the declared visibility instead of a JS re-implementation (`assertTaskVisible`).
- **needs-JS**: anything that calls `updateTask` / `createTaskIntent` (policy, reference
  validation, SLA, workflows, notifications, audit logs), loops over read rows with
  per-row branching, date or streak arithmetic, or external effects.

## Volume (production trace sample)

| Reducer | Calls | Share of all reducers | Share of non-presence | Class | What would make it native |
|---|---:|---:|---:|---|---|
| presence.beat | 964 | 39.8% | | handled by presence redesign (`perf/presence`) | |
| control.support.heartbeat | 898 | 37.1% | | handled by presence redesign | |
| presence.transition | 287 | 11.9% | | handled by presence redesign | |
| taskResources.recordTaskViewByTaskId | 72 | 3.0% | 26.5% | borderline | declared task visibility check, conditional `firstViewedAt` update; also calls `notifyReportedTaskSeen` (needs-JS as written) |
| gamification.recordDailyLogin | 54 | 2.2% | 19.9% | needs-JS | date and streak arithmetic, plugin settings logic |
| workspacePreferences.save | 29 | 1.2% | 10.7% | borderline | upsert on (userId, workspaceId) |
| memberAuthStates.recordMyAccountEmail | 15 | 0.6% | 5.5% | borderline | email normalization plus conditional upsert |
| pushNotifications.registerToken | 14 | 0.6% | 5.1% | borderline | `deleteWhere` on other owners plus upsert |
| members.markMyOnboardingTourSeen | 12 | 0.5% | 4.4% | borderline | JSON settings merge |
| calls.acknowledgeRoomSignals / acknowledgeSignals | 15 | 0.6% | 5.5% | needs-JS | per-row branching (could become two predicate writes) |
| notifications.markAllRead / clearAllNotifications | 9 | 0.4% | 3.3% | native-eligible | one `updateWhere` |
| tasks.* (start, create, requestTransition, bulkArchive, acknowledge…) | 24 | 1.0% | 8.8% | needs-JS | through `updateTask` / `createTaskIntent` |
| taskResources.addTag/removeTag/createNote | 13 | 0.5% | 4.8% | needs-JS | task policy and notifications |
| other (tags.create, templates.create, members.*, chat, calls.sendSignal, userLiveLocations.upsert) | 15 | 0.6% | 5.5% | mostly needs-JS | |
| **non-presence total** | **272** | **11.2%** | **100%** | | |

The load bench (`bench/whagons/run.ts`) adds edits that the trace sample barely shows
(description, due date, rename, priority, reschedule, start, transition, tags, spot: 40% of
foreground actions). All of them go through `updateTask` (`tasks/updatePolicy.ts`), which does
visibility, permission, reference, transition, guest-report, fleet and SLA checks, then writes a
task log, notifications and workflow events: dozens of awaited `ctx.db` calls. They are needs-JS.
`tasks.setPriority`, `rename`, `reschedule` and `setSpot` are not plain row writes.

**Result.** Of the non-presence reducer volume in the sample, **3.3% is native-eligible now** and
**about 52% more is borderline** (recordTaskView, workspacePreferences.save, recordMyAccountEmail,
registerToken, markMyOnboardingTourSeen) and would need upsert, conditional write, JSON merge and
declared-visibility checks in the DSL. Because the presence redesign already removes 89% of reducer
calls from V8, a native DSL would take at most about 6% of all reducer calls out of V8, and
none of the expensive task edits. The cheaper JS path helps every reducer, so it was done instead.

## Proposed declaration format (not implemented)

Recorded here in case it is picked up later. A reducer declares a serializable plan. The SDK
interprets the same plan in JS (the fallback and the local/offline executor), and the runtime
interprets it in Rust. The Rust interpreter issues the same `HostCallFrame`s
(`DbQuery`/`DbInsert`/`DbUpdate`/`DbDelete`) against the same `DatabaseHostCalls`, so change-feed
rows, SQL admission, receipts and lazy writer acquisition stay the same as on the JS path:

```ts
export const markAllRead = reducer.native({
  name: "notifications.markAllRead",
  args: schema.object({}),
  check: [activeMember()],
  writes: [updateWhere("notifications",
    { and: [{ column: "userId", op: "eq", value: ctx("member.id") }, { column: "deletedAt", op: "isNull" }] },
    { readAt: ctx("now"), isRead: true, updatedAt: ctx("now") })],
  result: { updated: affected(0) },
});
```

The manifest would get `native: { version: 1, plan }` on the function. `GONVEX_NATIVE_REDUCERS=0`
would force V8. Equivalence would be tested by running each plan through both interpreters
against the same database and comparing rows, change-feed rows, results and errors.
