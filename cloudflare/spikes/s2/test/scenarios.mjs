// The S2 workload: real Whagons Reducers, called in order on one tenant.
// Each step builds on the rows earlier steps committed, the way a client
// session would. Fixture rows are limited to what the Rust runtime itself
// provisions for a tenant (the members identity table); everything else is
// created through the app's own Reducers.

export const T0 = 1792324800123; // 2026-10-18T12:00:00.123Z, the vectors' clock
export const tenant = { id: "tenant-s2", projectId: "whagons-5", name: "S2 spike tenant" };
export const members = {
  owner: { id: "mem-owner", accountId: "acct-owner", email: "owner@s2.example", name: "Olive Owner", role: "owner" },
  worker: { id: "mem-worker", accountId: "acct-worker", email: "worker@s2.example", name: "Wade Worker", role: "member" },
};

/** Rows the runtime provisions (TENANT_IDENTITY_SQL `members`). */
export const fixture = [
  { table: "members", rows: Object.values(members).map((m) => ({ id: m.id, account_id: m.accountId, status: "active", display_name: m.name, role: m.role, permissions: "{}", membership_revision: 1 })) },
];

export function identityOf(key) {
  const m = members[key];
  return {
    account: { id: m.accountId, email: m.email, name: m.name },
    tenant,
    member: { id: m.id, accountId: m.accountId, status: "active", role: m.role, displayName: m.name, permissions: {} },
  };
}

const entropy = (index) => (index % 256).toString(16).padStart(2, "0").repeat(32);

/**
 * steps: { name, path, args, as?, expect?: "ok"|"error", why }
 * `expect: "error"` steps prove rollback: their writes must not survive.
 */
export const steps = [
  { why: "team (insert, record view)", path: "teams.create", args: { entity: { _id: "team-ops", name: "Operations", allowMultitasking: true } } },
  { why: "workspace", path: "workspaces.create", args: { entity: { _id: "ws-1", name: "Front desk" }, teams: ["team-ops"] } },
  { why: "category", path: "categories.create", args: { entity: { _id: "cat-1", name: "Maintenance", teamId: "team-ops", workspaceId: "ws-1", allowTemplatelessTasks: true } } },
  { why: "status", path: "statuses.create", args: { entity: { _id: "st-open", name: "Open", action: "NONE", initial: true, order: 1 } } },
  { why: "status", path: "statuses.create", args: { entity: { _id: "st-done", name: "Done", action: "FINISHED", final: true, order: 2 } } },
  { why: "priority", path: "priorities.create", args: { entity: { _id: "pri-high", name: "High", order: 1 } } },
  { why: "priority (to be removed)", path: "priorities.create", args: { entity: { _id: "pri-low", name: "Low", order: 2 } } },
  { why: "spot", path: "spots.create", args: { entity: { _id: "spot-101", name: "Room 101" } } },
  { why: "tag", path: "tags.create", args: { entity: { _id: "tag-urgent", name: "Urgent" }, categoryId: "cat-1" } },
  { why: "approval", path: "approvals.create", args: { entity: { _id: "appr-1", name: "Manager sign-off", approvalType: "SEQUENTIAL", requireAll: true, triggerType: "ON_CREATE" }, approvers: [{ _id: "apv-1", approverType: "user", approverId: "mem-owner", required: true, orderIndex: 0, scope: "global" }] } },
  { why: "approval approver", path: "approvals.createApprover", args: { entity: { _id: "apv-2", approvalId: "appr-1", approverType: "user", approverId: "mem-worker", required: false, orderIndex: 1, scope: "global" } } },
  { why: "task create", path: "tasks.create", args: { entityId: "task-1", name: "Fix the sink", workspaceId: "ws-1", categoryId: "cat-1", statusId: "st-open", priorityId: "pri-low", spotId: "spot-101", userIds: ["mem-owner"] } },
  { why: "task create with approval", path: "tasks.create", args: { entityId: "task-2", name: "Replace carpet", workspaceId: "ws-1", categoryId: "cat-1", statusId: "st-open", priorityId: "pri-high", approvalId: "appr-1" } },
  { why: "task update", path: "tasks.update", args: { taskId: "task-1", updates: { name: "Fix the kitchen sink", description: "Leaking since Monday" } } },
  { why: "task assign", path: "tasks.assign", args: { taskId: "task-1", userIds: ["mem-owner", "mem-worker"] } },
  { why: "task flag", path: "tasks.setFlag", args: { taskId: "task-1", color: "red" } },
  { why: "task note (comment)", path: "taskResources.createNote", args: { entity: { _id: "note-1", _creationTime: T0, taskId: "task-1", userId: "mem-owner", source: "web", note: "Plumber booked for 3pm" } } },
  { why: "approval decision", path: "tasks.decideApproval", args: { taskId: "task-2", decision: "approved", comment: "OK" } },
  { why: "cleaning status", path: "cleaningStatuses.create", args: { entity: { _id: "cs-dirty", name: "Dirty", code: "DIRTY", order: 1, isInitial: true } } },
  { why: "rollback: deferred foreign key fails at commit after an insert and a wakeAt (targetStatusId must also be a task status)", path: "cleaningAutomation.create", expect: "error", args: { entity: { _id: "rule-1", name: "Nightly reset", schedule: "FREQ=DAILY;TIME=03:00", timeZone: "UTC" }, targetStatusId: "cs-dirty" } },
  { why: "status shared with a cleaning status", path: "statuses.create", args: { entity: { _id: "cs-clean", name: "Clean", action: "NONE", order: 3 } } },
  { why: "cleaning status", path: "cleaningStatuses.create", args: { entity: { _id: "cs-clean", name: "Clean", code: "CLEAN", order: 2, isCleanState: true } } },
  { why: "wakeAt user (scheduler.wakeAt -> coalescing outbox job)", path: "cleaningAutomation.create", args: { entity: { _id: "rule-2", name: "Nightly reset", schedule: "FREQ=DAILY;TIME=03:00", timeZone: "UTC" }, targetStatusId: "cs-clean" } },
  { why: "wakeAt user (cleaning report schedule -> coalescing cleaning.tick job)", path: "cleaningReports.create", args: { entity: { _id: "crs-1", name: "Daily report", frequency: "daily", sendAt: "07:00", timeZone: "UTC", dateRangeMode: "previousDay", format: "pdf", recipients: ["owner@s2.example"], isActive: true }, reportTypeId: "housekeeping" } },
  { why: "applyDataWrites (savepoint) multi-table fanout", path: "priorities.remove", args: { id: "pri-low", replacementPriorityId: "pri-high" } },
  { why: "outbox enqueuer", path: "centers.askJev", args: {} },
  { why: "enable a powerup (plugins upsert)", path: "settings.togglePlugin", args: { slug: "boards", isEnabled: true } },
  { why: "board", path: "boards.create", args: { entity: { _id: "board-1", _creationTime: T0, createdBy: "mem-owner", name: "Announcements" } } },
  { why: "board message", path: "boards.createMessage", args: { entity: { _id: "msg-1", _creationTime: T0, boardId: "board-1", createdBy: "mem-owner", title: "Welcome", content: "Hello" } } },
  { why: "board comment", path: "boards.createComment", args: { commentId: "cmt-1", boardId: "board-1", createdBy: "mem-owner", createdAt: T0, boardMessageId: "msg-1", content: "Thanks!" } },
  { why: "rollback: handler throws before writing", path: "tasks.create", expect: "error", args: { entityId: "task-bad", name: "Bad", workspaceId: "ws-missing" } },
  { why: "rollback: JS throw after two inserts", path: "approvals.create", expect: "error", args: { entity: { _id: "appr-x", name: "Duplicate approvers" }, approvers: [{ _id: "apv-x", approverType: "user", approverId: "mem-owner", required: true, orderIndex: 0, scope: "global" }, { _id: "apv-x", approverType: "user", approverId: "mem-worker", required: true, orderIndex: 1, scope: "global" }] } },
  { why: "rollback: UNIQUE failure from a host call after an insert", path: "approvals.create", expect: "error", args: { entity: { _id: "appr-y", name: "Reused approver id" }, approvers: [{ _id: "apv-1", approverType: "user", approverId: "mem-owner", required: true, orderIndex: 0, scope: "global" }] } },
  { why: "rollback: deferred foreign key fails at commit after five writes", path: "tasks.assign", expect: "error", args: { taskId: "task-2", userIds: ["mem-ghost"] } },
  { why: "task archive", path: "tasks.archive", args: { taskId: "task-1" } },
].map((step, index) => ({
  as: "owner",
  expect: "ok",
  ...step,
  name: `${String(index + 1).padStart(2, "0")} ${step.path}`,
  now: T0 + index * 60_000,
  intentEntropy: entropy(index + 1),
  commandId: `cmd-s2-${String(index + 1).padStart(2, "0")}`,
}));

export function callOf(step, artifactHash = "s2-spike") {
  const identity = identityOf(step.as);
  return {
    path: step.path, args: step.args, identity, now: step.now, intentEntropy: step.intentEntropy,
    invocation: { channel: "ui", rootChannel: "ui", commandId: step.commandId, rootCommandId: step.commandId, artifactHash, actorAccountId: identity.account.id, actorMemberId: identity.member.id, onBehalfOfMemberId: null, delegation: null },
  };
}
