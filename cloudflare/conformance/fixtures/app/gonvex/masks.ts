import { reducer, replicaCollection, schema, visibility } from "@gonvex/module-sdk";

// Visibility masks v2 (K09): a principal set through two permission tables,
// a grant set, a creator context, and children that inherit their task's
// visibility with an onMissing fallback for orphans.

/** A task: visible through the Member's teams' workspaces, an assignment, or as its creator. */
export const maskTasksVisibility = visibility({
  version: 2,
  table: "mask_tasks",
  key: "id",
  permissionTables: ["mask_teams", "mask_workspace_teams"],
  sets: {
    workspaces: {
      kind: "principal",
      table: "mask_workspace_teams",
      select: "workspace_id",
      joins: [{ table: "mask_teams", leftColumn: "team_id", rightColumn: "team_id" }],
      where: [{ table: "mask_teams", column: "member_id", context: "member.id" }],
    },
    assigned: {
      kind: "grant",
      table: "mask_task_users",
      select: "task_id",
      joins: [],
      where: [{ table: "mask_task_users", column: "member_id", context: "member.id" }],
    },
  },
  where: {
    operator: "or",
    children: [
      { operator: "inSet", column: "workspace_id", set: "workspaces" },
      { operator: "inSet", column: "id", set: "assigned" },
      { operator: "eqContext", column: "creator_id", context: "member.id" },
    ],
  },
});

/** A note follows its task; an orphan note stays visible to its author only. */
export const maskNotesVisibility = visibility({
  version: 2,
  table: "mask_notes",
  key: "id",
  permissionTables: ["mask_teams", "mask_workspace_teams"],
  sets: {},
  where: {
    operator: "inherits",
    column: "task_id",
    table: "mask_tasks",
    key: "id",
    onMissing: { operator: "eqContext", column: "author_id", context: "member.id" },
  },
});

export const maskTasks = replicaCollection({
  args: schema.object({}),
  result: schema.array(schema.object({ id: schema.string(), workspace_id: schema.string(), creator_id: schema.string() })),
  replica: { table: "mask_tasks", key: "id", columns: ["id", "workspace_id", "creator_id"], orderBy: "id", orderDirection: "asc", mode: "eager", maxRows: 1000 },
});

export const maskNotes = replicaCollection({
  args: schema.object({}),
  result: schema.array(schema.object({ id: schema.string(), task_id: schema.string(), author_id: schema.string() })),
  replica: { table: "mask_notes", key: "id", columns: ["id", "task_id", "author_id"], orderBy: "id", orderDirection: "asc", mode: "eager", maxRows: 1000 },
});

/** Creates or moves a task; the caller is the creator of a new task. */
export const maskPutTask = reducer({
  args: schema.object({ id: schema.string(), workspaceId: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db, member }, args) => {
    if (!member) throw new Error("tenant member required");
    const [current] = await db.query<{ id: string }>("SELECT id FROM mask_tasks WHERE id = ?", [args.id]);
    if (current) await db.update("mask_tasks", args.id, { workspace_id: args.workspaceId });
    else await db.insert("mask_tasks", { id: args.id, workspace_id: args.workspaceId, creator_id: member.id });
    return null;
  },
});

export const maskAddNote = reducer({
  args: schema.object({ id: schema.string(), taskId: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db, member }, args) => {
    if (!member) throw new Error("tenant member required");
    await db.insert("mask_notes", { id: args.id, task_id: args.taskId, author_id: member.id });
    return null;
  },
});

/** The caller joins a team. */
export const maskJoinTeam = reducer({
  args: schema.object({ id: schema.string(), teamId: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db, member }, args) => {
    if (!member) throw new Error("tenant member required");
    await db.insert("mask_teams", { id: args.id, member_id: member.id, team_id: args.teamId });
    return null;
  },
});

export const maskLinkWorkspace = reducer({
  args: schema.object({ id: schema.string(), teamId: schema.string(), workspaceId: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => {
    await db.insert("mask_workspace_teams", { id: args.id, team_id: args.teamId, workspace_id: args.workspaceId });
    return null;
  },
});

export const maskAssign = reducer({
  args: schema.object({ id: schema.string(), taskId: schema.string(), memberId: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => {
    await db.insert("mask_task_users", { id: args.id, task_id: args.taskId, member_id: args.memberId });
    return null;
  },
});

const DELETABLE = ["mask_tasks", "mask_notes", "mask_teams", "mask_workspace_teams", "mask_task_users"];

export const maskRemove = reducer({
  args: schema.object({ table: schema.string(), id: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => {
    if (!DELETABLE.includes(args.table)) throw new Error("not a masks table");
    await db.delete(args.table, args.id);
    return null;
  },
});
