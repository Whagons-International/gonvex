// Exact live plan + related visibility from app f8336ddf/source 3ad4fa47.
// The tasks policy is shared with the V01 binding regression. All rows and
// identities below are synthetic. The unmodified 4c0 compiler fails in real
// workerd SQLite with "Expression tree is too large (maximum depth 100)".
import { describe, expect, it } from "vitest";
import fixture from "./fixtures/grid-policy.json";
import shared from "../../visibility/test/fixtures/task-relations-policy.json";
import { compilePlan, type Related } from "../compile";
import { fitParameters } from "../parameters";
import type { LiveQueryPlan } from "../plan";
import { kernelVisibility } from "../visibility";
import { VisibilityRegistry } from "../../visibility";
import { booleanSql } from "../../storage/boolean-sql";
import { objectFor } from "../../storage/test/helpers";
import { type JsonKind, type StorageSession } from "../../storage";
import { ParentAclIndex } from "../../visibility/acl-index";
import { rowMatches } from "../../visibility/mask";
import { aclColumns, parsePlan } from "../../visibility/plan";
import { emptyResolution, InheritedParent, type ResolvedVisibility } from "../../visibility/resolved";
import type { JsonObject } from "../../visibility/json";

const live: LiveQueryPlan = fixture.plan;
const plans = new Map(Object.entries({ ...fixture.visibility, tasks: shared.visibility.tasks }).map(([table, raw]) => [table, parsePlan(raw)]));
const kinds = new Map<string, JsonKind>([
  ["id", "integer"], ["updatedAt", "integer"], ["_creationTime", "real"],
  ["requiresSignature", "boolean"], ["audienceRules", "json"],
]);
const tasks: JsonObject[] = Array.from({ length: 144 }, (_, n) => ({
  ...Object.fromEntries([...new Set([...live.columns!, ...aclColumns(plans.get("tasks")!)])].map((column) => [column, null])),
  _id: `task-${String(n).padStart(3, "0")}`, id: n % 19 === 0 ? null : Math.floor(n / 2),
  workspaceId: n % 8 < 2 ? "workspace" : "other", createdBy: n % 8 === 1 ? "bob" : "alice",
  spotId: "ungranted", name: `Task ${n}`, description: n % 3 ? "ordinary" : "needle",
  updatedAt: 2 ** 40 + n, _creationTime: n + 0.25,
  requiresSignature: n % 2 === 0, audienceRules: { users: ["alice"], enabled: true },
}));
const contexts: JsonObject[] = tasks.filter((_, n) => n % 8 >= 2).map((task, n) => ({
  _id: `context-${n}`, taskId: task._id!, workspaceId: "workspace", revokedAt: n % 17 === 0 ? "revoked" : null,
  kind: Number(String(task._id).slice(-3)) % 8 >= 6 ? "finding" : "task",
  recipientUserIds: Number(String(task._id).slice(-3)) % 8 === 3 ? ["bob"] : Number(String(task._id).slice(-3)) % 8 === 2 ? ["alice"] : Number(String(task._id).slice(-3)) % 8 === 4 ? [] : null,
  targetUserId: Number(String(task._id).slice(-3)) % 8 === 4 ? "alice" : null,
  targetTeamId: Number(String(task._id).slice(-3)) % 8 === 5 ? (Number(String(task._id).slice(-3)) % 16 === 13 ? "revoked-team" : "team-alice") : null,
}));
// Duplicate relation keys must not duplicate outer rows. NULL/missing parents
// exercise inherited denial/onMissing without accidentally granting a task.
contexts.push({ ...contexts[1]!, _id: "duplicate-context" },
  { _id: "null-parent", taskId: null, workspaceId: "workspace", revokedAt: null, kind: null, recipientUserIds: [], targetUserId: null, targetTeamId: null },
  { _id: "missing-parent", taskId: "missing-granted", workspaceId: "workspace", revokedAt: null, kind: "task", recipientUserIds: [], targetUserId: null, targetTeamId: null });
const findings: JsonObject[] = tasks.filter((_, n) => n % 8 >= 6).map((task, n) => ({ _id: `finding-${n}`, taskId: task._id!, linkedTaskId: task._id!, deletedAt: n % 2 ? "deleted" : null }));
const teams: JsonObject[] = [
  { _id: "alice-team", teamId: "team-alice", memberId: "alice", deletedAt: null },
  { _id: "bob-team", teamId: "team-bob", memberId: "bob", deletedAt: null },
  { _id: "revoked-team", teamId: "revoked-team", memberId: "alice", deletedAt: "deleted" },
];
const tables = new Map<string, JsonObject[]>([["tasks", tasks], ["taskWorkspaceContexts", contexts], ["taskFindings", findings], ["memberTeams", teams]]);

function resolutions(member: string, viewAll = false): Map<string, ResolvedVisibility> {
  const resolved = new Map([...plans].map(([table, plan]) => [table, {
    ...emptyResolution({ "account.id": `account-${member}`, "member.id": member, "tenant.id": "tenant" }, "admin", { "tasks.viewAll": viewAll }),
    // Distinct sets force the same packed-parameter lowering as the shipped
    // query, rather than making the depth bug disappear with equal empties.
    sets: new Map([...plan.sets.keys()].map((name, i) => [name, new Set([`ungranted-${i}`])])),
  }]));
  for (const table of ["taskWorkspaceContexts", "taskFindings"]) {
    const own = resolved.get(table)!;
    own.inherited.set("tasks", new InheritedParent(plans.get("tasks")!, resolved.get("tasks")!, ParentAclIndex.fromSnapshot(1, tasks.map((task) => [String(task._id), task]))));
    own.sets.set("personalContextOwnerTasks", new Set(["missing-granted"]));
  }
  return resolved;
}

async function seeded() {
  const object = objectFor();
  const created = await object.invoke("reducer", async (db) => {
    db.run("CREATE TABLE members (account_id TEXT, status TEXT, role TEXT, permissions JSON)");
    for (const member of ["alice", "bob", "mallory"]) db.run("INSERT INTO members VALUES ($1, 'active', 'admin', '{}')", [`account-${member}`]);
    const columns = new Map([...plans].map(([table, plan]) => [table, new Set(aclColumns(plan))]));
    const walk = (expression: NonNullable<LiveQueryPlan["where"]>, table: string): void => {
      if (expression.column) columns.get(table)!.add(expression.column);
      if (expression.relation) {
        columns.get(expression.relation.table)!.add(expression.relation.column);
        if (expression.relation.where) walk(expression.relation.where, expression.relation.table);
      }
      for (const child of expression.children ?? []) walk(child, table);
    };
    walk(live.where!, "tasks");
    for (const column of live.columns!) columns.get("tasks")!.add(column);
    for (const [table, names] of columns) {
      db.run(`CREATE TABLE "${table}" (${[...names].map((name) => `"${name}" ${table === "tasks" ? kinds.get(name) ?? "TEXT" : name === "recipientUserIds" ? "JSON" : "TEXT"}`).join(", ")})`);
      for (const row of tables.get(table)!) db.run(`INSERT INTO "${table}" (${[...names].map((name) => `"${name}"`).join(", ")}) VALUES (${[...names].map((_, i) => `$${i + 1}`).join(", ")})`, [...names].map((name) => row[name] ?? null));
    }
    db.run(`CREATE TABLE taskGridRows (taskId TEXT, ${live.index!.columns.map((name) => `"${name}" ${kinds.get(name) ?? "TEXT"}`).join(", ")}, searchText TEXT, statusOrder INTEGER, statusName TEXT, priorityOrder INTEGER, priorityName TEXT)`);
    db.run(`INSERT INTO taskGridRows (taskId, ${live.index!.columns.map((name) => `"${name}"`).join(", ")}, searchText) SELECT _id, ${live.index!.columns.map((name) => `"${name}"`).join(", ")}, description FROM tasks`);
    db.run("CREATE TABLE taskRelatedSearchDocuments (taskId TEXT, searchText TEXT)");
    db.run("INSERT INTO taskRelatedSearchDocuments VALUES ('task-004', 'needle')");
    db.run("UPDATE tasks SET name = x'00ff80' WHERE _id = 'task-000'");
  });
  if (!created.ok) throw created.error;
  const read = async <T>(body: (db: StorageSession) => T): Promise<T> => {
    const result = await object.invoke("query", async (db) => body(db));
    if (!result.ok) throw result.error;
    expect(result.stats.rowsWritten).toBe(0);
    return result.value;
  };
  return { object, read };
}

function compiled(member: string, args: Record<string, unknown>, viewAll = false) {
  const resolved = resolutions(member, viewAll);
  const raw = { ...fixture.visibility, tasks: shared.visibility.tasks };
  const related: Related = new Map([...plans.keys()].map((table) => [table, { plan: raw[table as keyof typeof raw], resolved: resolved.get(table)! }]));
  const visibility = kernelVisibility(new VisibilityRegistry());
  return compilePlan({ plan: live, visibilityPlan: shared.visibility.tasks, resolved: resolved.get("tasks")!, related, args, visibility, kinds });
}

/** Row admission oracle: visibility interpreter + the declared relation rules. */
function allowed(member: string, workspace: string, viewAll = false): JsonObject[] {
  const resolved = resolutions(member, viewAll);
  const visible = (table: string, row: JsonObject) => rowMatches(plans.get(table)!, resolved.get(table)!, row);
  return tasks.filter((task) => visible("tasks", task) && (task.workspaceId === workspace || contexts.some((context) => {
    if (context.taskId !== task._id || context.workspaceId !== workspace || context.revokedAt !== null || !visible("taskWorkspaceContexts", context)) return false;
    const recipients = context.recipientUserIds;
    const addressed = Array.isArray(recipients) && recipients.includes(member) ||
      (recipients === null || Array.isArray(recipients) && recipients.length === 0) &&
      (context.targetUserId === member || context.targetUserId === null && (context.targetTeamId === null || teams.some((team) => visible("memberTeams", team) && team.memberId === member && team.deletedAt === null && team.teamId === context.targetTeamId)));
    return addressed && (context.kind !== null && context.kind !== "finding" || findings.some((finding) => finding.linkedTaskId === context.taskId && finding.deletedAt === null && visible("taskFindings", finding)));
  })));
}

// Query order is independent of the authorization SQL; NULLS FIRST/LAST and
// the key tie-breaker match the declared grid contract.
function ordered(rows: JsonObject[], direction: string): JsonObject[] {
  return [...rows].sort((a, b) => {
    const sign = direction === "asc" ? 1 : -1;
    if (a.id === null && b.id !== null) return sign;
    if (b.id === null && a.id !== null) return -sign;
    return (Number(a.id) - Number(b.id)) * sign || String(a._id).localeCompare(String(b._id));
  });
}

describe("actual grid expression depth", () => {
  it("runs the unchanged complete query and exact count for positive and denied principals, offsets, limits and sorts", async () => {
    const { read, object } = await seeded();
    const initial = compiled("alice", { workspaceId: "workspace", offset: 0, limit: 100 });
    const initialWindow = fitParameters(initial.statement, initial.parameters);
    expect(await read((db) => db.run(initialWindow.statement, initialWindow.parameters).rows.map(([text]) => (JSON.parse(String(text)) as JsonObject)._id)))
      .toEqual(ordered(allowed("alice", "workspace"), "desc").slice(0, 100).map((row) => row._id));
    for (const member of ["alice", "bob", "mallory"]) for (const direction of ["asc", "desc"]) for (const offset of [0, 7]) {
      const query = compiled(member, { workspaceId: "workspace", offset, limit: 11, sortDirection: direction });
      const fitted = fitParameters(query.statement, query.parameters);
      expect(fitted.packedAll).toBe(true); // same path as the original 115-slot query
      const count = fitParameters(query.count!.statement, query.count!.parameters);
      const expected = ordered(allowed(member, "workspace"), direction);
      const result = await read((db) => ({ rows: db.run(fitted.statement, fitted.parameters).rows.map(([text]) => JSON.parse(String(text)) as JsonObject), count: db.run(count.statement, count.parameters).rows[0]![0], statements: db.stats.statements, rowsRead: db.stats.rowsRead }));
      expect(result.rows.map((row) => row._id)).toEqual(expected.slice(offset, offset + 11).map((row) => row._id));
      expect(result.count).toBe(expected.length);
      expect(result.statements).toBe(2);
      for (const row of result.rows) {
        const original = tasks.find((task) => task._id === row._id)!;
        expect(row.updatedAt).toBe(original.updatedAt);
        expect(row.requiresSignature).toBe(original.requiresSignature);
        expect(row.audienceRules).toEqual(original.audienceRules);
        expect(row.name).toBe(row._id === "task-000" ? "00ff80" : original.name);
      }
    }
    expect(allowed("alice", "workspace").length).toBeGreaterThan(30);
    expect(allowed("bob", "workspace").length).toBe(18);
    expect(allowed("mallory", "workspace")).toEqual([]);
    await object.sql("UPDATE members SET status = 'suspended'");
    const query = compiled("alice", { workspaceId: "workspace", offset: 0, limit: 100 });
    const fitted = fitParameters(query.statement, query.parameters);
    expect(await read((db) => db.run(fitted.statement, fitted.parameters).rows)).toEqual([]);
  });

  it("keeps filters, search, empty workspaces and live permission checks inside both count and window", async () => {
    const { read, object } = await seeded();
    for (const viewAll of [false, true]) {
      await object.sql("UPDATE members SET permissions = ? WHERE account_id = 'account-alice'", JSON.stringify({ "tasks.viewAll": viewAll }));
      for (const workspace of ["workspace", "", "missing"]) {
        const args = { workspaceId: workspace, offset: 0, limit: 250, search: "needle", filters: [{ column: "updatedAt", operator: "greaterThan", value: String(2 ** 40 + 20) }] };
        const query = compiled("alice", args, viewAll), fitted = fitParameters(query.statement, query.parameters), count = fitParameters(query.count!.statement, query.count!.parameters);
        const expected = ordered(allowed("alice", workspace, viewAll).filter((row) => Number(row.updatedAt) > 2 ** 40 + 20 && (row.description === "needle" || row._id === "task-004")), "desc");
        const result = await read((db) => ({ keys: db.run(fitted.statement, fitted.parameters).rows.map(([text]) => (JSON.parse(String(text)) as JsonObject)._id), total: db.run(count.statement, count.parameters).rows[0]![0] }));
        expect(result.keys).toEqual(expected.map((row) => row._id));
        expect(result.total).toBe(expected.length);
      }
    }
    // A previously resolved permission cannot override its live DB revocation.
    await object.sql("UPDATE members SET permissions = '{}' WHERE account_id = 'account-alice'");
    const stale = compiled("alice", { workspaceId: "workspace", offset: 0, limit: 250 }, true);
    const fitted = fitParameters(stale.statement, stale.parameters);
    expect(await read((db) => db.run(fitted.statement, fitted.parameters).rows.map(([text]) => (JSON.parse(String(text)) as JsonObject)._id)))
      .toEqual(ordered(allowed("alice", "workspace", false), "desc").map((row) => row._id));
  });

  it("keeps nested IN/NOT IN NULLs, duplicate keys and CAST affinity identical to the original subqueries", async () => {
    const { read, object } = await seeded();
    await object.sql("CREATE TABLE _gonvex_relation_0 (id TEXT, link)");
    await object.sql("INSERT INTO _gonvex_relation_0 VALUES ('null',NULL), ('integer',1099511627777), ('large',9223372036854775807), ('text','1099511627777'), ('blob',x'31'), ('other','other')");
    await object.sql("CREATE TABLE links (token, edge TEXT, owner TEXT)");
    await object.sql("INSERT INTO links VALUES (1099511627777,'yes','alice'), (9223372036854775807,'yes','alice'), ('1099511627777','yes','alice'), (NULL,'yes','alice'), ('1','yes','alice'), ('other','yes','bob')");
    await object.sql("CREATE TABLE keys (id TEXT, enabled INTEGER)");
    await object.sql("INSERT INTO keys VALUES ('yes',1)");
    const publicPlan = { table: "_gonvex_relation_0", key: "id", where: { operator: "public" } };
    const linkPlan = { table: "links", key: "edge", where: { operator: "eqContext", column: "owner", context: "member.id" } };
    const resolved = emptyResolution({ "account.id": "account-alice", "member.id": "alice" }, "admin", {});
    const relation = { operator: "inRelation", column: "link", relation: { table: "links", column: "token", where: { operator: "inRelation", column: "edge", relation: { table: "keys", column: "id", where: { operator: "eq", column: "enabled", value: { literal: true } } } } } };
    for (const withNull of [true, false]) {
      if (!withNull) await object.sql("DELETE FROM links WHERE token IS NULL");
      for (const negate of [false, true]) {
        const query = compilePlan({
          plan: { table: "_gonvex_relation_0", key: "id", columns: ["id"], where: negate ? { operator: "not", children: [relation] } : relation },
          visibilityPlan: publicPlan, resolved, args: {}, kinds: new Map(), visibility: kernelVisibility(new VisibilityRegistry()),
          related: new Map([["links", { plan: linkPlan, resolved }], ["keys", { plan: { ...publicPlan, table: "keys" }, resolved }]]),
        });
        const nested = `CAST(r.link AS TEXT) IN (SELECT CAST(l.token AS TEXT) FROM links l WHERE l.owner = $1 AND CAST(l.edge AS TEXT) IN (SELECT CAST(k.id AS TEXT) FROM keys k WHERE k.enabled = $2))`;
        await read((db) => {
          const expected = db.run(`SELECT id FROM _gonvex_relation_0 r WHERE ${negate ? `NOT (${nested})` : nested} ORDER BY id`, ["alice", true]).rows.map(([id]) => id);
          expect(db.run(query.statement, query.parameters).rows.map(([text]) => (JSON.parse(String(text)) as JsonObject).id)).toEqual(expected);
        });
      }
    }
  });

  it("balanced booleans preserve all three SQL truth values and a wide nested policy", async () => {
    const { read } = await seeded();
    for (const op of ["AND", "OR"] as const) {
      for (const a of ["NULL", "0", "1"]) for (const b of ["NULL", "0", "1"]) for (const c of ["NULL", "0", "1"]) {
        const values = await read((db) => db.run(`SELECT ${booleanSql(op, [a, b, c])}, ${a} ${op} ${b} ${op} ${c}`).rows[0]!);
        expect(values[0]).toBe(values[1]);
      }
      const terms = Array.from({ length: 256 }, (_, n) => `${n} = 128`);
      expect(await read((db) => db.run(`SELECT ${booleanSql(op, terms)}`).rows[0]![0])).toBe(op === "OR" ? 1 : 0);
    }
  });
});
