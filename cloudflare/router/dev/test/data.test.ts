// operator_data.rs against a tenant object provisioned by /dev/sync, and the
// operator sockets (dashboard.rs, metrics and log streams).
import { runInDurableObject, SELF } from "cloudflare:test";
import { beforeAll, describe, expect, it } from "vitest";
import type { OperatorControlObject } from "../../../control/operator/object.ts";
import { ADMIN, call, control, createProject, manifest, sync, tenantObject, useBuilder } from "./helpers.ts";

let project: { id: string; key: string };
const TENANT = "data-tenant";
const q = (params: Record<string, string>) => new URLSearchParams({ project: project.id, tenant: TENANT, ...params }).toString();

beforeAll(async () => {
  await useBuilder();
  project = await createProject("data-project");
  expect((await sync(project.id, await manifest(project.id), project.key)).status).toBe(200);
  expect((await call("POST", "/dev/tenants", { body: { projectId: project.id, id: TENANT, name: "Data" } })).status).toBe(201);
});

describe("data browser", () => {
  it("lists application tables only, with exact counts", async () => {
    const reply = await call("GET", `/dev/data/tables?${q({})}`, { token: project.key });
    expect(reply.status).toBe(200);
    expect(reply.body.tables).toEqual([{ name: "notes", columns: ["id", "title", "done", "meta", "score", "created_at"], rowCount: 3, rowCountEstimated: false }]);
    expect((await call("GET", `/dev/data/tables?project=${project.id}&tenant=nope`, { token: project.key })).body).toEqual({ error: "tenant database was not found" });
    expect((await call("GET", "/dev/data/tables", { token: project.key })).status).toBe(401);
  });

  it("reads rows typed by declared type, sorted, filtered, searched and paged", async () => {
    const all = await call("GET", `/dev/data/tables/notes/rows?${q({})}`, { token: project.key });
    expect(all.body).toEqual({
      table: "notes",
      columns: ["id", "title", "done", "meta", "score", "created_at"],
      rows: [
        { id: "n3", title: "third", done: false, meta: null, score: 3, created_at: 3 },
        { id: "n2", title: "second note", done: true, meta: { tag: "b" }, score: null, created_at: 2 },
        { id: "n1", title: "first note", done: false, meta: { tag: "a", ref: "old-id" }, score: 1.5, created_at: 1 },
      ],
      total: 3,
      offset: 0,
      limit: 100,
    });
    const filtered = await call("GET", `/dev/data/tables/notes/rows?${q({ filters: JSON.stringify([{ column: "done", operator: "equals", value: "true" }]), columns: "id,done" })}`, { token: project.key });
    expect(filtered.body.rows).toEqual([{ id: "n2", done: true }]);
    const searched = await call("GET", `/dev/data/tables/notes/rows?${q({ search: "NOTE", sort: "id", direction: "asc", limit: "1", count: "estimate" })}`, { token: project.key });
    expect(searched.body).toMatchObject({ rows: [{ id: "n1" }], total: 2, limit: 1 });
    const oneOf = await call("GET", `/dev/data/tables/notes/rows?${q({ filters: JSON.stringify([{ column: "id", operator: "oneOf", value: '["n1","n3"]' }]), sort: "id" })}`, { token: project.key });
    expect(oneOf.body.rows.map((r: { id: string }) => r.id)).toEqual(["n1", "n3"]);
    const range = await call("GET", `/dev/data/tables/notes/rows?${q({ filters: JSON.stringify([{ column: "created_at", operator: "inRange", value: "2", valueTo: "3" }]) })}`, { token: project.key });
    expect(range.body.total).toBe(2);
    expect((await call("GET", `/dev/data/tables/notes/rows?${q({ sort: "nope" })}`, { token: project.key })).body).toEqual({ error: "invalid sort column" });
    expect((await call("GET", `/dev/data/tables/notes/rows?${q({ columns: "id,nope" })}`, { token: project.key })).body).toEqual({ error: "invalid selected column" });
    expect((await call("GET", `/dev/data/tables/notes/rows?${q({ filters: JSON.stringify([{ column: "nope", operator: "equals" }]) })}`, { token: project.key })).body).toEqual({ error: "invalid filter column" });
    expect((await call("GET", `/dev/data/tables/members/rows?${q({})}`, { token: project.key })).body).toEqual({ error: "table not found" });
    expect((await call("GET", `/dev/data/tables/_gonvex_sync_changes/rows?${q({})}`, { token: project.key })).status).toBe(404);
    expect((await call("GET", `/dev/data/tables/bad-name/rows?${q({})}`, { token: project.key })).body).toEqual({ error: "invalid table name" });
    expect((await call("GET", `/dev/data/tables/missing/rows?${q({})}`, { token: project.key })).body).toEqual({ error: "table does not exist" });
  });

  it("inserts and updates in tenant transactions; delete and reference replacement need the admin key", async () => {
    const inserted = await call("POST", `/dev/data/tables/notes/rows?${q({})}`, { token: project.key, body: { id: "n4", title: "fourth", done: "t", meta: { tag: "c" }, created_at: 4 } });
    expect([inserted.status, inserted.body]).toEqual([201, { table: "notes", row: { id: "n4", title: "fourth", done: true, meta: { tag: "c" }, score: null, created_at: 4 } }]);
    expect((await call("POST", `/dev/data/tables/notes/rows?${q({})}`, { token: project.key, body: { nope: 1 } })).body).toEqual({ error: "invalid column name" });
    expect((await call("POST", `/dev/data/tables/notes/rows?${q({})}`, { token: project.key, body: {} })).body).toEqual({ error: "no values provided" });
    const duplicate = await call("POST", `/dev/data/tables/notes/rows?${q({})}`, { token: project.key, body: { id: "n4", title: "again" } });
    expect([duplicate.status, duplicate.body]).toEqual([400, { error: "error returned from database: (code: 1555) UNIQUE constraint failed: notes.id" }]);
    const updated = await call("PATCH", `/dev/data/tables/notes/rows/n4?${q({})}`, { token: project.key, body: { title: "fourth (edited)", done: false } });
    expect(updated.body.row).toMatchObject({ id: "n4", title: "fourth (edited)", done: false });
    expect((await call("PATCH", `/dev/data/tables/notes/rows/n4?${q({})}`, { token: project.key, body: { id: "x" } })).body).toEqual({ error: "identity and tenant columns cannot be changed" });
    expect((await call("PATCH", `/dev/data/tables/notes/rows/zz?${q({})}`, { token: project.key, body: { title: "x" } })).body).toEqual({ error: "update did not find a row" });

    expect((await call("DELETE", `/dev/data/tables/notes/rows/n4?${q({})}`, { token: project.key })).body).toEqual({ error: "runtime admin key is required" });
    expect((await call("DELETE", `/dev/data/tables/notes/rows/n4?${q({})}`, { token: ADMIN })).body.row).toMatchObject({ id: "n4" });

    const dry = await call("POST", `/dev/data/references/replace?${q({})}`, { token: ADMIN, body: { replacements: { "old-id": "new-id" }, dryRun: true } });
    expect(dry.body).toEqual({ textRows: 0, jsonRows: 1, columns: [{ table: "notes", column: "meta", dataType: "json", rows: 1 }], dryRun: true });
    const replaced = await call("POST", `/dev/data/references/replace?${q({})}`, { token: ADMIN, body: { replacements: { "old-id": "new-id", same: "same" } } });
    expect(replaced.body).toMatchObject({ jsonRows: 1, dryRun: false });
    const n1 = await call("GET", `/dev/data/tables/notes/rows?${q({ filters: JSON.stringify([{ column: "id", operator: "equals", value: "n1" }]) })}`, { token: project.key });
    expect(n1.body.rows[0].meta).toEqual({ tag: "a", ref: "new-id" });
    expect((await call("POST", `/dev/data/references/replace?${q({})}`, { token: ADMIN, body: { replacements: {} } })).body).toEqual({ error: "at least one replacement is required" });
    // The scratch table never outlives the request.
    expect(await tenantObject(TENANT).debugQuery("SELECT name FROM sqlite_schema WHERE name LIKE '%reference_replacements%'")).toEqual([]);
  });
});

async function socket(path: string, headers: Record<string, string> = {}): Promise<{ ws: WebSocket; next: () => Promise<any>; response: Response }> {
  const response = await SELF.fetch(`http://kernel.test${path}`, { headers: { upgrade: "websocket", ...headers } });
  const ws = response.webSocket;
  if (!ws) throw new Error(`no websocket: ${response.status} ${await response.text()}`);
  ws.accept();
  const queue: any[] = [];
  const waiters: Array<(value: any) => void> = [];
  ws.addEventListener("message", (event) => {
    const value = JSON.parse(String(event.data));
    const waiter = waiters.shift();
    if (waiter) waiter(value);
    else queue.push(value);
  });
  const next = () =>
    queue.length
      ? Promise.resolve(queue.shift())
      : new Promise((resolve, reject) => {
          const timer = setTimeout(() => reject(new Error("no frame within 5 s")), 5000);
          waiters.push((value) => {
            clearTimeout(timer);
            resolve(value);
          });
        });
  return { ws, next, response };
}

// X11b (contract-changes/0091): the dev-only file remap. This deployment sets no
// GONVEX_DEV_TRANSFER_* variables, like staging and production, so it refuses.
describe("dev-only file remap (X11b)", () => {
  const path = "/dev/internal/files/remap-tenant";
  it("after a tenant reset, a data read recovers and an interrupted write answers 503 transient, never 500 (X10, run9)", async () => {
    const before = await call("GET", `/dev/data/tables/notes/rows?${q({})}`, { token: project.key });
    expect(before.status).toBe(200);
    // The first call on the old stub fails as calls queued behind a CPU reset do (run9 reducer-errors).
    const resetOnce = async () => runInDurableObject(control(), async (instance: OperatorControlObject) => {
      const target = instance as unknown as { tenantPort(id: string): unknown };
      const real = target.tenantPort.bind(instance);
      let broken = true;
      target.tenantPort = (id: string) => {
        const port = real(id) as Record<string, (...a: unknown[]) => Promise<unknown>>;
        if (!broken) return port;
        broken = false;
        return new Proxy({}, { get: () => async () => { throw new Error("Durable Object storage is no longer accessible."); } });
      };
    });
    await resetOnce();
    const after = await call("GET", `/dev/data/tables/notes/rows?${q({})}`, { token: project.key });
    expect([after.status, after.body]).toEqual([200, before.body]);
    await resetOnce();
    const write = await call("POST", `/dev/data/tables/notes/rows?${q({})}`, { token: project.key, body: { id: "n-reset", title: "x", done: "f", created_at: 9 } });
    expect(write.status).toBe(503);
    expect(write.body).toMatchObject({ class: "transient", retryable: true });
    expect(String(write.body.error)).toMatch(/^tenant "data-tenant" was reset while handling this request/);
    // Not replayed: the row is absent.
    const rows = await call("GET", `/dev/data/tables/notes/rows?${q({ search: "n-reset" })}`, { token: project.key });
    expect(JSON.stringify(rows.body)).not.toContain("n-reset");
    await runInDurableObject(control(), async (instance: OperatorControlObject) => {
      delete (instance as unknown as { tenantPort?: unknown }).tenantPort;
    });
  });

  it("needs the admin key, a complete body, and refuses on a non-dev deployment", async () => {
    const body = { projectId: project.id, tenantId: TENANT, fromTenantId: "tenant_source" };
    expect(await call("POST", path, { token: project.key, body })).toMatchObject({ status: 403, body: { error: "runtime admin key is required" } });
    expect(await call("POST", path, { token: null, body })).toMatchObject({ status: 403 });
    expect(await call("POST", path, { body: { projectId: project.id, tenantId: TENANT } })).toMatchObject({ status: 400, body: { error: "projectId, tenantId and fromTenantId are required" } });
    expect(await call("POST", path, { body: { ...body, dryRun: "yes" } })).toMatchObject({ status: 400, body: { error: "dryRun must be a boolean" } });
    for (const tenantId of [TENANT, "devcopy-stg1117-12"]) {
      expect(await call("POST", path, { body: { ...body, tenantId } })).toMatchObject({ status: 404, body: { error: "file remap is not available on this deployment" } });
    }
    expect((await call("GET", path)).status).toBe(405);
  });
});

describe("dev-only account transfer import", () => {
  const path = "/dev/internal/transfer/import-accounts";

  it("needs the admin key, validates fields, and refuses on a non-dev deployment", async () => {
    const body = { projectId: project.id, tenantId: "devcopy-am-demo", domain: "am-demo", accounts: [] };
    expect(await call("POST", path, { token: project.key, body })).toMatchObject({ status: 403, body: { error: "runtime admin key is required" } });
    expect(await call("POST", path, { token: null, body })).toMatchObject({ status: 403 });
    expect((await call("GET", path)).status).toBe(405);
    expect(await call("POST", path, { body: { projectId: project.id } })).toMatchObject({ status: 400 });
    expect(await call("POST", path, { body: { ...body, dryRun: "yes" } })).toMatchObject({ status: 400, body: { error: "dryRun must be a boolean" } });
    expect(await call("POST", path, { body: { ...body, domain: "BAD_DOMAIN!" } })).toMatchObject({ status: 400 });
    expect(await call("POST", path, { body })).toMatchObject({ status: 404, body: { error: "account transfer import is not available on this deployment" } });
  });

  it("preserves exact account IDs, pbkdf2 hashes, memberships, and tenant domain when dev-transfer gated", async () => {
    const devTenant = "devcopy-am-demo";
    const otherTenant = "devcopy-am-other";
    expect((await call("POST", "/dev/tenants", { body: { projectId: project.id, id: devTenant, name: "AM Demo" } })).status).toBe(201);
    expect((await call("POST", "/dev/tenants", { body: { projectId: project.id, id: otherTenant, name: "AM Other" } })).status).toBe(201);

    const { handleOperatorRoute } = await import("../../../control/operator/routes.ts");
    const { hashPassword, verifyPassword } = await import("../../../control/src/password.ts");
    const password = "am-demo-secret-pw-123";
    const passwordHash = hashPassword(password);
    const preservedAccountId = "acct_christian_exact_001";
    const loginAccountId = "acct_christian_login_002";
    const preservedMemberId = "mem_christian_exact_001";

    const result = await runInDurableObject(control(), async (instance: OperatorControlObject) => {
      const op = instance.operatorContext();
      op.config = {
        ...op.config,
        devTransferTarget: "gonvex-cf-dev-am-demo",
        devTransferTenants: `${devTenant},${otherTenant}`,
      };
      const invoke = async (payload: Record<string, unknown>) => {
        const res = await handleOperatorRoute(
          op,
          new Request(`http://kernel.test${path}`, {
            method: "POST",
            headers: { authorization: `Bearer ${ADMIN}`, "content-type": "application/json" },
            body: JSON.stringify(payload),
          }),
        );
        return { status: res.status, body: (await res.json()) as Record<string, unknown> };
      };

      // Rejects invalid password hash format
      const badHash = await invoke({
        projectId: project.id,
        tenantId: devTenant,
        passwords: [{ accountId: preservedAccountId, passwordHash: "not-a-valid-hash" }],
      });

      const payload = {
        projectId: project.id,
        tenantId: devTenant,
        domain: "am-demo",
        accounts: [
          { id: preservedAccountId, email: "Christian@AM-Demo.Test", name: "Christian" },
          { id: loginAccountId, email: "Login@AM-Demo.Test", name: "Login User" },
        ],
        identities: [
          { accountId: preservedAccountId, provider: "password", subject: "christian@am-demo.test", email: "christian@am-demo.test", verifiedEmail: true },
          { accountId: loginAccountId, provider: "password", subject: "login@am-demo.test", email: "login@am-demo.test", verifiedEmail: true },
        ],
        passwords: [
          { accountId: preservedAccountId, passwordHash },
          { accountId: loginAccountId, passwordHash },
        ],
        demoAccounts: [{ accountId: preservedAccountId, label: "AM Demo Admin" }],
        memberships: [{ accountId: preservedAccountId, memberId: preservedMemberId, status: "active", tenantMembershipRevision: 3 }],
      };

      // dryRun writes nothing
      const dry = await invoke({ ...payload, dryRun: true });
      const acctAfterDry = instance.sql.get("SELECT id FROM accounts WHERE id=$1", preservedAccountId);

      // Real import + idempotent retry
      const imported = await invoke(payload);
      const retried = await invoke(payload);

      // Domain conflict with another tenant -> 409
      const conflict = await invoke({ projectId: project.id, tenantId: otherTenant, domain: "am-demo" });

      // Verify getByDomain + real passwordLogin against the preserved pbkdf2 hash & account ID
      const domainLookup = await instance.query(
        { connectionId: "c1", projectId: project.id, authToken: "", identity: null, tenant: null, impersonationId: "" },
        "control.tenants.getByDomain",
        { domain: "am-demo" },
      );
      const loginOutcome = await instance.write(
        { connectionId: "c1", projectId: project.id, authToken: "", identity: null, tenant: null, impersonationId: "" },
        "control.auth.passwordLogin",
        { email: "login@am-demo.test", password },
        "idem-am-demo-login-1",
        "action",
      );

      const storedHash = instance.sql.value<string>(
        "SELECT password_hash FROM gonvex_account_passwords WHERE project_id=$1 AND account_id=$2",
        project.id,
        preservedAccountId,
      );
      const demoRow = instance.sql.get(
        "SELECT tenant_id, label FROM gonvex_demo_accounts WHERE project_id=$1 AND account_id=$2",
        project.id,
        preservedAccountId,
      );
      const indexRow = instance.sql.get(
        "SELECT member_id, status, tenant_membership_revision FROM account_tenant_index WHERE account_id=$1 AND tenant_id=$2",
        preservedAccountId,
        devTenant,
      );

      return { badHash, dry, acctAfterDry, imported, retried, conflict, domainLookup, loginOutcome, storedHash, demoRow, indexRow };
    });

    expect(result.badHash.status).toBe(400);
    expect(result.dry).toMatchObject({ status: 200, body: { ok: true, dryRun: true, counts: { accounts: 2, identities: 2, passwords: 2, demoAccounts: 1, memberships: 1 } } });
    expect(result.acctAfterDry).toBeUndefined();
    expect(result.imported).toMatchObject({ status: 200, body: { ok: true, dryRun: false, domain: "am-demo" } });
    expect(result.retried).toMatchObject({ status: 200, body: { ok: true, dryRun: false, domain: "am-demo" } });
    expect(result.conflict.status).toBe(409);
    expect(result.domainLookup).toMatchObject({ ok: true, value: { id: devTenant, domain: "am-demo" } });
    expect(result.loginOutcome).toMatchObject({
      ok: true,
      value: { account: { id: loginAccountId, email: "login@am-demo.test", name: "Login User" } },
    });
    expect(result.storedHash && verifyPassword(password, result.storedHash)).toBe(true);
    expect(result.demoRow).toMatchObject({ tenant_id: devTenant, label: "AM Demo Admin" });
    expect(result.indexRow).toMatchObject({ member_id: preservedMemberId, status: "active", tenant_membership_revision: 3 });
  });
});

describe("operator sockets", () => {
  it("dashboard: session.ready, auth, subscribe, change on a /dev write, errors", async () => {
    const { ws, next } = await socket("/dev/dashboard/ws");
    expect(await next()).toEqual({ type: "session.ready", capabilities: { protocolVersion: 2 } });
    ws.send(JSON.stringify({ type: "auth", id: "a1", token: "bad-token" }));
    expect(await next()).toEqual({ type: "auth.error", id: "a1", error: "dashboard authentication required" });
    ws.send(JSON.stringify({ type: "query.subscribe", id: "s0", path: "dashboard.read", args: { resource: "/dev/projects" } }));
    expect(await next()).toEqual({ type: "query.error", id: "s0", error: "dashboard sign-in or personal access token is required" });
    ws.send(JSON.stringify({ type: "auth", id: "a2", token: ADMIN }));
    expect(await next()).toEqual({ type: "auth.result", id: "a2", result: { system: "dashboard" } });
    ws.send(JSON.stringify({ type: "query.subscribe", id: "s1", path: "dashboard.read", args: { resource: "/dev/projects" } }));
    const initial = await next();
    expect(initial).toMatchObject({ type: "query.result", id: "s1", reason: "initial" });
    const before = initial.result.projects.length;
    ws.send(JSON.stringify({ type: "query.subscribe", id: "s2", path: "dashboard.read", args: { resource: "/dev/projects/x/env" } }));
    expect(await next()).toEqual({ type: "query.error", id: "s2", error: "unsupported dashboard subscription or subscription limit reached" });
    ws.send(JSON.stringify({ type: "query.subscribe", id: "s3", path: "dashboard.read", args: { resource: `/dev/data/tables/notes/rows?limit=10`, project: project.id, tenant: TENANT } }));
    const rows = await next();
    expect(rows).toMatchObject({ type: "query.result", id: "s3", reason: "initial" });
    // A /dev write re-runs every read; only changed results are sent.
    await createProject("dashboard-change");
    const change = await next();
    expect(change).toMatchObject({ type: "query.result", id: "s1", reason: "change" });
    expect(change.result.projects.length).toBe(before + 1);
    // A data write on the tenant changes the rows read.
    await call("POST", `/dev/data/tables/notes/rows?${q({})}`, { token: project.key, body: { id: "live-1", title: "live", created_at: 99 } });
    const live = await next();
    expect(live).toMatchObject({ type: "query.result", id: "s3", reason: "change" });
    expect(live.result.rows[0]).toMatchObject({ id: "live-1" });
    ws.close(1000, "done");
  });

  it("metrics stream: subprotocol auth, echoed, first frame on the alarm tick", async () => {
    const denied = await SELF.fetch(`http://kernel.test/dev/metrics/stream?project=${project.id}`, { headers: { upgrade: "websocket" } });
    expect(denied.status).toBe(401);
    const { response, next, ws } = await socket(`/dev/metrics/stream?project=${project.id}`, { "sec-websocket-protocol": `other, gonvex-dashboard-auth.${ADMIN}` });
    expect(response.headers.get("sec-websocket-protocol")).toBe(`gonvex-dashboard-auth.${ADMIN}`);
    await runInDurableObject(control(), async (instance: OperatorControlObject) => instance.sockets.tick(Date.now() + 5000));
    const frame = await next();
    expect(frame.type).toBe("metrics");
    expect(frame.metrics.functions["notes.list"]).toMatchObject({ kind: "query" });
    ws.close(1000, "done");
  });

  it("log stream: the admin or project key in ?key, ready, replay, then new rows", async () => {
    await runInDurableObject(control(), async (instance: OperatorControlObject) =>
      instance.sql.run(
        "INSERT INTO gonvex_performance_events(event_id,project_id,kind,path,outcome,client_received_at_ms,created_at) VALUES('l1',$1,'query','notes.list','ok',1,1000),('l2',$1,'reducer','notes.add','error',1,2000)",
        project.id,
      ),
    );
    expect((await SELF.fetch(`http://kernel.test/dev/logs/stream?project=${project.id}&key=wrong`, { headers: { upgrade: "websocket" } })).status).toBe(401);
    const { next, ws } = await socket(`/dev/logs/stream?project=${project.id}&key=${encodeURIComponent(project.key)}`);
    expect(await next()).toEqual({ type: "ready" });
    expect((await next()).log).toMatchObject({ operationId: "l1", time: "1970-01-01T00:00:01Z", path: "notes.list", durationMs: 0, source: "client" });
    expect((await next()).log).toMatchObject({ operationId: "l2", outcome: "error" });
    await runInDurableObject(control(), async (instance: OperatorControlObject) => {
      instance.sql.run("INSERT INTO gonvex_performance_events(event_id,project_id,kind,path,outcome,client_received_at_ms,created_at) VALUES('l3',$1,'query','notes.list','ok',1,3000)", project.id);
      await instance.sockets.tick(Date.now());
    });
    expect((await next()).log).toMatchObject({ operationId: "l3" });
    ws.close(1000, "done");
  });
});
