// /dev/sync end to end in workerd: router → R2 → control object → builder →
// control migrations → queue rollout → tenant objects (K02) → activation.
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { OperatorControlObject } from "../../../control/operator/object.ts";
import { HttpReleaseBuilder, type ReleaseBuilder } from "../../../control/operator/release.ts";
import { ADMIN, CONTROL_MIGRATION, FALLBACK_SYNC_KEY, NOTES_MIGRATION, call, control, createProject, manifest, sync, tenantObject, testEnv, useBuilder } from "./helpers.ts";
import { SyncCoordinator, type SyncPorts, type SyncOutcome } from "../../../control/operator/sync";
import { waitForRelease, type OperatorTenantPort } from "../../../control/operator/tenant/port";

describe("/dev/sync", () => {
  it("keeps publication private and reports builder HTTP status/request id without copying HTML", async () => {
    const path=`/_gonvex/k19/publications/${crypto.randomUUID()}`;
    expect((await call("POST",path,{token:null})).status).toBe(404);
    expect((await call("POST",path,{token:ADMIN})).status).toBe(404);
    const absent=await call("POST",path,{token:"k19-builder-token"});
    expect(absent.status).toBe(503);expect(absent.body.error).toContain("publication");
    const builder=new HttpReleaseBuilder(async()=>new Response('<html>private upstream context</html>',{status:503,headers:{'cf-ray':'synthetic-ray'}}));
    await expect(builder.inspect("incoming","p")).rejects.toThrow('release builder /inspect failed (HTTP 503; cf-ray synthetic-ray): non-JSON upstream response');
  });
  it("resumes a code-update reset with the same real tenant plan and never repeats its migration rows", async () => {
    await useBuilder();
    const project=await createProject("provision-reset");
    expect((await sync(project.id,await manifest(project.id),project.key)).status).toBe(200);
    const tenant="provision-reset-tenant";
    expect((await call("POST","/dev/tenants",{body:{projectId:project.id,id:tenant,name:tenant}})).status).toBe(201);
    const actual=tenantObject(tenant), status=await actual.operatorMigrationStatus();
    let calls=0;
    const port={operatorApplyRelease:async(plan:string,projectId:string)=>{
      const result=await actual.operatorApplyRelease(plan,projectId);
      if(++calls===1)throw new Error("Durable Object reset because its code was updated.");
      return result;
    },operatorMigrationStatus:()=>actual.operatorMigrationStatus()} as unknown as OperatorTenantPort;
    expect(await waitForRelease(port,status.planId!,project.id,Date.now()+5000,1)).toMatchObject({state:"done"});
    expect(calls).toBe(2);expect((await actual.operatorLedger()).map(row=>row.name)).toEqual(["0001_notes.sql"]);
    expect((await actual.operatorReadRows("notes",{limit:10,offset:0,search:"",sort:"",direction:"",filters:"",columns:"",count:""}))).toMatchObject({ok:true,value:{total:3}});
    const refused={operatorApplyRelease:async()=>{throw new Error("migration constraint failed");}} as unknown as OperatorTenantPort;
    await expect(waitForRelease(refused,status.planId!,project.id,Date.now()+5000,1)).rejects.toThrow("migration constraint failed");
  });
  it("retries one sync request without another rollout/generation and rejects request-id reuse for different content", async () => {
    await useBuilder();
    const project=await createProject("request-retry"),body=await manifest(project.id),headers={"x-gonvex-sync-request-id":crypto.randomUUID()};
    expect((await sync(project.id,body,project.key,headers)).status).toBe(200);
    const before=await call("GET",`/dev/manifest?project=${project.id}`,{token:project.key});
    expect((await sync(project.id,body,project.key,headers)).status).toBe(200);
    expect((await call("GET",`/dev/manifest?project=${project.id}`,{token:project.key})).body.module).toEqual(before.body.module);
    const changed=await manifest(project.id,{files:{"changed.txt":"new content"}});
    expect((await sync(project.id,changed,project.key,headers)).status).toBe(409);
  });

  it("persists project holder watches across a real Control eviction", async () => {
    const id="synthetic-holder",project="watch-persistence";
    await control().watch(project,"GATEWAY",id);
    await evictDurableObject(control());
    await runInDurableObject(control(),async(instance:OperatorControlObject)=>{
      const actual=instance as unknown as {watchersFor(project:string):Map<string,{binding:string;name:string}>};
      expect([...actual.watchersFor(project).values()]).toEqual([{binding:"GATEWAY",name:id}]);
      await instance.unwatch(project,"GATEWAY",id);
      expect([...actual.watchersFor(project).values()]).toEqual([]);
    });
  });
  it("publishes only after version and tenant activation, resumes after a real Control eviction, and awaits notification", async () => {
    await useBuilder();
    const project = await createProject("publication");
    const body = await manifest(project.id);
    await testEnv.RELEASES.put("incoming/publication.json", JSON.stringify(body));
    const outcome = await control().devSync({incoming:"publication",headerProject:project.id,key:project.key}) as SyncOutcome;
    expect(outcome.ok).toBe(true);
    if (!outcome.ok || !outcome.publication) throw new Error("publication not staged");
    const id = outcome.publication;
    expect((await call("GET", `/dev/manifest?project=${project.id}`, {token:project.key})).status).toBe(404);
    // Its durable publication survives losing the old request and builder override.
    await evictDurableObject(control());
    await runInDurableObject(control(), async (instance: OperatorControlObject) => {
      let version = false, tenant = false, delivered = false;
      const events: string[] = [];
      let release!: () => void;
      const gate = new Promise<void>(resolve => {release=resolve;});
      const ports = {
        sql:instance.sql,
        versionReady:()=>version,
        activateTenants:async()=>{events.push("tenant");return tenant;},
        syncCrons:async()=>{events.push("crons");},
        moduleReloaded:async()=>{events.push("reload");await gate;delivered=true;},
      } as unknown as SyncPorts;
      const coordinator = new SyncCoordinator(ports);
      expect(await coordinator.publish(id)).toEqual({ready:false});
      expect(events).toEqual([]);
      version=true;
      expect(await coordinator.publish(id)).toEqual({ready:false});
      expect(coordinator.activeRelease(project.id)).toBeNull();
      tenant=true;
      const publishing = coordinator.publish(id);
      await new Promise(resolve=>setTimeout(resolve,0));
      expect(delivered).toBe(false);
      expect(events).toEqual(["tenant","tenant","crons","reload"]);
      expect(instance.sql.value("SELECT completed FROM gonvex_cf_publications WHERE sync_id=$1",id)).toBe(0);
      release();
      expect(await publishing).toEqual({ready:true});
      expect(delivered).toBe(true);
      expect(await coordinator.publish(id)).toEqual({ready:true});
      expect(events).toEqual(["tenant","tenant","crons","reload"]);
      expect(coordinator.activeRelease(project.id)?.hash).toBe((body.module as {hash:string}).hash);
    });
  });

  it("does not complete a failed notification and retries it without rerunning the sync", async () => {
    await useBuilder();
    const project = await createProject("publication-retry");
    await testEnv.RELEASES.put("incoming/publication-retry.json",JSON.stringify(await manifest(project.id)));
    const outcome=await control().devSync({incoming:"publication-retry",headerProject:project.id,key:project.key}) as SyncOutcome;
    if (!outcome.ok || !outcome.publication) throw new Error("publication not staged");
    const id=outcome.publication;
    await runInDurableObject(control(),async(instance:OperatorControlObject)=>{
      let fail=true,calls=0;
      const coordinator=new SyncCoordinator({sql:instance.sql,versionReady:()=>true,activateTenants:async()=>true,
        syncCrons:async()=>{},moduleReloaded:async()=>{calls++;if(fail)throw new Error("holder unavailable");}} as unknown as SyncPorts);
      await expect(coordinator.publish(id)).rejects.toThrow("holder unavailable");
      expect(instance.sql.value("SELECT completed FROM gonvex_cf_publications WHERE sync_id=$1",id)).toBe(0);
      fail=false;expect(await coordinator.publish(id)).toEqual({ready:true});expect(calls).toBe(2);
    });
  });
  it("rejects a wrong key, a project mismatch and a body that is not a module, with Rust's texts", async () => {
    await useBuilder();
    const project = await createProject("sync-errors");
    const body = await manifest(project.id);
    let reply = await sync(project.id, body, "not-the-key");
    expect([reply.status, reply.body]).toEqual([401, { error: "invalid Gonvex sync key" }]);
    reply = await sync(project.id, { ...body, project: "another" }, project.key);
    expect([reply.status, reply.body]).toEqual([400, { error: "manifest project does not match x-gonvex-project-id" }]);
    reply = await sync(project.id, { project: project.id }, project.key);
    expect([reply.status, reply.body]).toEqual([422, { error: "manifest has no TypeScript module" }]);
    reply = await sync(project.id, await manifest(project.id, { hash: "f".repeat(64) }), project.key);
    expect(reply.status).toBe(422);
    expect(reply.body.error).toMatch(/^project ".+" has an invalid module artifact: module artifact hash "f{64}" does not match canonical contract hash "[0-9a-f]{64}"$/);
    reply = await sync(project.id, await manifest(project.id, { migrations: { "001_bad.sql": "-- gonvex:scope tenant\nSELECT 1;" } }), project.key);
    expect([reply.status, reply.body.error]).toEqual([422, '"001_bad.sql" must be named NNNN_description.sql'.replace(/^/, 'project "' + project.id + '" has an invalid module artifact: migration ')]);
    reply = await call("POST", "/dev/sync", { token: null, raw: "{not json", headers: { "content-type": "application/json", authorization: `Bearer ${project.key}`, "x-gonvex-project-id": project.id } });
    expect(reply.status).toBe(400);
    reply = await call("POST", "/dev/sync", { token: project.key, raw: "{}", headers: { "x-gonvex-project-id": project.id } });
    expect(reply.status).toBe(415);
  });

  it("an unknown project syncs under GONVEX_DEV_SYNC_KEY and installs with no tenant (0012 R3)", async () => {
    await useBuilder();
    const reply = await sync("unregistered-project", await manifest("unregistered-project"), FALLBACK_SYNC_KEY);
    expect(reply.status).toBe(200);
    expect(reply.body).toEqual({ ok: true, project: "unregistered-project", functionCount: 3, schemaDefinition: { tables: {} }, runtimeReloaded: true });
    const wrong = await sync("unregistered-project", await manifest("unregistered-project"), "something-else");
    expect(wrong.status).toBe(401);
  });

  it("migrates every tenant through the queue, applies control migrations, activates and serves the manifest", async () => {
    const target = await useBuilder();
    const project = await createProject("sync-rollout");
    // Tenants need an installed module first (lib.rs provision_runtime_tenant).
    const early = await call("POST", "/dev/tenants", { body: { projectId: project.id, id: "rollout-early", name: "Early" } });
    expect([early.status, early.body]).toEqual([422, { error: "install the TypeScript module before creating the first tenant shard" }]);

    const first = await sync(project.id, await manifest(project.id, { migrations: { "0001_notes.sql": NOTES_MIGRATION, "0002_control.sql": CONTROL_MIGRATION } }), project.key);
    expect(first.status).toBe(200);
    expect(first.body).toMatchObject({ ok: true, project: project.id, functionCount: 3, runtimeReloaded: true });
    expect(target.deployed).toHaveLength(0); // the router deploys through the builder URL (outboundService)
    expect(target.staged.size).toBe(1);

    for (const id of ["rollout-a", "rollout-b"]) {
      const created = await call("POST", "/dev/tenants", { body: { projectId: project.id, id, name: id.toUpperCase() } });
      expect(created.status).toBe(201);
      expect(created.body).toMatchObject({ id, projectId: project.id, name: id.toUpperCase(), status: "active", database: `projects/${project.id}/tenants/${id}.db` });
    }
    const ledgerA = await tenantObject("rollout-a").operatorLedger();
    expect(ledgerA.map((row) => row.name)).toEqual(["0001_notes.sql"]);

    // A second release with one more tenant migration rolls out to both tenants.
    const second = await sync(
      project.id,
      await manifest(project.id, { migrations: { "0001_notes.sql": NOTES_MIGRATION, "0002_control.sql": CONTROL_MIGRATION, "0003_more.sql": "-- gonvex:scope tenant\nALTER TABLE notes ADD COLUMN body TEXT NOT NULL DEFAULT '';\n" } }),
      project.key,
    );
    expect(second.status).toBe(200);
    for (const tenant of ["rollout-a", "rollout-b"]) {
      expect((await tenantObject(tenant).operatorLedger()).map((row) => row.name)).toEqual(["0001_notes.sql", "0003_more.sql"]);
    }
    const manifestReply = await call("GET", `/dev/manifest?project=${project.id}`, { token: project.key });
    expect(manifestReply.status).toBe(200);
    expect(Object.keys(manifestReply.body)).toEqual(["project", "functions", "schema", "visibility", "module"]);
    expect(manifestReply.body.module.generation).toBe(2);
    expect(Object.keys(manifestReply.body.functions)).toEqual(["notes.list", "notes.add", "notes.sync"]);

    // Changing an applied migration fails fast at the first tenant, with Rust's text; nothing else moves.
    const changed = await sync(
      project.id,
      await manifest(project.id, { migrations: { "0001_notes.sql": `${NOTES_MIGRATION}-- edited\n`, "0002_control.sql": CONTROL_MIGRATION } }),
      project.key,
    );
    expect(changed.status).toBe(422);
    expect(changed.body.error).toMatch(/^migration 0001_notes\.sql checksum mismatch: database has [0-9a-f]{64}, artifact has [0-9a-f]{64}$/);
    const after = await call("GET", `/dev/manifest?project=${project.id}`, { token: project.key });
    expect(after.body.module).toEqual(manifestReply.body.module);
  });

  it("a second concurrent sync of the same project is refused with 409 (0012 R5)", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    await useBuilder((core): ReleaseBuilder => ({
      inspect: (...a) => core.inspect(...a),
      prepare: async (...a) => {
        await gate;
        return core.prepare(...a);
      },
      activate: (...a) => core.activate(...a),
      deploy: (...a) => core.deploy(...a),
    }));
    const project = await createProject("sync-lock");
    const body = await manifest(project.id);
    const first = sync(project.id, body, project.key);
    await new Promise((resolve) => setTimeout(resolve, 200));
    const second = await sync(project.id, body, project.key);
    expect([second.status, second.body]).toEqual([409, { error: `a /dev/sync of project ${project.id} is already running` }]);
    release();
    expect((await first).status).toBe(200);
    expect((await sync(project.id, body, project.key)).status).toBe(200);
  });

  it("the Worker version carries every project's active bundle", async () => {
    const target = await useBuilder();
    const one = await createProject("bundle-one");
    const two = await createProject("bundle-two");
    expect((await sync(one.id, await manifest(one.id), one.key)).status).toBe(200);
    expect((await sync(two.id, await manifest(two.id), two.key)).status).toBe(200);
    const last = [...target.staged.values()].at(-1)!;
    const entry = last.get("releases.js")!;
    expect(entry).toContain(JSON.stringify(one.id));
    expect(entry).toContain(JSON.stringify(two.id));
    const app = [...last.entries()].find(([path]) => path.endsWith("/app.js"))![1];
    expect(app.startsWith('import { Date, Math, crypto, console, globalThis, self, fetch,')).toBe(true);
    expect(app.split("\n")[0]!.endsWith('from "gonvex:module-globals";')).toBe(true);
  });

  it("the local tenant installs its project's baked release into the K04 registry", async () => {
    const served = await tenantObject("served-tenant").operatorServedModule("fixture-project");
    expect(served).toMatchObject({ projectId: "fixture-project", hash: "fixture-hash", generation: 1, functions: 3, error: null });
    expect(await tenantObject("served-tenant").operatorServedModule("unknown-project")).toBeNull();
  });

  it("acknowledges only a matching release whose actual registry installation succeeded", async () => {
    const good = tenantObject("activation-good");
    const release = { projectId: "fixture-project", hash: "fixture-hash", generation: 1 };
    expect(await good.operatorActivateRelease(release)).toBe(true);
    expect(await good.operatorActivateRelease({ ...release, hash: "another-hash" })).toBe(false);
    expect(await good.operatorActivateRelease({ ...release, generation: 2 })).toBe(false);
    expect(await good.operatorActivateRelease({ ...release, projectId: "unknown-project" })).toBe(false);

    const broken = tenantObject("activation-broken");
    const served = await broken.operatorServedModule("fixture-broken-project");
    expect(served).toMatchObject({ hash: "fixture-broken-hash", generation: 1 });
    expect(served?.error).toEqual(expect.any(String));
    expect(served?.error).not.toBe("");
    expect(await broken.operatorActivateRelease({ projectId: "fixture-broken-project", hash: "fixture-broken-hash", generation: 1 })).toBe(false);
  });

  it("costs the control object a bounded number of rows per sync (PR numbers)", async () => {
    await useBuilder();
    const project = await createProject("sync-cost");
    expect((await sync(project.id, await manifest(project.id), project.key)).status).toBe(200);
    for (const id of ["cost-a", "cost-b"]) expect((await call("POST", "/dev/tenants", { body: { projectId: project.id, id, name: id } })).status).toBe(201);
    const mark = () => runInDurableObject(control(), async (instance: OperatorControlObject) => instance.sql.mark());
    const before = await mark();
    expect((await sync(project.id, await manifest(project.id), project.key)).status).toBe(200);
    const after = await mark();
    const cost = { rowsRead: after.read - before.read, rowsWritten: after.written - before.written };
    console.log(`control object, one /dev/sync with 2 tenants: ${JSON.stringify(cost)}`);
    // Lock, release row, two rollout reports, activation (release, active release, manifest), unlock; plus indexes,
    // plus the delete fence: the deletion check at lock and the retirement generation at catalog/staged (#481).
    // Measured 2026-11-18: 19 written; 33 read alone, 42 read after the rest of the suite (40 before #481).
    expect(cost.rowsWritten).toBeLessThanOrEqual(20);
    expect(cost.rowsRead).toBeLessThanOrEqual(45);
  });

  it("healthz answers {ok} and diagnostics only for an admin credential", async () => {
    const open = await call("GET", "/healthz", { token: null });
    expect([open.status, open.body, open.headers.get("cache-control")]).toEqual([200, { ok: true }, "no-store"]);
    const admin = await call("GET", "/healthz", { token: ADMIN });
    expect(admin.body).toMatchObject({ ok: true, version: "development", moduleHost: { ready: true } });
    void control;
  });
});
