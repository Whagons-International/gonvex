// The run protocol of direct agent calls (`ActionCalls`) with a controlled
// clock: fencing, the half-life persistence of renewals, the sweep's recovery
// rules, cancel, the attempt cap, receipts, and the rows each step writes.
// `_gonvex_action_outbox` is K11's (0037): nothing here touches it.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { ACTION_OBJECTS, ActionCalls, RECEIPT_RETENTION_MS, RUNS, type CallFinished, type CallJob } from "../src/calls.js";

const session = { projectId: "p", tenantId: "t", account: { id: "acct-1", email: "a@x" }, member: { id: "m-1" } };

interface Harness {
  calls: ActionCalls;
  jobs: CallJob[];
  finished: CallFinished[];
  clock: { now: number };
  sql: SqlStorage;
  fail: { dispatch: boolean; finished: boolean };
  make(): ActionCalls;
}

async function withCalls<T>(run: (h: Harness, state: DurableObjectState) => Promise<T> | T, options: { leaseMs?: number; maxAttempts?: number } = {}): Promise<T> {
  const stub = env.TENANT.getByName(`calls-${crypto.randomUUID()}`);
  return runInDurableObject(stub, async (_instance, state) => {
    const clock = { now: 1_000_000 };
    const jobs: CallJob[] = [];
    const finished: CallFinished[] = [];
    const fail = { dispatch: false, finished: false };
    let n = 0;
    const make = () => new ActionCalls({
      storage: state.storage, projectId: "p", tenantId: "t",
      dispatcher: { dispatch: async (job) => { if (fail.dispatch) throw new Error("create failed"); jobs.push(job); } },
      wake: () => undefined,
      onFinished: (f) => { if (fail.finished) throw new Error("gateway reply unavailable"); finished.push(f); },
      clock: () => clock.now,
      randomUUID: () => `uuid-${++n}`,
      leaseMs: options.leaseMs ?? 300_000,
      maxAttempts: options.maxAttempts ?? 8,
    });
    return run({ calls: make(), jobs, finished, clock, sql: state.storage.sql, fail, make }, state);
  });
}

describe("direct agent calls (ActionCalls)", () => {
  it("begin hands off once; claim fences on attempts; renewals write locked_at only below half a lease", async () => {
    await withCalls(async (h) => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: { q: 1 } });
      expect(id).toBe("call-uuid-1");
      expect(h.jobs).toEqual([{ v: 1, kind: "call", projectId: "p", tenantId: "t", id, n: 1 }]);
      const claim = await h.calls.claim({ v: 1, id, owner: "w1" });
      expect(claim).toMatchObject({ ok: true, attempt: 1, path: "agent.turn", args: { q: 1 }, session, leaseMs: 300_000, renewMs: 30_000 });
      expect(await h.calls.claim({ v: 1, id, owner: "w2" })).toEqual({ v: 1, ok: false, reason: "leased", retryAfterMs: 300_000 });
      let persisted = 0;
      for (let i = 1; i <= 20; i++) {
        h.clock.now = 1_000_000 + i * 30_000;
        const renewal = await h.calls.renew({ v: 1, id, attempt: 1 });
        expect(renewal.ok).toBe(true);
        if (renewal.persisted) persisted++;
      }
      expect(persisted).toBe(3); // 180 s, 360 s, 540 s: about 20 writes per running hour
      expect(await h.calls.renew({ v: 1, id, attempt: 2 })).toEqual({ v: 1, ok: false, persisted: false });
      expect(await h.calls.complete({ v: 1, id, attempt: 2, value: "1" })).toEqual({ v: 1, applied: false, rejected: true });
      expect(await h.calls.complete({ v: 1, id, attempt: 1, value: '{"answer":42}' })).toEqual({ v: 1, applied: true, appliedAttempt: 1 });
      expect(await h.calls.complete({ v: 1, id, attempt: 1, value: '{"answer":42}' })).toEqual({ v: 1, applied: false, duplicate: true, appliedAttempt: 1 });
      expect(h.finished).toEqual([{ id, ok: true, value: '{"answer":42}' }]);
      expect(await h.calls.claim({ v: 1, id, owner: "w3" })).toEqual({ v: 1, ok: false, reason: "done" });
    });
  });

  it("never touches K11's outbox", async () => {
    await withCalls(async (h) => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: {} });
      await h.calls.claim({ v: 1, id, owner: "w" });
      await h.calls.complete({ v: 1, id, attempt: 1, value: "null" });
      await h.calls.sweep();
      // The tenant has K03's outbox table (installed with the runtime tables); a direct call leaves it empty.
      expect(h.sql.exec("SELECT count(*) AS n FROM _gonvex_action_outbox").one()).toEqual({ n: 0 });
      expect(ACTION_OBJECTS.map(([, name]) => name)).toEqual([RUNS, "_gonvex_st_action_inputs", "_gonvex_st_action_input_chunks", "_gonvex_st_action_replies", "_gonvex_st_action_reply_chunks", "_gonvex_st_action_replies_due", "_gonvex_st_action_runs_live", "_gonvex_st_action_runs_finished"]);
    });
  });

  it("the same owner takes its own lease over at once (a retried Workflow step); an error is the answer, delivered once", async () => {
    await withCalls(async (h) => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: {} });
      await h.calls.claim({ v: 1, id, owner: "w1" });
      expect(await h.calls.claim({ v: 1, id, owner: "w1" })).toMatchObject({ ok: true, attempt: 2 });
      expect(await h.calls.renew({ v: 1, id, attempt: 1 })).toMatchObject({ ok: false });
      expect(await h.calls.complete({ v: 1, id, attempt: 2, error: "boom" })).toMatchObject({ applied: true });
      expect(h.finished).toEqual([{ id, ok: false, error: "boom" }]);
    });
  });

  it("the sweep hands off again an unclaimed hand-off after one lease and an expired lease; a failed create backs off", async () => {
    await withCalls(async (h) => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: {} });
      expect(h.calls.nextAt(h.clock.now)).toBe(1_300_000);
      h.clock.now += 299_999;
      await h.calls.sweep();
      expect(h.jobs.map((job) => job.n)).toEqual([1]);
      h.clock.now += 1;
      await h.calls.sweep();
      expect(h.jobs.map((job) => job.n)).toEqual([1, 2]);
      // Claimed, then the runner goes silent: due one lease after the persisted locked_at.
      await h.calls.claim({ v: 1, id, owner: "zombie" });
      const claimedAt = h.clock.now;
      h.clock.now = claimedAt + 200_000;
      expect(await h.calls.renew({ v: 1, id, attempt: 1 })).toMatchObject({ persisted: true });
      h.clock.now = claimedAt + 200_000 + 299_999;
      await h.calls.sweep();
      expect(h.jobs).toHaveLength(2);
      h.clock.now += 1;
      await h.calls.sweep();
      expect(h.jobs.map((job) => job.n)).toEqual([1, 2, 3]);
      expect(await h.calls.claim({ v: 1, id, owner: "fresh" })).toMatchObject({ ok: true, attempt: 2 });
      expect(await h.calls.complete({ v: 1, id, attempt: 1, value: "1" })).toMatchObject({ applied: false, rejected: true });
      // A create that fails is tried again after one renewal interval, not in a tight alarm loop.
      h.fail.dispatch = true;
      const other = await h.calls.begin({ session, path: "agent.turn", args: {} });
      expect(h.calls.nextAt(h.clock.now)).toBe(h.clock.now + 30_000);
      h.fail.dispatch = false;
      h.clock.now += 30_000;
      expect(await h.calls.sweep()).toBe(1);
      expect(h.jobs.at(-1)).toMatchObject({ id: other, n: 1 });
    });
  });

  it("a lease extended only in memory never outlives an object reset: recovery uses locked_at", async () => {
    await withCalls(async (h) => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: {} });
      await h.calls.claim({ v: 1, id, owner: "w" });
      h.clock.now += 100_000;
      expect(await h.calls.renew({ v: 1, id, attempt: 1 })).toMatchObject({ ok: true, persisted: false });
      const fresh = h.make();
      expect(fresh.inspect(id)).toMatchObject({ leaseExpiry: 1_300_000 });
      h.clock.now = 1_300_000;
      await fresh.sweep();
      expect(h.jobs.at(-1)).toMatchObject({ id, n: 2 });
    });
  });

  it("cancel ends a live call dead and answers it; a runner that dies too often fails the call", async () => {
    await withCalls(async (h) => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: {} });
      await h.calls.claim({ v: 1, id, owner: "w" });
      expect(await h.calls.cancel(id)).toBe(true);
      expect(await h.calls.renew({ v: 1, id, attempt: 1 })).toMatchObject({ ok: false });
      expect(await h.calls.cancel(id)).toBe(false);
      const dying = await h.calls.begin({ session, path: "agent.turn", args: {} });
      for (let owner = 1; owner <= 2; owner++) expect(await h.calls.claim({ v: 1, id: dying, owner: `w${owner}` })).toMatchObject({ ok: owner === 1 ? true : false });
      h.clock.now += 300_001;
      expect(await h.calls.claim({ v: 1, id: dying, owner: "w2" })).toMatchObject({ ok: true, attempt: 2 });
      h.clock.now += 300_001;
      expect(await h.calls.claim({ v: 1, id: dying, owner: "w3" })).toMatchObject({ ok: false, reason: "dead" });
      expect(h.finished).toEqual([
        { id, ok: false, error: "cancelled" },
        { id: dying, ok: false, error: "Action runner stopped 2 times without finishing" },
      ]);
    }, { maxAttempts: 2 });
  });

  it("a result is delivered whole and kept for a duplicate only up to 1 MiB; receipts are pruned after the retention", async () => {
    await withCalls(async (h) => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: {} });
      await h.calls.claim({ v: 1, id, owner: "w" });
      await h.calls.complete({ v: 1, id, attempt: 1, value: JSON.stringify("x".repeat(2 << 20)) });
      expect(h.finished[0]!.value!.length).toBe((2 << 20) + 2);
      expect(h.sql.exec(`SELECT result IS NULL AS dropped, result_bytes FROM ${RUNS} WHERE id = ?`, id).one()).toEqual({ dropped: 1, result_bytes: (2 << 20) + 2 });
      expect(h.calls.nextAt(h.clock.now)).toBe(h.clock.now + RECEIPT_RETENTION_MS);
      h.clock.now += RECEIPT_RETENTION_MS + 1;
      expect(await h.calls.sweep()).toBe(1);
      expect(h.calls.nextAt(h.clock.now)).toBeNull();
    });
  });

  it("redelivers the durable whole result after RPC failure and a fresh tenant instance without rerunning the Action", async () => {
    await withCalls(async h => {
      const routed = { ...session, reply: { host: "gw", connectionId: "socket", epoch: 3, generation: 9, id: "request", path: "agent.turn" } };
      const id = await h.calls.begin({ session: routed, path: "agent.turn", args: {} });
      await h.calls.claim({ v: 1, id, owner: "workflow" });
      const value = JSON.stringify("x".repeat((2 << 20) - 1) + "😀".repeat(40_000));
      h.fail.finished = true;
      await expect(h.calls.complete({ v: 1, id, attempt: 1, value, committedRevision: 42 })).rejects.toThrow("gateway reply unavailable");
      expect(h.calls.inspect(id)).toMatchObject({ state: "done", attempts: 1 });
      expect(h.sql.exec("SELECT count(*) n, max(length(body)) max FROM _gonvex_st_action_reply_chunks").one()).toMatchObject({ max: 65536 });
      expect((h.sql.exec("SELECT count(*) n FROM _gonvex_st_action_reply_chunks").one().n as number)).toBeGreaterThan(32);
      expect(await h.calls.claim({ v: 1, id, owner: "new-workflow" })).toMatchObject({ ok: false, reason: "done" });
      h.fail.finished = false;
      const fresh = h.make();
      expect(await fresh.complete({ v: 1, id, attempt: 1, value: '"must not replace stored answer"' })).toMatchObject({ duplicate: true });
      expect(h.finished).toEqual([{ id, ok: true, value, committedRevision: 42, reply: { ...routed.reply, tenantId: "t" } }]);
      expect(h.sql.exec("SELECT count(*) n FROM _gonvex_st_action_replies").one().n).toBe(0);
      expect(h.sql.exec("SELECT count(*) n FROM _gonvex_st_action_reply_chunks").one().n).toBe(0);
      expect(h.jobs).toHaveLength(1);
    });
  });

  it("chunks a legal large principal plus agent arguments into bounded rows and verifies complete input on claim",async()=>{
    await withCalls(async h=>{
      const permissions={value:"p".repeat(2_000_000)};
      const principal={...session,member:{...session.member,permissions},reply:{host:"gw",connectionId:"physical",epoch:2,generation:5,id:"request",path:"agent.turn"}};
      const args={value:"漢".repeat(100_000)},provenance={rootCommandId:"root",channel:"system",commandId:"root"};
      h.sql.exec("CREATE TABLE _k22_old_row_limit(args TEXT, session TEXT)");
      // Local workerd accepted this aggregate row (calls-6 preserves the failed
      // platform-limit expectation). This measures representation; platform
      // row-limit enforcement is a separate deployment proof obligation.
      h.sql.exec("INSERT INTO _k22_old_row_limit VALUES (?,?)",JSON.stringify(args),JSON.stringify(principal));
      const inlineBytes=new TextEncoder().encode(JSON.stringify(args)+JSON.stringify(principal)).byteLength;
      expect(h.sql.exec("SELECT length(CAST(args AS BLOB))+length(CAST(session AS BLOB)) bytes FROM _k22_old_row_limit").one().bytes).toBe(inlineBytes);
      console.log("K22_ACTION_INPUT_BYTES",JSON.stringify({legacyInlineBytes:inlineBytes}));
      const id=await h.calls.begin({session:principal,path:"agent.turn",args,provenance});
      expect(h.sql.exec("SELECT max(length(body)) bytes FROM _gonvex_st_action_input_chunks WHERE id=?",id).one().bytes).toBeLessThanOrEqual(65536);
      const representation=h.sql.exec("SELECT bytes,chunks,length(hash) digestChars FROM _gonvex_st_action_inputs WHERE id=?",id).one();
      expect(representation.chunks).toBe(Math.ceil((representation.bytes as number)/65536));
      expect(representation.digestChars).toBe(64);
      console.log("K22_ACTION_INPUT_MANIFEST",JSON.stringify(representation));
      const claim=await h.make().claim({v:1,id,owner:"workflow"});
      expect(claim).toMatchObject({ok:true,session:principal,args,provenance:JSON.stringify(provenance)});
      await h.calls.complete({v:1,id,attempt:1,value:'"finished"'});
      expect(h.finished).toEqual([{id,ok:true,value:'"finished"',reply:{...principal.reply,tenantId:"t"}}]);
      expect(h.sql.exec("SELECT count(*) n FROM _gonvex_st_action_inputs WHERE id=?",id).one().n).toBe(0);
      expect(h.sql.exec("SELECT count(*) n FROM _gonvex_st_action_input_chunks WHERE id=?",id).one().n).toBe(0);
    });
  });

  it("rolls back a partial input publication before any run is dispatched",async()=>{
    await withCalls(async(h,state)=>{
      let fail=true;
      const sql=new Proxy(state.storage.sql,{get(target,key){if(key==="exec")return(statement:string,...bindings:SqlStorageValue[])=>{if(fail&&statement.startsWith("INSERT INTO _gonvex_st_action_input_chunks")&&bindings[2]===1){fail=false;throw new Error("input publication fault");}return target.exec(statement,...bindings);};const value=Reflect.get(target,key);return typeof value==="function"?value.bind(target):value;}});
      const storage=new Proxy(state.storage,{get(target,key){if(key==="sql")return sql;const value=Reflect.get(target,key);return typeof value==="function"?value.bind(target):value;}});
      const calls=new ActionCalls({storage,projectId:"p",tenantId:"t",dispatcher:{dispatch:async job=>{h.jobs.push(job);}},wake:()=>{}});
      await expect(calls.begin({session,path:"agent.turn",args:{value:"x".repeat(1_100_000)}})).rejects.toThrow("input publication fault");
      for(const table of [RUNS,"_gonvex_st_action_inputs","_gonvex_st_action_input_chunks"])expect(h.sql.exec(`SELECT count(*) n FROM ${table}`).one().n).toBe(0);
      expect(h.jobs).toEqual([]);
      const id=await calls.begin({session,path:"agent.turn",args:{value:"x".repeat(1_100_000)}});
      expect(await calls.claim({v:1,id,owner:"workflow"})).toMatchObject({ok:true,args:{value:"x".repeat(1_100_000)}});
      expect(h.jobs).toHaveLength(1);
    });
  });

  it("blocks corrupt terminal replies without dispatch, truncation or automatic retry",async()=>{
    await withCalls(async h=>{
      const id=await h.calls.begin({session,path:"agent.turn",args:{}});await h.calls.claim({v:1,id,owner:"workflow"});
      h.fail.finished=true;await expect(h.calls.complete({v:1,id,attempt:1,value:JSON.stringify("x".repeat(100_000))})).rejects.toThrow("gateway reply unavailable");
      h.sql.exec("UPDATE _gonvex_st_action_reply_chunks SET body=? WHERE id=? AND part=1",new TextEncoder().encode("corrupt").buffer,id);
      h.fail.finished=false;
      await expect(h.make().complete({v:1,id,attempt:1,value:'"replacement forbidden"'})).rejects.toThrow(/integrity/);
      expect(h.calls.inspect(id)).toMatchObject({state:"done",attempts:1});expect(h.finished).toEqual([]);
      expect(h.sql.exec("SELECT blocked FROM _gonvex_st_action_replies WHERE id=?",id).one().blocked).toBeTruthy();
      h.clock.now+=RECEIPT_RETENTION_MS+1;await h.make().sweep();
      expect(h.calls.nextAt(h.clock.now)).toBeNull();expect(h.jobs).toHaveLength(1);
      expect(h.sql.exec("SELECT count(*) n FROM _gonvex_st_action_reply_chunks WHERE id=?",id).one().n).toBe(2);
    });
  });

  it("bounds automatic reply attempts while retaining the complete blocked terminal authority",async()=>{
    await withCalls(async h=>{
      const id=await h.calls.begin({session,path:"agent.turn",args:{}});await h.calls.claim({v:1,id,owner:"workflow"});
      h.fail.finished=true;await expect(h.calls.complete({v:1,id,attempt:1,value:'"retained"'})).rejects.toThrow("gateway reply unavailable");
      for(let retry=1;retry<8;retry++){h.clock.now+=30_000;await h.make().sweep();}
      expect(h.sql.exec("SELECT failures,blocked FROM _gonvex_st_action_replies WHERE id=?",id).one()).toMatchObject({failures:8,blocked:"delivery requires operator recovery"});
      h.fail.finished=false;h.clock.now+=RECEIPT_RETENTION_MS+1;await h.make().sweep();
      expect(h.calls.nextAt(h.clock.now)).toBeNull();expect(h.finished).toEqual([]);expect(h.jobs).toHaveLength(1);
      await expect(h.make().complete({v:1,id,attempt:1})).rejects.toThrow("operator recovery");
      expect(h.sql.exec("SELECT count(*) n FROM _gonvex_st_action_reply_chunks WHERE id=?",id).one().n).toBe(1);
    });
  });

  it("rolls back terminal input/reply transfer atomically and retries completion without a new execution claim",async()=>{
    await withCalls(async(h,state)=>{
      const routed={...session,reply:{host:"gw",connectionId:"physical",epoch:2,generation:5,id:"request",path:"agent.turn"}};
      const id=await h.calls.begin({session:routed,path:"agent.turn",args:{value:"x".repeat(1_100_000)}});
      await h.calls.claim({v:1,id,owner:"workflow"});let fail=true;
      const sql=new Proxy(state.storage.sql,{get(target,key){if(key==="exec")return(statement:string,...bindings:SqlStorageValue[])=>{if(fail&&statement.startsWith("DELETE FROM _gonvex_st_action_input_chunks")){fail=false;throw new Error("input transfer fault");}return target.exec(statement,...bindings);};const value=Reflect.get(target,key);return typeof value==="function"?value.bind(target):value;}});
      const storage=new Proxy(state.storage,{get(target,key){if(key==="sql")return sql;const value=Reflect.get(target,key);return typeof value==="function"?value.bind(target):value;}});
      const calls=new ActionCalls({storage,projectId:"p",tenantId:"t",dispatcher:{dispatch:async()=>{}},wake:()=>{},onFinished:finished=>{h.finished.push(finished);}});
      await expect(calls.complete({v:1,id,attempt:1,value:'"one-execution"'})).rejects.toThrow("input transfer fault");
      expect(h.calls.inspect(id)).toMatchObject({state:"live",status:"processing",attempts:1});
      expect(h.sql.exec("SELECT count(*) n FROM _gonvex_st_action_inputs WHERE id=?",id).one().n).toBe(1);
      expect(h.sql.exec("SELECT count(*) n FROM _gonvex_st_action_replies WHERE id=?",id).one().n).toBe(0);
      expect(h.finished).toEqual([]);
      await h.make().complete({v:1,id,attempt:1,value:'"one-execution"'});
      expect(h.finished).toEqual([{id,ok:true,value:'"one-execution"',reply:{...routed.reply,tenantId:"t"}}]);
      expect(h.jobs).toHaveLength(1);
    });
  });

  it("rejects corrupt, reordered or missing inputs before claiming and never hands blocked authority to another runner",async()=>{
    await withCalls(async h=>{
      for(const mode of ["corrupt","missing","manifest"]){
        const id=await h.calls.begin({session,path:"agent.turn",args:{value:"x".repeat(1_100_000)}});
        if(mode==="corrupt")h.sql.exec("UPDATE _gonvex_st_action_input_chunks SET body=? WHERE id=? AND part=1",new TextEncoder().encode("wrong").buffer,id);
        else if(mode==="missing")h.sql.exec("DELETE FROM _gonvex_st_action_input_chunks WHERE id=? AND part=1",id);
        else h.sql.exec("DELETE FROM _gonvex_st_action_inputs WHERE id=?",id);
        await expect(h.make().claim({v:1,id,owner:"workflow"})).rejects.toThrow(/input|context/);
        expect(h.calls.inspect(id)).toMatchObject({state:"live",attempts:0,status:"pending"});
        const dispatches=h.jobs.length;h.clock.now+=600_000;await h.make().sweep();expect(h.jobs).toHaveLength(dispatches);
        await expect(h.make().claim({v:1,id,owner:"late-other-owner"})).rejects.toThrow("operator recovery");
      }
    });
  });

  it("retains the full legal 8-MiB result through failed delivery and duplicate settlement",async()=>{
    await withCalls(async h=>{
      const id=await h.calls.begin({session,path:"agent.turn",args:{}});await h.calls.claim({v:1,id,owner:"workflow"});
      const value=JSON.stringify("x".repeat((8<<20)-2));
      expect(new TextEncoder().encode(value).byteLength).toBe(8<<20);
      h.fail.finished=true;await expect(h.calls.complete({v:1,id,attempt:1,value})).rejects.toThrow("gateway reply unavailable");
      h.fail.finished=false;await h.make().complete({v:1,id,attempt:1});
      expect(h.finished).toEqual([{id,ok:true,value}]);
      expect(h.jobs).toHaveLength(1);
    });
  });

  it("retains failed terminal replies through receipt expiry and retries dead-call replies on the alarm", async () => {
    await withCalls(async h => {
      const id = await h.calls.begin({ session, path: "agent.turn", args: {} });
      await h.calls.claim({ v: 1, id, owner: "workflow" });
      h.fail.finished = true;
      await expect(h.calls.cancel(id)).rejects.toThrow("gateway reply unavailable");
      h.clock.now += RECEIPT_RETENTION_MS + 1;
      await h.make().sweep();
      expect(h.calls.inspect(id)).toMatchObject({ state: "dead", attempts: 1 });
      h.fail.finished = false; h.clock.now += 30_000;
      await h.make().sweep();
      expect(h.finished).toEqual([{ id, ok: false, error: "cancelled" }]);
      expect(h.calls.inspect(id)).toBeNull();
    });
  });

  it("rows written per direct call (the PR's numbers)", async () => {
    await withCalls(async (h) => {
      let base = 0;
      const delta = () => { const d = h.calls.stats.rowsWritten - base; base = h.calls.stats.rowsWritten; return d; };
      const id = await h.calls.begin({ session, path: "agent.turn", args: { q: 1 } });
      const begin = delta();
      await h.calls.claim({ v: 1, id, owner: "w" });
      const claim = delta();
      for (let i = 1; i <= 120; i++) { h.clock.now += 30_000; await h.calls.renew({ v: 1, id, attempt: 1 }); }
      const renewalsPerHour = delta();
      const before={...h.calls.stats};
      await h.calls.complete({ v: 1, id, attempt: 1, value: "{}" });
      const completion={statements:h.calls.stats.statements-before.statements,rowsRead:h.calls.stats.rowsRead-before.rowsRead,rowsWritten:h.calls.stats.rowsWritten-before.rowsWritten};
      console.log(`K22 durable Action completion: ${JSON.stringify(completion)}`);
      expect(completion.statements).toBe(11);
      const complete = delta();
      // SQLite rows written, index entries included. Recorded in README.md; asserted so a change shows up here.
      expect({ begin, claim, renewalsPerHour, complete }).toEqual({ begin: 4, claim: 1, renewalsPerHour: 20, complete: 9 });
    });
  });
});


describe("K22 Workflow reply boundary", () => {
  it("retains the server reply target and nested commit revision for onFinished after the run row clears session", async () => {
    await withCalls(async (h) => {
      const routed = { ...session, reply: { host: "gw-1", connectionId: "socket-1", epoch: 7, id: "action-1", path: "agent.turn" } };
      const id = await h.calls.begin({ session: routed, path: "agent.turn", args: {} });
      await h.calls.claim({ v: 1, id, owner: "workflow-1" });
      await h.calls.complete({ v: 1, id, attempt: 1, value: "42", committedRevision: 19 });
      expect(h.sql.exec("SELECT session FROM _gonvex_st_action_runs WHERE id=?", id).one().session).toBeNull();
      expect(h.finished).toEqual([{ id, ok: true, value: "42", reply: { ...routed.reply, tenantId: routed.tenantId }, committedRevision: 19 }]);
    });
  });
});
