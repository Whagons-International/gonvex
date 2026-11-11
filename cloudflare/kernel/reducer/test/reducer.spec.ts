// The Reducer path end to end in workerd: client socket → router → K05
// gateway → K07 tenant object (K01 storage, K02 migrations, K03 capture, K04
// host). Expected texts and shapes are Rust's (rust/crates/runtime/src
// execution.rs, lib.rs) as the conformance traces record them.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { Client, member, seed, stub } from "./client.ts";
import { fixtureManifest } from "./fixture.ts";

describe("reducer.call", () => {
  it("commits, answers with the committed revision and completes with a watermark", async () => {
    const { projectId, tenant, accounts } = await seed("k07-basic", fixtureManifest());
    const { client } = await member(projectId, tenant, accounts.alice!.email);
    client.mark();
    const reply = await client.call("c1", "board.add", { id: "counter", title: "counter", position: 1 });
    expect(reply).toEqual({
      type: "reducer.result", id: "c1", path: "board.add", result: { id: "counter", title: "counter", position: 1, bumps: 0 },
      originCommandId: "c1", committedRevision: expect.any(Number),
    });
    const watermark = await client.next((f) => f.type === "replica.watermark");
    expect(watermark.revision).toBe(reply.committedRevision);
    const rows = (await stub("GET", `tables/board?project=${projectId}&tenant=${tenant}`)).rows;
    expect(rows).toEqual([{ id: "counter", title: "counter", position: 1, bumps: 0 }]);
    // The change log has the revision with the call's provenance.
    const changes = (await stub("GET", `tables/_gonvex_sync_changes?project=${projectId}&tenant=${tenant}`)).rows.filter((r: any) => r.command_id === "c1");
    expect(changes).toHaveLength(1);
    expect(changes[0]).toMatchObject({ table_name: "board", row_id: "counter", operation: "insert", revision: reply.committedRevision });
    const tx = (await stub("GET", `tables/_gonvex_sync_transactions?project=${projectId}&tenant=${tenant}`)).rows.find((r: any) => r.revision === reply.committedRevision);
    expect(tx).toMatchObject({ origin_command_id: "c1", root_command_id: "c1", invocation_channel: "ui", root_invocation_channel: "ui", actor_account_id: accounts.alice!.accountId, actor_member_id: accounts.alice!.memberId });
    client.close();
  });
});

describe("hooks armed for a call K04 refuses before opening storage", () => {
  it("are dropped: a quarantined keyed call leaves no receipt on the next call (R25 quarantine)", async () => {
    const { projectId, tenant, accounts } = await seed("k07-quarantine", fixtureManifest());
    const { client } = await member(projectId, tenant, accounts.alice!.email);
    await runInDurableObject((env as unknown as { TENANT_STUB: DurableObjectNamespace }).TENANT_STUB.getByName(tenant), (object) => {
      ((object as unknown as { kernel: { host: { quarantine: { add(id: string): void } } } }).kernel.host).quarantine.add("q-1");
    });
    const refused = await client.call("q-1", "board.add", { id: "q", title: "q", position: 1 }, { idempotencyKey: "k07-quarantine-key" });
    expect(refused).toMatchObject({ type: "reducer.error", class: "transient" });
    expect(String(refused.error)).toContain("module exceeded its 10000 ms execution deadline");
    expect((await client.call("next", "board.add", { id: "n", title: "n", position: 2 })).type).toBe("reducer.result");
    expect((await client.query("rq", "__gonvex.reducerReceipt", { idempotencyKey: "k07-quarantine-key", receiptPath: "board.add" })).result).toEqual({ found: false });
    client.close();
  });
});
