// The Reducer path end to end in workerd: client socket → router → K05
// gateway → K07 tenant object (K01 storage, K02 migrations, K03 capture, K04
// host). Expected texts and shapes are Rust's (rust/crates/runtime/src
// execution.rs, lib.rs) as the conformance traces record them.
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
