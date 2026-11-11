// Error classes, batches, row ids, the outbox, the deferred-FK check (mixed
// mode), permission bits and own-membership changes, through the socket path.
import { describe, expect, it } from "vitest";
// @ts-expect-error vitest alias (the SDK is resolved from its sources)
import { reducerRowId } from "@gonvex/module-sdk";
import { intentDeferredId } from "../../host/src/index.js";
import { member, numbers, seed, stub } from "./client.ts";
import { fixtureManifest } from "./fixture.ts";

const firstLine = (frame: Record<string, any>) => ({ ...frame, ...(typeof frame.error === "string" ? { error: frame.error.split("\n")[0] } : {}) });
const GRANTS = { table: "k07_grants", member: "memberId", permission: "permission", scope: "scope", whereNull: ["deletedAt"] };

describe("error classes (execution.rs, error_class.rs)", () => {
  it("refuses before running, with Rust's texts and classes", async () => {
    const { projectId, tenant, accounts } = await seed("k07-errors", fixtureManifest(1, { clientContract: { version: 3, offlineMaxAgeMs: null } }));
    const { client, auth } = await member(projectId, tenant, accounts.alice!.email, 3);
    const err = (id: string, path: string, error: string, errorClass: string) => ({
      type: "reducer.error", id, path, error, class: errorClass, retryable: errorClass === "transient" || errorClass === "unauthenticated",
    });
    expect(await client.call("e1", "board.missing", {})).toEqual(err("e1", "board.missing", 'function "board.missing" is not registered', "rejected"));
    expect(await client.call("e2", "board.all", {})).toEqual(err("e2", "board.all", 'function "board.all" is a query, not a reducer', "rejected"));
    expect(await client.call("e3", "board.add", { id: 1 })).toEqual(err("e3", "board.add", 'function "board.add" arguments do not match its schema: $.id: expected string, received number', "rejected"));
    expect(await client.call("e4", "board.add", { id: "x", title: "x", position: 1 }, { artifactHash: "old-hash", clientContract: 2 }))
      .toEqual(err("e4", "board.add", 'CLIENT_UPDATE_REQUIRED: expected contract 2, active contract is 3', "update_required"));
    expect(await client.call("e5", "board.add", { id: "x", title: "x", position: 1 }, { artifactHash: "old-hash" }))
      .toEqual(err("e5", "board.add", `STALE_REDUCER_ARTIFACT: expected "old-hash", active artifact is "${auth.artifactHash}"`, "update_required"));
    // An old artifact with the active client contract is accepted (accepts_client_artifact).
    expect((await client.call("e6", "board.add", { id: "x", title: "x", position: 1 }, { artifactHash: "old-hash", clientContract: 3 })).type).toBe("reducer.result");
    expect(await client.call("e7", "board.add", { id: "y", title: "y", position: 1 }, { intentEntropy: "AB".repeat(32) }))
      .toEqual(err("e7", "board.add", 'function "board.add" arguments do not match its schema: Invalid reducer entropy', "rejected"));
    expect(firstLine(await client.call("e8", "errors.badResult", {}))).toMatchObject({ class: "rejected", error: `TypeScript module host returned invalid_result: module ${projectId} function errors.badResult: $.n: expected number, received string` });
    client.close();
  });

  it("reducer.callMany answers every call in order", async () => {
    const { projectId, tenant, accounts } = await seed("k07-many", fixtureManifest());
    const { client } = await member(projectId, tenant, accounts.alice!.email);
    client.mark();
    client.send({ type: "reducer.callMany", calls: [
      { id: "m1", path: "board.add", args: { id: "a", title: "a", position: 1 } },
      { id: "m2", path: "board.missing", args: {} },
      { id: "m3", path: "board.bump", args: { id: "a" }, idempotencyKey: "k07-many-1" },
    ] });
    await client.next((f) => f.id === "m3");
    const replies = client.since().filter((f) => f.type === "reducer.result" || f.type === "reducer.error").map((f) => [f.id, f.type]);
    expect(replies).toEqual([["m1", "reducer.result"], ["m2", "reducer.error"], ["m3", "reducer.result"]]);
    client.close();
  });
});

describe("determinism", () => {
  it("local Reducers allocate the same row ids as the client (reducerRowId)", async () => {
    const { projectId, tenant, accounts } = await seed("k07-rowids", fixtureManifest());
    const { client } = await member(projectId, tenant, accounts.alice!.email);
    const reply = await client.call("intent-0001", "k07.addLocal", { title: "local" });
    const context = { tenant: { id: tenant }, auth: { account: { id: accounts.alice!.accountId } }, invocation: { commandId: "intent-0001" } };
    expect(reply.result).toEqual({ ids: [await reducerRowId(context, "board", 0), await reducerRowId(context, "board", 1)] });
    client.close();
  });

  it("outbox rows commit with the Reducer, with deterministic intent ids", async () => {
    const { projectId, tenant, accounts } = await seed("k07-outbox", fixtureManifest());
    const { client } = await member(projectId, tenant, accounts.alice!.email);
    const reply = await client.call("cmd-outbox", "k07.enqueue", {});
    const action = await intentDeferredId(crypto.subtle, tenant, accounts.alice!.accountId, "cmd-outbox", "action", 0);
    const job = await intentDeferredId(crypto.subtle, tenant, accounts.alice!.accountId, "cmd-outbox", "schedule", 0);
    expect(reply.result).toEqual({ action, job: `job_${job}` });
    const outbox = (await stub("GET", `tables/_gonvex_action_outbox?project=${projectId}&tenant=${tenant}`)).rows;
    expect(outbox.map((r: any) => [r.id, r.action_path, r.status])).toEqual(expect.arrayContaining([[action, "k07.notify", "pending"]]));
    expect(outbox).toHaveLength(2);
    client.close();
  });
});

describe("before commit", () => {
  it("a deferred foreign key fails the call with Rust's text and the object keeps serving (G0-5, mixed tracking)", async () => {
    const { projectId, tenant, accounts } = await seed("k07-fk", fixtureManifest());
    const { client } = await member(projectId, tenant, accounts.alice!.email);
    expect(await client.call("o", "k07.orphan", { id: "child-1" })).toEqual({
      type: "reducer.error", id: "o", path: "k07.orphan", class: "rejected", retryable: false,
      error: "module database capability failed: error returned from database: (code: 787) FOREIGN KEY constraint failed",
    });
    expect((await client.call("ok", "board.add", { id: "after", title: "after", position: 1 })).type).toBe("reducer.result");
    expect((await stub("GET", `tables/storage_children?project=${projectId}&tenant=${tenant}`)).rows).toEqual([]);
    client.close();
  });
});

describe("receipt size (CONTRACT E.a.4)", () => {
  // The 2 MB row limit is the platform's: local workerd stores larger rows, so the failure case is a platform follow-up (0030 #9).
  it("stores and replays a 1.9 MB result", async () => {
    const { projectId, tenant, accounts } = await seed("k07-big", fixtureManifest());
    const { client } = await member(projectId, tenant, accounts.alice!.email);
    const first = await client.call("b1", "k07.big", { size: 1_900_000 }, { idempotencyKey: "k07-big-1" });
    const replay = await client.call("b2", "k07.big", { size: 1_900_000 }, { idempotencyKey: "k07-big-1" });
    expect(first.type).toBe("reducer.result");
    expect(replay.result).toBe(first.result);
    expect(replay.committedRevision).toBe(first.committedRevision);
    client.close();
  });
});

describe("permission bits (permission_bits.rs)", () => {
  it("answers from committed grants only, and follows grants and revocations", async () => {
    const { projectId, tenant, accounts } = await seed("k07-perms", fixtureManifest(1, { permissionGrants: GRANTS }));
    const { client } = await member(projectId, tenant, accounts.alice!.email);
    expect((await client.call("c0", "k07.check", { permission: "tasks.edit" })).result).toBe(false);
    // Its own uncommitted grant is invisible to the Reducer that writes it.
    expect((await client.call("g", "k07.grantAndCheck", { id: "grant-1", permission: "tasks.edit" })).result).toEqual({ before: false, after: false });
    expect((await client.call("c1", "k07.check", { permission: "tasks.edit" })).result).toBe(true);
    expect((await client.call("c2", "k07.check", { permission: "tasks.edit", scope: "spot-1" })).result).toBe(false);
    await client.call("r", "k07.revoke", { id: "grant-1" });
    expect((await client.call("c3", "k07.check", { permission: "tasks.edit" })).result).toBe(false);
    client.close();
  });

  it("fails the host call when the module declares no projection", async () => {
    const { projectId, tenant, accounts } = await seed("k07-noperms", fixtureManifest());
    const { client } = await member(projectId, tenant, accounts.alice!.email);
    expect(firstLine(await client.call("c", "k07.check", { permission: "tasks.edit" }))).toMatchObject({
      type: "reducer.error", class: "rejected",
      error: `TypeScript module host returned execution_failed: module ${projectId} function k07.check: GonvexHostError: host call failed: db_read host call failed [host_call_failed]: the module declares no permissionGrants projection`,
    });
    client.close();
  });
});

describe("own membership (lib.rs own_membership_changed)", () => {
  it("a call that changes the caller's own Member ends the tenant session after its result", async () => {
    const { projectId, tenant, accounts } = await seed("k07-membership", fixtureManifest());
    const { client } = await member(projectId, tenant, accounts.alice!.email);
    client.mark();
    const reply = await client.call("p", "k07.promoteSelf", { role: "owner" });
    expect(reply.type).toBe("reducer.result");
    const authError = await client.next((f) => f.type === "auth.error");
    expect(authError).toEqual({ type: "auth.error", id: "membership-changed", error: "tenant membership changed; authenticate again", code: "transient" });
    const watermark = await client.next((f) => f.type === "replica.watermark");
    expect(watermark.revision).toBe(reply.committedRevision);
    const order = client.since().map((f) => f.type);
    expect(order.indexOf("reducer.result")).toBeLessThan(order.indexOf("auth.error"));
    expect(await client.call("after", "board.add", { id: "x", title: "x", position: 1 })).toEqual({
      type: "reducer.error", id: "after", path: "board.add", error: "authenticate with an active tenant before calling a Reducer", class: "unauthenticated", retryable: true,
    });
    client.close();
  });
});

describe("numbers", () => {
  it("rows read and written per call (warm object)", async () => {
    const { projectId, tenant, accounts } = await seed("k07-numbers", fixtureManifest());
    const { client } = await member(projectId, tenant, accounts.alice!.email);
    await client.call("warm", "board.add", { id: "warm", title: "warm", position: 0 });
    const out: unknown[] = [];
    const measure = async (label: string, path: string, args: unknown, extra: Record<string, unknown> = {}) => {
      await client.call(`n-${label}`, path, args, extra);
      out.push({ label, ...(await numbers(tenant)) });
    };
    await measure("board.add", "board.add", { id: "a", title: "a", position: 1 });
    await measure("board.add keyed", "board.add", { id: "b", title: "b", position: 2 }, { idempotencyKey: "k07-numbers-1" });
    await measure("board.bump keyed", "board.bump", { id: "a" }, { idempotencyKey: "k07-numbers-2" });
    await measure("notes.write", "notes.write", { id: "n1", body: "x" });
    await measure("errors.reject", "errors.reject", { id: "r" });
    console.log(JSON.stringify(out));
    expect(out).toHaveLength(5);
    client.close();
  });
});
