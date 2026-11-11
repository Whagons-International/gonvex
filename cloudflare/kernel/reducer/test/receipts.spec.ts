// Receipts (CONTRACT E.receipts, B.7, 0009) through the socket path: the
// cases of the conformance receipt scenarios (lost reply, duplicate send,
// receipt query, rejection, upgrade) plus expiry, which only the kernel has.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { Client, member, prune, seed, stub } from "./client.ts";
import { fixtureManifest } from "./fixture.ts";

const DAY = 24 * 60 * 60 * 1000;
/** Error text without JavaScript stack lines (Rust sends them too; conformance strips them, ruling 0015). */
const firstLine = (frame: Record<string, any>) => ({ ...frame, ...(typeof frame.error === "string" ? { error: frame.error.split("\n")[0] } : {}) });

describe("idempotency keys", () => {
  it("lost reply: the replay on a new socket returns the stored result once (receipt-lost-reply)", async () => {
    const { projectId, tenant, accounts } = await seed("k07-lost", fixtureManifest());
    let { client, token } = await member(projectId, tenant, accounts.alice!.email);
    await client.call("seed", "board.add", { id: "counter", title: "counter", position: 1 });
    const key = "conformance-lost-reply-key-0001";
    // The call is sent and the socket drops before the reply.
    client.send({ type: "reducer.call", id: "lost", path: "board.bump", args: { id: "counter" }, idempotencyKey: key, receiptPath: "board.bump" });
    client.close();
    client = await Client.connect();
    await client.auth(projectId, tenant, token);
    const replay = await client.call("replay", "board.bump", { id: "counter" }, { idempotencyKey: key, receiptPath: "board.bump" });
    const again = await client.call("again", "board.bump", { id: "counter" }, { idempotencyKey: key, receiptPath: "board.bump" });
    expect(replay).toMatchObject({ type: "reducer.result", result: { bumps: 1 }, originCommandId: "replay" });
    expect(again).toMatchObject({ type: "reducer.result", result: { bumps: 1 }, committedRevision: replay.committedRevision });
    // Same key, other arguments: Rust returns the stored result (conformance README "Reference behaviours" 1).
    const other = await client.call("other", "board.bump", { id: "other" }, { idempotencyKey: key, receiptPath: "board.bump" });
    expect(other).toMatchObject({ type: "reducer.result", result: { bumps: 1 } });
    const fresh = await client.call("fresh", "board.bump", { id: "counter" }, { idempotencyKey: "conformance-lost-reply-key-0002", receiptPath: "board.bump" });
    expect(fresh).toMatchObject({ result: { bumps: 2 } });
    expect(fresh.committedRevision).toBeGreaterThan(replay.committedRevision);
    const rows = (await stub("GET", `tables/board?project=${projectId}&tenant=${tenant}`)).rows;
    expect(rows).toEqual([{ id: "counter", title: "counter", position: 1, bumps: 2 }]);
    const receipts = (await stub("GET", `tables/_gonvex_reducer_idempotency?project=${projectId}&tenant=${tenant}`)).rows;
    expect(receipts.map((r: any) => [r.subject, r.idempotency_key, r.path, r.result])).toEqual([
      [accounts.alice!.accountId, key, "board.bump", '{"bumps":1}'],
      [accounts.alice!.accountId, "conformance-lost-reply-key-0002", "board.bump", '{"bumps":2}'],
    ]);
    client.close();
  });

  it("duplicate send: two frames with one key in a row apply once", async () => {
    const { projectId, tenant, accounts } = await seed("k07-dup", fixtureManifest());
    const { client } = await member(projectId, tenant, accounts.alice!.email);
    await client.call("seed", "board.add", { id: "counter", title: "counter", position: 1 });
    const key = "k07-duplicate-0001";
    client.send({ type: "reducer.call", id: "d1", path: "board.bump", args: { id: "counter" }, idempotencyKey: key });
    client.send({ type: "reducer.call", id: "d2", path: "board.bump", args: { id: "counter" }, idempotencyKey: key });
    const [first, second] = await Promise.all([
      client.next((f) => f.id === "d1" && f.type === "reducer.result"),
      client.next((f) => f.id === "d2" && f.type === "reducer.result"),
    ]);
    expect(first.result).toEqual({ bumps: 1 });
    expect(second.result).toEqual({ bumps: 1 });
    expect(second.committedRevision).toBe(first.committedRevision);
    client.close();
  });

  it("the receipt query: found with the clock's cursor, unknown key, other path, missing args", async () => {
    const { projectId, tenant, accounts } = await seed("k07-query", fixtureManifest());
    const { client } = await member(projectId, tenant, accounts.alice!.email);
    await client.call("seed", "board.add", { id: "counter", title: "counter", position: 1 });
    const key = "conformance-lost-reply-key-0001";
    // No receipt table yet: not found.
    expect(await client.query("q0", "__gonvex.reducerReceipt", { idempotencyKey: key, receiptPath: "board.bump" }))
      .toEqual({ type: "query.result", id: "q0", path: "__gonvex.reducerReceipt", reason: "initial", result: { found: false } });
    const bump = await client.call("b", "board.bump", { id: "counter" }, { idempotencyKey: key, receiptPath: "board.bump" });
    client.mark();
    const known = await client.query("q1", "__gonvex.reducerReceipt", { idempotencyKey: key, receiptPath: "board.bump" });
    expect(known).toEqual({ type: "query.result", id: "q1", path: "__gonvex.reducerReceipt", reason: "initial", result: { found: true, cursor: { epoch: expect.any(String), revision: bump.committedRevision } } });
    // A found receipt completes with a watermark at its cursor (lib.rs committed_revision).
    expect((await client.next((f) => f.type === "replica.watermark")).revision).toBe(bump.committedRevision);
    expect((await client.query("q2", "__gonvex.reducerReceipt", { idempotencyKey: "conformance-never-sent-0001", receiptPath: "board.bump" })).result).toEqual({ found: false });
    expect(await client.query("q3", "__gonvex.reducerReceipt", { idempotencyKey: key, receiptPath: "board.rename" })).toEqual({
      type: "query.error", id: "q3", path: "__gonvex.reducerReceipt",
      error: 'idempotency key "conformance-lost-reply-key-0001" was already used by reducer "board.bump"; refusing to replay it for "board.rename"',
    });
    expect((await client.query("q4", "__gonvex.reducerReceipt", { idempotencyKey: key })).error)
      .toBe('function "__gonvex.reducerReceipt" arguments do not match its schema: idempotencyKey and receiptPath are required');
    // The same key on another path is refused for a Reducer call too (IdempotencyPathMismatch, rejected).
    expect(await client.call("r", "board.rename", { id: "counter", title: "x" }, { idempotencyKey: key })).toEqual({
      type: "reducer.error", id: "r", path: "board.rename", class: "rejected", retryable: false,
      error: 'idempotency key "conformance-lost-reply-key-0001" was already used by reducer "board.bump"; refusing to replay it for "board.rename"',
    });
    client.close();
  });

  it("a rejected call keeps no receipt and rejects again on replay", async () => {
    const { projectId, tenant, accounts } = await seed("k07-reject", fixtureManifest());
    const { client } = await member(projectId, tenant, accounts.alice!.email);
    const key = "conformance-lost-reply-key-0003";
    const error = {
      type: "reducer.error", path: "errors.reject", class: "rejected", retryable: false,
      error: `TypeScript module host returned execution_failed: module ${projectId} function errors.reject: Error: fixture rejected the intent`,
    };
    expect(firstLine(await client.call("r1", "errors.reject", { id: "never" }, { idempotencyKey: key, receiptPath: "errors.reject" }))).toEqual({ ...error, id: "r1" });
    expect(firstLine(await client.call("r2", "errors.reject", { id: "never" }, { idempotencyKey: key, receiptPath: "errors.reject" }))).toEqual({ ...error, id: "r2" });
    expect((await client.query("q", "__gonvex.reducerReceipt", { idempotencyKey: key, receiptPath: "errors.reject" })).result).toEqual({ found: false });
    expect((await stub("GET", `tables/board?project=${projectId}&tenant=${tenant}`)).rows).toEqual([]);
    client.close();
  });

  it("receipts are per account (receipt-owner-switch)", async () => {
    const { projectId, tenant, accounts } = await seed("k07-owner", fixtureManifest(), ["alice", "bob"]);
    const alice = await member(projectId, tenant, accounts.alice!.email);
    const bob = await member(projectId, tenant, accounts.bob!.email);
    const key = "conformance-owner-key-0001";
    expect((await alice.client.call("a", "notes.write", { id: "n-keyed", body: "alice keyed" }, { idempotencyKey: key, receiptPath: "notes.write" })).result).toEqual({ id: "n-keyed", version: 1 });
    expect((await bob.client.query("q", "__gonvex.reducerReceipt", { idempotencyKey: key, receiptPath: "notes.write" })).result).toEqual({ found: false });
    expect((await bob.client.call("b", "notes.write", { id: "n-bob-keyed", body: "bob keyed" }, { idempotencyKey: key, receiptPath: "notes.write" })).result).toEqual({ id: "n-bob-keyed", version: 1 });
    const overwrite = firstLine(await bob.client.call("o", "notes.write", { id: "n-keyed", body: "bob overwrite attempt" }));
    expect(overwrite).toMatchObject({ type: "reducer.error", class: "rejected", error: `TypeScript module host returned execution_failed: module ${projectId} function notes.write: Error: note belongs to another member` });
    alice.client.close();
    bob.client.close();
  });
});

describe("receipt expiry (0009)", () => {
  it("expires results after the offline window plus 24 h, refuses the key, then deletes the tombstone after 365 days", async () => {
    const window = 7 * DAY;
    const { projectId, tenant, accounts } = await seed("k07-expiry", fixtureManifest(1, { clientContract: { version: 1, offlineMaxAgeMs: window } }));
    const { client } = await member(projectId, tenant, accounts.alice!.email, 1);
    // Activation armed the tenant alarm for the hourly expiry (0009 rule 5).
    const alarm = await runInDurableObject((env as unknown as { TENANT_STUB: DurableObjectNamespace }).TENANT_STUB.getByName(tenant), (_object, state) => state.storage.getAlarm());
    expect(alarm).toBeGreaterThan(Date.now() + 59 * 60_000);
    await client.call("seed", "board.add", { id: "counter", title: "counter", position: 1 });
    const key = "k07-expiry-0001";
    expect((await client.call("b1", "board.bump", { id: "counter" }, { idempotencyKey: key })).result).toEqual({ bumps: 1 });
    const now = Date.now();
    // Inside the window: kept.
    expect(await prune(tenant, now + window + DAY - 60_000)).toMatchObject({ expired: 0, deleted: 0, done: true });
    expect((await client.call("b2", "board.bump", { id: "counter" }, { idempotencyKey: key })).result).toEqual({ bumps: 1 });
    // Past window + 24 h: the result goes, the row stays as a tombstone.
    expect(await prune(tenant, now + window + DAY + 60_000)).toMatchObject({ expired: 1, deleted: 0, done: true });
    expect(await client.call("b3", "board.bump", { id: "counter" }, { idempotencyKey: key })).toEqual({
      type: "reducer.error", id: "b3", path: "board.bump", error: "Reducer receipt expired", class: "rejected", retryable: false,
    });
    expect(await client.query("q", "__gonvex.reducerReceipt", { idempotencyKey: key, receiptPath: "board.bump" }))
      .toEqual({ type: "query.error", id: "q", path: "__gonvex.reducerReceipt", error: "Reducer receipt expired" });
    const receipts = (await stub("GET", `tables/_gonvex_reducer_idempotency?project=${projectId}&tenant=${tenant}`)).rows;
    expect(receipts).toEqual([expect.objectContaining({ idempotency_key: key, path: "board.bump", result: null })]);
    // 365 days after expiry the tombstone is deleted; the key is new again.
    expect(await prune(tenant, now + window + DAY + 366 * DAY)).toMatchObject({ expired: 0, deleted: 1, done: true });
    expect((await client.call("b4", "board.bump", { id: "counter" }, { idempotencyKey: key })).result).toEqual({ bumps: 2 });
    client.close();
  });

  it("keeps receipts when the window is null (unlimited) or there is no client contract", async () => {
    const { projectId, tenant, accounts } = await seed("k07-unlimited", fixtureManifest(1, { clientContract: { version: 1, offlineMaxAgeMs: null } }));
    const { client } = await member(projectId, tenant, accounts.alice!.email, 1);
    await client.call("seed", "board.add", { id: "counter", title: "counter", position: 1 });
    await client.call("b1", "board.bump", { id: "counter" }, { idempotencyKey: "k07-unlimited-0001", clientContract: 1 });
    expect(await prune(tenant, Date.now() + 10_000 * DAY)).toMatchObject({ expired: 0, deleted: 0, pages: 0, done: true });
    expect((await client.call("b2", "board.bump", { id: "counter" }, { idempotencyKey: "k07-unlimited-0001", clientContract: 1 })).result).toEqual({ bumps: 1 });
    client.close();
  });
});

describe("generations (receipt-upgrade)", () => {
  it("runs the new generation after /dev/sync, refuses a stale artifact, and keeps generation 1 receipts", async () => {
    const { projectId, tenant, accounts } = await seed("k07-upgrade", fixtureManifest(1), ["alice", "bob"]);
    const { client, auth } = await member(projectId, tenant, accounts.alice!.email);
    const v1Hash = auth.artifactHash as string;
    expect((await client.call("g1", "board.generation", {})).result).toEqual({ generation: 1 });
    expect((await client.call("a1", "board.add", { id: "old", title: "before upgrade", position: 1 }, { idempotencyKey: "conformance-upgrade-key-0001", receiptPath: "board.add" })).result)
      .toEqual({ id: "old", title: "before upgrade", position: 1, bumps: 0 });
    await stub("POST", "sync", { ...fixtureManifest(2), project: projectId });
    await client.next((f) => f.type === "system.reload");
    expect((await client.call("g2", "board.generation", {})).result).toEqual({ generation: 2 });
    expect((await client.call("a2", "board.add", { id: "new", title: "after upgrade", position: 2 })).result).toEqual({ id: "new", title: "after upgrade", position: 2, bumps: 0, label: "v2" });
    const stale = await client.call("s", "board.rename", { id: "old", title: "stale client" }, { artifactHash: v1Hash });
    expect(stale).toEqual({
      type: "reducer.error", id: "s", path: "board.rename", class: "update_required", retryable: false,
      error: `STALE_REDUCER_ARTIFACT: expected "${v1Hash}", active artifact is "k07-fixture-v2"`,
    });
    expect((await client.query("q", "__gonvex.reducerReceipt", { idempotencyKey: "conformance-upgrade-key-0001", receiptPath: "board.add" })).result).toMatchObject({ found: true });
    expect((await client.call("a3", "board.add", { id: "old", title: "before upgrade", position: 1 }, { idempotencyKey: "conformance-upgrade-key-0001", receiptPath: "board.add" })).result)
      .toEqual({ id: "old", title: "before upgrade", position: 1, bumps: 0 });
    const rows = (await stub("GET", `tables/board?project=${projectId}&tenant=${tenant}`)).rows;
    expect(rows).toEqual([{ id: "old", title: "before upgrade", position: 1, bumps: 0, label: "v1" }, { id: "new", title: "after upgrade", position: 2, bumps: 0, label: "v2" }]);
    client.close();
  });
});
