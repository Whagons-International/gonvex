import assert from "node:assert/strict";
import test from "node:test";
import { reducer, schema, reducerIdGenerator, reducerToken } from "../dist/index.js";

test("a reducer defaults to local execution without an optimistic definition", async () => {
  const create = reducer({ args: schema.any(), result: schema.any(), run: (ctx) => ctx.db.insert("items", { name: "created" }) });
  assert.deepEqual(create.options.offline, { mode: "allowed" });
  const context = {
    auth: { account: { id: "a" } }, tenant: { id: "t" }, member: { id: "m" },
    invocation: { commandId: "c" }, now: 1,
    db: { insert: async (_table, row, allocation) => ({ _id: allocation.generatedId, ...row }) },
  };
  const local = await create.handler(context, {});
  const server = await create.handler({ ...context, now: 200 }, {});
  assert.equal(local._id, server._id);
  assert.match(local._id, /^[0-9a-f-]{36}$/);
  assert.notEqual((await create.handler({ ...context, invocation: { commandId: "other" } }, {}))._id, local._id);
});

test("generated row IDs remain stable across unrelated audit inserts", async () => {
  const create = reducer({ run: async (ctx, args) => {
    if (args.log) await ctx.db.insert("logs", { message: "audit" });
    return ctx.db.insert("items", { name: "created" });
  } });
  const context = { auth: { account: { id: "a" } }, tenant: { id: "t" }, invocation: { commandId: "c" },
    db: { insert: async (_table, row, allocation) => ({ _id: allocation.generatedId, ...row }) } };
  assert.equal((await create.handler(context, { log: true }))._id, (await create.handler(context, { log: false }))._id);
});

test("allocation leaves primary-key selection to the host and preserves explicit IDs", async () => {
  const create = reducer({ run: ctx => ctx.db.insert("messages", { id: "explicit", body: "hello" }) });
  const context = { auth: { account: { id: "a" } }, tenant: { id: "t" }, invocation: { commandId: "c" },
    db: { insert: async (_table, row, allocation) => ({ id: allocation.generatedId, ...row }) } };
  assert.deepEqual(await create.handler(context, {}), { id: "explicit", body: "hello" });
});

test("nested entity ID generators replay in order and separate intents and domains", async () => {
  const context = { auth: { account: { id: "a" } }, tenant: { id: "t" }, invocation: { commandId: "c" } };
  const local = await reducerIdGenerator(context, "slides");
  const server = await reducerIdGenerator(context, "slides");
  const first = local();
  assert.equal(first, server());
  const second = local();
  assert.equal(second, server());
  assert.notEqual(first, second);
  assert.notEqual(first, (await reducerIdGenerator(context, "sheets"))());
  assert.notEqual(first, (await reducerIdGenerator({ ...context, invocation: { commandId: "other" } }, "slides"))());
  await assert.rejects(reducerIdGenerator({ ...context, invocation: {} }, "slides"), /commandId/);
});

test("secret tokens replay using private intent entropy, not public command IDs", async () => {
  const context = { tenant: { id: "tenant" }, invocation: { commandId: "public-command" }, intentEntropy: "ab".repeat(32) };
  const token = await reducerToken(context, "share");
  assert.match(token, /^[0-9a-f]{64}$/);
  assert.equal(await reducerToken({ ...context }, "share"), token);
  assert.notEqual(await reducerToken({ ...context, intentEntropy: "cd".repeat(32) }, "share"), token);
  assert.notEqual(await reducerToken(context, "delivery"), token);
  await assert.rejects(reducerToken({ ...context, intentEntropy: "bad" }, "share"), /Invalid reducer entropy/);
});
