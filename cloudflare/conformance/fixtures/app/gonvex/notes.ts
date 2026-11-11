import { reducer, replicaCollection, schema, visibility } from "@gonvex/module-sdk";

/** A note is visible to the Member who owns it and to Members it is shared with. */
export const notesVisibility = visibility({
  table: "notes",
  key: "id",
  sets: {
    shared: {
      table: "note_shares",
      select: "note_id",
      joins: [],
      where: [{ table: "note_shares", column: "member_id", context: "member.id" }],
    },
  },
  where: {
    operator: "or",
    children: [
      { operator: "eqContext", column: "owner_id", context: "member.id" },
      { operator: "inSet", column: "id", set: "shared" },
    ],
  },
});

/** A share row is visible to the Member it names. */
export const noteSharesVisibility = visibility({
  table: "note_shares",
  key: "id",
  sets: {},
  where: { operator: "eqContext", column: "member_id", context: "member.id" },
});

export const mine = replicaCollection({
  args: schema.object({}),
  result: schema.array(schema.object({ id: schema.string(), owner_id: schema.string(), body: schema.string(), version: schema.integer() })),
  replica: { table: "notes", key: "id", columns: ["id", "owner_id", "body", "version"], orderBy: "id", orderDirection: "asc", mode: "eager", maxRows: 1000 },
});

/** Creates or rewrites the caller's note; version counts applications. */
export const write = reducer({
  args: schema.object({ id: schema.string(), body: schema.string() }),
  result: schema.object({ id: schema.string(), version: schema.integer() }),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db, member }, args) => {
    if (!member) throw new Error("tenant member required");
    const [current] = await db.query<{ owner_id: string; version: number }>("SELECT owner_id, version FROM notes WHERE id = ?", [args.id]);
    if (current && current.owner_id !== member.id) throw new Error("note belongs to another member");
    if (current) {
      await db.update("notes", args.id, { body: args.body, version: current.version + 1 });
      return { id: args.id, version: current.version + 1 };
    }
    await db.insert("notes", { id: args.id, owner_id: member.id, body: args.body, version: 1 });
    return { id: args.id, version: 1 };
  },
});

/** Shares the caller's note with another Member. */
export const share = reducer({
  args: schema.object({ id: schema.string(), noteId: schema.string(), memberId: schema.string() }),
  result: schema.object({ id: schema.string() }),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db, member }, args) => {
    if (!member) throw new Error("tenant member required");
    const [note] = await db.query<{ owner_id: string }>("SELECT owner_id FROM notes WHERE id = ?", [args.noteId]);
    if (!note || note.owner_id !== member.id) throw new Error("only the owner can share a note");
    await db.insert("note_shares", { id: args.id, note_id: args.noteId, member_id: args.memberId });
    return { id: args.id };
  },
});

export const unshare = reducer({
  args: schema.object({ id: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => { await db.delete("note_shares", args.id); return null; },
});
