import { test } from "node:test";
import assert from "node:assert/strict";
import { ReplicaDatabase } from "./build/database.js";
import { OPFSReplicaEngine } from "./build/replica.js";
import { crc32, encode, decode } from "./build/codec.js";
import { MemoryFile } from "./files.mjs";
import { schema, records, plan } from "./fixture.mjs";
import { DatabaseSync } from "node:sqlite";

test("invalid staged writes abort the whole transaction and internal index metadata is reserved", () => {
  const db = new ReplicaDatabase(new MemoryFile(), "scope"),
    tx = db.begin();
  tx.put("earlier", bytes("uncommitted"));
  assert.throws(() => tx.stageValue(new Uint8Array(262145)), /budget/);
  assert.throws(() => tx.commit(), /aborted/);
  tx.abort();
  assert.equal(db.get("earlier"), undefined);
  const engine = new OPFSReplicaEngine(new MemoryFile(), "scope", schema);
  assert.throws(
    () =>
      engine.apply(
        [],
        [{ key: "m/indexes", value: { tasks: { valid: [true] } } }],
      ),
    /metadata/,
  );
});
const bytes = (text) => encode(String(text));
const entries = (db) =>
  [...db.range()].map(({ key, value }) => [key, decode(value)]);
const load = (engine, rows, budget) => {
  const snapshot = engine.snapshot(new MemoryFile(), budget);
  for (const row of rows) snapshot.add(row);
  snapshot.commit();
};

test("CRC32 uses the standard check vector", () =>
  assert.equal(crc32(encode("123456789")), 0xcbf43926));
test("non-finite JSON and array query values are refused before execution", () => {
  const engine = new OPFSReplicaEngine(new MemoryFile(), "scope", {
    tasks: { key: "_id", fields: { _id: "text", data: "json" } },
  });
  const query = {
    table: "tasks",
    order: [{ field: "_id", direction: "asc" }],
    textOrder: "binary",
    limit: 1,
  };
  assert.throws(
    () =>
      engine.page({
        ...query,
        where: { op: "jsonEq", field: "data", value: { bad: Infinity } },
      }),
    /JSON/,
  );
  assert.throws(
    () =>
      engine.page({
        ...query,
        where: { op: "arrayContains", field: "data", value: NaN },
      }),
    /JSON/,
  );
});
test("bulk tree, arbitrary deltas, deletion and bounded ranges match an independent map", () => {
  const db = new ReplicaDatabase(new MemoryFile(), "test"),
    model = new Map();
  let tx = db.begin();
  tx.bulkReplace(
    Array.from({ length: 1500 }, (_, i) => {
      const key = `k${String(i).padStart(5, "0")}`,
        value = bytes(i);
      model.set(key, String(i));
      return { key, value };
    }),
  );
  tx.commit();
  let seed = 1337;
  const random = () => (seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0);
  for (let round = 0; round < 10; round++) {
    tx = db.begin();
    for (let i = 0; i < 30; i++) {
      const key = `k${String(random() % 1800).padStart(5, "0")}`;
      if (random() % 3 === 0) {
        tx.delete(key);
        model.delete(key);
      } else {
        const value = String(random());
        tx.put(key, bytes(value));
        model.set(key, value);
      }
    }
    tx.commit();
  }
  assert.equal(db.verify(), model.size);
  const expected = [...model].sort(([a], [b]) => a.localeCompare(b));
  assert.deepEqual(entries(db), expected);
  for (const key of ["k00000", "k00378", "k01799", "absent"])
    assert.equal(db.get(key) && decode(db.get(key)), model.get(key));
  assert.deepEqual(
    [...db.range("k00500", "k00600")].map((x) => x.key),
    expected.map((x) => x[0]).filter((x) => x >= "k00500" && x < "k00600"),
  );
});
test("wide keys split by encoded bytes and survive a reopen", () => {
  const file = new MemoryFile(),
    db = new ReplicaDatabase(file, "scope");
  const tx = db.begin();
  for (let i = 0; i < 70; i++)
    tx.put("x".repeat(i % 2 ? 900 : 10) + String(i).padStart(4, "0"), bytes(i));
  tx.commit();
  const reopen = new ReplicaDatabase(file.crash(), "scope");
  assert.equal(reopen.verify(), 70);
});
test("aborted and invalid transactions never expose partial rows", () => {
  const db = new ReplicaDatabase(new MemoryFile(), "scope");
  let tx = db.begin();
  tx.put("one", bytes("old"));
  tx.commit();
  tx = db.begin();
  tx.put("one", bytes("new"));
  assert.equal(decode(db.get("one")), "old");
  assert.throws(() => tx.put("", bytes("invalid")));
  assert.throws(() => tx.commit());
  tx.abort();
  tx = db.begin();
  tx.put("two", bytes("ok"));
  tx.commit();
  assert.deepEqual(entries(db), [
    ["one", "old"],
    ["two", "ok"],
  ]);
});
test("every injected I/O boundary and torn header recovers an entire old or new transaction", () => {
  const original = new MemoryFile();
  const before = new ReplicaDatabase(original, "scope");
  let tx = before.begin();
  tx.put("a", bytes("old"));
  tx.put("b", bytes("old"));
  tx.commit();
  const baseline = original.durable;
  const countFile = new MemoryFile(baseline),
    countDB = new ReplicaDatabase(countFile, "scope");
  countFile.operations = 0;
  tx = countDB.begin();
  tx.put("a", bytes("new"));
  tx.put("b", bytes("new"));
  tx.put("outbox", bytes("intent"));
  tx.commit();
  const count = countFile.operations;
  for (let failAt = 1; failAt <= count; failAt++)
    for (const failBytes of [0, 7, 48, 4095, 4096, 1000000]) {
      const file = new MemoryFile(baseline),
        db = new ReplicaDatabase(file, "scope");
      file.operations = 0;
      file.failAt = failAt;
      file.failBytes = failBytes;
      let operation;
      try {
        operation = db.begin();
        operation.put("a", bytes("new"));
        operation.put("b", bytes("new"));
        operation.put("outbox", bytes("intent"));
        operation.commit();
      } catch {
        operation?.abort();
      }
      const recovered = new ReplicaDatabase(file.crash(), "scope"),
        result = entries(recovered);
      assert.ok(
        JSON.stringify(result) ===
          JSON.stringify([
            ["a", "old"],
            ["b", "old"],
          ]) ||
          JSON.stringify(result) ===
            JSON.stringify([
              ["a", "new"],
              ["b", "new"],
              ["outbox", "intent"],
            ]),
        `${failAt}/${failBytes}: ${JSON.stringify(result)}`,
      );
      recovered.verify();
    }
});
test("acknowledged commit survives crash and corrupt committed bodies fail closed", () => {
  const file = new MemoryFile(),
    db = new ReplicaDatabase(file, "scope");
  const tx = db.begin();
  tx.put("row", bytes("payload"));
  tx.commit();
  assert.equal(
    decode(new ReplicaDatabase(file.crash(), "scope").get("row")),
    "payload",
  );
  assert.throws(
    () => new ReplicaDatabase(file.crash(), "another scope"),
    /scope/,
  );
  const corrupt = file.crash();
  corrupt.data[8204] ^= 1;
  corrupt.durable = corrupt.data.slice();
  const reopened = new ReplicaDatabase(corrupt, "scope");
  assert.throws(() => reopened.get("row"), /checksum/);
});
test("a 3100-row combined query uses trigrams and equals an independent filter and sort", () => {
  const engine = new OPFSReplicaEngine(new MemoryFile(), "test", schema);
  const rows = [...records(3100)];
  load(engine, rows, 512 * 1024);
  const result = engine.page(plan);
  const expected = rows
    .map((x) => x.row)
    .filter(
      (x) =>
        x.workspace === "0" &&
        x.due >= 30 &&
        x.description.includes("rare-needle"),
    )
    .sort(
      (a, b) =>
        b.due - a.due ||
        (a.name < b.name ? -1 : a.name > b.name ? 1 : 0) ||
        (a._id < b._id ? -1 : 1),
    )
    .slice(0, 50);
  assert.equal(result.access, "trigram");
  assert.deepEqual(result.rows, expected);
  assert.ok(result.examined < 100);
  assert.equal(engine.database.verify() > 3100, true);
});
test("exact substring matching handles short text, Unicode and literal wildcard characters", () => {
  const engine = new OPFSReplicaEngine(new MemoryFile(), "test", schema);
  const rows = [
    "*?[abc]%",
    "東京 café 🔧",
    "Straße",
    "İstanbul",
    "a\0bc",
    "rare-needle",
  ];
  load(
    engine,
    rows.map((name, i) => ({
      table: "tasks",
      id: String(i),
      row: { _id: String(i), name },
    })),
  );
  for (const search of [
    "*?",
    "?[abc]",
    "%",
    "東京",
    "café",
    "🔧",
    "İS",
    "a\0b",
    "rare",
    "not-found",
  ]) {
    const result = engine.page({
      table: "tasks",
      order: [{ field: "name", direction: "asc" }],
      textOrder: "binary",
      search,
      limit: 50,
    });
    assert.deepEqual(
      result.rows.map((x) => x.name).sort(),
      rows.filter((x) => x.toLowerCase().includes(search.toLowerCase())).sort(),
      search,
    );
  }
});
test("stale trigram postings merge changed rows exactly, including deletes and new matches", () => {
  const engine = new OPFSReplicaEngine(new MemoryFile(), "test", schema);
  load(engine, [
    { table: "tasks", id: "a", row: { _id: "a", name: "rare-needle" } },
    { table: "tasks", id: "b", row: { _id: "b", name: "other" } },
  ]);
  engine.apply(
    [
      { table: "tasks", id: "a", row: { _id: "a", name: "gone" } },
      { table: "tasks", id: "b", row: { _id: "b", name: "rare-needle" } },
      { table: "tasks", id: "c", row: { _id: "c", name: "rare-needle" } },
    ],
    [{ key: "o/intent", value: { id: "immutable-key" } }],
  );
  const query = {
    table: "tasks",
    order: [{ field: "name", direction: "asc" }],
    textOrder: "binary",
    search: "rare-needle",
    limit: 50,
  };
  assert.deepEqual(
    engine.page(query).rows.map((x) => x._id),
    ["b", "c"],
  );
  engine.apply([{ table: "tasks", id: "b", row: null }]);
  assert.deepEqual(
    engine.page(query).rows.map((x) => x._id),
    ["c"],
  );
  load(engine, [
    { table: "tasks", id: "z", row: { _id: "z", name: "rare-needle" } },
  ]);
  assert.deepEqual(
    engine.page(query).rows.map((x) => x._id),
    ["z"],
  );
  assert.deepEqual(engine.metadata("o/intent"), { id: "immutable-key" });
});
test("nullable filters preserve SQL UNKNOWN through NOT and stable multi-sort pagination", () => {
  const engine = new OPFSReplicaEngine(new MemoryFile(), "test", schema);
  load(
    engine,
    [null, -1, 2, 2, 7, null].map((due, i) => ({
      table: "tasks",
      id: String(i),
      row: { _id: String(i), name: i % 2 ? "a" : "b", due },
    })),
  );
  const query = {
    table: "tasks",
    where: { op: "not", child: { op: "gte", field: "due", value: 2 } },
    order: [{ field: "due", direction: "asc" }],
    limit: 2,
  };
  assert.deepEqual(
    engine.page(query).rows.map((x) => x._id),
    ["1"],
  );
  const all = {
    table: "tasks",
    order: [
      { field: "due", direction: "desc" },
      { field: "name", direction: "asc" },
    ],
    textOrder: "binary",
    limit: 2,
  };
  let cursor,
    ids = [];
  do {
    const page = engine.page({ ...all, after: cursor });
    ids.push(...page.rows.map((x) => x._id));
    cursor = page.next;
  } while (cursor);
  assert.deepEqual(ids, ["5", "0", "4", "3", "2", "1"]);
  const old = engine.page(all).next;
  assert.throws(
    () => engine.page({ ...all, search: "abc", after: old }),
    /cursor/,
  );
  engine.apply([{ table: "tasks", id: "new", row: { _id: "new", name: "n" } }]);
  assert.throws(() => engine.page({ ...all, after: old }), /cursor/);
});
test("JSON, array, relation and nested AND/OR filters operate together", () => {
  const shape = {
    tasks: {
      key: "_id",
      fields: { _id: "text", name: "text", tags: "json", workspace: "text" },
      search: ["name"],
    },
    workspaces: { key: "_id", fields: { _id: "text", active: "boolean" } },
  };
  const engine = new OPFSReplicaEngine(new MemoryFile(), "scope", shape);
  load(engine, [
    {
      table: "tasks",
      id: "a",
      row: { _id: "a", name: "needle", tags: [1, true], workspace: "w" },
    },
    {
      table: "tasks",
      id: "b",
      row: { _id: "b", name: "needle", tags: [true], workspace: "x" },
    },
    { table: "workspaces", id: "w", row: { _id: "w", active: true } },
  ]);
  const query = {
    table: "tasks",
    where: {
      op: "and",
      children: [
        { op: "arrayContains", field: "tags", value: 1 },
        {
          op: "relation",
          field: "workspace",
          table: "workspaces",
          relatedField: "_id",
          where: { op: "eq", field: "active", value: true },
        },
      ],
    },
    search: "needle",
    order: [{ field: "name", direction: "asc" }],
    textOrder: "binary",
    limit: 10,
  };
  assert.deepEqual(
    engine.page(query).rows.map((x) => x._id),
    ["a"],
  );
});
test("invalid rows/plans and exhausted budgets refuse without publishing partial state", () => {
  const engine = new OPFSReplicaEngine(new MemoryFile(), "scope", schema);
  load(engine, [{ table: "tasks", id: "a", row: { _id: "a", name: "abc" } }]);
  const sequence = engine.database.sequence;
  assert.throws(() =>
    engine.apply([{ table: "tasks", id: "b", row: { _id: "WRONG" } }]),
  );
  assert.equal(engine.database.sequence, sequence);
  const query = {
    table: "tasks",
    order: [{ field: "name", direction: "asc" }],
    textOrder: "binary",
    limit: 2,
  };
  assert.throws(() => engine.page(query, { examined: 0 }), /budget/);
  assert.throws(() => engine.page(query, { pageBytes: 1 }), /budget/);
  assert.throws(
    () => engine.page({ ...query, membership: { signature: "unsafe" } }),
    /Unsupported/,
  );
  assert.throws(() =>
    engine.page({
      ...query,
      where: { op: "eq", field: "due", value: "wrong" },
    }),
  );
  assert.throws(() =>
    engine.apply([{ table: "tasks", id: "b", row: { _id: "b", due: NaN } }]),
  );
  const snapshot = engine.snapshot(new MemoryFile());
  snapshot.add({ table: "tasks", id: "dup", row: { _id: "dup" } });
  snapshot.add({
    table: "tasks",
    id: "dup",
    row: { _id: "dup", name: "conflict" },
  });
  assert.throws(() => snapshot.commit(), /unique/);
  assert.equal(engine.entity("tasks", "a").name, "abc");
});
test("immutable intent receipts survive crash and snapshot replacement without duplicate effects", () => {
  const file = new MemoryFile(),
    engine = new OPFSReplicaEngine(file, "scope", schema),
    changes = [
      { table: "tasks", id: "a", row: { _id: "a", name: "offline edit" } },
    ];
  const first = engine.acceptIntent(
    "intent-1",
    { path: "tasks.edit", args: { name: "offline edit" } },
    changes,
  );
  const reopened = new OPFSReplicaEngine(file.crash(), "scope", schema),
    before = reopened.database.sequence;
  assert.deepEqual(
    reopened.acceptIntent(
      "intent-1",
      { args: { name: "offline edit" }, path: "tasks.edit" },
      changes,
    ),
    { sequence: first.sequence, duplicate: true },
  );
  assert.equal(reopened.database.sequence, before);
  assert.throws(
    () => reopened.acceptIntent("intent-1", { path: "different" }, changes),
    /different payload/,
  );
  load(reopened, [
    {
      table: "tasks",
      id: "server",
      row: { _id: "server", name: "server snapshot" },
    },
  ]);
  assert.equal(
    reopened.acceptIntent(
      "intent-1",
      { path: "tasks.edit", args: { name: "offline edit" } },
      changes,
    ).duplicate,
    true,
  );
  assert.throws(
    () =>
      engine.apply([{ table: "tasks", id: "\ud800", row: { _id: "\ud800" } }]),
    /identity/,
  );
});
test("indexed keyset pages seek rather than rereading preceding pages and merge dirty rows", () => {
  const engine = new OPFSReplicaEngine(new MemoryFile(), "scope", schema);
  load(engine, records(1000));
  const query = {
    table: "tasks",
    order: [{ field: "due", direction: "asc" }],
    limit: 7,
  };
  let after;
  for (let i = 0; i < 30; i++) {
    const page = engine.page({ ...query, after });
    assert.equal(page.access, "index");
    assert.ok(page.examined < 40);
    after = page.next;
  }
  engine.apply([
    {
      table: "tasks",
      id: "early",
      row: { _id: "early", name: "n", due: -100 },
    },
    { table: "tasks", id: "0", row: { _id: "0", name: "moved", due: 5000 } },
  ]);
  assert.equal(engine.page(query).rows[0]._id, "early");
  assert.equal(
    engine.page(query).rows.some((x) => x._id === "0"),
    false,
  );
});
test("oversized index entries invalidate only the acceleration, preserving exact results", () => {
  const engine = new OPFSReplicaEngine(new MemoryFile(), "scope", schema),
    name = "long-".repeat(300);
  load(engine, [
    { table: "tasks", id: "a", row: { _id: "a", name } },
    { table: "tasks", id: "b", row: { _id: "b", name: "short" } },
  ]);
  const page = engine.page({
    table: "tasks",
    where: { op: "eq", field: "name", value: name },
    order: [{ field: "name", direction: "asc" }],
    textOrder: "binary",
    limit: 10,
  });
  assert.equal(page.access, "scan");
  assert.deepEqual(
    page.rows.map((x) => x._id),
    ["a"],
  );
});
test("100 generated combined plans agree with SQLite, including nullable logic and pagination", () => {
  const engine = new OPFSReplicaEngine(new MemoryFile(), "scope", schema),
    sql = new DatabaseSync(":memory:");
  sql.exec(
    "CREATE TABLE tasks(_id TEXT,name TEXT,workspace TEXT,due REAL,version REAL,description TEXT,body TEXT)",
  );
  const insert = sql.prepare("INSERT INTO tasks VALUES(?,?,?,?,?,?,?)");
  const rows = Array.from({ length: 180 }, (_, i) => {
    const row = {
      _id: String(i),
      name: (i % 3 ? "Task " : "needle ") + String(i % 19),
      workspace: String(i % 5),
      due: i % 7 === 0 ? null : (i % 21) - 10,
      description: i % 11 ? "plain" : "another needle",
    };
    insert.run(
      row._id,
      row.name,
      row.workspace,
      row.due,
      null,
      row.description,
      JSON.stringify(row),
    );
    return { table: "tasks", id: row._id, row };
  });
  load(engine, rows);
  let seed = 123456;
  const random = () => (seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0);
  const leaf = () => {
    const op = ["eq", "ne", "lt", "lte", "gt", "gte"][random() % 6],
      field = random() % 3 ? "due" : "workspace";
    return {
      op,
      field,
      value:
        field === "due"
          ? random() % 5
            ? (random() % 21) - 10
            : null
          : String(random() % 5),
    };
  };
  const predicate = (depth = 0) => {
    if (depth >= 2 || random() % 3 === 0) return leaf();
    if (random() % 4 === 0) return { op: "not", child: predicate(depth + 1) };
    return {
      op: random() % 2 ? "and" : "or",
      children: [predicate(depth + 1), predicate(depth + 1)],
    };
  };
  const compile = (p, bindings) => {
    if (p.op === "and" || p.op === "or")
      return (
        "(" +
        p.children
          .map((x) => compile(x, bindings))
          .join(p.op === "and" ? " AND " : " OR ") +
        ")"
      );
    if (p.op === "not") return "(NOT " + compile(p.child, bindings) + ")";
    if (p.value === null && ["eq", "ne"].includes(p.op))
      return `"${p.field}" IS ${p.op === "ne" ? "NOT " : ""}NULL`;
    bindings.push(p.value);
    return `"${p.field}" ${{ eq: "=", ne: "<>", lt: "<", lte: "<=", gt: ">", gte: ">=" }[p.op]} ?`;
  };
  for (let i = 0; i < 100; i++) {
    const where = predicate(),
      search = ["", "needle", "ne", "absent"][random() % 4],
      direction = random() % 2 ? "asc" : "desc",
      query = {
        table: "tasks",
        where,
        search,
        order: [
          { field: "due", direction },
          { field: "name", direction: "asc" },
        ],
        textOrder: "binary",
        limit: 13,
      };
    const bindings = [];
    let expression = compile(where, bindings);
    if (search) {
      expression +=
        " AND (instr(lower(name),?)>0 OR instr(lower(description),?)>0)";
      bindings.push(search, search);
    }
    const expected = sql
      .prepare(
        `SELECT body FROM tasks WHERE ${expression} ORDER BY due ${direction} NULLS ${direction === "asc" ? "LAST" : "FIRST"},name ASC,_id ASC`,
      )
      .all(...bindings)
      .map((x) => JSON.parse(x.body)._id);
    const actual = [];
    let after;
    do {
      const page = engine.page({ ...query, after });
      actual.push(...page.rows.map((x) => x._id));
      after = page.next;
    } while (after);
    assert.deepEqual(actual, expected, JSON.stringify(query));
  }
  sql.close();
});
test("scratch corruption cannot be sealed into the committed replica", () => {
  const engine = new OPFSReplicaEngine(new MemoryFile(), "scope", schema);
  load(engine, [
    { table: "tasks", id: "old", row: { _id: "old", name: "old" } },
  ]);
  const scratch = new MemoryFile(),
    snapshot = engine.snapshot(scratch, 512 * 1024);
  for (const record of records(2000)) snapshot.add(record);
  assert.ok(scratch.data.length > 20);
  scratch.data[20] ^= 1;
  assert.throws(() => snapshot.commit(), /Corrupt/);
  assert.equal(engine.entity("tasks", "old").name, "old");
});
