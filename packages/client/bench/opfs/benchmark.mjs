import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { DatabaseSync } from "node:sqlite";
import { OPFSReplicaEngine } from "./build/replica.js";
import { nativeFile } from "./files.mjs";
import { schema, records, row, plan } from "./fixture.mjs";

const arm = process.argv[2] ?? "typescript",
  count = Number(process.argv[3] ?? 31012);
if (
  !["typescript", "sqlite"].includes(arm) ||
  !Number.isSafeInteger(count) ||
  count < 1
)
  throw Error("benchmark.mjs typescript|sqlite row-count");
const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gonvex-db-bench-")),
  file = path.join(dir, "replica.db");
const timings = {};
let result,
  bytes,
  scratchBytes = 0,
  engine;
const samples = [];
const measure = (key, fn) => {
  const at = performance.now();
  const result = fn();
  timings[key] = performance.now() - at;
  return result;
};
try {
  if (arm === "typescript") {
    engine = measure(
      "createMs",
      () =>
        new OPFSReplicaEngine(nativeFile(file), "synthetic-benchmark", schema),
    );
    measure("ingestAndIndexMs", () => {
      const snapshot = engine.snapshot(nativeFile(path.join(dir, "scratch")));
      for (const record of records(count)) snapshot.add(record);
      snapshot.commit();
    });
    scratchBytes = fs.statSync(path.join(dir, "scratch")).size;
    for (let i = 0; i < 20; i++) {
      const at = performance.now();
      result = engine.page(plan).rows;
      samples.push(performance.now() - at);
    }
    bytes = engine.database.bytes;
    engine.close();
    measure("reopenAndQueryMs", () => {
      engine = new OPFSReplicaEngine(
        nativeFile(file),
        "synthetic-benchmark",
        schema,
      );
      engine.page(plan);
    });
    measure("scanFilterSortMs", () =>
      engine.page({ ...plan, search: undefined }),
    );
    engine.close();
  } else {
    engine = measure("createMs", () => {
      const db = new DatabaseSync(file);
      db.exec(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE tasks(_id TEXT PRIMARY KEY,name TEXT,workspace TEXT,due REAL,version REAL,description TEXT)",
      );
      return db;
    });
    measure("ingestAndIndexMs", () => {
      engine.exec("BEGIN IMMEDIATE");
      const insert = engine.prepare("INSERT INTO tasks VALUES(?,?,?,?,?,?)");
      for (let id = 0; id < count; id++) {
        const r = row(id);
        insert.run(r._id, r.name, r.workspace, r.due, r.version, r.description);
      }
      engine.exec(
        "CREATE INDEX task_workspace_due ON tasks(workspace,due); CREATE INDEX task_due ON tasks(due); CREATE INDEX task_name ON tasks(name); CREATE VIRTUAL TABLE search USING fts5(name,description,content='tasks',content_rowid='rowid',tokenize='trigram',detail='none'); INSERT INTO search(search) VALUES('rebuild'); COMMIT;",
      );
    });
    const sql =
      "SELECT * FROM tasks WHERE workspace='0' AND due>=30 AND rowid IN (SELECT rowid FROM search WHERE description LIKE '%rare-needle%') ORDER BY due DESC,name ASC,_id ASC LIMIT 50";
    for (let i = 0; i < 20; i++) {
      const at = performance.now();
      result = engine.prepare(sql).all();
      samples.push(performance.now() - at);
    }
    engine.close();
    bytes = fs.statSync(file).size;
    measure("reopenAndQueryMs", () => {
      engine = new DatabaseSync(file);
      engine.prepare(sql).all();
    });
    measure("scanFilterSortMs", () =>
      engine
        .prepare(
          "SELECT * FROM tasks WHERE workspace='0' AND due>=30 ORDER BY due DESC,name ASC,_id ASC LIMIT 50",
        )
        .all(),
    );
    engine.close();
  }
  const expected = [...Array(count).keys()]
    .filter((id) => id % 10 === 0 && id % 365 >= 30 && id % 997 === 0)
    .map(row)
    .sort(
      (a, b) =>
        b.due - a.due ||
        (a.name < b.name ? -1 : a.name > b.name ? 1 : 0) ||
        (a._id < b._id ? -1 : 1),
    )
    .slice(0, 50);
  if (
    JSON.stringify(result.map((x) => x._id)) !==
    JSON.stringify(expected.map((x) => x._id))
  )
    throw Error("Result mismatch");
  samples.sort((a, b) => a - b);
  process.stdout.write(
    JSON.stringify(
      {
        arm,
        rows: count,
        environment: {
          node: process.version,
          platform: process.platform,
          arch: process.arch,
        },
        timings: {
          ...timings,
          combinedP50Ms: samples[10],
          combinedP95Ms: samples[19],
        },
        fileBytes: bytes,
        scratchBytes,
        processMemory: process.memoryUsage(),
        maxRSSKiB: process.resourceUsage().maxRSS,
        correct: true,
        limits:
          "Native component test. Not OPFS, app startup, whole-browser memory, network, owner failover or real-tenant measurement.",
      },
      null,
      2,
    ) + "\n",
  );
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
