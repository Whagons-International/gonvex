import { OPFSReplicaEngine } from "./build/replica.js";
import { opfsFile } from "./build/file.js";
import { schema, records, row, plan } from "./fixture.mjs";

globalThis.onmessage = async ({
  data: { arm = "typescript", rows = 31012 },
}) => {
  const directory = await navigator.storage.getDirectory(),
    scope = "benchmark-" + crypto.randomUUID();
  let engine, sqlite, pool, response;
  const files = [];
  const timings = {},
    samples = [];
  let result,
    fileBytes,
    scratchBytes = 0,
    wasmBytes = 0;
  const measure = async (key, fn) => {
    const at = performance.now();
    const value = await fn();
    timings[key] = performance.now() - at;
    return value;
  };
  const openFile = async (name) => {
    files.push(name);
    const handle = await directory.getFileHandle(name, { create: true });
    return opfsFile(await handle.createSyncAccessHandle());
  };
  try {
    if (arm === "typescript") {
      engine = await measure(
        "openMs",
        async () =>
          new OPFSReplicaEngine(await openFile(scope + ".db"), scope, schema),
      );
      await measure("ingestAndIndexMs", async () => {
        const scratch = await openFile(scope + ".scratch"),
          snapshot = engine.snapshot(scratch);
        for (const record of records(rows)) snapshot.add(record);
        snapshot.commit();
      });
      scratchBytes = (
        await (await directory.getFileHandle(scope + ".scratch")).getFile()
      ).size;
      for (let i = 0; i < 20; i++) {
        const at = performance.now();
        result = engine.page(plan).rows;
        samples.push(performance.now() - at);
      }
      await measure("scanFilterSortMs", () =>
        engine.page({ ...plan, search: undefined }),
      );
      fileBytes = engine.database.bytes;
      engine.close();
      await measure("reopenAndQueryMs", async () => {
        engine = new OPFSReplicaEngine(
          await openFile(scope + ".db"),
          scope,
          schema,
        );
        engine.page(plan);
      });
      engine.close();
      engine = undefined;
    } else {
      sqlite = await measure("moduleMs", async () => {
        const module = await import("./build/sqlite3.mjs");
        return module.default();
      });
      pool = await measure("poolMs", () =>
        sqlite.installOpfsSAHPoolVfs({
          directory: "." + scope,
          initialCapacity: 4,
          preserveOnInitFailure: true,
        }),
      );
      engine = await measure(
        "openMs",
        () => new pool.OpfsSAHPoolDb("/replica.sqlite"),
      );
      engine.exec(
        "PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA temp_store=FILE; PRAGMA cache_size=-1024; CREATE TABLE tasks(_id TEXT PRIMARY KEY,name TEXT,workspace TEXT,due REAL,version REAL,description TEXT)",
      );
      await measure("ingestAndIndexMs", () => {
        engine.exec("BEGIN IMMEDIATE");
        const insert = engine.prepare("INSERT INTO tasks VALUES(?,?,?,?,?,?)");
        try {
          for (let id = 0; id < rows; id++) {
            const r = row(id);
            insert
              .bind([
                r._id,
                r.name,
                r.workspace,
                r.due,
                r.version,
                r.description,
              ])
              .stepReset();
          }
        } finally {
          insert.finalize();
        }
        engine.exec(
          "CREATE INDEX task_workspace_due ON tasks(workspace,due);CREATE INDEX task_due ON tasks(due);CREATE INDEX task_name ON tasks(name);CREATE VIRTUAL TABLE search USING fts5(name,description,content='tasks',content_rowid='rowid',tokenize='trigram',detail='none');INSERT INTO search(search) VALUES('rebuild');COMMIT;",
        );
      });
      const sql =
        "SELECT * FROM tasks WHERE workspace='0' AND due>=30 AND rowid IN (SELECT rowid FROM search WHERE description LIKE '%rare-needle%') ORDER BY due DESC,name ASC,_id ASC LIMIT 50";
      for (let i = 0; i < 20; i++) {
        const at = performance.now();
        result = engine.exec({
          sql,
          rowMode: "object",
          returnValue: "resultRows",
        });
        samples.push(performance.now() - at);
      }
      await measure("scanFilterSortMs", () =>
        engine.exec(
          "SELECT * FROM tasks WHERE workspace='0' AND due>=30 ORDER BY due DESC,name ASC,_id ASC LIMIT 50",
        ),
      );
      fileBytes =
        engine.selectValue("PRAGMA page_count") *
        engine.selectValue("PRAGMA page_size");
      wasmBytes = sqlite.wasm.heap8u().buffer.byteLength;
      engine.close();
      await measure("reopenAndQueryMs", () => {
        engine = new pool.OpfsSAHPoolDb("/replica.sqlite");
        engine.exec(
          "PRAGMA locking_mode=EXCLUSIVE;PRAGMA synchronous=FULL;PRAGMA cache_size=-1024",
        );
        engine.exec({ sql, rowMode: "object", returnValue: "resultRows" });
      });
      engine.close();
      engine = undefined;
    }
    const expected = [];
    for (let id = 0; id < rows; id++)
      if (id % 10 === 0 && id % 365 >= 30 && id % 997 === 0)
        expected.push(row(id));
    expected.sort(
      (a, b) =>
        b.due - a.due ||
        (a.name < b.name ? -1 : a.name > b.name ? 1 : 0) ||
        (a._id < b._id ? -1 : 1),
    );
    if (
      JSON.stringify(result.map((x) => x._id)) !==
      JSON.stringify(expected.slice(0, 50).map((x) => x._id))
    )
      throw Error("Combined result mismatch");
    samples.sort((a, b) => a - b);
    response = {
      arm,
      rows,
      environment: { userAgent: navigator.userAgent },
      timings: {
        ...timings,
        combinedP50Ms: samples[10],
        combinedP95Ms: samples[19],
      },
      fileBytes,
      scratchBytes,
      wasmBytes,
      correct: true,
      limits:
        "Synthetic dedicated-worker OPFS component test. No application/network/real-tenant timings or whole-browser memory claim.",
    };
  } catch (error) {
    response = { error: String(error.stack ?? error) };
  } finally {
    try {
      engine?.close();
    } catch {}
    if (pool) {
      try {
        await pool.removeVfs();
      } catch {}
      try {
        await directory.removeEntry("." + scope, { recursive: true });
      } catch {}
    }
    for (const name of new Set(files))
      try {
        await directory.removeEntry(name);
      } catch {}
  }
  globalThis.postMessage(response);
};
