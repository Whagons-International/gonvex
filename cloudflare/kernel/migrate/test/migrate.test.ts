// K02 against real Durable Object SQLite in workerd: release plans, chunked
// apply with resume, the ledger, directives, restore points.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  buildReleasePlan,
  MemoryPlanStore,
  migrationsFromFiles,
  parseMigration,
  PROGRESS_KEY,
  RECORD_STASH,
  TenantMigrator,
  type MigratorOptions,
  type PlanStore,
  type Progress,
  type ReleasePlan,
} from "../index.ts";
import { APP, fakeCapture } from "./fixtures.ts";

let objects = 0;
async function withObject<T>(fn: (state: DurableObjectState) => Promise<T>): Promise<T> {
  objects += 1;
  const stub = env.HELLO.getByName(`k02-test-${objects}-${Math.random()}`);
  return runInDurableObject(stub, (_instance, state) => fn(state));
}

const small = { chunk: { maxStatements: 3 } };
const plan = async (files: Record<string, string> = APP, options = small) => buildReleasePlan(await migrationsFromFiles(files), options);

/**
 * The object's storage with the alarm and point-in-time recovery recorded instead of
 * performed: the test object has no alarm handler (the tests drive `alarm()` themselves),
 * and local workerd has no point-in-time recovery (the platform run covers it).
 */
function testStorage(state: DurableObjectState) {
  const alarms: Array<number | null> = [];
  const restores: string[] = [];
  const storage = new Proxy(state.storage, {
    get(target, key) {
      if (key === "setAlarm") return async (t: number) => void alarms.push(t);
      if (key === "deleteAlarm") return async () => void alarms.push(null);
      if (key === "onNextSessionRestoreBookmark") return async (b: string) => (restores.push(b), b);
      const value = Reflect.get(target, key);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  return { storage, alarms, restores };
}

function migrator(state: DurableObjectState, plans: PlanStore, extra: Partial<MigratorOptions> = {}) {
  const aborts: string[] = [];
  const capture = fakeCapture();
  const { storage, alarms, restores } = testStorage(state);
  const m = new TenantMigrator({ storage, plans, capture, abort: (r) => aborts.push(r), budget: { maxChunksPerEvent: 1 }, ...extra });
  return { m, aborts, capture, alarms, restores };
}

/** Runs alarm slices until the migrator is done or failed (the tests' stand-in for the platform's alarm). */
async function drain(m: TenantMigrator, limit = 500) {
  for (let i = 0; i < limit; i += 1) {
    const s = await m.alarm();
    if (s.state !== "running") return s;
  }
  throw new Error("did not finish");
}

const schema = (sql: SqlStorage) =>
  sql.exec<{ type: string; name: string; sql: string | null }>("SELECT type, name, sql FROM sqlite_schema WHERE name NOT LIKE '\\_cf\\_%' ESCAPE '\\' ORDER BY type, name").toArray();

describe("migration files (modules.rs rules)", () => {
  it("parses directives, checksums and names as Rust does", async () => {
    const m = await parseMigration("0001_x.sql", "\r\n-- comment\r\n--   gonvex:scope   control-plane  \r\n-- gonvex:no-transaction\r\nSELECT 1;\n-- gonvex:foreign-key-rebuild\n");
    expect(m).toMatchObject({ scope: "control-plane", noTransaction: true, foreignKeyRebuild: true });
    expect((await parseMigration("0002_y.sql", "-- gonvex:scope tenant\nabc")).checksum).toBeDefined();
    // SHA-256 hex of the text, as SqlMigration::new.
    await expect(parseMigration("0004_w.sql", "SELECT 1;\n-- gonvex:scope tenant")).rejects.toThrow("migration 0004_w.sql: missing required -- gonvex:scope tenant|control-plane directive");
    await expect(parseMigration("0005_v.sql", "-- gonvex:scope everywhere")).rejects.toThrow('migration 0005_v.sql: invalid scope "everywhere"');
    await expect(parseMigration("001_short.sql", "-- gonvex:scope tenant")).rejects.toThrow('migration "001_short.sql" must be named NNNN_description.sql');
    await expect(parseMigration("0006_a b.sql", "-- gonvex:scope tenant")).rejects.toThrow("must be named NNNN_description.sql");
    // A directive after the first statement is not a directive.
    expect((await parseMigration("0007_u.sql", "-- gonvex:scope tenant\nSELECT 1;\n-- gonvex:no-transaction")).noTransaction).toBe(false);
  });

  it("takes only migrations/*.sql, sorted, and plans only tenant scope", async () => {
    const list = await migrationsFromFiles(APP);
    expect(list.map((m) => m.name)).toEqual(["0001_init.sql", "0002_comments.sql", "0003_rebuild_tasks.sql", "0004_control_side.sql"]);
    const p = await plan();
    expect(p.migrations.map((m) => m.name)).toEqual(["0001_init.sql", "0002_comments.sql", "0003_rebuild_tasks.sql"]);
    expect(p.migrations[2]).toMatchObject({ foreignKeyRebuild: true, refusal: null });
    expect(p.prelude.map((u) => u.name)).toEqual(["tenant-identity", "reference-ledger", "schema-options", "migration-ledger"]);
    // Deterministic: the same migrations give the same plan, byte for byte.
    expect(JSON.stringify(await plan())).toBe(JSON.stringify(p));
  });

  it("checksums are SHA-256 hex of the file text", async () => {
    const m = await parseMigration("0001_a.sql", "-- gonvex:scope tenant\n");
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode("-- gonvex:scope tenant\n"));
    expect(m.checksum).toBe([...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join(""));
  });
});

describe("provisioning a fresh object", () => {
  it("applies the prelude, every tenant migration and the change-feed install, chunk by chunk", async () => {
    const release = await plan();
    const sources = await migrationsFromFiles(APP);
    await withObject(async (state) => {
      const { m, capture } = migrator(state, new MemoryPlanStore(release));
      const first = await m.applyRelease(release.id);
      expect(first.state).toBe("running");
      const done = await drain(m);
      expect(done.error).toBeNull();
      expect(done).toMatchObject({ state: "done", gateClosed: false, applied: ["0001_init.sql", "0002_comments.sql", "0003_rebuild_tasks.sql"], error: null });
      expect(done.events).toBeGreaterThan(10);
      // The ledger equals Rust's: name and SHA-256 of each tenant migration, nothing else.
      const ledger = m.ledger();
      expect(ledger.map((r) => [r.name, r.checksum])).toEqual(sources.filter((s) => s.scope === "tenant").map((s) => [s.name, s.checksum]));
      for (const row of ledger) expect(row.applied_at).toBeGreaterThan(1.7e12);
      // Runtime-owned objects from the prelude.
      const names = new Set(schema(state.storage.sql).map((o) => o.name));
      for (const n of ["members", "_gonvex_control_plane_membership_outbox", "_gonvex_reference_catalog", "_gonvex_schema_capabilities", "_gonvex_record_pending", "gonvex_migrations"]) expect(names).toContain(n);
      expect(state.storage.sql.exec("SELECT name FROM _gonvex_schema_capabilities ORDER BY name").toArray().map((r) => r.name)).toEqual([
        "atomic-foreign-key-rebuild-v1",
        "derived-table-capture-v1",
        "logical-record-storage-v1",
      ]);
      expect(names).not.toContain("control_only");
      expect(names).not.toContain("tasks_new");
      expect(state.storage.sql.exec("PRAGMA foreign_keys").one()).toEqual({ foreign_keys: 1 });
      // Each migration finalized once with Rust's provenance.
      expect(capture.finalizeCalls.map((c) => c.commandId)).toEqual(["migration:0001_init.sql", "migration:0002_comments.sql", "migration:0003_rebuild_tasks.sql"]);
      expect(capture.installed).toContain("infrastructure");
      expect(capture.installed).toContain("table:tasks");
      // The transform version is kept beside Rust's ledger.
      expect(state.storage.kv.get(`gonvex/migrate/applied/0001_init.sql`)).toMatchObject({ transformVersion: release.transformVersion, planId: release.id });
      expect(state.storage.sql.exec("SELECT count(*) AS n FROM tasks").one().n).toBe(3);
    });
  });

  it("a second sync of the same release applies nothing and keeps the ledger", async () => {
    const release = await plan();
    await withObject(async (state) => {
      const { m, capture } = migrator(state, new MemoryPlanStore(release), { budget: { maxChunksPerEvent: 50 } });
      await m.applyRelease(release.id);
      await drain(m);
      const before = m.ledger();
      const calls = capture.finalizeCalls.length;
      capture.installed.length = 0;
      await m.applyRelease(release.id);
      const again = await drain(m);
      expect(again).toMatchObject({ state: "done", applied: [] });
      expect(m.ledger()).toEqual(before);
      expect(capture.finalizeCalls.length).toBe(calls);
      expect(capture.installed).toEqual([]);
    });
  });

  it("a later release applies only its new migration", async () => {
    const v1 = await plan();
    const v2files = { ...APP, "migrations/0005_flags.sql": "-- gonvex:scope tenant\nCREATE TABLE flags (id TEXT PRIMARY KEY);\n" };
    const v2 = await plan(v2files);
    expect(v2.id).not.toBe(v1.id);
    await withObject(async (state) => {
      const { m, capture } = migrator(state, new MemoryPlanStore(v1, v2), { budget: { maxChunksPerEvent: 50 } });
      await m.applyRelease(v1.id);
      await drain(m);
      capture.installed.length = 0;
      await m.applyRelease(v2.id);
      expect(await drain(m)).toMatchObject({ state: "done", applied: ["0005_flags.sql"] });
      expect(capture.installed).toEqual(["table:flags"]);
    });
  });
});

describe("resume after interruption", () => {
  it("an event that dies mid-migration resumes from the last committed chunk, with the same result", async () => {
    const release = await plan();
    // Reference: an uninterrupted run.
    const reference = await withObject(async (state) => {
      const { m } = migrator(state, new MemoryPlanStore(release));
      await m.applyRelease(release.id);
      await drain(m);
      return { schema: schema(state.storage.sql), ledger: m.ledger().map((r) => [r.name, r.checksum]) };
    });
    await withObject(async (state) => {
      const store = new MemoryPlanStore(release);
      // The plan store fails once in the middle of 0003 (a transient fetch error): nothing is lost or rolled back.
      let failures = 1;
      const flaky: PlanStore = {
        manifest: (id) => store.manifest(id),
        async chunk(id, unit, index) {
          if (failures > 0 && unit === "migration/0003_rebuild_tasks.sql" && index === 2) {
            failures -= 1;
            throw new Error("R2 unavailable");
          }
          return store.chunk(id, unit, index);
        },
      };
      // An event can yield at its time/chunk budget before it reaches the injected fetch failure.
      // Drive the real alarm slices to that fault; require it to occur, then keep the exact retry assertion.
      const { m } = migrator(state, flaky, { budget: { maxChunksPerEvent: 1 } });
      let s = await m.applyRelease(release.id);
      for (let i = 0; failures > 0 && s.state === "running" && i < 200; i += 1) {
        expect(s.error).toBeNull();
        s = await m.alarm();
      }
      expect(failures).toBe(0);
      expect(s).toMatchObject({ state: "running", gateClosed: true, error: "retrying: R2 unavailable" });
      const done = await drain(m);
      expect(done).toMatchObject({ state: "done", error: null });
      expect(schema(state.storage.sql)).toEqual(reference.schema);
    });
    await withObject(async (state) => {
      const store = new MemoryPlanStore(release);
      const first = migrator(state, store, { budget: { maxChunksPerEvent: 1 } });
      await first.m.applyRelease(release.id);
      // Run until 0003 has committed two chunks, then drop the instance: a reset between events.
      for (let i = 0; i < 200; i += 1) {
        const p = state.storage.kv.get<Progress>(PROGRESS_KEY)!;
        if (p.phase === "migrations" && p.unit === 2 && p.chunk === 2) break;
        await first.m.alarm();
      }
      const mid = state.storage.kv.get<Progress>(PROGRESS_KEY)!;
      expect(mid).toMatchObject({ phase: "migrations", unit: 2, chunk: 2, foreignKeysOff: true, unitState: "running" });
      expect(first.m.gateClosed()).toBe(true);
      // A new session starts with foreign keys on; the next instance must turn them off again.
      state.storage.sql.exec("PRAGMA foreign_keys=ON");
      const second = migrator(state, store, { budget: { maxChunksPerEvent: 1 } });
      const done = await drain(second.m);
      expect(done.state).toBe("done");
      expect(schema(state.storage.sql)).toEqual(reference.schema);
      expect(second.m.ledger().map((r) => [r.name, r.checksum])).toEqual(reference.ledger);
      // A reset inside a transaction leaves no trace: progress is in the same transaction.
      expect(state.storage.sql.exec("PRAGMA foreign_keys").one()).toEqual({ foreign_keys: 1 });
    });
  });

  it("a chunk that keeps resetting the object fails the migration instead of looping", async () => {
    const release = await plan();
    await withObject(async (state) => {
      const { m, aborts, restores } = migrator(state, new MemoryPlanStore(release));
      await m.applyRelease(release.id);
      for (let i = 0; i < 200; i += 1) {
        const p = state.storage.kv.get<Progress>(PROGRESS_KEY)!;
        if (p.phase === "migrations" && p.unit === 1 && p.chunk === 1) break;
        await m.alarm();
      }
      // Three tries of 0002's 2nd chunk that never committed (each reset the object).
      const p = state.storage.kv.get<Progress>(PROGRESS_KEY)!;
      p.attempts = 3;
      state.storage.kv.put(PROGRESS_KEY, p);
      const failed = await m.alarm();
      expect(failed.state).toBe("failed");
      expect(failed.error).toBe("migration 0002_comments.sql: migration 0002_comments.sql chunk 2/2 reset the object 3 times without committing (CPU limit or a deferred foreign-key violation at commit)");
      expect(failed.gateClosed).toBe(true);
      expect(restores).toEqual([p.bookmark]);
      // The next event restarts the object so the restore takes effect.
      await m.alarm();
      expect(aborts).toHaveLength(1);
    });
  });

  it("after a restore, the restore-point state reads as a failed, rolled-back migration with the gate open", async () => {
    const release = await plan();
    await withObject(async (state) => {
      const { m } = migrator(state, new MemoryPlanStore(release));
      await m.applyRelease(release.id);
      // What the bookmark captured: the unit about to start, marked as a restore point.
      const p = state.storage.kv.get<Progress>(PROGRESS_KEY)!;
      state.storage.kv.put(PROGRESS_KEY, { ...p, phase: "migrations", unit: 1, chunk: 0, unitState: "restore-point" });
      const s = await m.alarm();
      expect(s).toMatchObject({ state: "failed", gateClosed: false, error: "the migration failed and was rolled back to its restore point; sync again to retry" });
      // Syncing again starts over and finishes.
      await m.applyRelease(release.id);
      expect(await drain(m)).toMatchObject({ state: "done" });
    });
  });
});

describe("ledger and directives", () => {
  it("a changed applied migration fails with Rust's checksum error", async () => {
    const v1 = await plan();
    const edited = await plan({ ...APP, "migrations/0002_comments.sql": APP["migrations/0002_comments.sql"] + "\n-- edited\n" });
    await withObject(async (state) => {
      const { m } = migrator(state, new MemoryPlanStore(v1, edited), { budget: { maxChunksPerEvent: 50 } });
      await m.applyRelease(v1.id);
      await drain(m);
      await m.applyRelease(edited.id);
      const s = await drain(m);
      const recorded = m.ledger().find((r) => r.name === "0002_comments.sql")!.checksum;
      const artifact = edited.migrations.find((x) => x.name === "0002_comments.sql")!.checksum;
      expect(s.error).toBe(`migration 0002_comments.sql: migration 0002_comments.sql checksum mismatch: database has ${recorded}, artifact has ${artifact}`);
      expect(s.gateClosed).toBe(false);
    });
  });

  it("a foreign-key rebuild that leaves a dangling reference is rejected and rolled back", async () => {
    const files = {
      "migrations/0001_a.sql": "-- gonvex:scope tenant\nCREATE TABLE p (id TEXT PRIMARY KEY);\nCREATE TABLE c (id TEXT PRIMARY KEY, p TEXT REFERENCES p(id));\nINSERT INTO p VALUES ('x');\nINSERT INTO c VALUES ('1','x');\n",
      "migrations/0002_b.sql": "-- gonvex:scope tenant\n-- gonvex:foreign-key-rebuild\nDELETE FROM p;\nCREATE TABLE extra (id INTEGER PRIMARY KEY);\n",
    };
    const release = await plan(files, { chunk: { maxStatements: 1 } });
    await withObject(async (state) => {
      const failures: unknown[] = [];
      // One table per check step, so the check spans several events before it reaches `c`.
      const { m, restores } = migrator(state, new MemoryPlanStore(release), { reportFailure: (f) => void failures.push(f), budget: { maxChunksPerEvent: 1, foreignKeyTablesPerStep: 1 } });
      await m.applyRelease(release.id);
      const s = await drain(m);
      expect(s.error).toBe("migration 0002_b.sql: foreign-key rebuild rejected: c -> p (constraint 0)");
      expect(s.events).toBeGreaterThan(12);
      expect(failures).toMatchObject([{ unit: "migration 0002_b.sql", restored: true }]);
      expect(restores).toHaveLength(1);
      expect(state.storage.sql.exec("PRAGMA foreign_keys").one()).toEqual({ foreign_keys: 1 });
      expect(m.ledger().map((r) => r.name)).toEqual(["0001_a.sql"]);
    });
  });

  it("no-transaction migrations keep earlier statements after a failure; VACUUM is refused clearly", async () => {
    const files = {
      "migrations/0001_a.sql": "-- gonvex:scope tenant\n-- gonvex:no-transaction\nCREATE TABLE IF NOT EXISTS kept (id INTEGER PRIMARY KEY);\nINSERT INTO missing VALUES (1);\n",
    };
    const release = await plan(files, { chunk: { maxStatements: 1 } });
    await withObject(async (state) => {
      const { m } = migrator(state, new MemoryPlanStore(release));
      await m.applyRelease(release.id);
      const s = await drain(m);
      expect(s.error).toMatch(/^migration 0001_a\.sql: no such table: missing/);
      expect(state.storage.sql.exec("SELECT count(*) AS n FROM sqlite_schema WHERE name='kept'").one().n).toBe(1);
      expect(m.ledger()).toEqual([]);
    });
    const vacuum = await plan({ "migrations/0001_v.sql": "-- gonvex:scope tenant\n-- gonvex:no-transaction\nVACUUM;\n" });
    expect(vacuum.migrations[0]!.refusal).toBe("migration 0001_v.sql: VACUUM is not available in a Durable Object (the platform manages its storage)");
    const rebuildNoTx = await plan({ "migrations/0001_r.sql": "-- gonvex:scope tenant\n-- gonvex:no-transaction\n-- gonvex:foreign-key-rebuild\nSELECT 1;\n" });
    expect(rebuildNoTx.migrations[0]!.refusal).toBe("foreign-key rebuild requires an atomic tenant migration");
  });

  it("refuses a pre-identity-v2 members table, as install_tenant_identity does", async () => {
    const release = await plan();
    await withObject(async (state) => {
      state.storage.sql.exec("CREATE TABLE members (id TEXT PRIMARY KEY, user_id TEXT)");
      const { m } = migrator(state, new MemoryPlanStore(release));
      const s = await m.applyRelease(release.id);
      expect(s.error).toBe("prelude tenant-identity: identity-v2 migration required: tenant members still use user_id");
    });
  });
});

describe("record capture across chunks", () => {
  it("first-before images staged in one chunk survive to the migration's single finalize", async () => {
    // An existing tenant: a record-captured table with a BEFORE UPDATE trigger staging the first image.
    const base = {
      "migrations/0001_a.sql": `-- gonvex:scope tenant
CREATE TABLE items (id TEXT PRIMARY KEY, n INTEGER NOT NULL);
INSERT INTO items VALUES ('a', 1);
CREATE TRIGGER items_stage BEFORE UPDATE ON items BEGIN INSERT INTO _gonvex_record_pending(table_name,row_id,old_value) SELECT 'items', OLD.id, json_object('n', OLD.n) WHERE NOT EXISTS (SELECT 1 FROM _gonvex_record_pending WHERE table_name='items' AND row_id=OLD.id); END;
`,
    };
    const next = { ...base, "migrations/0002_b.sql": "-- gonvex:scope tenant\nUPDATE items SET n = 2;\nCREATE TABLE x1 (id INTEGER PRIMARY KEY);\nUPDATE items SET n = 3;\n" };
    const v1 = await plan(base);
    const v2 = await plan(next, { chunk: { maxStatements: 1 } });
    expect(v2.migrations[1]!.chunks).toBe(3);
    await withObject(async (state) => {
      const { m, capture } = migrator(state, new MemoryPlanStore(v1, v2));
      await m.applyRelease(v1.id);
      await drain(m);
      await m.applyRelease(v2.id);
      for (let i = 0; i < 100; i += 1) {
        const p = state.storage.kv.get<Progress>(PROGRESS_KEY)!;
        if (p.phase === "migrations" && p.unit === 1 && p.chunk === 1) break;
        await m.alarm();
      }
      // Between chunks the staged image waits outside the guarded table, so the commit could not reset the object.
      expect(state.storage.sql.exec("SELECT count(*) AS n FROM _gonvex_record_pending").one().n).toBe(0);
      expect(state.storage.sql.exec(`SELECT row_id, old_value FROM ${RECORD_STASH}`).toArray()).toEqual([{ row_id: "a", old_value: '{"n":1}' }]);
      const s = await drain(m);
      expect(s.state).toBe("done");
      const call = capture.finalizeCalls.find((c) => c.commandId === "migration:0002_b.sql")!;
      expect(call.pendingRecords).toEqual([{ table_name: "items", row_id: "a", old_value: '{"n":1}' }]);
      expect(state.storage.sql.exec("SELECT count(*) AS n FROM sqlite_schema WHERE name=?", RECORD_STASH).one().n).toBe(0);
    });
  });
});

describe("plan", () => {
  it("is refused while another release is partly applied", async () => {
    const v1 = await plan();
    const v2 = await plan({ ...APP, "migrations/0005_x.sql": "-- gonvex:scope tenant\nCREATE TABLE x (id INTEGER PRIMARY KEY);\n" });
    await withObject(async (state) => {
      const { m } = migrator(state, new MemoryPlanStore(v1, v2));
      await m.applyRelease(v1.id);
      for (let i = 0; i < 100; i += 1) {
        const p = state.storage.kv.get<Progress>(PROGRESS_KEY)!;
        if (p.phase === "migrations" && p.chunk > 0) break;
        await m.alarm();
      }
      await expect(m.applyRelease(v2.id)).rejects.toThrow(`tenant migration ${v1.id} is in progress; a sync of ${v2.id} must wait for it`);
    });
  });

  it("an unknown plan id fails clearly", async () => {
    await withObject(async (state) => {
      const { m } = migrator(state, new MemoryPlanStore());
      await expect(m.applyRelease("nope")).rejects.toThrow("release plan nope not found");
    });
  });

  it("keeps the transform's added objects in the manifest", async () => {
    const p: ReleasePlan = await plan({ "migrations/0001_t.sql": "-- gonvex:scope tenant\nCREATE TEMP TABLE guard (x);\nDROP TABLE guard;\n" });
    expect(p.migrations[0]!.statements).toBe(2);
  });
});
