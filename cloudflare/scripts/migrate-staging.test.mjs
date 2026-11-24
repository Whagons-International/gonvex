import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "node:test";
import { openFile } from "../tools/tenant-transfer/src/file.mjs";
import { fileChecksums } from "../tools/tenant-transfer/src/transfer.mjs";
import {
  inspectTenantFile,
  loadState,
  planMigration,
  rollbackTenants,
  runMigrationWave,
} from "./migrate-staging.mjs";

function makeTempTenantDb(dir, name, rows) {
  const dbPath = join(dir, `${name}.sqlite`);
  const db = openFile(dbPath);
  try {
    db.sql.run("CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL, rank INTEGER NOT NULL)");
    for (let i = 0; i < rows; i++) {
      db.sql.run("INSERT INTO tasks VALUES (?, ?, ?)", `t-${i}`, `Task ${i}`, i);
    }
  } finally {
    db.close();
  }
  return dbPath;
}

function makeFakeClient(calls, { failVerify = false } = {}) {
  return ({ tenant }) => {
    const client = {
      requests: 0,
      async call(op, body) {
        client.requests++;
        calls.push({ tenant, op });
        if (op === "status") return { objects: 0 };
        if (op === "wipe") return { wiped: true };
        if (op === "schema") return { consumed: body.statements.length, failed: [] };
        if (op === "insert") return { rowsWritten: body.parts.reduce((acc, p) => acc + p.rows.length, 0) };
        if (op === "check") return { quickCheck: "ok", foreignKeyViolations: 0, fts: {} };
        if (op === "verify") {
          if (failVerify) throw new Error("verify: HTTP 409: P03 table checksum mismatch");
          return { verified: Object.keys(body.checksums).length };
        }
        if (op === "complete") return { ready: true };
        return {};
      },
    };
    return client;
  };
}

describe("staging-to-serverless migration orchestrator", () => {
  it("plans, executes, resumes idempotently, and rolls back cleanly", async () => {
    const dir = mkdtempSync(join(tmpdir(), "gonvex-mig-"));
    try {
      const dbA = makeTempTenantDb(dir, "stg-alpha", 12);
      const dbB = makeTempTenantDb(dir, "stg-beta", 7);
      const statePath = join(dir, "state.json");
      const manifest = {
        transferUrl: "https://example.invalid/__tenant-transfer",
        tenants: [
          { tenantId: "stg-alpha", ring: 1, dbPath: dbA },
          { tenantId: "stg-beta", ring: 2, dbPath: dbB },
        ],
      };

      // 1. Plan Ring 1 only
      const plan = planMigration(manifest, { ring: 1 });
      assert.equal(plan.selected, 1);
      assert.equal(plan.ready, 1);
      assert.equal(plan.blocked, 0);
      assert.equal(plan.totalRows, 12);
      assert.equal(inspectTenantFile({ tenantId: "missing", dbPath: join(dir, "nope.sqlite") }).ok, false);
      assert.equal(fileChecksums(dbA).tasks.rows, 12);

      // 2. Run Ring 1 wave
      const calls = [];
      const flips = [];
      const router = {
        async setBackend(payload) {
          flips.push(payload);
          return { ok: true };
        },
      };

      const run1 = await runMigrationWave(manifest, {
        ring: 1,
        statePath,
        router,
        log: () => {},
        clientFactory: makeFakeClient(calls),
      });
      assert.equal(run1.failed, 0);
      assert.equal(run1.processed, 1);
      assert.deepEqual(
        calls.map((c) => c.op),
        ["wipe", "status", "schema", "insert", "pragma", "check", "verify", "complete"],
      );
      assert.equal(flips.length, 1);
      assert.equal(flips[0].backend, "cf-serverless");
      assert.equal(loadState(statePath).tenants["stg-alpha"].phase, "complete");

      // 3. Idempotent re-run skips already-complete tenant with zero calls
      calls.length = 0;
      const run2 = await runMigrationWave(manifest, {
        ring: 1,
        statePath,
        router,
        log: () => {},
        clientFactory: makeFakeClient(calls),
      });
      assert.equal(run2.failed, 0);
      assert.equal(calls.length, 0);

      // 4. Instant rollback flips routing back to legacy
      const rb = await rollbackTenants({ statePath, router, tenantId: "stg-alpha", log: () => {} });
      assert.equal(rb.count, 1);
      assert.equal(flips[1].backend, "legacy");
      assert.equal(loadState(statePath).tenants["stg-alpha"].phase, "rolled_back");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("fails closed on checksum mismatch and never flips routing", async () => {
    const dir = mkdtempSync(join(tmpdir(), "gonvex-mig-fail-"));
    try {
      const dbA = makeTempTenantDb(dir, "stg-corrupt", 5);
      const statePath = join(dir, "state.json");
      const manifest = {
        transferUrl: "https://example.invalid/__tenant-transfer",
        tenants: [{ tenantId: "stg-corrupt", ring: 1, dbPath: dbA }],
      };
      const calls = [];
      const flips = [];
      const router = {
        async setBackend(payload) {
          flips.push(payload);
          return { ok: true };
        },
      };

      const res = await runMigrationWave(manifest, {
        ring: 1,
        statePath,
        router,
        log: () => {},
        clientFactory: makeFakeClient(calls, { failVerify: true }),
      });
      assert.equal(res.failed, 1);
      assert.equal(res.halted, true);
      assert.equal(flips.length, 0); // Routing was NEVER flipped!
      assert.equal(loadState(statePath).tenants["stg-corrupt"].phase, "failed");
      assert.equal(loadState(statePath).tenants["stg-corrupt"].backend, "legacy");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
