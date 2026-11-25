import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  F_PAGE,
  decodeFrame,
  layoutFromCols,
  type PageCol,
} from "../../codec/index";
import {
  EdgeAuthVerifier,
  generateEdgeSigningKey,
  mintSessionToken,
} from "../../edge-auth/index";
import {
  buildBatchFanout,
  ensureTxLogSchema,
  readChangesSince,
  runTx,
  type FanoutTableSpec,
} from "../../tx-log/index";
import {
  CheckpointCompactor,
  parseLatestPointer,
  parseManifest,
  serveCheckpointRequest,
  type CheckpointBucket,
  type CheckpointHttpMetadata,
  type CheckpointListResult,
  type CheckpointObjectBody,
  type CheckpointObjectMeta,
  type CheckpointTableSpec,
} from "../index";

// Compile-time guarantee that Cloudflare's native R2Bucket satisfies CheckpointBucket directly.
const _r2AssignabilityCheck: (b: R2Bucket) => CheckpointBucket = (b) => b;
void _r2AssignabilityCheck;

const te = new TextEncoder();
const td = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });

interface MemEntry {
  bytes: Uint8Array;
  etag: string;
  httpMetadata?: CheckpointHttpMetadata;
}

class MemoryCheckpointBucket implements CheckpointBucket {
  readonly store = new Map<string, MemEntry>();
  getCalls = 0;
  headCalls = 0;
  putCalls = 0;
  private etagSeq = 1;

  async put(
    key: string,
    value: Uint8Array | ArrayBuffer | string,
    options?: { readonly httpMetadata?: CheckpointHttpMetadata },
  ): Promise<CheckpointObjectMeta> {
    this.putCalls++;
    const bytes = typeof value === "string"
      ? te.encode(value)
      : value instanceof Uint8Array
        ? value.slice()
        : new Uint8Array(value.slice(0));
    const etag = `etag-${this.etagSeq++}`;
    const entry: MemEntry = options?.httpMetadata !== undefined
      ? { bytes, etag, httpMetadata: options.httpMetadata }
      : { bytes, etag };
    this.store.set(key, entry);
    return { key, size: bytes.length, etag };
  }

  async get(key: string): Promise<CheckpointObjectBody | null> {
    this.getCalls++;
    const entry = this.store.get(key);
    if (entry === undefined) return null;
    const bytes = entry.bytes;
    const base: CheckpointObjectBody = {
      key,
      size: bytes.length,
      etag: entry.etag,
      body: new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(bytes);
          controller.close();
        },
      }),
      async arrayBuffer() {
        return bytes.slice().buffer;
      },
      async text() {
        return td.decode(bytes);
      },
    };
    return entry.httpMetadata !== undefined
      ? { ...base, httpMetadata: entry.httpMetadata }
      : base;
  }

  async head(key: string): Promise<CheckpointObjectMeta | null> {
    this.headCalls++;
    const entry = this.store.get(key);
    if (entry === undefined) return null;
    return { key, size: entry.bytes.length, etag: entry.etag };
  }

  async list(options?: { readonly prefix?: string; readonly limit?: number; readonly cursor?: string }): Promise<CheckpointListResult> {
    const prefix = options?.prefix ?? "";
    const limit = options?.limit ?? 1000;
    const allKeys = [...this.store.keys()].filter((k) => k.startsWith(prefix)).sort();
    const start = options?.cursor !== undefined ? Number(options.cursor) : 0;
    const slice = allKeys.slice(start, start + limit);
    const objects: CheckpointObjectMeta[] = slice.map((k) => {
      const e = this.store.get(k)!;
      return { key: k, size: e.bytes.length, etag: e.etag };
    });
    const next = start + slice.length;
    if (next < allKeys.length) {
      return { objects, truncated: true, cursor: String(next) };
    }
    return { objects, truncated: false };
  }

  async delete(keys: string | string[]): Promise<void> {
    if (typeof keys === "string") {
      this.store.delete(keys);
      return;
    }
    for (let i = 0; i < keys.length; i++) {
      this.store.delete(keys[i]!);
    }
  }
}

const TASK_COLS: readonly PageCol[] = [
  { id: 1, k: "r" }, // workspace_id
  { id: 2, k: "i" }, // status
  { id: 3, k: "d" }, // priority
  { id: 4, k: "x" }, // title
  { id: 5, k: "t" }, // updated_at
];

const TASK_TABLE_SPEC: CheckpointTableSpec = {
  tbl: 0,
  tableName: "tasks",
  selectColumnsSql: "rid, ver, workspace_id, status, priority, title, updated_at",
  cols: TASK_COLS,
};

const TASK_LAYOUT = layoutFromCols(0, TASK_COLS);
const PRIORITIES = ["low", "medium", "high", "urgent"] as const;

describe("kernel/checkpoint (Phase 3 R2 WLP1 checkpoints + zero-DO-wake edge serving)", () => {
  it("compacts 2,000 tasks to R2, cold-boots with 0 Tenant DO reads, and applies 5-row incremental tail", async () => {
    const id = env.HELLO.idFromName("checkpoint-e2e-2000");
    const stub = env.HELLO.get(id);
    const bucket = new MemoryCheckpointBucket();
    const tenantId = "tenant_am_demo";

    // 1. Seed 2,000 tasks in Tenant DO SQLite + compact into R2 WLP1 pages.
    const compact1 = await runInDurableObject(stub, async (_instance, state) => {
      ensureTxLogSchema(state.storage.sql);
      state.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS tasks (
          rid INTEGER PRIMARY KEY,
          ver INTEGER NOT NULL,
          workspace_id INTEGER NOT NULL,
          status INTEGER NOT NULL,
          priority TEXT NOT NULL,
          title TEXT NOT NULL,
          updated_at INTEGER NOT NULL
        )
      `);

      const seedCommit = runTx(state.storage, (tx) => {
        for (let i = 1; i <= 2000; i++) {
          tx.exec(
            "INSERT INTO tasks (rid, ver, workspace_id, status, priority, title, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            i,
            1,
            (i % 4) + 1,
            i % 5,
            PRIORITIES[i % 4]!,
            `Task #${i} - inspect valve`,
            1700000000000 + i * 1000,
          );
          tx.recordUpsert(0, i, (i % 4) + 1);
        }
      });
      expect(seedCommit.seq).toBe(1);

      const compactor = new CheckpointCompactor(state.storage);
      const res1 = await compactor.compact({
        tenantId,
        bucket,
        tables: [TASK_TABLE_SPEC],
        nowMs: 1700005000000,
      });

      // Second compact with no new commits must skip cleanly.
      const resSkip = await compactor.compact({
        tenantId,
        bucket,
        tables: [TASK_TABLE_SPEC],
        nowMs: 1700005001000,
      });

      return { res1, resSkip };
    });

    expect(compact1.res1.skipped).toBe(false);
    expect(compact1.res1.epoch).toBe(1);
    expect(compact1.res1.seq).toBe(1);
    expect(compact1.res1.pagesUploaded).toBe(2); // 1024 + 976
    expect(compact1.res1.rowsRead).toBe(2001); // 1 seq + 2000 tasks
    expect(compact1.res1.manifest?.totalRows).toBe(2000);
    // 2,000 tasks with unique 26-byte UTF-8 titles (~52 KB raw string bytes alone) compress to ~71 KB
    // vs ~340 KB raw JSON (~4.8x smaller raw wire bytes, zero per-row JSON parsing).
    expect(compact1.res1.bytesUploaded).toBeLessThan(80_000);
    expect(compact1.resSkip.skipped).toBe(true);
    expect(compact1.resSkip.pagesUploaded).toBe(0);

    // 2. Post-checkpoint: mutate 5 tasks (seq = 2) while checkpoint remains at seq = 1.
    const seqAfterTail = await runInDurableObject(stub, async (_instance, state) => {
      const c2 = runTx(state.storage, (tx) => {
        for (let rid = 1996; rid <= 2000; rid++) {
          tx.exec("UPDATE tasks SET ver = 2, status = 4, title = ? WHERE rid = ?", `Updated #${rid}`, rid);
          tx.recordUpsert(0, rid, (rid % 4) + 1);
        }
      });
      return c2.seq;
    });
    expect(seqAfterTail).toBe(2);

    // 3. Cold client boots purely from stateless Edge Worker + R2 (0 DO calls!).
    const key = await generateEdgeSigningKey("k2026-p3");
    const verifier = new EdgeAuthVerifier();
    verifier.addKey({ kid: key.kid, publicKey: key.publicKey });

    const token = await mintSessionToken(key, {
      jti: "tok_cold_1",
      projectId: "proj_1",
      tenantId,
      accountId: "acc_1",
      memberId: "mem_1",
      role: "admin",
      membershipRev: 1,
      permsLo: 0xff,
      permsHi: 0,
      anonymous: false,
      iatSec: 1700005000,
      expSec: 1700009000,
    });
    const authHeaders = { Authorization: `Bearer ${token}` };

    const latestResp = await serveCheckpointRequest(
      new Request(`https://edge.whagons.com/sync/checkpoint/${tenantId}/latest`, { headers: authHeaders }),
      { bucket, verifier, nowSec: 1700005100 },
    );
    expect(latestResp?.status).toBe(200);
    expect(latestResp?.headers.get("X-Gonvex-Checkpoint-Zero-DO")).toBe("1");
    expect(latestResp?.headers.get("Cache-Control")).toBe("private, max-age=5");
    const latest = parseLatestPointer(await latestResp!.text());
    expect(latest.epoch).toBe(1);
    expect(latest.seq).toBe(1);
    expect(latest.totalRows).toBe(2000);

    const manifestResp = await serveCheckpointRequest(
      new Request(`https://edge.whagons.com/sync/checkpoint/${tenantId}/${latest.epoch}/${latest.seq}/manifest`, {
        headers: authHeaders,
      }),
      { bucket, verifier, nowSec: 1700005100 },
    );
    expect(manifestResp?.status).toBe(200);
    expect(manifestResp?.headers.get("Cache-Control")).toContain("immutable");
    const manifest = parseManifest(await manifestResp!.text());
    expect(manifest.tables).toHaveLength(1);
    expect(manifest.tables[0]!.pages).toHaveLength(2);

    // Fetch both immutable WLP1 frame pages and decode every row.
    const restoredRows = new Map<number, { ver: number; status: number; title: string }>();
    for (let p = 0; p < manifest.tables[0]!.pages.length; p++) {
      const pageUrl = `https://edge.whagons.com/sync/checkpoint/${tenantId}/${latest.epoch}/${latest.seq}/0/${p}`;
      const pageResp = await serveCheckpointRequest(
        new Request(pageUrl, { headers: authHeaders }),
        { bucket, verifier, nowSec: 1700005100 },
      );
      expect(pageResp?.status).toBe(200);
      expect(pageResp?.headers.get("Content-Type")).toBe("application/x-wl-frames");
      expect(pageResp?.headers.get("Cache-Control")).toContain("immutable");

      // Verify HEAD + 304 conditional cache hit
      const etag = pageResp!.headers.get("ETag")!;
      expect(etag.length).toBeGreaterThan(2);
      const notMod = await serveCheckpointRequest(
        new Request(pageUrl, { headers: { ...authHeaders, "If-None-Match": etag } }),
        { bucket, verifier, nowSec: 1700005100 },
      );
      expect(notMod?.status).toBe(304);

      const frameBytes = new Uint8Array(await pageResp!.arrayBuffer());
      const decoded = decodeFrame(frameBytes, () => TASK_LAYOUT);
      expect(decoded.k).toBe(F_PAGE);
      if (decoded.k !== F_PAGE) throw new Error("expected F_PAGE");
      const { page } = decoded;
      const ridLane = page.lane(0);
      const verLane = page.lane(1);
      const statusLane = page.lane(3);
      for (let r = 0; r < page.rows; r++) {
        const rid = ridLane.num(r);
        restoredRows.set(rid, {
          ver: verLane.num(r),
          status: statusLane.num(r),
          title: page.get(5, r) as string,
        });
      }
    }

    expect(restoredRows.size).toBe(2000);
    expect(restoredRows.get(1)).toEqual({ ver: 1, status: 1, title: "Task #1 - inspect valve" });
    expect(restoredRows.get(2000)).toEqual({ ver: 1, status: 0, title: "Task #2000 - inspect valve" });

    // 4. Client now catches up from checkpoint seq=1 to live seq=2 via tiny 5-row tail!
    const tailStats = await runInDurableObject(stub, async (_instance, state) => {
      const changes = readChangesSince(state.storage.sql, latest.seq, seqAfterTail);
      const specs = new Map<number, FanoutTableSpec>([
        [0, {
          tbl: 0,
          tableName: "tasks",
          selectColumnsSql: TASK_TABLE_SPEC.selectColumnsSql,
          cols: TASK_COLS,
          visibleToClass: () => true,
        }],
      ]);
      const fanout = buildBatchFanout(
        state.storage.sql,
        specs,
        changes,
        [{ memberId: "mem_1", classKey: 0 }],
      );
      return {
        changeCount: changes.length,
        rowsRead: fanout.stats.rowsRead,
        pagesEncoded: fanout.stats.pagesEncoded,
      };
    });

    expect(tailStats.changeCount).toBe(5);
    expect(tailStats.rowsRead).toBe(10); // 5 IN-list + 5 PK lookups — NOT 2,000!
    expect(tailStats.pagesEncoded).toBe(1);
  });

  it("enforces Ed25519 auth, revocation, and cross-tenant isolation before touching R2", async () => {
    const bucket = new MemoryCheckpointBucket();
    await bucket.put("checkpoints/tenant_a/latest.json", '{"v":1,"tenantId":"tenant_a","epoch":1,"seq":1,"manifestKey":"x","createdAtMs":1,"frameWrapped":true,"totalRows":0,"totalPages":0,"totalBytes":0}');

    const key = await generateEdgeSigningKey("k-auth");
    const verifier = new EdgeAuthVerifier();
    verifier.addKey({ kid: key.kid, publicKey: key.publicKey });

    const url = "https://edge.whagons.com/sync/checkpoint/tenant_a/latest";

    // Missing token -> 401
    const rMissing = await serveCheckpointRequest(new Request(url), { bucket, verifier, nowSec: 1000 });
    expect(rMissing?.status).toBe(401);

    // Legacy opaque token -> 401
    const rLegacy = await serveCheckpointRequest(
      new Request(url, { headers: { Authorization: "Bearer legacy-opaque-token" } }),
      { bucket, verifier, nowSec: 1000 },
    );
    expect(rLegacy?.status).toBe(401);

    // Cross-tenant valid token -> 403
    const otherTenantTok = await mintSessionToken(key, {
      jti: "jti_b",
      projectId: "p1",
      tenantId: "tenant_b",
      accountId: "a1",
      memberId: "m1",
      role: "admin",
      membershipRev: 1,
      permsLo: 1,
      permsHi: 0,
      anonymous: false,
      iatSec: 900,
      expSec: 2000,
    });
    const rCross = await serveCheckpointRequest(
      new Request(url, { headers: { Authorization: `Bearer ${otherTenantTok}` } }),
      { bucket, verifier, nowSec: 1000 },
    );
    expect(rCross?.status).toBe(403);

    // Revoked token -> 401
    const goodTok = await mintSessionToken(key, {
      jti: "jti_a",
      projectId: "p1",
      tenantId: "tenant_a",
      accountId: "a1",
      memberId: "m1",
      role: "admin",
      membershipRev: 1,
      permsLo: 1,
      permsHi: 0,
      anonymous: false,
      iatSec: 900,
      expSec: 2000,
    });
    verifier.revocations.revokeToken("jti_a", 2000);
    const rRevoked = await serveCheckpointRequest(
      new Request(url, { headers: { Authorization: `Bearer ${goodTok}` } }),
      { bucket, verifier, nowSec: 1000 },
    );
    expect(rRevoked?.status).toBe(401);

    // None of the rejected/forbidden requests touched R2!
    expect(bucket.getCalls).toBe(0);
    expect(bucket.headCalls).toBe(0);

    // Non-GET/HEAD -> 405
    const rPost = await serveCheckpointRequest(new Request(url, { method: "POST" }), { bucket, verifier, nowSec: 1000 });
    expect(rPost?.status).toBe(405);

    // Non-matching path -> null (router fallthrough)
    const rOtherPath = await serveCheckpointRequest(
      new Request("https://edge.whagons.com/api/health"),
      { bucket, verifier, nowSec: 1000 },
    );
    expect(rOtherPath).toBeNull();
  });

  it("prunes old checkpoint runs beyond retainCheckpoints while preserving latest runs", async () => {
    const id = env.HELLO.idFromName("checkpoint-prune-test");
    const stub = env.HELLO.get(id);
    const bucket = new MemoryCheckpointBucket();
    const tenantId = "tenant_gc";

    await runInDurableObject(stub, async (_instance, state) => {
      ensureTxLogSchema(state.storage.sql);
      state.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS tasks (
          rid INTEGER PRIMARY KEY,
          ver INTEGER NOT NULL,
          workspace_id INTEGER NOT NULL,
          status INTEGER NOT NULL,
          priority TEXT NOT NULL,
          title TEXT NOT NULL,
          updated_at INTEGER NOT NULL
        )
      `);
      const compactor = new CheckpointCompactor(state.storage);

      for (let step = 1; step <= 4; step++) {
        runTx(state.storage, (tx) => {
          tx.exec(
            "INSERT OR REPLACE INTO tasks (rid, ver, workspace_id, status, priority, title, updated_at) VALUES (?, ?, 1, 0, 'high', ?, ?)",
            step,
            step,
            `Row ${step}`,
            1700000000000 + step,
          );
          tx.recordUpsert(0, step, 1);
        });
        await compactor.compact({
          tenantId,
          bucket,
          tables: [TASK_TABLE_SPEC],
          nowMs: 1700000000000 + step * 1000,
          retainCheckpoints: 2,
        });
      }
    });

    const keys = [...bucket.store.keys()].sort();
    // Only latest.json + runs seq=3 and seq=4 (each: 0-0.wlp1 + manifest.json) should remain!
    expect(keys).toEqual([
      "checkpoints/tenant_gc/1/3/0-0.wlp1",
      "checkpoints/tenant_gc/1/3/manifest.json",
      "checkpoints/tenant_gc/1/4/0-0.wlp1",
      "checkpoints/tenant_gc/1/4/manifest.json",
      "checkpoints/tenant_gc/latest.json",
    ]);
  });
});
