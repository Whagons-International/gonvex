// Tier-1 hot-path tests: warm/resumable SnapshotCache reuse and per-host shared payload scoping.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { CaptureState, ChangeWake, directSql, installChangeFeed, invokeCaptured } from "../../capture/index.ts";
import type { Negotiated } from "../../session/capabilities.ts";
import type { SocketView, TenantSession } from "../../session/ports.ts";
import { TenantStorage, type StorageSession } from "../../storage";
import { VisibilityRegistry } from "../../visibility";
import { parsePlan } from "../../visibility/plan.ts";
import { parseDefinition, type ReplicaModule } from "../definition.ts";
import { ReplicaDeliveryEngine } from "../engine.ts";
import { LocalOutlet } from "../outlet.ts";
import type { ReplicaDelivery, ReplicaReceipt } from "../protocol.ts";
import { snapshotCache } from "../snapshot-cache.ts";
import { openReplica } from "../subscription.ts";

const NEGOTIATED: Negotiated = {
  snapshotPages: true, noOldValues: true, fieldPatches: false, batchFrames: true,
  nativeRows: false, setDigest: true, telemetryBatch: false, presence: false,
};

function makeSession(member: string): TenantSession {
  return {
    identity: { projectId: "p", account: { id: `acct-${member}`, email: "", emailVerified: false, name: member, avatarUrl: "", provider: "" } },
    route: { projectId: "p", tenantId: "t", databaseUrl: "sqlite:///t.db" },
    member: { id: `m-${member}`, accountId: `acct-${member}`, status: "active", displayName: member, avatarUrl: "", role: "admin", permissions: {}, membershipRevision: 0 },
    admissionRevision: 0, delegation: null, developerSession: null, anonymous: false,
  };
}

function makeSocket(id: string, member: string, host: string): SocketView {
  const tenant = makeSession(member);
  return {
    connectionId: id,
    connection: { projectId: "p", identity: tenant.identity, tenant, impersonationId: "", authToken: "" },
    tenant, negotiated: NEGOTIATED, publicTransport: false, host,
    replica: { scope: "scope-1", visibilityScope: "vscope-1" }, subscriptionEpoch: 0,
  };
}

const MODULE: ReplicaModule = {
  projectId: "p",
  artifactHash: "h-warm",
  functions: new Map([
    ["items.all", {
      path: "items.all", kind: "query", delivery: "replica",
      replica: parseDefinition({ table: "items", key: "id", columns: ["id", "rank", "label"], orderBy: "rank", orderDirection: "asc", mode: "eager", maxRows: 10000 }),
    }],
  ]),
  visibility: new Map([["items", parsePlan({ version: 2, table: "items", key: "id", sets: {}, where: { operator: "public" } })]]),
};

describe("Tier-1 delivery hot-path optimizations", () => {
  it("reuses SnapshotCache on warm/resumable aggregate and reconcile opens with 0 table scan rows", async () => {
    await runInDurableObject(env.HELLO.getByName(`warm-cache-${crypto.randomUUID()}`), async (_inst, state) => {
      const sql = directSql(state.storage.sql);
      state.storage.transactionSync(() => {
        installChangeFeed(sql);
        sql.run("CREATE TABLE members (id TEXT PRIMARY KEY, account_id TEXT, status TEXT, role TEXT, permissions JSON, membership_revision INTEGER NOT NULL DEFAULT 0)");
        sql.run("INSERT INTO members (id, account_id, status, role, permissions) VALUES ('m-alice', 'acct-alice', 'active', 'admin', '{}')");
        sql.run("CREATE TABLE items (id TEXT PRIMARY KEY, rank INTEGER, label TEXT)");
        installChangeFeed(sql);
      });

      const storage = new TenantStorage(state.storage);
      const context = { state: new CaptureState(), wake: new ChangeWake() };
      const registry = new VisibilityRegistry();
      const session = makeSession("alice");

      const seeded = await invokeCaptured(storage, "reducer", { commandId: "seed" }, async (db) => {
        for (let i = 0; i < 60; i++) db.run("INSERT INTO items VALUES ($1, $2, $3)", [`item-${String(i).padStart(3, "0")}`, i, `label-${i}`]);
      }, context);
      if (!seeded.ok) throw seeded.error;

      const read = async <T>(body: (db: StorageSession) => T): Promise<T> => {
        const res = await storage.invoke("query", async (db) => body(db));
        if (!res.ok) throw res.error;
        return res.value;
      };

      const coldFrames: string[] = [];
      const coldProgress = { statements: 0, rowsRead: 0 };
      const coldSub = await openReplica(
        { id: "c1", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false },
        { session, module: MODULE, sumNegotiated: true, paged: false, scope: "scope-1", visibilityScope: "vscope-1", registry, read, emit: (f) => coldFrames.push(...f), drain: async () => {}, progress: coldProgress },
      );
      expect(coldSub).toBeDefined();
      expect(coldProgress.rowsRead).toBeGreaterThanOrEqual(60);
      const ready = coldFrames.map((f) => JSON.parse(f)).find((f) => f.type === "replica.ready");
      expect(ready).toBeDefined();

      // Warm/resumable aggregate open hits SnapshotCache with 0 scanned table rows.
      const hitsBefore = snapshotCache.stats.hits;
      const warmFrames: string[] = [];
      const warmProgress = { statements: 0, rowsRead: 0 };
      const warmSub = await openReplica(
        { id: "c2", path: "items.all", args: {}, cursor: ready.cursor, digest: ready.digest, keys: [], hashes: {}, fullIntegrity: false },
        { session, module: MODULE, sumNegotiated: true, paged: false, scope: "scope-1", visibilityScope: "vscope-1", registry, read, emit: (f) => warmFrames.push(...f), drain: async () => {}, progress: warmProgress },
      );
      expect(warmSub).toBeDefined();
      expect(snapshotCache.stats.hits).toBe(hitsBefore + 1);
      expect(warmProgress.rowsRead).toBe(0);
      expect(warmFrames.map((f) => JSON.parse(f)).some((f) => f.type === "replica.ready" && f.digest === ready.digest)).toBe(true);

      // Warm/resumable reconcile open also hits SnapshotCache and returns exact diff (upserts + deleted).
      const recFrames: string[] = [];
      const recProgress = { statements: 0, rowsRead: 0 };
      const recSub = await openReplica(
        { id: "c3", path: "items.all", args: {}, cursor: ready.cursor, keys: ["item-000", "ghost-row"], hashes: { "item-000": "stale-hash", "ghost-row": "x" }, fullIntegrity: true },
        { session, module: MODULE, sumNegotiated: true, paged: false, scope: "scope-1", visibilityScope: "vscope-1", registry, read, emit: (f) => recFrames.push(...f), drain: async () => {}, progress: recProgress },
      );
      expect(recSub).toBeDefined();
      expect(snapshotCache.stats.hits).toBe(hitsBefore + 2);
      expect(recProgress.rowsRead).toBe(0);
      const delta = recFrames.map((f) => JSON.parse(f)).find((f) => f.type === "replica.delta");
      expect(delta.deleted).toEqual(["ghost-row"]);
      expect(delta.upserts).toHaveLength(60);
    });
  });

  it("scopes shared frame texts per gateway host during commit fan-out", async () => {
    await runInDurableObject(env.HELLO.getByName(`per-host-shared-${crypto.randomUUID()}`), async (_inst, state) => {
      const sql = directSql(state.storage.sql);
      state.storage.transactionSync(() => {
        installChangeFeed(sql);
        sql.run("CREATE TABLE members (id TEXT PRIMARY KEY, account_id TEXT, status TEXT, role TEXT, permissions JSON, membership_revision INTEGER NOT NULL DEFAULT 0)");
        sql.run("INSERT INTO members (id, account_id, status, role, permissions) VALUES ('m-alice', 'acct-alice', 'active', 'admin', '{}'), ('m-bob', 'acct-bob', 'active', 'admin', '{}')");
        sql.run("CREATE TABLE items (id TEXT PRIMARY KEY, owner TEXT, label TEXT)");
        installChangeFeed(sql);
      });

      const storage = new TenantStorage(state.storage);
      const context = { state: new CaptureState(), wake: new ChangeWake() };
      const payloads: Array<{ host: string; delivery: ReplicaDelivery }> = [];

      const scopedModule: ReplicaModule = {
        projectId: "p",
        artifactHash: "h-scoped",
        functions: new Map([
          ["items.all", {
            path: "items.all", kind: "query", delivery: "replica",
            replica: parseDefinition({ table: "items", key: "id", columns: ["id", "owner", "label"], orderBy: "id", orderDirection: "asc", mode: "eager", maxRows: 1000 }),
          }],
        ]),
        visibility: new Map([["items", parsePlan({ version: 2, table: "items", key: "id", sets: {}, where: { operator: "eqContext", column: "owner", context: "member.id" } })]]),
      };

      let queue: Promise<unknown> = Promise.resolve();
      const serial = <R>(fn: () => Promise<R>): Promise<R> => {
        const next = queue.then(fn, fn);
        queue = next.catch(() => undefined);
        return next;
      };
      const receive = (host: string, delivery: ReplicaDelivery): ReplicaReceipt => {
        payloads.push({ host, delivery });
        return { v: 1, sockets: Object.keys(delivery.sockets).length };
      };

      const engine = new ReplicaDeliveryEngine({
        tenantId: "t",
        delivered: () => {},
        serial,
        read: async (fn) => {
          const r = await storage.invoke("query", async (s) => fn(s));
          if (!r.ok) throw r.error;
          return r.value;
        },
        module: () => scopedModule,
        outlet: new LocalOutlet(receive, () => ["gw-a", "gw-b"], undefined, receive),
      });

      engine.dispatch(makeSocket("conn-a", "alice", "gw-a"), { type: "replica.open", id: "r1", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      engine.dispatch(makeSocket("conn-b", "bob", "gw-b"), { type: "replica.open", id: "r1", path: "items.all", args: {}, keys: [], hashes: {}, fullIntegrity: false });
      await engine.settled();
      payloads.length = 0;

      // Commit touches only Alice's item; gw-b (Bob) must NOT receive Alice's shared payload text.
      const res = await serial(() => invokeCaptured(storage, "reducer", { commandId: "c1" }, async (db) => {
        db.run("INSERT INTO items VALUES ('a1', 'm-alice', 'alice-secret-payload')");
      }, context));
      if (!res.ok) throw res.error;
      engine.wake(res.notice?.revision ?? 0);
      await engine.settled();

      const gwA = payloads.filter((p) => p.host === "gw-a");
      const gwB = payloads.filter((p) => p.host === "gw-b");
      expect(gwA.some((p) => p.delivery.shared.some((s) => s.includes("alice-secret-payload")))).toBe(true);
      expect(gwB.some((p) => p.delivery.shared.some((s) => s.includes("alice-secret-payload")))).toBe(false);
    });
  });
});
