// Real workerd KV/transaction coverage for 0058's holder metadata exception.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { HolderRecoveryStore, attachmentRecord, withoutRecovery } from "../recovery-store";
import { Session, type SessionRecord } from "../session";
import type { Ports } from "../ports";

function record(): SessionRecord {
  return Session.open({ connectionId: crypto.randomUUID(), publicTransport: false, host: "h", ports: {} as Ports,
    ready: { presence: false, runtimeVersion: "test" }, sink: { send: () => {}, close: () => {} }, timers: { later: () => () => {} },
  }).record;
}
async function withStore(body: (storage: DurableObjectStorage, store: HolderRecoveryStore) => void) {
  await runInDurableObject(env.HELLO.getByName(`recovery-${crypto.randomUUID()}`), (_instance, state) => body(state.storage, new HolderRecoveryStore(state.storage)));
}

describe("holder recovery metadata", () => {
  it("stores legal large Unicode args and multiple maps in <=64KiB chunks, restores with a small attachment", async () => {
    await withStore((storage, store) => {
      const r = record(), value = "🍉漢字".repeat(150_000);
      r.liveQueries = { q: { type: "query.subscribe", id: "q", path: "items.live", args: { value }, windowRevision: "memory-only" } };
      r.pendingLiveQueries = { pending: [{ type: "query.subscribe", id: "pending", path: "items.other", args: { value: value.slice(0, 100_000) } }] };
      r.controlQueries = { control: { request: { type: "query.subscribe", id: "control", path: "control.tenants.list", scope: "control", args: {} }, last: "memory-only-result" } };
      r.replicas = Object.fromEntries(Array.from({ length: 256 }, (_, i) => [`r${i}`, "items.all"]));
      r.pendingActions = { action: "agent.run" };
      const ref = store.write(r), attachment = { session: withoutRecovery(r), recovery: ref };
      expect(new TextEncoder().encode(JSON.stringify(attachment)).length).toBeLessThan(16_384);
      expect(ref.chunks).toBeGreaterThan(100);
      for (const [name, chunk] of storage.kv.list<{ text: string }>({ prefix: `session-recovery/${ref.generation}/c/` })) {
        expect(name).toContain(ref.generation);
        expect(new TextEncoder().encode(chunk.text).length).toBeLessThanOrEqual(65_536);
      }
      const restored = store.restore(structuredClone(attachment.session), ref);
      expect(restored.liveQueries?.q?.args).toEqual({ value });
      expect(restored.liveQueries?.q?.windowRevision).toBeUndefined();
      expect(restored.pendingLiveQueries?.pending?.[0]?.args).toEqual(r.pendingLiveQueries.pending?.[0]?.args);
      expect(restored.controlQueries?.control?.last).toBe("");
      expect(restored.replicas).toEqual(r.replicas);
      expect(restored.pendingActions).toEqual(r.pendingActions);
      store.abandon(ref.socketOwner);
      expect([...storage.kv.list({ prefix: "session-recovery/" })]).toEqual([]);
    });
  });

  it("rejects missing/corrupt generations and scope changes before mutating the recovered maps", async () => {
    await withStore((storage, store) => {
      const r = record(); r.pendingActions = { a: "agent.run" };
      const ref = store.write(r), attachment = withoutRecovery(r);
      expect(() => store.restore({ ...attachment, subscriptionEpoch: 9 }, ref)).toThrow("scope/manifest mismatch");
      const first = [...storage.kv.list<{ text: string; end: boolean }>({ prefix: `session-recovery/${ref.generation}/c/` })].find(([, chunk]) => chunk.text.includes("agent.run"))!;
      storage.kv.put(first[0], { ...first[1], text: first[1].text.replace("agent.run", "other.run") });
      const target = structuredClone(attachment);
      expect(() => store.restore(target, ref)).toThrow("digest mismatch");
      expect(target.pendingActions).toBeUndefined();
      store.abandon(ref.socketOwner);
      expect(() => store.restore(attachment, ref)).toThrow("scope/manifest mismatch");
    });
  });

  it("moves a legal large identity out of the attachment and fences token rotation and tenant/Member switches", async () => {
    await withStore((_storage, store) => {
      const r = record();
      const identity = { projectId: "p", account: { id: "a", email: "", emailVerified: false, name: "", avatarUrl: "", provider: "" } };
      const tenant = { identity, route: { projectId: "p", tenantId: "t", databaseUrl: "sqlite:///t.db" },
        member: { id: "m", accountId: "a", status: "active", role: "member", displayName: "", avatarUrl: "", permissions: { value: "x".repeat(2_000_000) }, membershipRevision: 1 },
        admissionRevision: 0, delegation: null, developerSession: null, anonymous: false };
      r.tenant = tenant;
      r.connection = { projectId: "p", tenant, identity, impersonationId: "", authToken: "private-synthetic-token" };
      const before = new TextEncoder().encode(JSON.stringify(withoutRecovery(r))).length;
      const ref = store.write(r), packed = attachmentRecord(r);
      const after = new TextEncoder().encode(JSON.stringify({ session: packed, recovery: ref })).length;
      console.log(`K22 recovery identity fixture: JSON attachment bytes ${before} -> ${after}; private generation chunks ${ref.chunks}`);
      expect(before).toBeGreaterThan(4_000_000);
      expect(after).toBeLessThan(16_384);
      expect(packed.connection).toBeUndefined();
      const restored = store.restore(structuredClone(packed), ref);
      expect(restored.tenant?.member.permissions).toEqual(tenant.member.permissions);
      expect(restored.connection.authToken).toBe(r.connection.authToken);
      expect(() => store.restore({ ...packed, subscriptionEpoch: 2 }, ref)).toThrow("scope/manifest mismatch");
      const rotated = structuredClone(r); rotated.connection.authToken = "rotated";
      expect(() => store.restore(withoutRecovery(rotated), ref)).toThrow("scope/manifest mismatch");
      const switched = structuredClone(r); switched.tenant!.member.id = "other-member";
      expect(() => store.restore(withoutRecovery(switched), ref)).toThrow("scope/manifest mismatch");
      store.abandon(ref.socketOwner);
      expect(() => store.restore(packed, ref)).toThrow("scope/manifest mismatch");
    });
  });

  it("rolls back partial writes; sweeping abandons only generations with no live attachment reference", async () => {
    await withStore((storage, store) => {
      const r = record(); r.pendingActions = { a: "agent.run" };
      const old = store.write(r);
      let puts = 0;
      const failing = new HolderRecoveryStore({ transactionSync: (fn) => storage.transactionSync(fn), kv: {
        get: (key) => storage.kv.get(key), list: (options) => storage.kv.list(options), delete: (key) => storage.kv.delete(key),
        put: (key, value) => { if (++puts === 2) throw new Error("injected partial write"); storage.kv.put(key, value); },
      } });
      expect(() => failing.write(r)).toThrow("injected partial write");
      expect(store.restore(withoutRecovery(r), old).pendingActions).toEqual({ a: "agent.run" });
      // A published (or uncertain pending) socket owner remains protected even
      // when getWebSockets omits its closing socket. Time is not authority.
      const now = Date.now();
      store.sweep(new Set(), now + 365 * 24 * 3600_000);
      expect(store.restore(withoutRecovery(r), old).pendingActions).toEqual({ a: "agent.run" });
      // Another physical socket with the same synthetic connection id has a
      // different owner token; a late close cannot erase its generation.
      const newer = store.write(r);
      store.abandon(old.socketOwner);
      expect(store.restore(withoutRecovery(r), newer).pendingActions).toEqual({ a: "agent.run" });
      store.abandon(newer.socketOwner);
      expect([...storage.kv.list({ prefix: "session-recovery/" })]).toEqual([]);
    });
  });
});
