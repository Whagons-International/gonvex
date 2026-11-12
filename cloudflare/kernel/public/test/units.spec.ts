/**
 * K16 rules without sockets: address buckets, configuration, the fixed
 * window, the address budget (slots and audit) and the refused-frame errors.
 * Vectors follow Rust's own unit tests in public_functions.rs (286-470).
 */
import { describe, expect, it } from "vitest";
import { addressBucket, clientAddress, UNKNOWN_ADDRESS } from "../address";
import { DEFAULT_PUBLIC_LIMITS, PublicConfigError, publicLimits } from "../config";
import { frameCost, publicAdmissionErrors, publicLimitErrors, runsTenantCode } from "../frames";
import { AddressBudget, AUDIT_GRACE_MS, AUDIT_INTERVAL_MS, type AdmitRequest, MemorySlotStore, PENDING_LIMIT_ERROR, PUBLIC_LIMIT_ERROR } from "../limiter";
import { admitUpgrade, LIMITER_UNAVAILABLE, publicGuard } from "../guard";
import { allowCalls, newWindow } from "../window";
import type { ClientFrame } from "../../session/decode";

describe("client address", () => {
  it("buckets IPv4 alone, IPv4-mapped IPv6 with its IPv4 address, and other IPv6 by /64 (ipv6_clients_share_all_budgets_within_a_64)", () => {
    expect(addressBucket("198.51.100.23")).toBe("198.51.100.23");
    expect(addressBucket("2001:db8:1234:5678::1")).toBe(addressBucket("2001:db8:1234:5678:ffff::abcd"));
    expect(addressBucket("2001:db8:1234:5678::1")).not.toBe(addressBucket("2001:db8:1234:5679::1"));
    expect(addressBucket("::ffff:192.0.2.1")).toBe("192.0.2.1");
    expect(addressBucket("::ffff:c000:201")).toBe("192.0.2.1");
    expect(addressBucket("2001:DB8:0:0:0:0:0:1")).toBe(addressBucket("2001:db8::2"));
    expect(addressBucket("::1")).toBe("0:0:0:0::/64");
  });

  it("refuses what Rust's IpAddr parser refuses", () => {
    for (const bad of ["unknown", "", "198.51.100.23:80", "[2001:db8::1]", "2001:db8::1%eth0", "01.2.3.4", "256.1.1.1", "1.2.3", "1::2::3", "1:2:3:4:5:6:7:8:9", "1.2.3.4::"]) {
      expect(addressBucket(bad), bad).toBeNull();
    }
  });

  it("takes CF-Connecting-IP, and one shared bucket when it is missing, repeated or invalid (configured_headers_require_cidr_trust_and_valid_ips)", () => {
    expect(clientAddress(new Headers({ "cf-connecting-ip": " 198.51.100.23 " }))).toBe("198.51.100.23");
    expect(clientAddress(new Headers({ "x-real-ip": "192.0.2.99" }))).toBe(UNKNOWN_ADDRESS);
    expect(clientAddress(new Headers({ "cf-connecting-ip": "198.51.100.23, 192.0.2.1" }))).toBe(UNKNOWN_ADDRESS);
    const repeated = new Headers();
    repeated.append("cf-connecting-ip", "198.51.100.23");
    repeated.append("cf-connecting-ip", "192.0.2.1");
    expect(clientAddress(repeated)).toBe(UNKNOWN_ADDRESS);
    expect(clientAddress(new Headers({ "cf-connecting-ip": "unknown" }))).toBe(UNKNOWN_ADDRESS);
  });
});

describe("configuration", () => {
  it("keeps Rust's defaults and reads each variable as a positive integer", () => {
    expect(publicLimits({})).toEqual(DEFAULT_PUBLIC_LIMITS);
    expect(publicLimits({ GONVEX_PUBLIC_CALLS_PER_IP: "", GONVEX_PUBLIC_CALLS_PER_CONNECTION: " 7 " })).toMatchObject({ addressCalls: 120, connectionCalls: 7 });
    expect(publicLimits({ GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS: "5" }).actionTimeoutMs).toBe(5_000);
    for (const bad of ["0", "-1", "1.5", "ten", "1e3"]) expect(() => publicLimits({ GONVEX_PUBLIC_CONNECTIONS_PER_IP: bad }), bad).toThrow(PublicConfigError);
  });
});

describe("call window", () => {
  it("charges until the limit, refuses what does not fit, and restarts after 60 s (budgets_count_calls_and_release_connection_slots)", () => {
    const w = newWindow(0);
    expect(allowCalls(w, 2, 2, 0)).toBe(true);
    expect(allowCalls(w, 1, 2, 59_999)).toBe(false);
    expect(allowCalls(w, 2, 2, 61_000)).toBe(true);
    expect(w).toEqual({ startedAt: 61_000, count: 2 });
    const batch = newWindow(0);
    expect(allowCalls(batch, 3, 4, 0)).toBe(true);
    // A batch that does not fit whole is refused whole and charges nothing.
    expect(allowCalls(batch, 2, 4, 0)).toBe(false);
    expect(allowCalls(batch, 1, 4, 0)).toBe(true);
  });
});

describe("address budget", () => {
  const request = (connectionId: string, publicTransport: boolean, gateway = "gw-0"): AdmitRequest =>
    ({ v: 1, connectionId, gateway, publicTransport, pendingLimit: 3, publicLimit: 2 });

  it("reserves pending and public slots at the limits; a refused public slot leaves no pending slot", async () => {
    const budget = new AddressBudget(() => ({ liveConnections: async (ids) => ids }));
    expect(await budget.admit(request("a", true))).toEqual({ ok: true });
    expect(await budget.admit(request("b", true))).toEqual({ ok: true });
    expect(await budget.admit(request("c", true))).toEqual({ ok: false, error: PUBLIC_LIMIT_ERROR });
    expect(budget.count("pending")).toBe(2);
    expect(await budget.admit(request("c", false))).toEqual({ ok: true });
    expect(await budget.admit(request("d", false))).toEqual({ ok: false, error: PENDING_LIMIT_ERROR });
    budget.release({ v: 1, connectionId: "a", kinds: ["pending"] });
    expect(budget.count("pending")).toBe(2);
    expect(budget.count("public")).toBe(2);
    expect(await budget.admit(request("d", false))).toEqual({ ok: true });
    budget.release({ v: 1, connectionId: "b", kinds: ["pending", "public"] });
    budget.release({ v: 1, connectionId: "b", kinds: ["pending", "public"] });
    expect(budget.count("public")).toBe(1);
  });

  it("audits leaked slots only when full, after the grace, at most once per interval", async () => {
    let now = 0;
    const asked: string[][] = [];
    const live = new Set(["a"]);
    const budget = new AddressBudget(() => ({ liveConnections: async (ids) => { asked.push(ids); return ids.filter(id => live.has(id)); } }), () => now);
    await budget.admit(request("a", true, "gw-1"));
    await budget.admit(request("b", true, "gw-2"));
    // Full, but both slots are inside the grace: nothing is asked and the upgrade is refused.
    expect(await budget.admit(request("c", true))).toEqual({ ok: false, error: PUBLIC_LIMIT_ERROR });
    expect(asked).toEqual([]);
    now = AUDIT_GRACE_MS + AUDIT_INTERVAL_MS;
    // `b` is gone from its gateway (a reset): its slots are dropped and `c` gets them.
    expect(await budget.admit(request("c", true))).toEqual({ ok: true });
    expect(asked.flat().sort()).toEqual(["a", "b"]);
    expect(await budget.admit(request("d", true))).toEqual({ ok: false, error: PUBLIC_LIMIT_ERROR });
    expect(asked.length).toBe(2);
  });

  it("keeps the slots of a gateway it cannot reach", async () => {
    let now = 0;
    const budget = new AddressBudget(() => ({ liveConnections: async () => { throw new Error("unreachable"); } }), () => now);
    await budget.admit(request("a", true));
    await budget.admit(request("b", true));
    now = 60_000;
    expect(await budget.admit(request("c", true))).toEqual({ ok: false, error: PUBLIC_LIMIT_ERROR });
    expect(budget.count("public")).toBe(2);
  });

  it("persists its slots: a budget rebuilt from the store (an evicted object) keeps the caps", async () => {
    const store = new MemorySlotStore();
    const live = { liveConnections: async (ids: string[]) => ids };
    const first = new AddressBudget(() => live, () => 0, store);
    await first.admit(request("a", true, "gw-1"));
    await first.admit(request("b", true, "gw-2"));
    await first.admit(request("m", false, "gw-3"));
    expect([...store.entries]).toEqual([
      ["a", { gateway: "gw-1", at: 0, kinds: ["pending", "public"] }],
      ["b", { gateway: "gw-2", at: 0, kinds: ["pending", "public"] }],
      ["m", { gateway: "gw-3", at: 0, kinds: ["pending"] }],
    ]);
    first.release({ v: 1, connectionId: "a", kinds: ["pending"] });
    expect(store.entries.get("a")).toEqual({ gateway: "gw-1", at: 0, kinds: ["public"] });
    const woken = new AddressBudget(() => live, () => 0, store);
    expect([woken.count("pending"), woken.count("public")]).toEqual([2, 2]);
    expect(await woken.admit(request("c", true))).toEqual({ ok: false, error: PUBLIC_LIMIT_ERROR });
    woken.release({ v: 1, connectionId: "a", kinds: ["public"] });
    expect(store.entries.has("a")).toBe(false);
    expect(await woken.admit(request("c", true))).toEqual({ ok: true });
  });

  it("the sweep drops leaked slots past the grace whatever the fill, and the store with them", async () => {
    let now = 0;
    const store = new MemorySlotStore();
    const budget = new AddressBudget(() => ({ liveConnections: async () => ["kept"] }), () => now, store);
    await budget.admit(request("kept", false));
    await budget.admit(request("leaked", true));
    now = AUDIT_GRACE_MS - 1;
    await budget.sweep();
    expect(budget.count("pending")).toBe(2);
    now = AUDIT_GRACE_MS;
    await budget.sweep();
    expect([budget.count("pending"), budget.count("public")]).toEqual([1, 0]);
    expect([...store.entries.keys()]).toEqual(["kept"]);
    expect(budget.holding).toBe(true);
  });

  it("charges the address window from the first call", () => {
    let now = 5_000;
    const budget = new AddressBudget(() => ({ liveConnections: async (ids) => ids }), () => now);
    expect(budget.allow({ v: 1, cost: 2, limit: 3 })).toBe(true);
    expect(budget.allow({ v: 1, cost: 2, limit: 3 })).toBe(false);
    now = 64_999;
    expect(budget.allow({ v: 1, cost: 1, limit: 3 })).toBe(true);
    now = 65_000;
    expect(budget.allow({ v: 1, cost: 3, limit: 3 })).toBe(true);
  });
});

describe("refused frames", () => {
  const call = (id: string) => ({ id, path: "p.r", args: {} });
  it("costs one per call, one per batch item (at least one), nothing for a native-rows ack", () => {
    expect(frameCost({ type: "auth", id: "a", controlOnly: false, public: true } as ClientFrame, false)).toBe(1);
    expect(frameCost({ type: "reducer.callMany", calls: [call("1"), call("2"), call("3")] } as ClientFrame, false)).toBe(3);
    expect(frameCost({ type: "reducer.callMany", calls: [] } as unknown as ClientFrame, false)).toBe(1);
    expect(frameCost({ type: "replica.snapshotAck", id: "s", snapshotId: "x", sequence: 1 }, true)).toBe(0);
    expect(frameCost({ type: "replica.snapshotAck", id: "s", snapshotId: "x", sequence: 1 }, false)).toBe(1);
  });

  it("answers each refused call with Rust's correlated error", () => {
    expect(publicLimitErrors({ type: "auth", id: "a1", controlOnly: false, public: true } as ClientFrame))
      .toEqual(['{"type":"auth.error","id":"a1","error":"public call rate limit exceeded","code":"transient"}']);
    expect(publicLimitErrors({ type: "reducer.callMany", calls: [call("1"), call("2")] } as ClientFrame)).toEqual([
      '{"type":"reducer.error","id":"1","path":"p.r","error":"public call rate limit exceeded","class":"rejected","retryable":false}',
      '{"type":"reducer.error","id":"2","path":"p.r","error":"public call rate limit exceeded","class":"rejected","retryable":false}',
    ]);
    expect(publicLimitErrors({ type: "action.call", id: "x", path: "p.a", args: {}, trace: { clientSentAtMs: 1 } } as ClientFrame))
      .toEqual(['{"type":"action.error","id":"x","path":"p.a","error":"public call rate limit exceeded"}']);
    expect(publicLimitErrors({ type: "query.unsubscribe", id: "q" }))
      .toEqual(['{"type":"auth.error","id":"public-rate-limit","error":"public call rate limit exceeded","code":"transient"}']);
    expect(publicAdmissionErrors({ type: "query.unsubscribe", id: "q" }))
      .toEqual(['{"type":"auth.error","id":"public-rate-limit","error":"public tenant session is unavailable","code":"transient"}']);
    expect(publicAdmissionErrors({ type: "replica.openMany", opens: [{ id: "o", path: "c.l", args: {}, keys: [], hashes: {}, fullIntegrity: false }] }))
      .toEqual(['{"type":"replica.error","id":"o","path":"c.l","error":"public tenant session is unavailable"}']);
  });

  it("re-reads admission before the frames that run tenant code", () => {
    const tenantCode = ["query.call", "query.subscribe", "query.subscribeMany", "replica.open", "replica.openMany", "reducer.call", "action.call"];
    const other = ["auth", "query.unsubscribe", "replica.close", "reducer.callMany", "presence.set", "telemetry.batch"];
    for (const type of tenantCode) expect(runsTenantCode({ type } as ClientFrame), type).toBe(true);
    for (const type of other) expect(runsTenantCode({ type } as ClientFrame), type).toBe(false);
  });
});

describe("limiter failure modes (0045)", () => {
  const unreachable = { getByName: () => ({ admit: async () => { throw new Error("object unreachable"); }, allow: async () => { throw new Error("object unreachable"); }, release: async () => { throw new Error("object unreachable"); } }) };
  const env = { PUBLIC_LIMITER: unreachable as unknown as DurableObjectNamespace };

  it("refuses an upgrade with 503 when the limiter cannot be reached", async () => {
    const response = await admitUpgrade(env, { address: "198.51.100.9", connectionId: "c", gateway: "gw-0", publicTransport: true });
    expect(response?.status).toBe(503);
    expect(await response?.text()).toBe(LIMITER_UNAVAILABLE);
  });

  it("keeps charging calls fail-open, and a failed release does not throw", async () => {
    const guard = publicGuard(env);
    expect(await guard.allowAddress("198.51.100.9", 1)).toBe(true);
    await expect(guard.release("198.51.100.9", "c", ["pending"])).resolves.toBeUndefined();
  });
});
