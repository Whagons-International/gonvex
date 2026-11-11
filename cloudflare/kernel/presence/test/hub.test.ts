/**
 * The hub against the unit tests of `rust/crates/runtime/src/presence.rs`
 * (`mod tests`, at `0bf3ad13`), one `it` per Rust test where the behaviour
 * lives in the hub, plus the parsing and ordering helpers.
 */
import { describe, expect, it } from "vitest";
import {
  DEFAULT_PRESENCE_CONFIG, PresenceHub, type Publication, compareCodePoints, mergePublications, parseState, presenceConfig, scopeKey,
} from "../hub";

const A = scopeKey("project", "a");
const hub = (over: Partial<typeof DEFAULT_PRESENCE_CONFIG> = {}) => new PresenceHub({ ...DEFAULT_PRESENCE_CONFIG, ...over });
const changes = (p: Publication | undefined) => p?.changes.map(c => [c.memberId, c.state]);
const T0 = 1_000_000;

describe("presence hub (presence.rs tests)", () => {
  it("transitions are published once per tick and only on change", () => {
    const h = hub();
    const snapshot = JSON.parse(h.subscribe("viewer", "gw", A, "p1", T0, 1));
    expect(snapshot).toEqual({ type: "presence.snapshot", id: "p1", serverTime: 1, members: [] });
    h.setState("c1", "gw", A, "m1", "active", false, T0);
    h.setState("c1", "gw", A, "m1", "idle", false, T0);
    h.setState("c1", "gw", A, "m1", "active", false, T0);
    h.setState("c2", "gw", A, "m2", "idle", false, T0);
    const [p] = h.tick(T0, 1_000);
    expect(changes(p)).toEqual([["m1", "active"], ["m2", "idle"]]);
    expect(JSON.parse(p!.encoded)).toEqual({ type: "presence.patch", serverTime: 1_000, changes: [{ memberId: "m1", state: "active", since: 1_000 }, { memberId: "m2", state: "idle", since: 1_000 }] });
    expect(p!.gateways).toEqual(["gw"]);
    // Re-reporting the same state publishes nothing.
    h.setState("c1", "gw", A, "m1", "active", false, T0);
    expect(h.tick(T0, 2_000)).toEqual([]);
    // A flap inside one tick is invisible.
    h.setState("c1", "gw", A, "m1", "idle", false, T0);
    h.setState("c1", "gw", A, "m1", "active", false, T0);
    expect(h.tick(T0, 3_000)).toEqual([]);
  });

  it("the leading edge publishes the first change at once", () => {
    const h = hub();
    h.subscribe("viewer", "gw", A, "p", T0, 0);
    expect(h.runDue(T0, 1).next).toBeNull();
    h.setState("c1", "gw", A, "m1", "active", false, T0);
    const first = h.runDue(T0, 1);
    expect(changes(first.publications[0])).toEqual([["m1", "active"]]);
    expect(first.next).toBeNull();
    h.setState("c1", "gw", A, "m1", "idle", false, T0 + 400);
    const second = h.runDue(T0 + 400, 2);
    expect([changes(second.publications[0]), second.next]).toEqual([[["m1", "idle"]], null]);
  });

  it("changes inside the window coalesce into one trailing patch", () => {
    const h = hub();
    h.subscribe("viewer", "gw", A, "p", T0, 0);
    h.setState("c1", "gw", A, "m1", "active", false, T0);
    expect(h.runDue(T0, 1).publications).toHaveLength(1);
    h.setState("c1", "gw", A, "m1", "idle", false, T0 + 100);
    h.setState("c2", "gw", A, "m2", "active", false, T0 + 150);
    const inside = h.runDue(T0 + 100, 2);
    expect(inside.publications).toEqual([]);
    expect(inside.next).toBe(T0 + 250);
    const trailing = h.runDue(T0 + 250, 3);
    expect(changes(trailing.publications[0])).toEqual([["m1", "idle"], ["m2", "active"]]);
    expect(trailing.publications[0]!.serverTime).toBe(3);
    expect(trailing.next).toBeNull();
    // The trailing publication opened the next window.
    h.setState("c1", "gw", A, "m1", "active", false, T0 + 300);
    expect(h.runDue(T0 + 300, 4).next).toBe(T0 + 500);
  });

  it("grace and window schedule without polling", () => {
    const h = hub();
    h.setState("c1", "gw", A, "m1", "active", false, T0);
    h.runDue(T0, 1);
    h.disconnect("c1", T0 + 1_000);
    const during = h.runDue(T0 + 1_000, 2);
    expect([during.publications.length, during.next]).toEqual([0, T0 + 11_000]);
    const after = h.runDue(T0 + 11_000, 3);
    expect([changes(after.publications[0]), after.next]).toEqual([[["m1", "offline"]], null]);
    expect(h.published(A)).toEqual([]);
  });

  it("presence never crosses tenants or projects", () => {
    const h = hub();
    const B = scopeKey("project", "b"), P = scopeKey("other", "a");
    h.subscribe("viewer-a", "gw", A, "a", T0, 0);
    const snapshotB = h.subscribe("viewer-b", "gw", B, "b", T0, 0);
    h.subscribe("viewer-p", "gw", P, "p", T0, 0);
    h.setState("c1", "gw", A, "same-member-id", "active", false, T0);
    const out = h.tick(T0, 1);
    expect(out.map(p => p.scope)).toEqual([A]);
    expect(snapshotB).not.toContain("same-member-id");
    expect(h.published(P)).toEqual([]);
    expect(h.published(A)).toHaveLength(1);
  });

  it("multiple tabs aggregate to the most present state", () => {
    const h = hub();
    h.setState("tab1", "gw", A, "m1", "idle", false, T0);
    h.setState("tab2", "gw", A, "m1", "active", false, T0);
    expect(changes(h.tick(T0, 1)[0])).toEqual([["m1", "active"]]);
    h.setState("tab2", "gw", A, "m1", "idle", false, T0);
    expect(changes(h.tick(T0, 2)[0])).toEqual([["m1", "idle"]]);
    // One tab going offline explicitly leaves the other.
    h.setState("tab2", "gw", A, "m1", "offline", false, T0);
    expect(h.tick(T0, 3)).toEqual([]);
    h.setState("tab1", "gw", A, "m1", "offline", false, T0);
    expect(changes(h.tick(T0, 4)[0])).toEqual([["m1", "offline"]]);
    expect(h.trackedMembers()).toBe(0);
  });

  it("disconnect goes offline only after the grace, and a reconnect hides the gap", () => {
    const h = hub();
    h.setState("c1", "gw", A, "m1", "active", false, T0);
    h.tick(T0, 1);
    h.disconnect("c1", T0);
    h.setState("c2", "gw", A, "m1", "active", false, T0 + 5_000);
    expect(h.tick(T0 + 20_000, 2)).toEqual([]);
    h.disconnect("c2", T0 + 20_000);
    expect(h.tick(T0 + 25_000, 5)).toEqual([]);
    const [p] = h.tick(T0 + 30_000, 6);
    expect(changes(p)).toEqual([["m1", "offline"]]);
    expect(p!.changes[0]!.since).toBe(6);
    expect(h.published(A)).toEqual([]);
  });

  it("the snapshot lists online Members sorted by id; patches follow it", () => {
    const h = hub();
    h.setState("c2", "gw", A, "m2", "idle", false, T0);
    h.setState("c1", "gw", A, "m1", "active", false, T0);
    h.tick(T0, 7);
    const snapshot = JSON.parse(h.subscribe("viewer", "gw", A, "s", T0, 8));
    expect(snapshot.members).toEqual([{ memberId: "m1", state: "active", since: 7 }, { memberId: "m2", state: "idle", since: 7 }]);
    h.setState("c3", "gw", A, "m3", "active", false, T0);
    expect(h.tick(T0, 9)[0]!.gateways).toEqual(["gw"]);
  });

  it("moving a connection to another tenant leaves the old one without grace", () => {
    const h = hub();
    const B = scopeKey("project", "b");
    h.setState("c1", "gw", A, "m1", "active", false, T0);
    h.tick(T0, 1);
    h.setState("c1", "gw", B, "m9", "active", false, T0);
    const out = h.tick(T0, 2);
    expect(out.map(p => [p.scope, changes(p)])).toEqual([[A, [["m1", "offline"]]], [B, [["m9", "active"]]]]);
    expect(h.published(B)[0]!.memberId).toBe("m9");
  });

  it("unsubscribed or closed connections receive nothing", () => {
    const h = hub();
    h.subscribe("viewer", "gw", A, "s", T0, 0);
    h.unsubscribe("viewer");
    h.setState("c1", "gw", A, "m1", "active", false, T0);
    expect(h.tick(T0, 1)[0]!.gateways).toEqual([]);
    h.subscribe("viewer2", "gw2", A, "s", T0, 0);
    h.disconnect("viewer2", T0);
    h.setState("c1", "gw", A, "m1", "idle", false, T0);
    expect(h.tick(T0, 2)[0]!.gateways).toEqual([]);
  });

  it("a legacy beat registers once and never overrides a reported state", () => {
    const h = hub();
    h.legacyBeat("c1", "gw", A, "m1", T0);
    h.tick(T0, 1);
    expect(h.published(A)[0]!.state).toBe("active");
    h.setState("c1", "gw", A, "m1", "idle", false, T0);
    h.legacyBeat("c1", "gw", A, "m1", T0);
    h.tick(T0, 2);
    expect(h.published(A)[0]!.state).toBe("idle");
    h.disconnect("c1", T0);
    h.tick(T0 + 10_001, 3);
    expect(h.published(A)).toEqual([]);
  });

  it("subscriber counts follow gateways", () => {
    const h = hub();
    h.subscribe("v1", "gw1", A, "s", T0, 0);
    h.subscribe("v2", "gw1", A, "s", T0, 0);
    h.subscribe("v3", "gw2", A, "s", T0, 0);
    expect(h.subscriberGateways(A).sort()).toEqual(["gw1", "gw2"]);
    h.unsubscribe("v1");
    expect(h.subscriberGateways(A).sort()).toEqual(["gw1", "gw2"]);
    h.disconnect("v2", T0);
    expect(h.subscriberGateways(A)).toEqual(["gw2"]);
  });

  it("restore rebuilds published state silently, with the sockets' own change times", () => {
    const h = hub();
    h.restore({ connectionId: "c1", projectId: "project", memberId: "m1", state: "idle", subscriptionId: null, since: 50 }, "gw", A, T0, 100);
    h.restore({ connectionId: "c2", projectId: "project", memberId: "m1", state: "active", subscriptionId: "s", since: 70 }, "gw", A, T0, 100);
    h.restore({ connectionId: "c3", projectId: "project", memberId: "m1", state: "active", subscriptionId: null, since: 60 }, "gw", A, T0, 100);
    expect(h.published(A)).toEqual([{ memberId: "m1", state: "active", since: 60 }]);
    expect(h.runDue(T0, 100).publications).toEqual([]);
    expect(h.subscriberGateways(A)).toEqual(["gw"]);
    h.disconnect("c2", T0);
    h.disconnect("c3", T0);
    expect(changes(h.tick(T0, 101)[0])).toEqual([["m1", "idle"]]);
  });

  it("the fixed tick (leading edge off) publishes on tick boundaries", () => {
    const h = hub({ leadingEdge: false, tickMs: 1_000 });
    h.setState("c1", "gw", A, "m1", "active", false, 10_200);
    expect(h.runDue(10_200, 1)).toEqual({ publications: [], next: 11_000 });
    expect(changes(h.runDue(11_000, 2).publications[0])).toEqual([["m1", "active"]]);
  });
});

describe("helpers", () => {
  it("parse_state trims Unicode whitespace as Rust does and lowercases ASCII only", () => {
    expect(parseState(" ACTIVE\t")).toBe("active");
    expect(parseState("Idle")).toBe("idle");
    expect(parseState("GONE")).toBe("offline");
    expect(parseState("offline")).toBe("offline");
    expect(parseState("\u0085active\u3000")).toBe("active");
    expect(parseState("\ufeffactive")).toBeNull(); // U+FEFF is not White_Space in Rust
    expect(parseState("busy")).toBeNull();
    expect(parseState("AC\u0130VE")).toBeNull();
  });

  it("orders Member ids by code point (Rust String order)", () => {
    const ids = ["\uffff", "\u{10000}", "b", "a"];
    expect([...ids].sort(compareCodePoints)).toEqual(["a", "b", "\uffff", "\u{10000}"]);
  });

  it("merges publications last state per Member wins, first-seen order", () => {
    const p = (serverTime: number, list: Array<[string, "active" | "idle" | "offline"]>): Publication => ({
      scope: A, serverTime, gateways: [], encoded: "", changes: list.map(([memberId, state]) => ({ memberId, state, since: serverTime })),
    });
    expect(JSON.parse(mergePublications([p(1, [["m2", "active"], ["m1", "active"]]), p(2, [["m1", "idle"]]), p(3, [["m3", "offline"]])])!)).toEqual({
      type: "presence.patch", serverTime: 3,
      changes: [{ memberId: "m2", state: "active", since: 1 }, { memberId: "m1", state: "idle", since: 2 }, { memberId: "m3", state: "offline", since: 3 }],
    });
    expect(mergePublications([])).toBeNull();
  });

  it("reads Rust's environment names", () => {
    expect(presenceConfig({})).toEqual(DEFAULT_PRESENCE_CONFIG);
    const c = presenceConfig({
      GONVEX_PRESENCE_TICK_MS: "500", GONVEX_PRESENCE_OFFLINE_GRACE_MS: "0", GONVEX_PRESENCE_LEADING_EDGE: "off",
      GONVEX_PRESENCE_LEGACY_BEAT_PATHS: " presence.beat , ", GONVEX_PRESENCE_LEGACY_TRANSITION_PATHS: "presence.transition",
    });
    expect([c.tickMs, c.offlineGraceMs, c.leadingEdge, c.legacyBeatPaths, c.legacyTransitionPaths]).toEqual([500, 0, false, ["presence.beat"], ["presence.transition"]]);
    expect(presenceConfig({ GONVEX_PRESENCE: "0" }).enabled).toBe(false);
    expect(presenceConfig({ GONVEX_PRESENCE_V2: "false" }).enabled).toBe(false);
  });
});
