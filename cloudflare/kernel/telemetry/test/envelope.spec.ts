/**
 * The pure port of telemetry.rs: scrubbing, attribution, fingerprints and
 * event ids, RFC 3339 timestamps, envelope limits, `error.ack` bytes and
 * performance-event validation. Expected values come from the Rust source
 * and, for fingerprints, from Rust's own answers in the golden run.
 */
import { describe, expect, it } from "vitest";
import type { Json } from "../../session/decode";
import {
  ACK_ERRORS, LIMITS, authoritativeEvent, boundedText, checkEnvelope, errorAck, fingerprint, generatedEventId, isSecretKey,
  parseRfc3339Millis, performanceRow, prepareEvents, validPerformanceEvent, withoutKeys,
} from "../envelope";

const MEMBER = { projectId: "project-real", tenantId: "tenant-real", connectionId: "connection-real", accountId: "account-real" };
const ANONYMOUS = { ...MEMBER, tenantId: "", accountId: null };

describe("scrubbing (scrub_object / scrub_value)", () => {
  it("removes secret keys at every depth, by ASCII-folded substring", () => {
    for (const key of ["password", "PassWD", "client_secret", "refresh-token", "Authorization", "set_cookie", "x-API_key", "apiKey", "tokens"]) {
      expect(isSecretKey(key), key).toBe(true);
    }
    for (const key of ["route", "author", "cook", "api", "TOKEN", "pass"]) expect(isSecretKey(key), key).toBe(false);
    const event = authoritativeEvent(MEMBER, {
      message: "boom", password: "x",
      context: { safe: "yes", Token: "x", nested: { "api-key": "x", list: [{ cookie: "c", ok: 1 }] } },
    } as Json) as any;
    expect(event.password).toBeUndefined();
    expect(event.context).toEqual({ safe: "yes", nested: { list: [{ ok: 1 }] } });
  });

  it("keeps `__proto__` keys as data at every depth (serde has no prototype)", async () => {
    const raw = JSON.parse('{"message":"m","__proto__":{"polluted":true},"context":{"__proto__":"v","safe":1,"list":[{"__proto__":2}]}}') as Json;
    const event = authoritativeEvent(MEMBER, raw)!;
    expect(Object.prototype.hasOwnProperty.call(event, "__proto__")).toBe(true);
    expect(Object.getPrototypeOf(event)).toBe(Object.prototype);
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
    const text = JSON.stringify(event);
    expect(text).toContain('"__proto__":{"polluted":true}');
    expect(text).toContain('"context":{"__proto__":"v","safe":1,"list":[{"__proto__":2}]}');
    const [prepared] = await prepareEvents(MEMBER, [raw]);
    expect(prepared!.payload).toBe(text);
  });

  it("bounds strings by Unicode scalar values, arrays to 50, objects to their first 100 keys", () => {
    const wide = Object.fromEntries(Array.from({ length: 105 }, (_, i) => [`k${i}`, i]));
    const event = authoritativeEvent(MEMBER, { message: "m", long: "\u{1F600}".repeat(4_001), list: Array.from({ length: 60 }, (_, i) => i), wide } as Json) as any;
    expect([...event.long]).toHaveLength(4_000);
    expect(event.list).toHaveLength(50);
    expect(Object.keys(event.wide)).toEqual(Array.from({ length: 100 }, (_, i) => `k${i}`));
    expect(boundedText("ab\u{1F600}cd", 3)).toBe("ab\u{1F600}");
  });

  it("truncates below depth 8: top-level values are depth 1, an object adds two levels, an array one", () => {
    const event = authoritativeEvent(MEMBER, { message: "m", a: { b: { c: { d: "deep" } } }, l: [[[[[[[["x"]]]]]]]] } as Json) as any;
    // a: depth 1 -> its values depth 3 -> 5 -> d at depth 7 (kept)
    expect(event.a.b.c.d).toBe("deep");
    const deeper = authoritativeEvent(MEMBER, { message: "m", a: { b: { c: { d: { e: "x" } } } } } as Json) as any;
    // e would be at depth 9
    expect(deeper.a.b.c.d.e).toBe("[Truncated]");
    // arrays: l at 1, then 2..8, the 8th nesting level's element is at depth 9
    expect(JSON.stringify(event.l)).toBe('[[[[[[[["[Truncated]"]]]]]]]]');
  });
});

describe("authoritative attribution", () => {
  it("replaces spoofed project, tenant, session and account (telemetry.rs unit test)", () => {
    const event = authoritativeEvent(MEMBER, {
      message: "boom", project: "spoof", tenant: "spoof", sessionId: "spoof", account: { id: "spoof" }, context: { password: "bad", safe: "yes" },
    } as Json) as any;
    expect(event.project).toBe("project-real");
    expect(event.tenant).toBe("tenant-real");
    expect(event.sessionId).toBe("connection-real");
    expect(event.account).toEqual({ id: "account-real" });
    expect(event.context.password).toBeUndefined();
    // Existing keys keep their position (IndexMap insert).
    expect(Object.keys(event)).toEqual(["message", "project", "tenant", "sessionId", "account", "context"]);
  });

  it("drops account, context and breadcrumbs for an anonymous connection with swap_remove order", () => {
    const event = authoritativeEvent(ANONYMOUS, { message: "m", account: { id: "x" }, a: 1, context: {}, b: 2, breadcrumbs: [], c: 3 } as Json)!;
    expect(Object.keys(event)).toEqual(["message", "sessionId", "a", "tenant", "b", "project", "c"]);
    expect(withoutKeys({ a: 1, b: 2, c: 3, d: 4 }, ["a"])).toEqual({ d: 4, b: 2, c: 3 });
    expect(Object.keys(withoutKeys({ a: 1, b: 2, c: 3, d: 4 }, ["a"]))).toEqual(["d", "b", "c"]);
  });

  it("needs an object with a non-blank string message (Rust's trim keeps U+FEFF)", () => {
    for (const value of [42, "x", null, [], {}, { message: 7 }, { message: " \t\n　" }] as Json[]) expect(authoritativeEvent(MEMBER, value)).toBeNull();
    expect(authoritativeEvent(MEMBER, { message: "﻿" })).not.toBeNull();
  });
});

describe("fingerprints and event ids", () => {
  it("matches the Rust runtime's fingerprints", async () => {
    // From the golden run: the scenario's EVENT_A and its anonymous event.
    expect(await fingerprint({ name: "TypeError", message: "Cannot read properties of undefined (reading 'id')", culprit: "src/tasks/grid.ts:42" })).toBe("fb488fd60ea17ac8dd3956d675caf332");
    expect(await fingerprint({ name: "AnonError", message: "anonymous failure" })).toBe("74706e8958662f3e627e69829343f877");
    expect(await fingerprint({ name: "Warn", message: "slow render", level: "warning" })).toBe("93372d47d3fc240c1f46d788fe9626e4");
  });

  it("generates `generated-` + 24 hex digits from timestamp|message|stack|deviceId", async () => {
    const id = await generatedEventId({ timestamp: "t", message: "m", stack: "s", deviceId: "d" });
    expect(id).toMatch(/^generated-[0-9a-f]{24}$/);
    expect(await generatedEventId({ timestamp: "t", message: "m", stack: "s", deviceId: "d", other: 1 })).toBe(id);
    expect(await generatedEventId({ timestamp: "t", message: "m", stack: "s", deviceId: "e" })).not.toBe(id);
  });

  it("prepares only kept events, in order, with persist_event's fields", async () => {
    const events = await prepareEvents(MEMBER, [
      7,
      { message: "a", eventId: "e1", level: "warning", release: "1", environment: "prod", deviceId: "d", culprit: "c".repeat(600), timestamp: "2025-01-01T00:00:00Z" },
      { message: "m".repeat(600), level: "fatal", timestamp: "nope" },
    ], () => 1234);
    expect(events).toHaveLength(2);
    expect(events[0]).toMatchObject({ eventId: "e1", level: "warning", release: "1", environment: "prod", device: "d", tenant: "tenant-real", account: "account-real", occurredAt: Date.UTC(2025, 0, 1) });
    expect(events[0]!.culprit).toHaveLength(500);
    expect(events[1]).toMatchObject({ level: "error", occurredAt: 1234 });
    expect(events[1]!.title).toHaveLength(500);
    expect(events[1]!.eventId).toMatch(/^generated-/);
    expect(JSON.parse(events[0]!.payload).sessionId).toBe("connection-real");
  });
});

describe("RFC 3339 (chrono parse_from_rfc3339)", () => {
  const cases: Array<[string, number | null]> = [
    ["2025-03-04T05:06:07Z", Date.UTC(2025, 2, 4, 5, 6, 7)],
    ["2025-03-04t05:06:07z", Date.UTC(2025, 2, 4, 5, 6, 7)],
    ["2025-03-04 05:06:07+02:00", Date.UTC(2025, 2, 4, 3, 6, 7)],
    ["2025-03-04T05:06:07.123456789-01:30", Date.UTC(2025, 2, 4, 6, 36, 7, 123)],
    ["2025-03-04T05:06:07.1234567891−05:30", Date.UTC(2025, 2, 4, 10, 36, 7, 123)],
    ["2016-12-31T23:59:60.5Z", Date.UTC(2017, 0, 1, 0, 0, 0, 500)],
    ["0001-01-01T00:00:00Z", -62_135_596_800_000],
    ["2024-02-29T00:00:00Z", Date.UTC(2024, 1, 29)],
    ["2023-02-29T00:00:00Z", null],
    ["2025-13-01T00:00:00Z", null],
    ["2025-01-01T24:00:00Z", null],
    ["2025-01-01T23:60:00Z", null],
    ["2025-01-01T23:59:61Z", null],
    // chrono accepts a leap second at the end of any minute (only `sec == 59` is checked).
    ["2025-01-01T10:00:60Z", Date.UTC(2025, 0, 1, 10, 1, 0)],
    ["2025-01-01T00:00:00+24:00", null],
    ["2025-01-01T00:00:00+23:60", null],
    ["2025-01-01T00:00:00+2300", null],
    ["2025-01-01T00:00:00", null],
    ["2025-01-01T00:00:00.Z", null],
    ["2025-01-01T00:00:00Z ", null],
    ["2025-01-01", null],
    ["not a time", null],
    ["", null],
  ];
  it.each(cases)("%s", (text, expected) => expect(parseRfc3339Millis(text)).toBe(expected));
});

describe("envelope limits (20 events, 32 KiB each, 256 KiB in total)", () => {
  const sized = (bytes: number, id = "e") => {
    const base = { eventId: id, message: "m", pad: "" };
    return { ...base, pad: "p".repeat(bytes - JSON.stringify(base).length) };
  };
  it("refuses an empty or a 21-event envelope as invalid", () => {
    expect(checkEnvelope([])).toBe(ACK_ERRORS.invalid);
    expect(checkEnvelope(Array.from({ length: 21 }, () => ({ message: "m" })))).toBe(ACK_ERRORS.invalid);
    expect(checkEnvelope(Array.from({ length: 20 }, () => ({ message: "m" })))).toBeNull();
  });
  it("counts serialized UTF-8 bytes: exactly 32 KiB and 256 KiB pass, one byte more does not", () => {
    expect(checkEnvelope([sized(LIMITS.maxEventBytes)])).toBeNull();
    expect(checkEnvelope([sized(LIMITS.maxEventBytes + 1)])).toBe(ACK_ERRORS.tooLarge);
    const eight = Array.from({ length: 8 }, (_, i) => sized(LIMITS.maxEventBytes, `e${i}`));
    expect(checkEnvelope(eight)).toBeNull();
    expect(checkEnvelope([...eight, {}])).toBe(ACK_ERRORS.tooLarge);
    // Multi-byte text counts in UTF-8: 11,000 three-byte characters exceed 32 KiB though the string is shorter.
    expect(checkEnvelope([{ message: "€".repeat(11_000) }])).toBe(ACK_ERRORS.tooLarge);
  });
});

describe("error.ack", () => {
  it("serializes the id, then accepted, error, fingerprints (BTreeMap order)", () => {
    expect(errorAck("a")).toBe('{"type":"error.ack","id":"a"}');
    expect(errorAck("a", { error: ACK_ERRORS.rateLimited })).toBe('{"type":"error.ack","id":"a","error":"error telemetry rate limit exceeded"}');
    expect(errorAck("a", { accepted: 0, fingerprints: [] })).toBe('{"type":"error.ack","id":"a","accepted":0,"fingerprints":[]}');
    expect(errorAck("a\"b", { accepted: 2, fingerprints: ["x", "y"] })).toBe('{"type":"error.ack","id":"a\\"b","accepted":2,"fingerprints":["x","y"]}');
  });
});

describe("performance events", () => {
  const ok = { id: "p", kind: "query", path: "q", outcome: "ok", clientReceivedAtMs: 1 };
  it("validates as capture_performance_event does", () => {
    expect(validPerformanceEvent("p", ok)).toBe(true);
    expect(validPerformanceEvent("", ok)).toBe(false);
    for (const bad of [
      { kind: "mutation" }, { outcome: "maybe" }, { id: "" }, { id: "i".repeat(201) }, { id: "é".repeat(101) }, { path: "" }, { path: "p".repeat(501) },
      { clientReceivedAtMs: Number.NaN }, { clientSentAtMs: Infinity }, { clientDurationMs: -0.5 }, { clientDurationMs: Number.NaN },
    ]) expect(validPerformanceEvent("p", { ...ok, ...bad }), JSON.stringify(bad)).toBe(false);
    expect(validPerformanceEvent("p", { ...ok, id: "i".repeat(200), path: "p".repeat(500), clientDurationMs: 0 })).toBe(true);
  });
  it("bounds the error to 1,000 chars and falls back to the auth device", () => {
    const row = performanceRow({ tenantId: "t", accountId: null }, { ...ok, error: "e".repeat(1_200) }, { browserName: "x" });
    expect(row.error).toHaveLength(1_000);
    expect(row.accountId).toBe("");
    expect(row.device).toBe('{"browserName":"x"}');
    expect(performanceRow({ tenantId: "t", accountId: "a" }, { ...ok, device: { platform: "y" } }, { browserName: "x" }).device).toBe('{"platform":"y"}');
  });
});
