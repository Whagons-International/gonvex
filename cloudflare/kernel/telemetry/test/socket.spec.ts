/**
 * `SocketTelemetry` with a recording port: the rate limits (20 per anonymous
 * connection, 120 per account, per 60 s window, shared by error and
 * performance frames), the envelope limits, error-session de-duplication,
 * the answers without a store, performance batches, the Analytics Engine sink
 * and the forwarding budget. The last test goes through K05's `Session`, so
 * the frames reach telemetry with or without a tenant.
 */
import { describe, expect, it } from "vitest";
import type { Json } from "../../session/decode";
import type { Connection, Ports, TenantSession } from "../../session/ports";
import { NOTHING_NEGOTIATED } from "../../session/capabilities";
import { Session } from "../../session/session";
import { ACK_ERRORS, LIMITS, type PerformanceRow, type PreparedErrorEvent } from "../envelope";
import { TelemetryLimiter, telemetrySubject } from "../limiter";
import type { TelemetryPort } from "../port";
import type { ErrorSessionInput } from "../store";
import { SocketTelemetry, type TelemetryFrame, type TelemetryHostState, type TelemetryRecordHost, telemetryHostState } from "../socket";

class RecordingPort implements TelemetryPort {
  sessions: ErrorSessionInput[] = [];
  errors: Array<{ projectId: string; events: PreparedErrorEvent[] }> = [];
  performance: Array<{ projectId: string; rows: PerformanceRow[] }> = [];
  failStore = false;
  async registerErrorSession(input: ErrorSessionInput) { if (this.failStore) return false; this.sessions.push(input); return true; }
  async recordErrors(projectId: string, events: PreparedErrorEvent[]) { if (this.failStore) return null; this.errors.push({ projectId, events }); return events.map(() => true); }
  async recordPerformance(projectId: string, rows: PerformanceRow[]) { this.performance.push({ projectId, rows }); }
}

const tenant = (projectId: string): TenantSession => ({
  identity: { projectId, account: { id: "acct", email: "", emailVerified: true, name: "", avatarUrl: "", provider: "" } },
  route: { projectId, tenantId: "tenant", databaseUrl: "" },
  member: { id: "member", accountId: "acct", status: "active", displayName: "", avatarUrl: "", role: "member", permissions: null, membershipRevision: 1 },
  admissionRevision: 0, delegation: null, developerSession: null, anonymous: false,
});
const MEMBER: Connection = { projectId: "p", identity: tenant("p").identity, tenant: tenant("p"), impersonationId: "", authToken: "t" };
const ANONYMOUS: Connection = { projectId: "p", identity: null, tenant: null, impersonationId: "", authToken: "" };
const NONE: Connection = { projectId: "", identity: null, tenant: null, impersonationId: "", authToken: "" };

function socket(connection: Connection, options: { port?: RecordingPort | null; host?: TelemetryHostState; connectionId?: string; batch?: boolean; device?: Json } = {}) {
  const port = options.port === null ? undefined : (options.port ?? new RecordingPort());
  const record: TelemetryRecordHost = {
    connectionId: options.connectionId ?? "conn-1",
    connection,
    negotiated: { ...NOTHING_NEGOTIATED, telemetryBatch: options.batch ?? false },
    ...(options.device ? { telemetryDevice: options.device } : {}),
  };
  let persisted = 0;
  const host = options.host ?? telemetryHostState({ presenceEnabled: true });
  const telemetry = new SocketTelemetry({ record, persist: () => { persisted++; } }, port, host);
  const send = async (frame: Record<string, unknown>) => (await telemetry.handle(frame as unknown as TelemetryFrame)).map((text) => JSON.parse(text));
  return { telemetry, port, record, send, host, persisted: () => persisted };
}

const heartbeat = (id: string) => ({ type: "error.heartbeat", id });
const perf = (id: string, extra: Record<string, unknown> = {}) => ({ id, kind: "query", path: "q", outcome: "ok", clientReceivedAtMs: 1, ...extra });

describe("rate limits", () => {
  it("allows 20 frames a minute to an anonymous connection, then refuses until the window rolls", async () => {
    let now = 1_000_000;
    const host = telemetryHostState({ presenceEnabled: true, now: () => now });
    const s = socket(ANONYMOUS, { host });
    for (let i = 0; i < 20; i++) expect(await s.send(heartbeat(`h${i}`))).toEqual([{ type: "error.ack", id: `h${i}` }]);
    expect(await s.send(heartbeat("h20"))).toEqual([{ type: "error.ack", id: "h20", error: ACK_ERRORS.rateLimited }]);
    expect(await s.send({ type: "error.envelope", id: "e", events: [{ message: "m" }] })).toEqual([{ type: "error.ack", id: "e", error: ACK_ERRORS.rateLimited }]);
    // Another anonymous connection has its own budget.
    const other = socket(ANONYMOUS, { host, connectionId: "conn-2" });
    expect(await other.send(heartbeat("o"))).toEqual([{ type: "error.ack", id: "o" }]);
    now += LIMITS.windowMs;
    expect(await s.send(heartbeat("again"))).toEqual([{ type: "error.ack", id: "again" }]);
  });

  it("allows 120 a minute per account across its sockets, and performance events spend the same budget", async () => {
    const port = new RecordingPort();
    const host = telemetryHostState({ presenceEnabled: true });
    const one = socket(MEMBER, { host, port, connectionId: "c1", batch: true });
    const two = socket(MEMBER, { host, port, connectionId: "c2" });
    await one.send({ type: "telemetry.batch", events: Array.from({ length: 50 }, (_, i) => perf(`p${i}`)) });
    // Invalid events cost nothing: the limit is the last check.
    await one.send({ type: "telemetry.event", ...perf("bad", { kind: "mutation" }) });
    let accepted = 0;
    for (let i = 0; i < 100; i++) {
      const [ack] = await (i % 2 ? one : two).send(heartbeat(`h${i}`));
      if (ack.error) { expect(ack.error).toBe(ACK_ERRORS.rateLimited); break; }
      accepted++;
    }
    expect(accepted).toBe(LIMITS.accountPerWindow - 50);
    await two.send({ type: "telemetry.event", ...perf("late") });
    expect(port.performance.flatMap((p) => p.rows.map((r) => r.eventId))).not.toContain("late");
  });

  it("forgets windows 120 s after they started", () => {
    const limiter = new TelemetryLimiter();
    expect(limiter.allow("a", 1, 0)).toBe(true);
    expect(limiter.allow("a", 1, 59_999)).toBe(false);
    expect(limiter.allow("b", 1, 60_000)).toBe(true);
    expect(limiter.size).toBe(2);
    limiter.allow("c", 1, 120_000);
    expect(limiter.size).toBe(2);
    expect(telemetrySubject("p", "a", "c")).toEqual({ key: "p:account:a", limit: 120 });
    expect(telemetrySubject("p", null, "c")).toEqual({ key: "p:connection:c", limit: 20 });
  });
});

describe("error envelopes", () => {
  it("checks availability, then the rate limit, then the envelope", async () => {
    expect(await socket(NONE).send({ type: "error.envelope", id: "a", events: [] })).toEqual([{ type: "error.ack", id: "a", error: ACK_ERRORS.unavailable }]);
    expect(await socket(MEMBER, { port: null }).send({ type: "error.envelope", id: "b", events: [{ message: "m" }] })).toEqual([{ type: "error.ack", id: "b", error: ACK_ERRORS.unavailable }]);
    const s = socket(MEMBER);
    expect(await s.send({ type: "error.envelope", id: "c", events: [] })).toEqual([{ type: "error.ack", id: "c", error: ACK_ERRORS.invalid }]);
    expect(await s.send({ type: "error.envelope", id: "d", events: Array.from({ length: 21 }, () => ({ message: "m" })) })).toEqual([{ type: "error.ack", id: "d", error: ACK_ERRORS.invalid }]);
    expect(await s.send({ type: "error.envelope", id: "e", events: [{ message: "x".repeat(LIMITS.maxEventBytes) }] })).toEqual([{ type: "error.ack", id: "e", error: ACK_ERRORS.tooLarge }]);
    const nine = Array.from({ length: 9 }, (_, i) => ({ message: `${i}`, pad: "x".repeat(30_000) }));
    expect(await s.send({ type: "error.envelope", id: "f", events: nine })).toEqual([{ type: "error.ack", id: "f", error: ACK_ERRORS.tooLarge }]);
    expect(s.port!.errors).toHaveLength(0);
  });

  it("stores only kept events and reports their fingerprints in order", async () => {
    const s = socket(MEMBER);
    const [ack] = await s.send({ type: "error.envelope", id: "g", events: [1, { message: " " }, { message: "a", name: "A" }, { message: "b", name: "B", eventId: "x" }] });
    expect(ack.accepted).toBe(2);
    expect(ack.fingerprints).toEqual(s.port!.errors[0]!.events.map((e) => e.fingerprint));
    expect(s.port!.errors[0]!.events.map((e) => e.eventId)[1]).toBe("x");
    const [none] = await s.send({ type: "error.envelope", id: "h", events: [{ message: "" }] });
    expect(none).toEqual({ type: "error.ack", id: "h", accepted: 0, fingerprints: [] });
    expect(s.port!.errors).toHaveLength(1);
    s.port!.failStore = true;
    expect(await s.send({ type: "error.envelope", id: "i", events: [{ message: "m" }] })).toEqual([{ type: "error.ack", id: "i", error: ACK_ERRORS.store }]);
  });
});

describe("error support sessions", () => {
  it("writes a new or changed registration once; repeats and heartbeats are answered from the record", async () => {
    const s = socket(MEMBER);
    await s.send({ type: "error.register", id: "1", release: " 1.0 ", environment: "prod" });
    await s.send({ type: "error.register", id: "2", release: " 1.0 ", environment: "prod" });
    await s.send(heartbeat("3"));
    expect(s.port!.sessions).toEqual([{ connectionId: "conn-1", projectId: "p", tenantId: "tenant", accountId: "acct", release: " 1.0 ", environment: "prod", live: true }]);
    expect(s.record.errorSession).toEqual({ project: "p", tenant: "tenant", account: "acct", release: " 1.0 ", environment: "prod" });
    expect(s.persisted()).toBe(1);
    // An empty release is a change (Some("")), but keeps the registered value.
    await s.send({ type: "error.register", id: "4", release: "", environment: "prod" });
    expect(s.port!.sessions).toHaveLength(2);
    expect(s.record.errorSession?.release).toBe(" 1.0 ");
    await s.send({ type: "error.register", id: "5", release: "r".repeat(250), environment: "e".repeat(150) });
    expect(s.port!.sessions[2]).toMatchObject({ release: "r".repeat(200), environment: "e".repeat(100) });
  });

  it("writes every time while presence v2 is off, and never for an anonymous connection", async () => {
    const off = socket(MEMBER, { host: telemetryHostState({ presenceEnabled: false }) });
    await off.send(heartbeat("1"));
    await off.send(heartbeat("2"));
    expect(off.port!.sessions).toHaveLength(2);
    // Rust's hub registers nothing while presence is off: the control object must not hold it live.
    expect(off.port!.sessions.every((input) => input.live === false)).toBe(true);
    expect(off.record.errorSession).toBeUndefined();
    const anonymous = socket(ANONYMOUS);
    expect(await anonymous.send({ type: "error.register", id: "a", release: "1" })).toEqual([{ type: "error.ack", id: "a" }]);
    expect(anonymous.port!.sessions).toHaveLength(0);
  });

  it("answers `error store unavailable` when the upsert fails", async () => {
    const s = socket(MEMBER);
    s.port!.failStore = true;
    expect(await s.send(heartbeat("1"))).toEqual([{ type: "error.ack", id: "1", error: ACK_ERRORS.store }]);
    expect(s.record.errorSession).toBeUndefined();
  });
});

describe("performance events", () => {
  it("drops batches that were not negotiated or carry more than 50 events; uses the auth device as fallback", async () => {
    const plain = socket(MEMBER);
    await plain.send({ type: "telemetry.batch", events: [perf("a")] });
    expect(plain.port!.performance).toHaveLength(0);
    const s = socket(MEMBER, { batch: true, device: { browserName: "auth" } });
    await s.send({ type: "telemetry.batch", events: Array.from({ length: 51 }, (_, i) => perf(`o${i}`)) });
    expect(s.port!.performance).toHaveLength(0);
    await s.send({ type: "telemetry.batch", events: [perf("b"), perf("c", { device: { platform: "own" } }), perf("bad", { outcome: "x" })] });
    expect(s.port!.performance).toEqual([{ projectId: "p", rows: [
      expect.objectContaining({ eventId: "b", device: '{"browserName":"auth"}', tenantId: "tenant", accountId: "acct" }),
      expect.objectContaining({ eventId: "c", device: '{"platform":"own"}' }),
    ] }]);
    // No project: dropped before the limiter.
    await socket(NONE).send({ type: "telemetry.event", ...perf("n") });
  });

  it("writes every accepted event to Analytics Engine and forwards at most the budget to the control object", async () => {
    const points: unknown[] = [];
    const sink = { write: (_: string, rows: readonly PerformanceRow[]) => { points.push(...rows); } };
    const host = telemetryHostState({ presenceEnabled: true, sink, storePerMinute: 3 });
    const s = socket(MEMBER, { host, batch: true });
    await s.send({ type: "telemetry.batch", events: Array.from({ length: 5 }, (_, i) => perf(`b${i}`)) });
    expect(points).toHaveLength(5);
    expect(s.port!.performance[0]!.rows.map((r) => r.eventId)).toEqual(["b0", "b1", "b2"]);
  });
});

describe("through K05's Session", () => {
  it("answers error frames on sockets without a tenant, and keeps the auth device", async () => {
    const port = new RecordingPort();
    const sent: string[] = [];
    const ports = {
      control: {
        project: async () => ({ ok: true, value: null }),
        loadSessionIdentity: async () => ({ ok: false, kind: "Other", message: "x" }),
        authenticateSession: async () => ({ ok: false, kind: "Other", message: "x" }),
        authenticateImpersonation: async () => ({ ok: false, kind: "Other", message: "x" }),
        resolvePublicTenant: async () => ({ ok: false, kind: "Other", message: "x" }),
        revalidate: async () => [],
        dispatch: async () => [],
      },
      modules: { module: async () => null, epoch: async () => "" },
      tenants: { dispatch: async () => [] },
      telemetry: port,
    } as unknown as Ports;
    const session = Session.open({
      ports, ready: { runtimeVersion: "development", presence: true }, host: "gw-test", connectionId: "c-1", publicTransport: false,
      sink: { send: (text) => sent.push(text), close: () => {} },
    });
    await session.receive(JSON.stringify({ type: "error.heartbeat", id: "before-auth" }));
    await session.receive(JSON.stringify({ type: "auth", id: "a", project: "p", controlOnly: true, device: { browserName: "Firefox" } }));
    await session.receive(JSON.stringify({ type: "error.register", id: "anon", release: "1" }));
    await session.receive(JSON.stringify({ type: "error.envelope", id: "env", events: [{ message: "m", context: { a: 1 } }] }));
    const acks = sent.map((text) => JSON.parse(text)).filter((m) => m.type === "error.ack");
    expect(acks).toEqual([
      { type: "error.ack", id: "before-auth", error: ACK_ERRORS.unavailable },
      { type: "error.ack", id: "anon" },
      { type: "error.ack", id: "env", accepted: 1, fingerprints: [port.errors[0]!.events[0]!.fingerprint] },
    ]);
    expect(JSON.parse(port.errors[0]!.events[0]!.payload)).toEqual({ message: "m", sessionId: "c-1", tenant: "", project: "p" });
    expect(session.record.telemetryDevice).toEqual({ browserName: "Firefox" });
  });
});
