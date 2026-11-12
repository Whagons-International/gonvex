/**
 * K15 `done_when`: for the same input, the error-triage routes return the
 * same groups as the Rust runtime.
 *
 * Replays `golden/rust-error-telemetry.json` (Rust's run of the conformance
 * scenario `error-telemetry`, recorded by
 * `cloudflare/conformance/src/error-telemetry-golden.mjs`): every frame each
 * socket role sent goes through the kernel's `SocketTelemetry` with one shared
 * host state (one gateway), into the real control object (K06 + K19) over RPC.
 * Every `error.ack`, both rate-limit floods and every route response must
 * equal Rust's, after the scenario's own named scrub rules and the same id
 * aliases. Stored payloads are also compared as text, so the key order of
 * `latest` (serde `preserve_order`, `swap_remove`) is checked too.
 */
import { env, SELF } from "cloudflare:test";
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import golden from "./golden/rust-error-telemetry.json";
// @ts-expect-error The scenario is plain JS shared with the conformance harness.
import { HTTP, expand, metricsView, scrub } from "../../../conformance/scenarios/error-telemetry.mjs";
import type { Json } from "../../session/decode";
import type { Connection, TenantSession } from "../../session/ports";
import { NOTHING_NEGOTIATED } from "../../session/capabilities";
import { controlTelemetryPort, type TelemetryControlRpc } from "../port";
import { SocketTelemetry, type TelemetryFrame, type TelemetryRecordHost, telemetryHostState } from "../socket";
import type { TelemetryTestEnv } from "./worker";

const ADMIN = "k15-admin-key";
const TENANT = "error-telemetry-a";
const ACCOUNT = "k15-alice-account";
const testEnv = env as unknown as TelemetryTestEnv;

interface GoldenStep {
  step: string;
  role?: string;
  acks?: Record<string, unknown>;
  flood?: { accepted: number; refusal: unknown };
  http?: Record<string, { status: number; body: any }>;
}
interface ScriptEntry {
  step: string;
  role?: string;
  frames?: Json[];
  flood?: string;
  http?: keyof typeof HTTP;
}
const G = golden as { script: ScriptEntry[]; steps: GoldenStep[]; fingerprintA: string };

const hash = (text: string) => createHash("sha256").update(text, "utf8").digest("hex");
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function tenantSession(projectId: string): TenantSession {
  return {
    identity: { projectId, account: { id: ACCOUNT, email: "alice@conformance.invalid", emailVerified: true, name: "alice", avatarUrl: "", provider: "password" } },
    route: { projectId, tenantId: TENANT, databaseUrl: `do://tenant/${TENANT}` },
    member: { id: "k15-alice-member", accountId: ACCOUNT, status: "active", displayName: "alice", avatarUrl: "", role: "member", permissions: null, membershipRevision: 1 },
    admissionRevision: 0,
    delegation: null,
    developerSession: null,
    anonymous: false,
  };
}

function connectionOf(role: string, projectId: string): Connection {
  if (role === "none") return { projectId: "", identity: null, tenant: null, impersonationId: "", authToken: "" };
  if (role === "anonymous") return { projectId, identity: null, tenant: null, impersonationId: "", authToken: "" };
  const tenant = tenantSession(projectId);
  return { projectId, identity: tenant.identity, tenant, impersonationId: "", authToken: "token" };
}

describe("error triage parity with the Rust runtime", () => {
  it("answers every frame and serves every route as Rust did", async () => {
    const from = Date.now() - 60_000;
    const created = await SELF.fetch("http://kernel.test/dev/projects", {
      method: "POST",
      headers: { authorization: `Bearer ${ADMIN}`, "content-type": "application/json" },
      body: JSON.stringify({ name: "conformance-error-telemetry", databaseMode: "multiTenant" }),
    });
    expect(created.status).toBe(201);
    const projectId = ((await created.json()) as { project: { id: string } }).project.id;
    const aliases: Array<[string, string]> = [[projectId, "<project>"], [TENANT, "<tenant:a>"], [ACCOUNT, "<account:alice>"]];
    const mask = (value: unknown) => {
      let text = JSON.stringify(value);
      for (const [raw, alias] of aliases) text = text.split(raw).join(alias);
      return JSON.parse(text);
    };

    const port = controlTelemetryPort(() => testEnv.CONTROL.getByName("control") as unknown as TelemetryControlRpc);
    const host = telemetryHostState({ presenceEnabled: true });
    const sockets = new Map<string, SocketTelemetry>();
    const socket = (role: string) => {
      let s = sockets.get(role);
      if (!s) {
        const record: TelemetryRecordHost = {
          connectionId: `k15-connection-${role}`,
          connection: connectionOf(role, projectId),
          negotiated: { ...NOTHING_NEGOTIATED, telemetryBatch: role === "alice" },
        };
        s = new SocketTelemetry({ record, persist: () => {} }, port, host);
        sockets.set(role, s);
      }
      return s;
    };
    const send = async (role: string, frame: Json) => (await socket(role).handle(frame as unknown as TelemetryFrame)).map((text) => JSON.parse(text));

    let fingerprintA: string | undefined;
    const resolve = (path: string) => path.replace("$P", encodeURIComponent(projectId)).replace("$A", fingerprintA ?? "unknown");
    expect(G.script.length).toBe(G.steps.length);

    for (const [index, entry] of G.script.entries()) {
      const want = G.steps[index]!;
      expect(want.step).toBe(entry.step);
      if (entry.frames) {
        for (const frame of expand(entry.frames) as Array<{ type: string; id?: string }>) {
          const replies = await send(entry.role!, frame as Json);
          if (frame.type.startsWith("error.")) {
            expect(replies, `${entry.step}: ${frame.id}`).toHaveLength(1);
            expect(mask(replies[0]), `${entry.step}: ack ${frame.id}`).toEqual(want.acks![frame.id!]);
            if (frame.id === "grp-1") fingerprintA = replies[0].fingerprints[0];
          } else {
            expect(replies).toEqual([]);
          }
          // Separate frames land in separate milliseconds, as they do over a network (`created_at` order).
          await sleep(3);
        }
      }
      if (entry.flood) {
        let accepted = 0;
        let refusal: unknown;
        for (let i = 0; i < 200; i++) {
          const [reply] = await send(entry.role!, { type: "error.heartbeat", id: `${entry.flood}-${i}` });
          if (reply.error) { refusal = reply; break; }
          accepted++;
        }
        expect({ accepted, refusal }, entry.step).toEqual(want.flood);
      }
      if (entry.http) {
        for (const [label, method, path, body] of HTTP[entry.http] as Array<[string, string, string, unknown?]>) {
          const response = await SELF.fetch(`http://kernel.test${resolve(path)}`, {
            method,
            headers: { authorization: `Bearer ${ADMIN}`, ...(body === undefined ? {} : { "content-type": "application/json" }) },
            ...(body === undefined ? {} : { body: JSON.stringify(body) }),
          });
          const text = await response.text();
          let parsed: unknown;
          try { parsed = JSON.parse(text); } catch { parsed = text; }
          const view = label.startsWith("metrics") ? { status: response.status, body: metricsView(parsed) } : { status: response.status, body: parsed };
          const got = mask(scrub(view, { from, to: Date.now() + 60_000, hash }));
          const expected = want.http![label]!;
          expect(got, `${entry.step}: ${label}`).toEqual(expected);
          // Stored payloads keep Rust's key order (serde preserve_order; swap_remove on anonymous sockets).
          const latest = (body: any) => (body?.groups ?? (body?.latest ? [body] : [])).map((group: any) => JSON.stringify(group.latest));
          expect(latest(got.body), `${entry.step}: ${label} key order`).toEqual(latest(expected.body));
        }
      }
    }
    expect(fingerprintA).toBe(G.fingerprintA);
  });
});
