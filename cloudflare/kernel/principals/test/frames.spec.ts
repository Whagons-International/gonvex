// The service socket's frame boundary and the manifest decision, as units:
// exact bytes, the allowlist refused before the tenant is asked, ignores.
import { describe, expect, it } from "vitest";
import type { ClientFrame } from "../../session/decode";
import { handleServiceFrame, type ServiceCalls, type ServiceTenantCall } from "../frames";
import type { ServiceGrant } from "../grant";
import { serviceAuthResult, serviceSession } from "../grant";
import { bearer, manifestAccess, type ManifestPrincipal } from "../manifest";

const grant: ServiceGrant = {
  principalId: "gateway",
  route: { projectId: "p", tenantId: "t", databaseUrl: "do://tenant/t" },
  functions: ["keys.check", "keys.read", "keys.run"],
  maxDelegationSeconds: 900,
};

function calls(answer: { result?: string; committedRevision?: number; error?: string } = { result: '{"ok":true}' }) {
  const tenant: ServiceTenantCall[] = [];
  const control: Array<[string, unknown]> = [];
  const port: ServiceCalls = {
    control: async (_g, path, args) => { control.push([path, args]); return { ok: true, value: { id: "svcdel_1", token: "gvx_imp_x", expiresAt: "t", expiresInSeconds: 900 } }; },
    tenant: async (_g, call) => {
      tenant.push(call);
      if (answer.error !== undefined) return { ok: false, error: answer.error };
      return answer.committedRevision === undefined ? { ok: true, result: answer.result! } : { ok: true, result: answer.result!, committedRevision: answer.committedRevision };
    },
  };
  return { port, tenant, control };
}

const run = (frame: unknown, port: ServiceCalls) => handleServiceFrame(port, grant, frame as ClientFrame);

describe("service frames (service_principal.rs handle_service_frame)", () => {
  it("an allowlisted Reducer reaches the tenant and answers in serde order, without a trace", async () => {
    const c = calls({ result: '{"ok":true}', committedRevision: 7 });
    const out = await run({ type: "reducer.call", id: "r1", path: "keys.check", args: { a: 1 }, idempotencyKey: "k", trace: { clientSentAtMs: 1 } }, c.port);
    expect(out).toEqual(['{"type":"reducer.result","id":"r1","path":"keys.check","result":{"ok":true},"originCommandId":"r1","committedRevision":7}']);
    expect(c.tenant).toEqual([{ kind: "reducer", id: "r1", path: "keys.check", args: { a: 1 }, idempotencyKey: "k" }]);
  });

  it("a refusal is unclassed, and a path outside the allowlist never reaches the tenant", async () => {
    const c = calls({ error: 'function "keys.check" arguments do not match its schema: x' });
    expect(await run({ type: "reducer.call", id: "r1", path: "keys.check", args: {} }, c.port)).toEqual([
      '{"type":"reducer.error","id":"r1","path":"keys.check","error":"function \\"keys.check\\" arguments do not match its schema: x"}',
    ]);
    const outside = calls();
    expect(await run({ type: "reducer.call", id: "r2", path: "board.add", args: {} }, outside.port)).toEqual([
      '{"type":"reducer.error","id":"r2","path":"board.add","error":"function \\"board.add\\" is not allowed for this service principal"}',
    ]);
    // Rust's `{:?}` quoting of the path inside the text.
    const odd = 'x"y';
    expect(await run({ type: "action.call", id: "a1", path: odd, args: {} }, outside.port)).toEqual([
      JSON.stringify({ type: "action.error", id: "a1", path: odd, error: 'function "x\\"y" is not allowed for this service principal' }),
    ]);
    expect(outside.tenant).toEqual([]);
  });

  it("Queries answer {path, result} with no reason; Actions carry committedRevision", async () => {
    const c = calls({ result: "[1]" });
    expect(await run({ type: "query.call", id: "q", path: "keys.read", args: {} }, c.port)).toEqual(['{"type":"query.result","id":"q","path":"keys.read","result":[1]}']);
    const a = calls({ result: "null", committedRevision: 3 });
    expect(await run({ type: "action.call", id: "a", path: "keys.run", args: {} }, a.port)).toEqual(['{"type":"action.result","id":"a","path":"keys.run","result":null,"committedRevision":3}']);
  });

  it("control scope: only the two delegation Reducers reach the control object", async () => {
    const c = calls();
    expect(await run({ type: "reducer.call", id: "d", path: "control.servicePrincipals.delegate", args: { x: 1 }, scope: "control" }, c.port)).toEqual([
      '{"type":"reducer.result","id":"d","path":"control.servicePrincipals.delegate","result":{"id":"svcdel_1","token":"gvx_imp_x","expiresAt":"t","expiresInSeconds":900},"originCommandId":"d"}',
    ]);
    expect(await run({ type: "reducer.call", id: "o", path: "control.tenants.create", args: {}, scope: "control" }, c.port)).toEqual([
      '{"type":"reducer.error","id":"o","path":"control.tenants.create","error":"Control Plane function \\"control.tenants.create\\" is not available to service principals"}',
    ]);
    expect(await run({ type: "query.call", id: "q", path: "control.tenants.mine", args: {}, scope: "control" }, c.port)).toEqual([
      '{"type":"query.error","id":"q","path":"control.tenants.mine","error":"service principals cannot call Control Plane Queries"}',
    ]);
    expect(await run({ type: "action.call", id: "a", path: "control.auth.logout", args: {}, scope: "control" }, c.port)).toEqual([
      '{"type":"action.error","id":"a","path":"control.auth.logout","error":"service principals cannot call Control Plane Actions"}',
    ]);
    expect(c.control).toEqual([["control.servicePrincipals.delegate", { x: 1 }]]);
    expect(c.tenant).toEqual([]);
  });

  it("subscriptions, replicas and batches are refused; everything else is ignored; auth passes through", async () => {
    const c = calls();
    expect(await run({ type: "query.subscribe", id: "s", path: "keys.read", args: {} }, c.port)).toEqual(['{"type":"query.error","id":"s","path":"keys.read","error":"service principals cannot subscribe"}']);
    expect(await run({ type: "replica.open", id: "o", path: "board.all", args: {} }, c.port)).toEqual(['{"type":"replica.error","id":"o","path":"board.all","error":"service principals cannot open replicas"}']);
    const batch = '{"type":"reducer.error","id":"","error":"batched frames are not available to service principals","class":"rejected","retryable":false}';
    for (const frame of [{ type: "reducer.callMany", calls: [] }, { type: "query.subscribeMany", subscribes: [] }, { type: "replica.openMany", opens: [] }]) {
      expect(await run(frame, c.port)).toEqual([batch]);
    }
    for (const frame of [{ type: "presence.set", state: "active" }, { type: "query.unsubscribe", id: "x" }, { type: "replica.close", id: "x" }, { type: "error.heartbeat", id: "e" }, { type: "telemetry.batch", events: [] }]) {
      expect(await run(frame, c.port)).toEqual([]);
    }
    expect(await run({ type: "auth", id: "a", controlOnly: false, public: false }, c.port)).toBeNull();
    expect(c.tenant).toEqual([]);
  });

  it("the auth result and the system identity are the reference's", () => {
    expect(JSON.stringify(serviceAuthResult(grant))).toBe('{"projectId":"p","tenantId":"t","accountId":"","servicePrincipal":"gateway","functions":["keys.check","keys.read","keys.run"],"maxDelegationSeconds":900}');
    const session = serviceSession(grant);
    expect(session.identity.account).toEqual({ id: "_gonvex_service:gateway", email: "", emailVerified: false, name: "gateway", avatarUrl: "", provider: "service" });
    expect(session.member).toMatchObject({ id: "_gonvex_service:gateway", accountId: "_gonvex_service:gateway", status: "system", role: "system", permissions: {} });
    expect(session.delegation).toBeNull();
  });
});

describe("/dev/manifest access (manifest_access)", () => {
  const principals: Record<string, ManifestPrincipal> = {
    "gvx_svc_a": { id: "a", projects: ["p"], manifest: true },
    "gvx_svc_b": { id: "b", projects: ["p"], manifest: false },
  };
  // K06's findPrincipal trims and requires the prefix; this stand-in does the same.
  const find = (token: string) => principals[token.trim()] ?? null;

  it("allows only manifest:true for a listed project, and never falls back for a known credential", () => {
    expect(manifestAccess(find, "Bearer gvx_svc_a", "p")).toEqual({ kind: "allowed", principal: "a" });
    expect(manifestAccess(find, "Bearer gvx_svc_a", "q")).toEqual({ kind: "denied", principal: "a" });
    expect(manifestAccess(find, "Bearer gvx_svc_b", "p")).toEqual({ kind: "denied", principal: "b" });
    expect(manifestAccess(find, "Bearer gvx_svc_c", "p")).toEqual({ kind: "none" });
    expect(manifestAccess(find, null, "p")).toEqual({ kind: "none" });
  });

  it("reads the bearer as operations::bearer does", () => {
    expect(bearer("  Bearer  gvx_svc_a ")).toBe(" gvx_svc_a");
    expect(manifestAccess(find, "  Bearer  gvx_svc_a ", "p")).toEqual({ kind: "allowed", principal: "a" });
    expect(bearer("gvx_svc_a")).toBe("gvx_svc_a");
    expect(bearer("bearer gvx_svc_a")).toBe("bearer gvx_svc_a");
  });
});
