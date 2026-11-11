// Service principals end to end in workerd: K05's gateway sockets, K06's
// control object (credentials, delegation grants, redemption) and K07's
// tenant object (allowlisted Reducers), as the harness composes them.
import { env, runInDurableObject, SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { Client } from "../../reducer/test/client";
import { fixtureManifest } from "./fixture";

const ADMIN = "k17-test-admin";
const PROJECT = "k17-project";
const sha = async (text: string) => [...new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text)))].map((b) => b.toString(16).padStart(2, "0")).join("");
const token = async (name: string) => `gvx_svc_${await sha(`k17-test-${name}`)}`;

type Env = { CONTROL: DurableObjectNamespace<any>; TENANT: DurableObjectNamespace<any>; GATEWAY: DurableObjectNamespace<any> };
const bindings = env as unknown as Env;
const control = () => bindings.CONTROL.getByName("control") as any;

async function dev(path: string, body: unknown): Promise<any> {
  const response = await SELF.fetch(`http://k17.test${path}`, { method: "POST", headers: { authorization: `Bearer ${ADMIN}`, "content-type": "application/json" }, body: JSON.stringify(body) });
  const text = await response.text();
  if (!response.ok) throw new Error(`${path} -> ${response.status}: ${text}`);
  return JSON.parse(text);
}

/** The project the principals name (fixed id), with the fixture deployed, a tenant and Members. */
async function seed(tenant: string, members: string[]) {
  if (!(await control().devProjectKey(PROJECT))) await control().devCreateProject({ name: "k17", id: PROJECT });
  await dev("/dev/sync", { ...fixtureManifest(), project: PROJECT });
  await dev("/dev/tenants", { projectId: PROJECT, id: tenant, name: tenant });
  const out: Record<string, { accountId: string; memberId: string; email: string }> = {};
  for (const name of members) {
    const email = `${name}-${tenant}@k17.invalid`;
    out[name] = { ...(await dev("/dev/internal/e2e/members", { projectId: PROJECT, tenantId: tenant, email, password: "k17-password", name })), email };
  }
  return out;
}

async function serviceSocket(tenant: string, principal = "gateway") {
  const client = await Client.connect();
  client.send({ type: "auth", id: "svc", token: await token(principal), project: PROJECT, tenant });
  const auth = await client.next((f) => f.id === "svc");
  return { client, auth };
}

/** Every session record on the gateway shards (GONVEX_GATEWAY_SHARDS=2). */
async function records(): Promise<any[]> {
  const out: any[] = [];
  for (const name of ["gw-0", "gw-1"]) {
    await runInDurableObject(bindings.GATEWAY.getByName(name), (instance: any) => {
      for (const session of instance.host.sessions.values()) out.push(structuredClone(session.record));
    });
  }
  return out;
}

describe("service sockets (K05 + K17 over K06 and K07)", () => {
  it("binds to one tenant, clears the auth deadline and keeps the grant in the hibernation record", async () => {
    await seed("k17-t1", ["alice"]);
    const refused = await serviceSocket("k17-nowhere");
    expect(refused.auth).toEqual({ type: "auth.error", id: "svc", error: "tenant is not available", code: "transient" });
    const { client, auth } = await serviceSocket("k17-t1");
    expect(auth).toEqual({
      type: "auth.result", id: "svc",
      result: { projectId: PROJECT, tenantId: "k17-t1", accountId: "", servicePrincipal: "gateway", functions: ["principals.verifyKey", "principals.whoami", "principals.record", "k17.enqueue"], maxDelegationSeconds: 600 },
    });
    const all = await records();
    const service = all.find((r) => r.service?.route?.tenantId === "k17-t1");
    expect(service).toMatchObject({ pendingUntil: null, tenant: null, connection: { projectId: PROJECT, authToken: "" }, service: { principalId: "gateway", maxDelegationSeconds: 600 } });
    // A refused credential leaves the socket pending (it still closes at the 10 s deadline).
    expect(all.some((r) => !r.service && r.pendingUntil !== null)).toBe(true);
    client.close();
    refused.client.close();
  });

  it("runs allowlisted Reducers as the principal on the api channel, with receipts under its identity", async () => {
    await seed("k17-t2", ["alice"]);
    const { client } = await serviceSocket("k17-t2");
    const verify = await client.call("v1", "principals.verifyKey", { keyHash: "k17-valid-key-digest" });
    expect(verify).toEqual({
      type: "reducer.result", id: "v1", path: "principals.verifyKey", originCommandId: "v1",
      result: { ok: true, seen: { account: "_gonvex_service:gateway", member: "_gonvex_service:gateway", role: "system", channel: "api", rootChannel: "api", actorAccountId: "_gonvex_service:gateway", actorMemberId: "_gonvex_service:gateway", commandIsRoot: true, delegation: null } },
    });
    const verifyNumbers = await bindings.TENANT.getByName("k17-t2").lastNumbers();
    const write = await client.call("w1", "principals.record", { id: "svc-row", title: "t" }, { idempotencyKey: "key-1" });
    expect(write.type).toBe("reducer.result");
    expect(typeof write.committedRevision).toBe("number");
    expect(client.frames.some((f) => f.type === "replica.watermark")).toBe(false); // no change feed on a service socket
    const writeNumbers = await bindings.TENANT.getByName("k17-t2").lastNumbers();
    const replay = await client.call("w2", "principals.record", { id: "svc-row", title: "t" }, { idempotencyKey: "key-1" });
    expect(replay.result).toEqual(write.result);
    const receipts = await bindings.TENANT.getByName("k17-t2").readTable("_gonvex_reducer_idempotency");
    expect(receipts.map((r: any) => [r.subject, r.idempotency_key, r.path])).toEqual([["_gonvex_service:gateway", "key-1", "principals.record"]]);
    const provenance = await bindings.TENANT.getByName("k17-t2").readTable("_gonvex_sync_transactions");
    expect(provenance.at(-1)).toMatchObject({ root_invocation_channel: "api", invocation_channel: "api", actor_account_id: "_gonvex_service:gateway", actor_member_id: "_gonvex_service:gateway" });
    console.log(`k17 numbers: verifyKey ${JSON.stringify(verifyNumbers)}; record ${JSON.stringify(writeNumbers)}`);
    // The internal function stays internal for a member.
    const member = await Client.connect();
    const session = await member.login(PROJECT, "alice-k17-t2@k17.invalid", "k17-password");
    await member.auth(PROJECT, "k17-t2", session);
    expect(await member.call("m1", "principals.verifyKey", { keyHash: "x" })).toMatchObject({ type: "reducer.error", class: "rejected", error: 'internal function "principals.verifyKey" cannot be called by a client' });
    client.close();
    member.close();
  });

  it("a delegated session is the member's own, and its durable work keeps the delegation", async () => {
    const seeded = await seed("k17-t3", ["alice"]);
    const { client } = await serviceSocket("k17-t3");
    const grant = await client.call("d1", "control.servicePrincipals.delegate", {
      accountId: seeded.alice!.accountId, memberId: seeded.alice!.memberId, reason: "api-key:k1",
      actor: { kind: "agent", name: "CI deploy key", reference: "apiKey:k1" },
    }, { scope: "control" });
    expect(grant).toMatchObject({ type: "reducer.result", result: { expiresInSeconds: 600 } });
    expect(grant.class).toBeUndefined();
    const member = await Client.connect();
    member.send({ type: "auth", id: "m", token: grant.result.token, project: PROJECT, tenant: "k17-t3", clientContract: 1 });
    const auth = await member.next((f) => f.id === "m");
    expect(auth).toMatchObject({ type: "auth.result", result: { impersonationId: grant.result.id, impersonatorId: "service:gateway", accountId: seeded.alice!.accountId } });
    const delegation = { principal: "gateway", actor: { kind: "agent", name: "CI deploy key", reference: "apiKey:k1" } };
    expect((await member.call("w", "principals.whoami", {})).result).toMatchObject({ channel: "api", rootChannel: "api", member: seeded.alice!.memberId, delegation });
    expect((await member.call("e", "k17.enqueue", {})).type).toBe("reducer.result");
    const outbox = await bindings.TENANT.getByName("k17-t3").readTable("_gonvex_action_outbox");
    expect(JSON.parse(String(outbox.at(-1).provenance))).toMatchObject({ channel: "api", rootChannel: "api", actorAccountId: seeded.alice!.accountId, delegation });
    // The grant record: actor `service:<id>`, reason prefixed, the actor kept for attribution.
    const rows = await runInDurableObject(control(), (instance: any) => instance.sql.all("SELECT actor_account_id, reason, delegation_actor FROM gonvex_impersonation_grants WHERE id=$1", grant.result.id));
    expect(rows).toEqual([{ actor_account_id: "service:gateway", reason: "service:gateway: api-key:k1", delegation_actor: JSON.stringify(delegation.actor) }]);
    client.close();
    member.close();
  });
});
