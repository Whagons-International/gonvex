import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import schema from "../../../rust/crates/store/src/control_schema.sql";
import type { ControlConfig } from "../src/context";
import type { ControlObject } from "../src/control-object";
import { DEFINITION_NAMES } from "../src/definitions";
import { clearJwksCache, verifyExternalToken, type ExternalAuthConfiguration } from "../src/external-auth";
import { PASSWORD_ROUNDS, derive, hashPassword, verifyPassword } from "../src/password";
import { pbkdf2Sha256 } from "../src/pbkdf2";
import { pbkdf2Sync } from "node:crypto";
import { decryptControlSecret, encryptControlSecret } from "../src/secrets";
import type { StubTenantObject } from "../test-support/stub-tenant";
import { Client, PASSWORD, dev, login, seed, unique, worker } from "./helpers";

const SERVICE_TOKEN = "gvx_svc_test_gateway_credential";
const SERVICE_PROJECT = "project-service-principal";

const control = () => env.CONTROL.getByName("control") as DurableObjectStub<ControlObject>;
const tenantStub = (id: string) => env.TENANT.getByName(id) as DurableObjectStub<StubTenantObject>;

describe("schema", () => {
  it("loads control_schema.sql unchanged: 36 tables and 1 trigger", async () => {
    const counts = await runInDurableObject(control(), (_instance: ControlObject, state) =>
      state.storage.sql
        .exec<{ type: string; n: number }>("SELECT type, count(*) AS n FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' GROUP BY type")
        .toArray(),
    );
    expect(Object.fromEntries(counts.map((row) => [row.type, row.n]))).toMatchObject({ table: 36, trigger: 1 });
    expect(schema).toContain("CREATE TABLE IF NOT EXISTS gonvex_impersonation_grants");
  });

  it("registers the 60 Control function names of the contract", () => {
    expect(DEFINITION_NAMES).toHaveLength(60);
  });
});

describe("passwords (decision 0007)", () => {
  // Produced by the Rust runtime (`hash_password`) for "conformance-password-1".
  const RUST_HASH = "pbkdf2_sha256$210000$kzVCPvFqWe0ZqLxnpflZsw$6JjmaRJDd-g_TKx7pzto2ST_GlagDA77vcXlpjXjz-Q";

  it("verifies a hash the Rust runtime stored, at its own round count", () => {
    expect(verifyPassword("conformance-password-1", RUST_HASH)).toBe(true);
    expect(verifyPassword("conformance-password-2", RUST_HASH)).toBe(false);
  });

  it("hashes in the reference format at 210,000 rounds", () => {
    const hash = hashPassword("a-new-password-1");
    expect(hash).toMatch(/^pbkdf2_sha256\$210000\$[A-Za-z0-9_-]{22}\$[A-Za-z0-9_-]{43}$/);
    expect(PASSWORD_ROUNDS).toBe(210_000);
    expect(verifyPassword("a-new-password-1", hash)).toBe(true);
  });

  it("derives identically in WebAssembly, in plain JavaScript and in node:crypto", () => {
    const cases: Array<[string, number, number, number]> = [["p", 16, 1, 32], ["x".repeat(100), 8, 1000, 32], ["é😀", 0, 3, 20], ["abc", 70, 10, 33], ["k", 16, 2, 64]];
    for (const [password, saltLength, rounds, length] of cases) {
      const salt = new Uint8Array(saltLength).map((_, i) => (i * 37 + 11) & 0xff);
      const bytes = new TextEncoder().encode(password);
      const reference = new Uint8Array(pbkdf2Sync(bytes, salt, rounds, length, "sha256"));
      expect(derive(bytes, salt, rounds, length)).toEqual(reference);
      expect(pbkdf2Sha256(bytes, salt, rounds, length)).toEqual(reference);
    }
    // Inputs larger than the module buffer take the JavaScript path.
    const long = new Uint8Array(5000).fill(7);
    expect(derive(long, new Uint8Array(16), 2, 32)).toEqual(new Uint8Array(pbkdf2Sync(long, new Uint8Array(16), 2, 32, "sha256")));
  });

  it("refuses malformed hashes", () => {
    for (const bad of ["", "pbkdf2_sha256$0$a$b", "bcrypt$1$a$b", "pbkdf2_sha256$x$a$b", "pbkdf2_sha256$1000$a$"]) expect(verifyPassword("x", bad)).toBe(false);
  });

  it("verifies within the 0007 CPU budget", () => {
    const started = performance.now();
    for (let i = 0; i < 3; i++) verifyPassword("conformance-password-1", RUST_HASH);
    const perVerification = (performance.now() - started) / 3;
    console.log(`PBKDF2-SHA256 210k verification: ${perVerification.toFixed(1)} ms per call (local workerd)`);
    expect(perVerification).toBeLessThan(500);
  });
});

describe("provider secrets (C.3.3)", () => {
  // Rust `encrypt_control_secret("apple-secret")` with GONVEX_DASHBOARD_SESSION_SECRET="conformance-control-secret".
  const RUST_BLOB = "C174B95172DB2B13C43E243FA66A61D3C9786A567A480EEAA615D583EADFBA2CADBA530D4CE4EBFE";

  it("decrypts a blob the Rust runtime stored", () => {
    expect(decryptControlSecret("conformance-control-secret", new Uint8Array(Buffer.from(RUST_BLOB, "hex")))).toBe("apple-secret");
  });

  it("round-trips and fails closed", () => {
    const blob = encryptControlSecret("k", "secret value");
    expect(decryptControlSecret("k", blob)).toBe("secret value");
    expect(() => decryptControlSecret("other", blob)).toThrow("provider credential decryption failed");
    expect(() => decryptControlSecret("k", new Uint8Array(5))).toThrow("stored provider credential is invalid");
    expect(() => encryptControlSecret(undefined, "x")).toThrow("GONVEX_DASHBOARD_SESSION_SECRET is required to store provider credentials");
  });
});

describe("external tokens (C.3.4)", () => {
  const ISSUER = "https://issuer.gonvex.test";
  const JWKS = "https://keys.gonvex.test/jwks";

  async function keyPair(kid: string) {
    const pair = (await crypto.subtle.generateKey(
      { name: "RSASSA-PKCS1-v1_5", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" },
      true,
      ["sign", "verify"],
    )) as CryptoKeyPair;
    const jwk = (await crypto.subtle.exportKey("jwk", pair.publicKey)) as JsonWebKey;
    return { pair, jwk: { kty: "RSA", n: jwk.n, e: jwk.e, kid, alg: "RS256" } };
  }

  async function sign(key: CryptoKey, kid: string, claims: Record<string, unknown>, alg = "RS256"): Promise<string> {
    const header = Buffer.from(JSON.stringify({ alg, kid, typ: "JWT" })).toString("base64url");
    const payload = Buffer.from(JSON.stringify(claims)).toString("base64url");
    const signature = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, new TextEncoder().encode(`${header}.${payload}`));
    return `${header}.${payload}.${Buffer.from(signature).toString("base64url")}`;
  }

  const config: ControlConfig = { servicePrincipals: [], runtimeVersion: "test" };
  const oidc: ExternalAuthConfiguration = {
    provider: "external-oidc",
    issuer: ISSUER,
    audience: "app",
    jwksUrl: JWKS,
    firebaseTenantId: "",
    signupMode: "personal",
    firebaseProjectId: "",
    firebaseAdminCredentials: null,
  };

  it("verifies RS256 with the JWKS, refetching once for an unknown kid", async () => {
    clearJwksCache();
    const first = await keyPair("k1");
    const second = await keyPair("k2");
    let served = [first.jwk];
    let fetches = 0;
    const fetcher = (async () => {
      fetches++;
      return Response.json({ keys: served }, { headers: { "cache-control": "public, max-age=600" } });
    }) as typeof fetch;
    const now = Math.floor(Date.now() / 1000);
    const claims = { iss: ISSUER, aud: "app", sub: " user-1 ", iat: now, exp: now + 600, email: " Ada@Example.TEST ", email_verified: "true", name: " Ada ", picture: "p" };
    const identity = await verifyExternalToken(config, fetcher, oidc, await sign(first.pair.privateKey, "k1", claims));
    expect(identity).toMatchObject({ provider: "external-oidc", subject: "user-1", email: "ada@example.test", emailVerified: true, name: "Ada", signInProvider: "external-oidc", authTime: now });
    expect(fetches).toBe(1);
    await verifyExternalToken(config, fetcher, oidc, await sign(first.pair.privateKey, "k1", claims));
    expect(fetches).toBe(1); // cached for max-age
    served = [first.jwk, second.jwk];
    await verifyExternalToken(config, fetcher, oidc, await sign(second.pair.privateKey, "k2", claims));
    expect(fetches).toBe(2); // unknown kid forced one refetch
  });

  it("maps each failure to the reference's error", async () => {
    clearJwksCache();
    const { pair, jwk } = await keyPair("k1");
    const other = await keyPair("k1");
    const fetcher = (async () => Response.json({ keys: [jwk] })) as typeof fetch;
    const now = Math.floor(Date.now() / 1000);
    const good = { iss: ISSUER, aud: "app", sub: "u", iat: now, exp: now + 600 };
    const attempt = async (token: string, configuration = oidc) => {
      try {
        await verifyExternalToken(config, fetcher, configuration, token);
        return "ok";
      } catch (error) {
        return (error as Error).message;
      }
    };
    expect(await attempt(await sign(pair.privateKey, "k1", { ...good, iss: "https://other" }))).toBe("external identity token issuer is invalid");
    expect(await attempt(await sign(pair.privateKey, "k1", { ...good, aud: ["x", "app"] }))).toBe("ok");
    expect(await attempt(await sign(pair.privateKey, "k1", { ...good, aud: "x" }))).toBe("external identity token audience is invalid");
    expect(await attempt(await sign(pair.privateKey, "k1", { ...good, exp: now - 31 }))).toBe("external identity token is expired");
    expect(await attempt(await sign(pair.privateKey, "k1", { ...good, exp: now - 10 }))).toBe("ok"); // 30 s leeway
    expect(await attempt(await sign(pair.privateKey, "k1", { ...good, iat: now + 120 }))).toBe("external identity token is invalid");
    expect(await attempt(await sign(pair.privateKey, "k1", { ...good, sub: " " }))).toBe("external identity token subject is missing");
    expect(await attempt(await sign(other.pair.privateKey, "k1", good))).toBe("external identity token is invalid");
    expect(await attempt(await sign(pair.privateKey, "k1", good, "HS256"))).toBe("external identity token is invalid");
    const { exp: _exp, ...noExp } = good;
    expect(await attempt(await sign(pair.privateKey, "k1", noExp))).toBe("external identity token is invalid");
    expect(await attempt("x.y.z")).toBe("external identity token is invalid");
    expect(await attempt(await sign(pair.privateKey, "k1", good), { ...oidc, jwksUrl: "https://localhost/keys" })).toBe("external identity JWKS endpoint is unsafe");
    const firebase = { ...oidc, provider: "firebase", firebaseTenantId: "tenant-1" };
    expect(await attempt(await sign(pair.privateKey, "k1", { ...good, auth_time: now, firebase: { tenant: "other" } }), firebase)).toBe("Firebase tenant does not match this auth realm");
    expect(await attempt(await sign(pair.privateKey, "k1", good), firebase)).toBe("external identity token is invalid"); // auth_time required
  });

  it("checks Firebase accounts:lookup when admin credentials are configured", async () => {
    clearJwksCache();
    const { pair, jwk } = await keyPair("k1");
    const service = (await crypto.subtle.generateKey(
      { name: "RSASSA-PKCS1-v1_5", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" },
      true,
      ["sign", "verify"],
    )) as CryptoKeyPair;
    const pkcs8 = Buffer.from(await crypto.subtle.exportKey("pkcs8", service.privateKey)).toString("base64");
    const credentials = JSON.stringify({ project_id: "demo-x", client_email: "svc@demo-x.iam.test", private_key: `-----BEGIN PRIVATE KEY-----\n${pkcs8}\n-----END PRIVATE KEY-----\n` });
    let user: Record<string, unknown> = { localId: "uid-1", disabled: false, validSince: "0" };
    const calls: string[] = [];
    const fetcher = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      calls.push(url);
      if (url === JWKS) return Response.json({ keys: [jwk] });
      if (url === "https://token.gonvex.test") {
        const assertion = new URLSearchParams(String(init?.body)).get("assertion")!;
        const [h, p, s] = assertion.split(".");
        const ok = await crypto.subtle.verify("RSASSA-PKCS1-v1_5", service.publicKey, Buffer.from(s!, "base64url"), new TextEncoder().encode(`${h}.${p}`));
        return ok ? Response.json({ access_token: "access" }) : new Response("bad", { status: 401 });
      }
      if (url === "https://identity.gonvex.test/v1/projects/demo-x/accounts:lookup") return Response.json({ users: [user] });
      return new Response("unexpected", { status: 500 });
    }) as typeof fetch;
    const firebase: ExternalAuthConfiguration = {
      ...oidc,
      provider: "firebase",
      issuer: "https://securetoken.google.com/demo-x",
      audience: "demo-x",
      firebaseProjectId: "demo-x",
      firebaseAdminCredentials: credentials,
    };
    const lookupConfig: ControlConfig = { ...config, googleTokenUrl: "https://token.gonvex.test", identityToolkitUrl: "https://identity.gonvex.test" };
    const now = Math.floor(Date.now() / 1000);
    const token = await sign(pair.privateKey, "k1", { iss: firebase.issuer, aud: "demo-x", sub: "uid-1", iat: now, exp: now + 600, auth_time: now - 100, firebase: { sign_in_provider: "google.com" } });
    expect((await verifyExternalToken(lookupConfig, fetcher, firebase, token)).signInProvider).toBe("google.com");
    expect(calls).toContain("https://identity.gonvex.test/v1/projects/demo-x/accounts:lookup");
    user = { localId: "uid-1", validSince: String(now) };
    await expect(verifyExternalToken(lookupConfig, fetcher, firebase, token)).rejects.toThrow("Firebase account is disabled or its sessions were revoked");
    user = { localId: "uid-1", disabled: true };
    await expect(verifyExternalToken(lookupConfig, fetcher, firebase, token)).rejects.toThrow("Firebase account is disabled or its sessions were revoked");
    await expect(verifyExternalToken(lookupConfig, fetcher, { ...firebase, firebaseAdminCredentials: "{}" }, token)).rejects.toThrow("Firebase account status is unavailable");
  });
});

describe("membership projection (C.4 item 6)", () => {
  it("tenant objects push outbox rows; the upsert is revision-guarded", async () => {
    const seeded = await seed({ a: ["ann"] });
    const tenant = seeded.tenants.a!;
    const member = seeded.members.ann!;
    const index = () =>
      runInDurableObject(control(), (instance: ControlObject) =>
        instance.sql.get("SELECT status,tenant_membership_revision AS revision FROM account_tenant_index WHERE account_id=$1 AND tenant_id=$2", member.accountId, tenant),
      );
    await tenantStub(tenant).setMemberStatus(member.memberIds.a!, "suspended");
    await tenantStub(tenant).flushMemberships();
    expect(await index()).toEqual({ status: "suspended", revision: 2 });
    expect(await tenantStub(tenant).outboxSize()).toBe(0);
    // A late, older revision does not win.
    await control().projectMemberships(seeded.project, tenant, [{ accountId: member.accountId, memberId: member.memberIds.a!, status: "active", membershipRevision: 1 }]);
    expect(await index()).toEqual({ status: "suspended", revision: 2 });
    // The suspended member is not admitted and the directory no longer lists the tenant.
    const { client, session } = await login(seeded.project, member.email);
    expect(session.tenants).toEqual([]);
    await client.auth({ project: seeded.project, token: session.accessToken, controlOnly: true });
    const tenants = await client.call("query.call", "control.tenants.mine");
    expect(tenants.result).toEqual([]);
  });
});

describe("service principals (C.1 rows 61-62)", () => {
  it("authenticates, delegates a member session, and revokes it", async () => {
    const seeded = await seed({ a: ["sam"] }, { projectId: SERVICE_PROJECT });
    const service = await Client.open();
    expect(await service.auth({ project: seeded.project, tenant: "nope", token: SERVICE_TOKEN })).toMatchObject({ type: "auth.error", error: "tenant is not available" });
    expect(await service.auth({ project: "other", tenant: seeded.tenants.a, token: SERVICE_TOKEN })).toMatchObject({
      type: "auth.error",
      error: "service credential is invalid or not permitted for this project",
    });
    const authed = await service.auth({ project: seeded.project, tenant: seeded.tenants.a, token: SERVICE_TOKEN });
    expect(authed).toMatchObject({ type: "auth.result", result: { servicePrincipal: "gateway", functions: ["keys.check"], maxDelegationSeconds: 600, accountId: "" } });
    const sam = seeded.members.sam!;
    const delegate = (args: unknown) => service.call("reducer.call", "control.servicePrincipals.delegate", args);
    expect((await delegate({ accountId: sam.accountId, memberId: "member_other", reason: "api" })).error).toBe("target is not an active tenant member");
    expect((await delegate({ accountId: sam.accountId, memberId: sam.memberIds.a, reason: "api", expiresInSeconds: 601 })).error).toBe(
      "invalid arguments: expiresInSeconds must be between 30 and 600",
    );
    expect((await delegate({ accountId: sam.accountId, memberId: sam.memberIds.a, reason: "x".repeat(201) })).error).toBe("invalid arguments: reason exceeds 200 bytes");
    expect((await delegate({ accountId: sam.accountId, memberId: sam.memberIds.a, reason: "api", actor: { kind: "API key", name: "k" } })).error).toBe(
      "invalid arguments: actor.kind must be 1-32 characters of [a-z0-9_-]",
    );
    const granted = await delegate({ accountId: sam.accountId, memberId: sam.memberIds.a, reason: "api", expiresInSeconds: 60, actor: { kind: "api_key", name: "Key one", reference: "k1" } });
    expect(granted.type).toBe("reducer.result");
    const grant = granted.result as { id: string; token: string; expiresInSeconds: number };
    expect(grant.id).toMatch(/^svcdel_[0-9a-f]{32}$/);
    expect(grant.token).toMatch(/^gvx_imp_/);
    expect(grant.expiresInSeconds).toBe(60);
    expect((await service.call("query.call", "control.tenants.mine")).error).toBe("service principals cannot call Control Plane Queries");
    expect((await service.call("action.call", "control.auth.refreshSession", {})).error).toBe("service principals cannot call Control Plane Actions");
    const member = await Client.open();
    const redeemed = await member.auth({ project: seeded.project, tenant: seeded.tenants.a, token: grant.token });
    expect(redeemed).toMatchObject({ type: "auth.result", result: { accountId: sam.accountId, impersonationId: grant.id, impersonatorId: "service:gateway" } });
    expect((await member.call("query.call", "control.accounts.me")).result).toMatchObject({ id: sam.accountId });
    expect((await service.call("reducer.call", "control.servicePrincipals.revokeDelegation", { id: grant.id })).result).toEqual({ updated: true });
    expect((await service.call("reducer.call", "control.servicePrincipals.revokeDelegation", { id: grant.id })).result).toEqual({ updated: false });
    expect((await service.call("reducer.call", "control.servicePrincipals.revokeDelegation", { id: grant.id, x: 1 })).error).toBe("invalid arguments: id is required");
    // The revoked delegation drops at the next revalidation round.
    const expired = await member.next((f) => f.type === "auth.error" && f.id === "session-expired");
    expect(expired).toMatchObject({ code: "transient", error: "authentication expired or was revoked" });
  });
});

describe("socket owner timers", () => {
  it("revalidates sessions and drops a revoked one (C.3.8 item 10)", async () => {
    const seeded = await seed({ a: ["rae"] });
    const { client, session } = await login(seeded.project, seeded.members.rae!.email);
    await client.auth({ project: seeded.project, token: session.accessToken, controlOnly: true });
    const other = await login(seeded.project, seeded.members.rae!.email);
    await other.client.auth({ project: seeded.project, token: other.session.accessToken, controlOnly: true });
    expect((await other.client.call("reducer.call", "control.auth.logout", { refreshToken: "x", all: true })).result).toEqual({ updated: true });
    const expired = await client.next((f) => f.type === "auth.error" && f.id === "session-expired");
    expect(expired.error).toBe("authentication expired or was revoked");
    expect((await client.call("query.call", "control.accounts.me")).error).toBe("authentication is required");
  });

  it("closes a socket that never authenticates (C.5 auth deadline)", async () => {
    const client = await Client.open();
    await client.next((f) => f.type === "session.ready");
    await client.auth({ project: (await seed({})).project, controlOnly: true });
    expect(await client.waitClosed(4000)).toEqual({ code: 1008, reason: "authentication deadline exceeded" });
  });
});

describe("idempotency receipts expire after 24 h (0012 R9)", () => {
  it("replays within 24 h, runs again after, and the alarm prunes", async () => {
    const seeded = await seed({ a: ["ivy"] });
    const { client } = await login(seeded.project, seeded.members.ivy!.email);
    const first = await client.call("action.call", "control.auth.passwordLogin", { email: seeded.members.ivy!.email, password: PASSWORD }, "fixed-key");
    const replay = await client.call("action.call", "control.auth.passwordLogin", { email: seeded.members.ivy!.email, password: "wrong-password-1" }, "fixed-key");
    expect((replay.result as { accessToken: string }).accessToken).toBe((first.result as { accessToken: string }).accessToken);
    await runInDurableObject(control(), (instance: ControlObject) =>
      instance.sql.run("UPDATE gonvex_control_idempotency SET created_at=created_at-86400001 WHERE idempotency_key='fixed-key'"),
    );
    const expired = await client.call("action.call", "control.auth.passwordLogin", { email: seeded.members.ivy!.email, password: "wrong-password-1" }, "fixed-key");
    expect(expired.error).toBe("invalid arguments: invalid email or password");
    await runInDurableObject(control(), async (instance: ControlObject) => {
      instance.sql.run("INSERT INTO gonvex_control_idempotency(project_id,subject_id,idempotency_key,kind,path,created_at) VALUES('p','s','old','action','x',1)");
      await instance.alarm();
      expect(instance.sql.value("SELECT count(*) FROM gonvex_control_idempotency WHERE idempotency_key='old'")).toBe(0);
    });
  });
});

describe("rows read and written per call", () => {
  it("records the cost of sign-in and tenant listing", async () => {
    const seeded = await seed({ a: ["kai"], b: ["kai"], c: ["kai"] });
    const kai = seeded.members.kai!;
    const { client, session } = await login(seeded.project, kai.email);
    const loginCost = await runInDurableObject(control(), (instance: ControlObject) => instance.lastCost);
    const refresh = await client.call("action.call", "control.auth.refreshSession", { refreshToken: session.refreshToken });
    expect(refresh.type).toBe("action.result");
    const refreshCost = await runInDurableObject(control(), (instance: ControlObject) => instance.lastCost);
    await client.auth({ project: seeded.project, token: session.accessToken, controlOnly: true });
    const mine = await client.call("query.call", "control.tenants.mine");
    expect((mine.result as unknown[]).length).toBe(3);
    const mineCost = await runInDurableObject(control(), (instance: ControlObject) => instance.lastCost);
    console.log(`costs: passwordLogin ${JSON.stringify(loginCost)}, refreshSession ${JSON.stringify(refreshCost)}, tenants.mine (3 tenants) ${JSON.stringify(mineCost)}`);
    expect(loginCost.tenantCalls).toBe(3); // one admission RPC per directory tenant (0012 R15)
    expect(mineCost.tenantCalls).toBe(3);
    expect(loginCost.rowsWritten).toBeGreaterThan(0);
  });
});

describe("router", () => {
  it("serves health, refuses seeding without the admin key, and forwards /auth", async () => {
    expect((await worker("/healthz")).status).toBe(200);
    expect((await worker("/dev/projects", { method: "POST", body: "{}" })).status).toBe(403);
    const config = await worker("/auth/config?project=nope");
    expect(config.status).toBe(404);
    expect(config.headers.get("access-control-allow-origin")).toBe("*");
    void dev;
    void unique;
  });
});
