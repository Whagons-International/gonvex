// The real Whagons Actions behind the ingress (K18 done_when): the complete
// module bundle of whagons5-client, built by `whagons/prepare.mjs` into
// `.build/` (git-ignored; the app is private). Without a build this file is
// skipped. The Actions run on K04's engine with their declared secrets; the
// internal Reducers they call (`calling.control`, `relay.recordWebhookEvent`,
// `github.webhookConnection`, `taskPublicShares.resolvePublicPreview`) are
// answered by recorded stand-ins, since they need the tenant database.
import { beforeAll, beforeEach, describe, expect, it } from "vitest";
// @ts-expect-error resolved by the vitest alias.
import * as whagonsApp from "gonvex:whagons-app";
import { resetIngressDirectory } from "../routes.ts";
import { GITHUB_PING, GITHUB_PULL_REQUEST, TELNYX_CALL_HANGUP, TELNYX_CALL_INITIATED } from "./fixtures/payloads.ts";
import { calls, ed25519, githubSignature, nowSeconds, recorder, seed, send, telnyxRequest, telnyxSignature, testEnv } from "./helpers.ts";

const PROJECT = "proj-whagons";
const TENANT = "tenant_07ea17fe";
const HOOK_SECRET = "whagons-hook-secret-1234567890";
const CREDENTIAL_KEY = "test-github-credential-key";
const built = testEnv.WHAGONS_BUILT === "1";
let telnyxKey: CryptoKey;
let strangerKey: CryptoKey;
let serial = 0;
const fresh = (body: string) => body.replace(/"id":"[0-9a-f]{8}/, () => `"id":"${(++serial).toString(16).padStart(8, "a")}`);

describe.skipIf(!built)("the real Whagons Actions behind the ingress", () => {
  beforeAll(async () => {
    await seed(PROJECT, [{ id: TENANT, domain: "whagons" }]);
    const telnyx = await ed25519(21);
    telnyxKey = telnyx.privateKey;
    strangerKey = (await ed25519(22)).privateKey;
    const app = whagonsApp as { github: { sealSecret(ctx: unknown, secret: string): Promise<{ ciphertext: string; iv: string; tag: string }> } };
    const sealed = await app.github.sealSecret({ secrets: { GITHUB_CREDENTIAL_KEY: CREDENTIAL_KEY } }, HOOK_SECRET);
    await recorder(PROJECT).setSecrets(PROJECT, {
      // calling: one Telnyx account per tenant id (calling/provider.ts `configuration`).
      TELNYX_TENANTS_JSON: JSON.stringify({ [TENANT]: { publicKey: telnyx.publicRaw, apiKey: "KEY_TEST", connectionId: "7267xxxxxxxxxxxxxx" } }),
      // relay: the account's public key, base64 (relay.ts accepts raw or SPKI/PEM).
      TELNYX_PUBLIC_KEY: telnyx.publicRaw,
      GITHUB_CREDENTIAL_KEY: CREDENTIAL_KEY,
      WHAGONS_APP_BASE_URL: "https://app.example.test",
    });
    await recorder(PROJECT).setValue("githubConnection", {
      _id: "github",
      login: "octo-bot",
      repositories: ["octo-org/allowed-repo"],
      autoLinkAuthors: [],
      encryptedWebhookSecret: sealed.ciphertext,
      webhookSecretIv: sealed.iv,
      webhookSecretTag: sealed.tag,
    });
  });

  beforeEach(async () => {
    await recorder(PROJECT).clear();
    resetIngressDirectory();
  });

  async function telnyx(path: string, body: string, key = telnyxKey, timestamp = nowSeconds()) {
    const signature = await telnyxSignature(key, timestamp, body);
    return send(`${path}?tenant=whagons&project=${PROJECT}`, telnyxRequest(body, { "telnyx-signature-ed25519": signature, "telnyx-timestamp": timestamp }));
  }

  async function github(body: string, event: string, secret = HOOK_SECRET) {
    return send(`/webhooks/github?tenant=whagons&project=${PROJECT}`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-github-event": event, "x-github-delivery": crypto.randomUUID(), "x-hub-signature-256": await githubSignature(secret, body) },
      body,
    });
  }

  it("calling.ingestWebhook verifies the forwarded Telnyx body and applies the event", async () => {
    const body = fresh(TELNYX_CALL_INITIATED);
    const reply = await telnyx("/webhooks/telnyx", body);
    expect(reply.status).toBe(200);
    const [call] = await calls(PROJECT);
    expect(call!.request.args.rawBody).toBe(body);
    expect(call!.report).toEqual({ ok: true, value: { id: "call_1" } });
    // Verified, then handed to the internal Reducer: forwarding first, then the call event.
    expect(call!.tools.map((entry) => [entry.tool, (entry.args as { op: string }).op])).toEqual([["control", "forwarding-event"], ["control", "webhook"]]);
    expect((call!.tools[1]!.args as { event: { id: string } }).event.id).toBe(JSON.parse(body).data.id);
  });

  it("calling.ingestWebhook refuses a wrong signature (401) and a delivery 31 s in the future (401)", async () => {
    expect((await telnyx("/webhooks/telnyx", fresh(TELNYX_CALL_INITIATED), strangerKey)).status).toBe(401);
    const future = String(Math.floor(Date.now() / 1000) + 40);
    expect((await telnyx("/webhooks/telnyx", fresh(TELNYX_CALL_INITIATED), telnyxKey, future)).status).toBe(401);
    const reports = (await calls(PROJECT)).map((call) => call.report);
    expect(reports).toEqual([
      { ok: false, error: expect.stringContaining("Invalid Telnyx webhook signature") },
      { ok: false, error: expect.stringContaining("Invalid Telnyx webhook signature") },
    ]);
    expect((await calls(PROJECT)).flatMap((call) => call.tools)).toEqual([]);
  });

  it("relay.ingestTelnyxWebhook verifies the forwarded body and records the event through its internal Reducer", async () => {
    const body = fresh(TELNYX_CALL_HANGUP);
    expect((await telnyx("/webhooks/telnyx/relay", body)).status).toBe(200);
    const [call] = await calls(PROJECT);
    expect(call!.report).toEqual({ ok: true, value: { webhookEventId: "telnyx-event", callId: null, duplicate: false } });
    expect(call!.tools).toEqual([{ tool: "recordWebhookEvent", args: { rawBody: body, signatureTimestamp: expect.any(Number) } }]);
    expect((await telnyx("/webhooks/telnyx/relay", fresh(TELNYX_CALL_HANGUP), strangerKey)).status).toBe(401);
    expect((await calls(PROJECT))[1]!.report).toEqual({ ok: false, error: expect.stringContaining("Telnyx webhook signature is invalid") });
  });

  it("github.ingestWebhook verifies the forwarded body against the sealed hook secret", async () => {
    expect((await github(GITHUB_PING, "ping")).status).toBe(200);
    expect((await github(GITHUB_PULL_REQUEST, "pull_request")).status).toBe(200);
    expect((await github(GITHUB_PULL_REQUEST, "pull_request", "not-the-secret")).status).toBe(401);
    const reports = (await calls(PROJECT)).map((call) => call.report);
    expect(reports).toEqual([
      { ok: true, value: { ok: true, pong: true } },
      // Verified and parsed; the repository is not one the connection allows, so nothing is fetched.
      { ok: true, value: { ok: true, ignored: "repository" } },
      { ok: false, error: expect.stringContaining("GitHub webhook signature is invalid") },
    ]);
  });

  it("serves the share page from taskPublicShares.getPublicPreviewByToken", async () => {
    const page = await send(`/share/task?token=share-ok&tenant=whagons&project=${PROJECT}`);
    expect(page.status).toBe(200);
    expect(page.text).toContain('<meta property="og:title" content="Fix the &lt;lobby&gt; door &amp; &quot;lock&quot; - Front desk • Open">');
    const image = await send(`/share/task-image?token=share-ok&tenant=whagons&project=${PROJECT}`);
    expect(image.status).toBe(200);
    expect(image.text).toContain("<svg");
    expect((await send(`/share/task?token=revoked&tenant=whagons&project=${PROJECT}`)).status).toBe(404);
    const [call] = await calls(PROJECT);
    expect(call!.tools).toEqual([{ tool: "resolvePublicPreview", args: { token: "share-ok" } }]);
  });
});
