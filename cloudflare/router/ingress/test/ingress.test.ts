import { runInDurableObject } from "cloudflare:test";
import { beforeAll, beforeEach, describe, expect, it } from "vitest";
import { ControlDirectory, type ControlAuthenticator } from "../directory.ts";
import { resetIngressDirectory } from "../routes.ts";
import { GITHUB_DOC_VECTOR, GITHUB_PING, GITHUB_PULL_REQUEST, TELNYX_CALL_HANGUP, TELNYX_CALL_INITIATED } from "./fixtures/payloads.ts";
import {
  bytesOf,
  calls,
  control,
  ed25519,
  githubSignature,
  nowSeconds,
  recorder,
  seed,
  send,
  sha256Hex,
  telnyxRequest,
  telnyxSignature,
} from "./helpers.ts";

const PROJECT = "proj-fixture";
/** vitest.config.ts `PLAIN_TOKEN`: a principal configured without `channel`. */
const PLAIN_TOKEN = "gvx_svc_k18_plain_test_credential";
const ALPHA = "tenant_alpha";
const GITHUB_SECRET = GITHUB_DOC_VECTOR.secret;
let telnyxKey: CryptoKey;
let serial = 0;
let INITIATED = "";
let HANGUP = "";
/** The recorded body with a fresh event id, so tests never replay each other's deliveries. */
const fresh = (body: string) => body.replace(/"id":"[0-9a-f]{8}/, () => `"id":"${(++serial).toString(16).padStart(8, "0")}`);
let strangerKey: CryptoKey;

beforeAll(async () => {
  await seed(PROJECT, [{ id: ALPHA, domain: "alpha" }, { id: "tenant_off", domain: "off", status: "disabled" }]);
  const telnyx = await ed25519(7);
  telnyxKey = telnyx.privateKey;
  strangerKey = (await ed25519(9)).privateKey;
  await recorder(PROJECT).setSecrets(PROJECT, { TELNYX_PUBLIC_KEY: telnyx.publicRaw, GITHUB_WEBHOOK_SECRET: GITHUB_SECRET });
});

beforeEach(async () => {
  await recorder(PROJECT).clear();
  INITIATED = fresh(TELNYX_CALL_INITIATED);
  HANGUP = fresh(TELNYX_CALL_HANGUP);
  resetIngressDirectory();
});

async function signedTelnyx(path: string, body: string | Uint8Array, options: { key?: CryptoKey; timestamp?: string; tenant?: string; query?: string } = {}) {
  const timestamp = options.timestamp ?? nowSeconds();
  const signature = await telnyxSignature(options.key ?? telnyxKey, timestamp, body);
  const query = options.query ?? `?tenant=${options.tenant ?? ALPHA}`;
  return { reply: await send(`${path}${query}`, telnyxRequest(body, { "telnyx-signature-ed25519": signature, "telnyx-timestamp": timestamp })), signature, timestamp };
}

async function github(body: string | Uint8Array, options: { secret?: string; event?: string; delivery?: string; tenant?: string; signature?: string } = {}) {
  const signature = options.signature ?? (await githubSignature(options.secret ?? GITHUB_SECRET, body));
  const headers: Record<string, string> = { "content-type": "application/json", "x-github-event": options.event ?? "pull_request", "x-hub-signature-256": signature };
  if (options.delivery !== "") headers["x-github-delivery"] = options.delivery ?? crypto.randomUUID();
  return send(`/webhooks/github?tenant=${options.tenant ?? ALPHA}`, { method: "POST", headers, body: bytesOf(body) });
}

describe("Telnyx (calling.ingestWebhook)", () => {
  it("passes the raw signed body to the Action, which verifies it, as the ingress service principal", async () => {
    const { reply, signature, timestamp } = await signedTelnyx("/webhooks/telnyx", INITIATED);
    expect(reply.status).toBe(200);
    expect(reply.body).toEqual({ ok: true });
    expect(reply.headers.get("content-type")).toBe("application/json; charset=utf-8");
    const [call, ...rest] = await calls();
    expect(rest).toEqual([]);
    // 0002: the invocation the adapter made (path, args, principal), with the channel the principal is configured with (0045 §3).
    expect(call!.request.path).toBe("calling.ingestWebhook");
    expect(call!.request.args).toEqual({ rawBody: INITIATED, signature, timestamp });
    expect(call!.request.principal).toMatchObject({ id: "ingress", projectId: PROJECT, tenantId: ALPHA });
    expect(call!.request.channel).toBe("system");
    expect(call!.report).toMatchObject({
      ok: true,
      value: { bodySha256: await sha256Hex(INITIATED), channel: "system", rootChannel: "system", member: "_gonvex_service:ingress", role: "system", tenant: ALPHA },
    });
  });

  it("keeps every byte: escapes, non-ASCII, CRLF and a trailing newline", async () => {
    const variants = [
      HANGUP,
      `${HANGUP}\n`,
      HANGUP.replace("{", "{ \r\n  "),
    ];
    for (const body of variants) {
      const bytes = bytesOf(body);
      const { reply } = await signedTelnyx("/webhooks/telnyx", bytes);
      expect(reply.status, JSON.stringify(body.slice(0, 12))).toBe(200);
    }
    const reports = (await calls()).map((call) => call.report);
    expect(reports.map((report) => (report.ok ? (report.value as { bodySha256: string }).bodySha256 : report.error))).toEqual(
      await Promise.all(variants.map((body) => sha256Hex(bytesOf(body)))),
    );
    expect((await calls()).map((call) => call.request.args.rawBody)).toEqual(variants);
    // A leading byte-order mark is not JSON: refused like the gateway's serde_json does.
    expect((await signedTelnyx("/webhooks/telnyx", `\uFEFF${HANGUP}`)).reply.status).toBe(400);
  });

  it("refuses a wrong signature: 401, as the Action's verification failed", async () => {
    const forged = await signedTelnyx("/webhooks/telnyx", INITIATED, { key: strangerKey });
    expect(forged.reply.status).toBe(401);
    expect(forged.reply.body).toEqual({ ok: false });
    // Signed for one body, delivered with another.
    const timestamp = nowSeconds();
    const signature = await telnyxSignature(telnyxKey, timestamp, INITIATED);
    const tampered = await send(`/webhooks/telnyx?tenant=${ALPHA}`, telnyxRequest(INITIATED.replace("+35319605860", "+35319605869"), { "telnyx-signature-ed25519": signature, "telnyx-timestamp": timestamp }));
    expect(tampered.status).toBe(401);
    expect((await calls()).map((call) => call.report)).toEqual([
      { ok: false, error: expect.stringContaining("Invalid Telnyx webhook signature") },
      { ok: false, error: expect.stringContaining("Invalid Telnyx webhook signature") },
    ]);
  });

  it("refuses unsigned, stale and future-dated deliveries before any tenant is touched", async () => {
    const unsigned = await send(`/webhooks/telnyx?tenant=${ALPHA}`, telnyxRequest(INITIATED, { "telnyx-timestamp": nowSeconds() }));
    expect(unsigned.status).toBe(401);
    const noTimestamp = await send(`/webhooks/telnyx?tenant=${ALPHA}`, telnyxRequest(INITIATED, { "telnyx-signature-ed25519": "c2ln" }));
    expect(noTimestamp.status).toBe(401);
    const now = Math.floor(Date.now() / 1000);
    for (const timestamp of [String(now - 301), String(now + 301), "17e8", "-1", ""]) {
      const { reply } = await signedTelnyx("/webhooks/telnyx", INITIATED, { timestamp });
      expect(reply.status, timestamp).toBe(401);
    }
    const { reply } = await signedTelnyx("/webhooks/telnyx", INITIATED, { timestamp: String(now - 290) });
    expect(reply.status).toBe(200);
    expect((await calls()).length).toBe(1);
  });

  it("refuses an unknown, disabled or missing tenant and an unknown project", async () => {
    expect((await signedTelnyx("/webhooks/telnyx", INITIATED, { tenant: "nobody" })).reply.status).toBe(404);
    expect((await signedTelnyx("/webhooks/telnyx", INITIATED, { tenant: "tenant_off" })).reply.status).toBe(404);
    expect((await signedTelnyx("/webhooks/telnyx", INITIATED, { tenant: "off" })).reply.status).toBe(404);
    expect((await signedTelnyx("/webhooks/telnyx", INITIATED, { query: "" })).reply.status).toBe(400);
    expect((await signedTelnyx("/webhooks/telnyx", INITIATED, { query: `?tenant=${"x".repeat(129)}` })).reply.status).toBe(400);
    expect((await signedTelnyx("/webhooks/telnyx", INITIATED, { query: `?tenant=${ALPHA}&project=elsewhere` })).reply.status).toBe(404);
    // Served by this ingress, but the control object knows no such project.
    expect((await signedTelnyx("/webhooks/telnyx", INITIATED, { query: `?tenant=${ALPHA}&project=proj-elsewhere` })).reply.status).toBe(503);
    expect(await calls()).toEqual([]);
  });

  it("resolves the tenant by its domain and runs the Action in the canonical tenant", async () => {
    const { reply } = await signedTelnyx("/webhooks/telnyx", INITIATED, { tenant: "ALPHA" });
    expect(reply.status).toBe(200);
    expect((await calls())[0]!.request.principal.tenantId).toBe(ALPHA);
  });

  it("keys the tenant cache on the exact selector: a wrong-case miss cannot block the real tenant (review of #368)", async () => {
    // K06 matches tenant_id case-sensitively, so TENANT_ALPHA is unknown; it must not poison tenant_alpha.
    expect((await signedTelnyx("/webhooks/telnyx", fresh(TELNYX_CALL_INITIATED), { tenant: "TENANT_ALPHA" })).reply.status).toBe(404);
    expect((await signedTelnyx("/webhooks/telnyx", fresh(TELNYX_CALL_INITIATED), { tenant: ALPHA })).reply.status).toBe(200);
    // The domain matches case-insensitively in K06; each spelling is its own cache entry.
    expect((await signedTelnyx("/webhooks/telnyx", fresh(TELNYX_CALL_INITIATED), { tenant: "Alpha" })).reply.status).toBe(200);
    expect((await calls()).map((call) => call.request.principal.tenantId)).toEqual([ALPHA, ALPHA]);
  });

  it("rate-limits on (project, selector) before the tenant lookup, so an unknown selector answers 429 past the burst", async () => {
    const statuses: number[] = [];
    // The burst is 100 and the bucket refills at 50/s while the loop runs, so the first 429 comes a little after 100.
    for (let n = 0; n < 160 && !statuses.includes(429); n++) statuses.push((await signedTelnyx("/webhooks/telnyx", fresh(TELNYX_CALL_INITIATED), { tenant: "ghost" })).reply.status);
    expect(statuses.slice(0, 100)).toEqual(new Array(100).fill(404));
    expect(statuses.at(-1)).toBe(429);
    expect(new Set(statuses)).toEqual(new Set([404, 429]));
    // Another selector has its own bucket.
    expect((await signedTelnyx("/webhooks/telnyx", fresh(TELNYX_CALL_INITIATED), { tenant: ALPHA })).reply.status).toBe(200);
  });

  it("bounds control lookups for unknown selectors: past the project's budget, misses answer 429 without a lookup", async () => {
    // GONVEX_INGRESS_LOOKUP_RATE is 1/10 in the test Worker.
    const statuses: number[] = [];
    for (let n = 0; n < 12; n++) statuses.push((await signedTelnyx("/webhooks/telnyx", fresh(TELNYX_CALL_INITIATED), { tenant: `random-${n}` })).reply.status);
    expect(statuses.slice(0, 10)).toEqual(new Array(10).fill(404));
    expect(statuses.slice(10)).toEqual([429, 429]);
    expect(await calls()).toEqual([]);
  });

  it("takes the channel from the principal's configuration (0045 §3)", async () => {
    const grantOf = (token: string) => runInDurableObject(control(), async (instance) =>
      new ControlDirectory(() => instance as unknown as ControlAuthenticator, token).resolve(PROJECT, ALPHA));
    expect((await grantOf("gvx_svc_k18_ingress_test_credential"))?.channel).toBe("system");
    const plain = await grantOf(PLAIN_TOKEN);
    expect(plain?.principalId).toBe("plain");
    expect(plain && "channel" in plain).toBe(false);
  });

  it("refuses a disabled tenant within the directory cache window", async () => {
    await control().devCreateTenant(PROJECT, "tenant_later", "later");
    expect((await signedTelnyx("/webhooks/telnyx", INITIATED, { tenant: "tenant_later" })).reply.status).toBe(200);
    await control().testTenant(PROJECT, "tenant_later", { status: "disabled" });
    resetIngressDirectory();
    const event = fresh(TELNYX_CALL_INITIATED);
    expect((await signedTelnyx("/webhooks/telnyx", event, { tenant: "tenant_later" })).reply.status).toBe(404);
  });

  it("answers 400 for a body that is not UTF-8, not JSON, or has no event id", async () => {
    const invalid = new Uint8Array([...bytesOf('{"data":{"id":"x"}}'), 0xff]);
    expect((await signedTelnyx("/webhooks/telnyx", invalid)).reply.status).toBe(400);
    expect((await signedTelnyx("/webhooks/telnyx", "not json")).reply.status).toBe(400);
    expect((await signedTelnyx("/webhooks/telnyx", '{"data":{}}')).reply.status).toBe(400);
    expect((await signedTelnyx("/webhooks/telnyx", `{"data":{"id":"${"x".repeat(513)}"}}`)).reply.status).toBe(400);
    expect(await calls()).toEqual([]);
  });

  it("answers 405 with Allow: POST to anything but POST", async () => {
    const reply = await send(`/webhooks/telnyx?tenant=${ALPHA}`);
    expect(reply.status).toBe(405);
    expect(reply.headers.get("allow")).toBe("POST");
    expect(reply.body).toEqual({ ok: false });
  });

  it("refuses more than 1 MiB, declared or streamed", async () => {
    const big = `{"data":{"id":"big","pad":"${"a".repeat(1 << 20)}"}}`;
    expect((await signedTelnyx("/webhooks/telnyx", big)).reply.status).toBe(413);
    const timestamp = nowSeconds();
    const signature = await telnyxSignature(telnyxKey, timestamp, big);
    const chunks = bytesOf(big);
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        for (let offset = 0; offset < chunks.length; offset += 65536) controller.enqueue(chunks.slice(offset, offset + 65536));
        controller.close();
      },
    });
    const streamed = await send(`/webhooks/telnyx?tenant=${ALPHA}`, { method: "POST", headers: { "telnyx-signature-ed25519": signature, "telnyx-timestamp": timestamp }, body: stream, duplex: "half" } as RequestInit);
    expect(streamed.status).toBe(413);
    expect(await calls()).toEqual([]);
  });

  it("answers a replayed delivery without running the Action again, and runs concurrent duplicates once", async () => {
    const timestamp = nowSeconds();
    const signature = await telnyxSignature(telnyxKey, timestamp, INITIATED);
    const request = () => send(`/webhooks/telnyx?tenant=${ALPHA}`, telnyxRequest(INITIATED, { "telnyx-signature-ed25519": signature, "telnyx-timestamp": timestamp }));
    expect((await request()).status).toBe(200);
    expect((await request()).status).toBe(200);
    expect((await calls()).length).toBe(1);

    const other = fresh(TELNYX_CALL_INITIATED);
    const otherSignature = await telnyxSignature(telnyxKey, timestamp, other);
    const burst = await Promise.all(Array.from({ length: 5 }, () =>
      send(`/webhooks/telnyx?tenant=${ALPHA}`, telnyxRequest(other, { "telnyx-signature-ed25519": otherSignature, "telnyx-timestamp": timestamp }))));
    expect(burst.map((reply) => reply.status)).toEqual([200, 200, 200, 200, 200]);
    expect((await calls()).length).toBe(2);
  });
});

describe("Telnyx Relay (relay.ingestTelnyxWebhook)", () => {
  it("routes to the Relay Action, which verifies the forwarded body", async () => {
    const { reply, signature, timestamp } = await signedTelnyx("/webhooks/telnyx/relay", HANGUP);
    expect(reply.status).toBe(200);
    const [call] = await calls();
    expect(call!.request.path).toBe("relay.ingestTelnyxWebhook");
    expect(call!.request.args).toEqual({ rawBody: HANGUP, signature, timestamp });
    expect(call!.report).toMatchObject({ ok: true, value: { rootChannel: "system", bodySha256: await sha256Hex(HANGUP) } });
  });

  it("refuses a wrong signature with 401", async () => {
    const { reply } = await signedTelnyx("/webhooks/telnyx/relay", HANGUP, { key: strangerKey });
    expect(reply.status).toBe(401);
    expect((await calls())[0]!.report).toEqual({ ok: false, error: expect.stringContaining("Telnyx webhook signature is invalid") });
  });
});

describe("GitHub (github.ingestWebhook)", () => {
  it("verifies GitHub's documented vector on the forwarded body", async () => {
    const reply = await github(GITHUB_DOC_VECTOR.body, { signature: GITHUB_DOC_VECTOR.signature, event: "ping" });
    expect(reply.status).toBe(200);
    const [call] = await calls();
    expect(call!.request.path).toBe("github.ingestWebhook");
    expect(call!.request.args).toEqual({ rawBody: GITHUB_DOC_VECTOR.body, signature: GITHUB_DOC_VECTOR.signature, event: "ping" });
    expect(call!.report).toMatchObject({ ok: true, value: { event: "ping", member: "_gonvex_service:ingress" } });
  });

  it("passes provider-formatted deliveries through untouched", async () => {
    expect((await github(GITHUB_PING, { event: "ping" })).status).toBe(200);
    expect((await github(GITHUB_PULL_REQUEST)).status).toBe(200);
    const reports = (await calls()).map((call) => (call.report.ok ? (call.report.value as { bodySha256: string }).bodySha256 : call.report.error));
    expect(reports).toEqual([await sha256Hex(GITHUB_PING), await sha256Hex(GITHUB_PULL_REQUEST)]);
  });

  it("refuses a wrong secret (401), a missing signature or event header (401), and an unknown tenant (404)", async () => {
    expect((await github(GITHUB_PULL_REQUEST, { secret: "not the hook secret" })).status).toBe(401);
    const unsigned = await send(`/webhooks/github?tenant=${ALPHA}`, { method: "POST", headers: { "x-github-event": "ping" }, body: GITHUB_PING });
    expect(unsigned.status).toBe(401);
    const noEvent = await send(`/webhooks/github?tenant=${ALPHA}`, { method: "POST", headers: { "x-hub-signature-256": await githubSignature(GITHUB_SECRET, GITHUB_PING) }, body: GITHUB_PING });
    expect(noEvent.status).toBe(401);
    expect((await github(GITHUB_PULL_REQUEST, { tenant: "nobody" })).status).toBe(404);
    expect((await calls()).length).toBe(1);
  });

  it("keeps a leading byte-order mark (the default TextDecoder drops it)", async () => {
    const body = `\uFEFF${GITHUB_PING}`;
    expect((await github(body, { event: "ping" })).status).toBe(200);
    const [call] = await calls();
    expect(call!.request.args.rawBody).toBe(body);
    expect(call!.report).toMatchObject({ ok: true, value: { bodySha256: await sha256Hex(body), bodyBytes: bytesOf(body).byteLength } });
  });

  it("accepts 5 MiB and refuses 5 MiB + 1", async () => {
    const pad = (size: number) => `{"zen":"${"z".repeat(size - 10)}"}`;
    const exact = pad(5 << 20);
    expect(bytesOf(exact).byteLength).toBe(5 << 20);
    expect((await github(exact, { event: "ping" })).status).toBe(200);
    expect((await github(pad((5 << 20) + 1), { event: "ping" })).status).toBe(413);
  });

  it("acknowledges a redelivery of a handled delivery once, and runs a failed one again", async () => {
    const delivery = crypto.randomUUID();
    expect((await github(GITHUB_PING, { event: "ping", delivery })).status).toBe(200);
    expect((await github(GITHUB_PING, { event: "ping", delivery })).status).toBe(200);
    expect((await calls()).length).toBe(1);
    // The Action fails: 503, the provider retries, and the retry runs.
    const failing = crypto.randomUUID();
    expect((await github(GITHUB_PING, { event: "fail", delivery: failing })).status).toBe(503);
    expect((await github(GITHUB_PING, { event: "fail", delivery: failing })).status).toBe(503);
    expect((await calls()).length).toBe(3);
  });
});

describe("share pages", () => {
  it("renders /share/task server-side with escaped Open Graph tags, links and a nonce-bound script", async () => {
    const reply = await send(`/share/task?token=share-ok&tenant=alpha`);
    expect(reply.status).toBe(200);
    expect(reply.headers.get("content-type")).toBe("text/html; charset=utf-8");
    expect(reply.headers.get("cache-control")).toBe("no-store");
    expect(reply.headers.get("referrer-policy")).toBe("no-referrer");
    const html = reply.text;
    expect(html).toContain('<meta property="og:title" content="Fix the &lt;lobby&gt; door &amp; &quot;lock&quot; - Front desk • Open">');
    expect(html).toContain('<meta property="og:image" content="https://ingress.test/share/task-image?token=share-ok&amp;tenant=alpha">');
    expect(html).toContain('<meta property="og:image:type" content="image/svg+xml">');
    expect(html).toContain('<meta property="og:url" content="https://ingress.test/share/task?token=share-ok&amp;tenant=alpha">');
    expect(html).toContain('<meta property="al:ios:url" content="whagons://task-share/share-ok?tenantId=alpha">');
    expect(html).toContain('href="https://alpha.app.example.test/share/task?token=share-ok&amp;tenant=alpha"');
    expect(html).toContain("Due</span><strong>Nov 20, 2026</strong>");
    expect(html).toContain("2 assignees");
    expect(html).not.toContain("<script>alert");
    expect(html).toContain("&lt;script&gt;alert(1)&lt;/script&gt;");
    const nonce = /<script nonce="([^"]+)">/.exec(html)?.[1];
    expect(nonce).toBeTruthy();
    expect(reply.headers.get("content-security-policy")).toContain(`script-src 'nonce-${nonce}'`);
    // The preview came from the Action the web app calls, through the ingress principal.
    const [call] = await calls();
    expect(call!.request).toMatchObject({ path: "taskPublicShares.getPublicPreviewByToken", args: { token: "share-ok" }, principal: { tenantId: ALPHA } });
    expect(call!.tools).toEqual([{ tool: "resolvePublicPreview", args: { token: "share-ok" } }]);
  });

  it("renders /share/task-image as the 1200x630 SVG card", async () => {
    const reply = await send(`/share/task-image?token=share-ok&tenant=${ALPHA}`);
    expect(reply.status).toBe(200);
    expect(reply.headers.get("content-type")).toBe("image/svg+xml; charset=utf-8");
    expect(reply.headers.get("cache-control")).toBe("public, max-age=300");
    expect(reply.text).toContain('<svg width="1200" height="630"');
    expect(reply.text).toContain("Fix the &lt;lobby&gt; door &amp; &quot;lock&quot;</text>");
    expect(reply.text).toContain("Front desk • Open • High");
    expect(reply.text).toContain("Spot Lobby • Due Nov 20, 2026 • 2 assignees");
    expect(reply.text).toContain("Shared by Ana");
  });

  it("answers 404 for an unknown token, an unknown or missing tenant, or no token", async () => {
    for (const path of ["/share/task?token=nope&tenant=alpha", "/share/task?token=share-ok&tenant=nobody", "/share/task?token=share-ok", "/share/task?tenant=alpha", `/share/task?token=${"t".repeat(257)}&tenant=alpha`]) {
      const reply = await send(path);
      expect(reply.status, path).toBe(404);
      expect(reply.text).toContain("Task link unavailable");
      expect(reply.headers.get("x-robots-tag")).toBe("noindex");
    }
    const image = await send("/share/task-image?token=nope&tenant=alpha");
    expect(image.status).toBe(404);
    expect(image.headers.get("content-type")).toBe("image/svg+xml; charset=utf-8");
  });

  it("answers HEAD without a body and refuses other methods", async () => {
    const head = await send("/share/task?token=share-ok&tenant=alpha", { method: "HEAD" });
    expect(head.status).toBe(200);
    expect(head.text).toBe("");
    const post = await send("/share/task?token=share-ok&tenant=alpha", { method: "POST" });
    expect(post.status).toBe(405);
    expect(post.headers.get("allow")).toBe("GET, HEAD");
  });

  it("leaves other paths to the rest of the router", async () => {
    expect((await send("/webhooks/unknown?tenant=alpha", { method: "POST" })).status).toBe(404);
    expect((await send("/share/other")).status).toBe(404);
  });
});

// 0002: compare the invocation the external adapter makes today with the
// ingress's. Cases and bodies from whagons-api-gateway `tests/webhooks.rs`
// (01e8193); its fake runtime verifies nothing, so here every delivery is
// signed with the test key and a current timestamp instead of "sig" and
// "1790000000".
describe("parity with the adapter it replaces (whagons-api-gateway tests/webhooks.rs)", () => {
  const EVENT = '{"data":{"id":"evt-1","event_type":"call.answered","payload":{"call_control_id":"cc-1"}}}';

  it("forwards_the_raw_signed_body_as_the_service_principal", async () => {
    const { reply, signature, timestamp } = await signedTelnyx("/webhooks/telnyx", EVENT);
    expect([reply.status, reply.body]).toEqual([200, { ok: true }]);
    const forwarded = await calls();
    expect(forwarded.map((call) => [call.request.path, call.request.args, call.identity.member])).toEqual([
      // The gateway: path calling.ingestWebhook, these exact args, member "service" (its fake's name for the service socket).
      ["calling.ingestWebhook", { rawBody: EVENT, timestamp, signature }, "_gonvex_service:ingress"],
    ]);
  });

  it("a_bad_signature_is_unauthorized / an_unknown_tenant_is_not_found", async () => {
    const bad = await signedTelnyx("/webhooks/telnyx", EVENT.replace("evt-1", "evt-2"), { key: strangerKey });
    expect([bad.reply.status, bad.reply.body]).toEqual([401, { ok: false }]);
    expect((await signedTelnyx("/webhooks/telnyx", EVENT.replace("evt-1", "evt-3"), { tenant: "nobody" })).reply.status).toBe(404);
  });

  it("malformed_requests_never_reach_the_runtime", async () => {
    const timestamp = nowSeconds();
    const sign = (body: string) => telnyxSignature(telnyxKey, timestamp, body);
    const cases: [string, RequestInit, number][] = [
      [`/webhooks/telnyx?tenant=${ALPHA}`, telnyxRequest(EVENT, {}), 401],
      ["/webhooks/telnyx", telnyxRequest(EVENT, { "telnyx-signature-ed25519": await sign(EVENT), "telnyx-timestamp": timestamp }), 400],
      [`/webhooks/telnyx?tenant=${ALPHA}`, telnyxRequest("not json", { "telnyx-signature-ed25519": await sign("not json"), "telnyx-timestamp": timestamp }), 400],
      [`/webhooks/telnyx?tenant=${ALPHA}`, telnyxRequest('{"data":{}}', { "telnyx-signature-ed25519": await sign('{"data":{}}'), "telnyx-timestamp": timestamp }), 400],
      [`/webhooks/telnyx?tenant=${ALPHA}&project=elsewhere`, telnyxRequest(EVENT, { "telnyx-signature-ed25519": await sign(EVENT), "telnyx-timestamp": timestamp }), 404],
      [`/webhooks/telnyx?tenant=${ALPHA}`, { method: "GET" }, 405],
    ];
    for (const [path, init, status] of cases) expect((await send(path, init)).status, path).toBe(status);
    expect(await calls()).toEqual([]);
  });

  it("an_action_failure_asks_telnyx_to_retry (GitHub here: the fixture's Telnyx Action cannot fail after verifying)", async () => {
    const delivery = crypto.randomUUID();
    expect((await github(GITHUB_PING, { event: "fail", delivery })).status).toBe(503);
    expect((await github(GITHUB_PING, { event: "ping", delivery })).status).toBe(200);
  });

  it("github_deliveries_reach_github_ingest_webhook", async () => {
    const body = '{"action":"opened","pull_request":{"number":7}}';
    const reply = await github(body, { event: "pull_request", delivery: "delivery-1" });
    expect(reply.status).toBe(200);
    const [call] = await calls();
    expect([call!.request.path, call!.request.args]).toEqual(["github.ingestWebhook", { rawBody: body, event: "pull_request", signature: await githubSignature(GITHUB_SECRET, body) }]);
  });
});

describe("cost", () => {
  it("resolves a tenant in the control object with no write, reading each tenant row of the project once (K06 resolveTenant, 0043 §10)", async () => {
    // Twenty tenants, so a scan would show.
    for (let n = 0; n < 20; n++) await control().devCreateTenant(PROJECT, `tenant_cost_${n}`, `cost ${n}`);
    const cost = await runInDurableObject(control(), async (instance) => {
      const directory = new ControlDirectory(() => instance as unknown as ControlAuthenticator, "gvx_svc_k18_ingress_test_credential");
      const before = instance.sql.mark();
      const byId = await directory.resolve(PROJECT, "tenant_cost_7");
      const middle = instance.sql.mark();
      const byDomain = await directory.resolve(PROJECT, "alpha");
      const after = instance.sql.mark();
      return { byId: byId?.route.tenantId, byDomain: byDomain?.route.tenantId, id: { read: middle.read - before.read, written: middle.written - before.written }, domain: { read: after.read - middle.read, written: after.written - middle.written } };
    });
    console.log(JSON.stringify({ target: "cost", ...cost }));
    expect(cost.byId).toBe("tenant_cost_7");
    expect(cost.byDomain).toBe(ALPHA);
    expect(cost.id.written).toBe(0);
    expect(cost.domain.written).toBe(0);
    // `tenant_id = $2 OR lower(domain) = lower($2)` scans the project's tenants (plus one row for the query itself).
    const tenants = await runInDurableObject(control(), async (instance) => Number(instance.sql.value("SELECT count(*) FROM gonvex_runtime_tenants WHERE project_id=$1", PROJECT)));
    expect(cost.id.read).toBeLessThanOrEqual(tenants + 1);
    expect(cost.domain.read).toBeLessThanOrEqual(tenants + 1);
  });
});
