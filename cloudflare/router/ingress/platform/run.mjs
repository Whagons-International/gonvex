// Platform run of the Ingress Worker (K18) on the development account:
// deploys the test composition as gonvex-cf-dev-k18-ingress, seeds a project
// and two tenants, sends signed Telnyx and GitHub deliveries and share-page
// requests over the public internet, records statuses and latencies, and
// deletes the Worker whatever happens.
//
//   whagons-dev credentials exec full-cloudflare-access -- node router/ingress/platform/run.mjs [--keep]
//
// Writes results.json beside this file. Never prints a credential: wrangler's
// output (which lists vars) is captured, not shown.
import { spawnSync } from "node:child_process";
import { createHash, createHmac, createPrivateKey, createPublicKey, randomBytes, randomUUID, sign } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const cloudflare = join(here, "..", "..", "..");
const NAME = "gonvex-cf-dev-k18-ingress";
const PROJECT = "proj-fixture";
const keep = process.argv.includes("--keep");

function credentials() {
  const file = process.env.WHAGONS_CREDENTIAL_FILE;
  if (!file) throw new Error("run under `whagons-dev credentials exec full-cloudflare-access --`");
  const parsed = JSON.parse(readFileSync(file, "utf8"));
  if (!parsed.accountId || !parsed.apiToken) throw new Error("credential is missing accountId or apiToken");
  return { CLOUDFLARE_ACCOUNT_ID: parsed.accountId, CLOUDFLARE_API_TOKEN: parsed.apiToken };
}

const env = { ...process.env, ...credentials(), WRANGLER_SEND_METRICS: "false" };
const wrangler = (args) => spawnSync("npx", ["wrangler", ...args], { cwd: cloudflare, env, encoding: "utf8", maxBuffer: 64 << 20 });

const token = `gvx_svc_${randomBytes(24).toString("hex")}`;
const admin = randomBytes(24).toString("hex");
const principals = JSON.stringify([{
  id: "ingress",
  tokenSha256: createHash("sha256").update(token).digest("hex"),
  projects: [PROJECT],
  functions: ["calling.ingestWebhook", "relay.ingestTelnyxWebhook", "github.ingestWebhook", "taskPublicShares.getPublicPreviewByToken"],
}]);

// Ed25519 from a fixed seed, as the workerd tests do; GitHub's documented secret.
const seed = Buffer.alloc(32, 7);
const privateKey = createPrivateKey({ key: Buffer.concat([Buffer.from("302e020100300506032b657004220420", "hex"), seed]), format: "der", type: "pkcs8" });
const publicRaw = createPublicKey(privateKey).export({ format: "der", type: "spki" }).subarray(-32).toString("base64");
const stranger = createPrivateKey({ key: Buffer.concat([Buffer.from("302e020100300506032b657004220420", "hex"), Buffer.alloc(32, 9)]), format: "der", type: "pkcs8" });
const GITHUB_SECRET = "It's a Secret to Everybody";

const telnyxSign = (key, timestamp, body) => sign(null, Buffer.concat([Buffer.from(`${timestamp}|`), Buffer.from(body)]), key).toString("base64");
const githubSign = (body, secret = GITHUB_SECRET) => `sha256=${createHmac("sha256", secret).update(body).digest("hex")}`;
const now = () => String(Math.floor(Date.now() / 1000));
const sha = (body) => createHash("sha256").update(body).digest("hex");
let serial = 0;
const event = (type = "call.initiated") => JSON.stringify({ data: { event_type: type, id: `${randomUUID()}`, occurred_at: new Date().toISOString(), payload: { call_control_id: `v3:${++serial}`, caller_id_name: "José Müller 📞" }, record_type: "event" }, meta: { attempt: 1 } });

async function timed(url, init) {
  const started = performance.now();
  const response = await fetch(url, init);
  const text = await response.text();
  return { status: response.status, text, ms: Math.round(performance.now() - started), headers: response.headers };
}

const percentile = (values, p) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
};

const results = { date: new Date().toISOString(), worker: NAME, checks: [], latencyMs: {} };
const check = (name, ok, detail) => {
  results.checks.push({ name, ok, ...(detail === undefined ? {} : { detail }) });
  console.log(`${ok ? "ok  " : "FAIL"} ${name}${detail === undefined ? "" : ` ${JSON.stringify(detail)}`}`);
};

let base;
try {
  const deploy = wrangler(["deploy", "-c", "router/ingress/platform/wrangler.jsonc", "--var", `GONVEX_INGRESS_SERVICE_TOKEN:${token}`, "--var", `GONVEX_SERVICE_PRINCIPALS:${principals}`, "--var", `K18_ADMIN_KEY:${admin}`]);
  const output = `${deploy.stdout}\n${deploy.stderr}`;
  const url = /https:\/\/[a-z0-9.-]+\.workers\.dev/.exec(output)?.[0];
  const upload = /Total Upload: ([^\n]+)/.exec(output)?.[1];
  if (deploy.status !== 0 || !url) throw new Error(`deploy failed (exit ${deploy.status}): ${output.split("\n").filter((line) => /error|✘/i.test(line)).join(" | ").slice(0, 800)}`);
  base = url;
  results.upload = upload;
  console.log(`deployed ${NAME} (${upload})`);

  for (let attempt = 0; attempt < 30; attempt++) {
    const health = await fetch(`${base}/share/task`).catch(() => null);
    if (health && health.status !== 522 && health.status !== 530) break;
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
  const adminInit = (body) => ({ method: "POST", headers: { authorization: `Bearer ${admin}`, "content-type": "application/json" }, body: JSON.stringify(body) });
  const seeded = await timed(`${base}/_k18/seed`, adminInit({ project: PROJECT, tenants: [{ id: "tenant_alpha", domain: "alpha" }, { id: "tenant_off", status: "disabled" }], secrets: { TELNYX_PUBLIC_KEY: publicRaw, GITHUB_WEBHOOK_SECRET: GITHUB_SECRET } }));
  check("seed", seeded.status === 200, seeded.status === 200 ? undefined : seeded.text.slice(0, 300));

  const telnyx = async (path, body, { key = privateKey, timestamp = now(), tenant = "alpha" } = {}) =>
    timed(`${base}${path}?tenant=${tenant}`, { method: "POST", headers: { "content-type": "application/json", "telnyx-signature-ed25519": telnyxSign(key, timestamp, body), "telnyx-timestamp": timestamp }, body });
  const github = async (body, { event: kind = "ping", secret, delivery = randomUUID(), signature } = {}) =>
    timed(`${base}/webhooks/github?tenant=alpha`, { method: "POST", headers: { "content-type": "application/json", "x-github-event": kind, "x-github-delivery": delivery, "x-hub-signature-256": signature ?? githubSign(body, secret) }, body });

  // First delivery wakes the control, ingress and recorder objects.
  const first = await telnyx("/webhooks/telnyx", event());
  results.latencyMs.firstDelivery = first.ms;
  check("telnyx: first signed delivery 200", first.status === 200, { ms: first.ms });

  const bodies = [];
  const warm = [];
  for (let n = 0; n < 30; n++) {
    const body = event(n % 2 ? "call.answered" : "call.hangup");
    bodies.push(body);
    const reply = await telnyx("/webhooks/telnyx", body);
    warm.push(reply);
  }
  check("telnyx: 30 signed deliveries 200", warm.every((r) => r.status === 200), warm.filter((r) => r.status !== 200).map((r) => r.status));
  const ms = warm.map((r) => r.ms);
  results.latencyMs.telnyxWarm = { p50: percentile(ms, 50), p95: percentile(ms, 95), max: Math.max(...ms) };

  const calls = await (await fetch(`${base}/_k18/calls?project=${PROJECT}`, { headers: { authorization: `Bearer ${admin}` } })).json();
  const forwarded = calls.filter((call) => call.request.path === "calling.ingestWebhook" && call.report.ok);
  const byteExact = bodies.every((body) => forwarded.some((call) => call.request.args.rawBody === body && call.report.value.bodySha256 === sha(body)));
  check("telnyx: every body reached the Action byte for byte and verified there", byteExact, { forwarded: forwarded.length });
  check("telnyx: Action ran as _gonvex_service:ingress on the system channel", forwarded.every((call) => call.report.value.member === "_gonvex_service:ingress" && call.report.value.rootChannel === "system"));

  const forged = await telnyx("/webhooks/telnyx", event(), { key: stranger });
  check("telnyx: wrong signature 401", forged.status === 401, { status: forged.status, ms: forged.ms });
  const stale = await telnyx("/webhooks/telnyx", event(), { timestamp: String(Math.floor(Date.now() / 1000) - 400) });
  check("telnyx: stale timestamp 401", stale.status === 401, { status: stale.status });
  const unknown = await telnyx("/webhooks/telnyx", event(), { tenant: "nobody" });
  check("telnyx: unknown tenant 404", unknown.status === 404, { status: unknown.status });
  const disabled = await telnyx("/webhooks/telnyx", event(), { tenant: "tenant_off" });
  check("telnyx: disabled tenant 404", disabled.status === 404, { status: disabled.status });
  const replayBody = event();
  const timestamp = now();
  const replayed = [await telnyx("/webhooks/telnyx", replayBody, { timestamp }), await telnyx("/webhooks/telnyx", replayBody, { timestamp })];
  const replayCalls = (await (await fetch(`${base}/_k18/calls?project=${PROJECT}`, { headers: { authorization: `Bearer ${admin}` } })).json()).filter((call) => call.request.args.rawBody === replayBody);
  check("telnyx: replayed delivery answered 200 and run once", replayed.every((r) => r.status === 200) && replayCalls.length === 1, { statuses: replayed.map((r) => r.status), runs: replayCalls.length });
  const relay = await telnyx("/webhooks/telnyx/relay", event("call.hangup"));
  check("relay: signed delivery 200", relay.status === 200, { ms: relay.ms });

  const vector = await github("Hello, World!", { signature: "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17" });
  check("github: documented vector 200", vector.status === 200, { ms: vector.ms });
  const wrong = await github("Hello, World!", { secret: "wrong" });
  check("github: wrong secret 401", wrong.status === 401, { status: wrong.status });
  const max = `{"zen":"${"z".repeat((5 << 20) - 10)}"}`;
  const big = await github(max);
  check("github: exactly 5 MiB 200", big.status === 200, { status: big.status, ms: big.ms });
  const over = await github(`${max} `);
  check("github: 5 MiB + 1 413", over.status === 413, { status: over.status });

  const page = await timed(`${base}/share/task?token=share-ok&tenant=alpha`);
  check("share page 200 with Open Graph tags", page.status === 200 && page.text.includes('property="og:title"') && page.text.includes("/share/task-image?token=share-ok&amp;tenant=alpha"), { status: page.status, ms: page.ms });
  const image = await timed(`${base}/share/task-image?token=share-ok&tenant=alpha`);
  check("share image 200 svg", image.status === 200 && image.headers.get("content-type")?.startsWith("image/svg+xml") && image.text.includes("<svg"), { status: image.status, ms: image.ms });
  const missing = await timed(`${base}/share/task?token=nope&tenant=alpha`);
  check("share page for an unknown token 404", missing.status === 404, { status: missing.status });
  results.latencyMs.sharePage = page.ms;
} catch (error) {
  check("run", false, String(error?.message ?? error));
} finally {
  if (!keep && base !== undefined) {
    const removed = wrangler(["delete", "--name", NAME, "--force"]);
    check("worker deleted", removed.status === 0, removed.status === 0 ? undefined : `${removed.stderr}`.slice(0, 300));
  } else if (!keep) {
    // A failed deploy may still have created the script.
    wrangler(["delete", "--name", NAME, "--force"]);
  }
  writeFileSync(join(here, "results.json"), `${JSON.stringify(results, null, 2)}\n`);
  if (results.checks.some((c) => !c.ok)) process.exitCode = 1;
}
