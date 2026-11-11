// Bundle size against Worker limits and startup cost, local workerd.
// For each app bundle (none, the Query/Reducer split, the complete bundle):
//   - raw and gzip bytes of the module set, and wrangler's dry-run upload size;
//   - workerd spawn -> first /ping (all modules compiled and evaluated),
//     then the first Durable Object request (constructor), and RSS. 7 runs.
// Writes .artifact/startup-results.json.
import { spawn, execFileSync } from "node:child_process";
import { readFileSync, writeFileSync, rmSync, mkdirSync, readdirSync, statSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "node:net";

const spike = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const freePort = () => new Promise((done) => { const server = createServer().listen(0, "127.0.0.1", () => { const { port } = server.address(); server.close(() => done(port)); }); });
const median = (values) => { const s = [...values].sort((a, b) => a - b); return s[Math.floor(s.length / 2)]; };
const files = (dir) => readdirSync(dir, { recursive: true }).map((f) => join(dir, f)).filter((f) => statSync(f).isFile() && /\.(js|json)$/.test(f) && !f.endsWith("config.capnp"));

const results = {};
for (const bundle of ["none", "reducers", "complete"]) {
  const runs = [];
  let sizes;
  for (let run = 0; run < 7; run++) {
    const port = await freePort();
    const storage = join(spike, ".storage", `startup-${bundle}`);
    rmSync(storage, { recursive: true, force: true });
    mkdirSync(storage, { recursive: true });
    execFileSync("node", [join(spike, "scripts/prepare.mjs"), "--bundle", bundle, "--port", String(port), "--storage", storage], { stdio: "ignore" });
    if (!sizes) {
      const all = files(join(spike, ".build")).map((f) => readFileSync(f));
      const app = readFileSync(join(spike, ".build/app.js"));
      sizes = { appBytes: app.length, appGzipBytes: gzipSync(app).length, moduleSetBytes: all.reduce((n, b) => n + b.length, 0), moduleSetGzipBytes: all.reduce((n, b) => n + gzipSync(b).length, 0) };
      try {
        const out = execFileSync(join(spike, "node_modules/.bin/wrangler"), ["deploy", "--dry-run", "--outdir", join(spike, ".storage", `wrangler-${bundle}`), "--config", join(spike, "wrangler.jsonc")], { cwd: spike, env: { ...process.env, WRANGLER_SEND_METRICS: "false", CI: "1" }, stdio: ["ignore", "pipe", "pipe"] }).toString();
        const match = /Total Upload: ([\d.]+) KiB \/ gzip: ([\d.]+) KiB/.exec(out);
        sizes.wrangler = match ? { uploadKiB: Number(match[1]), gzipKiB: Number(match[2]) } : { output: out.slice(-400) };
      } catch (error) { sizes.wrangler = { error: String(error.stderr ?? error.message).slice(-600) }; }
    }
    const started = performance.now();
    const child = spawn(join(spike, "node_modules/.bin/workerd"), ["serve", "--experimental", join(spike, ".build/config.capnp")], { stdio: "ignore" });
    const base = `http://127.0.0.1:${port}`;
    for (;;) { try { if ((await fetch(`${base}/ping`)).ok) break; } catch { /* starting */ } await new Promise((r) => setTimeout(r, 5)); }
    const readyMs = performance.now() - started;
    const t0 = performance.now();
    const meta = await (await fetch(`${base}/meta?tenant=startup`)).json();
    const firstObjectMs = performance.now() - t0;
    const rssKb = Number(execFileSync("ps", ["-o", "rss=", "-p", String(child.pid)]).toString().trim());
    child.kill();
    await new Promise((r) => child.once("exit", r));
    runs.push({ readyMs, firstObjectMs, rssKb, appExports: meta.appExports });
  }
  results[bundle] = { ...sizes, appExports: runs[0].appExports, runs: runs.length, readyMsMedian: Math.round(median(runs.map((r) => r.readyMs))), readyMsMin: Math.round(Math.min(...runs.map((r) => r.readyMs))), firstObjectMsMedian: Math.round(median(runs.map((r) => r.firstObjectMs)) * 10) / 10, rssMbMedian: Math.round(median(runs.map((r) => r.rssKb)) / 1024) };
  console.log(bundle, JSON.stringify(results[bundle]));
}
writeFileSync(join(spike, ".artifact/startup-results.json"), JSON.stringify(results, null, 2));
