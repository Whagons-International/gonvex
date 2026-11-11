// Shared driver helpers. S5_BASE is the loader Worker, S5_BAKED_BASE the baked one, S5_KEY the admin key.
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

export const spike = resolve(dirname(fileURLToPath(import.meta.url)), "..");
export const BASE = process.env.S5_BASE ?? "http://127.0.0.1:8798";
export const BAKED_BASE = process.env.S5_BAKED_BASE ?? BASE;
export const KEY = process.env.S5_KEY ?? "local-spike-key";
export const ENV = process.env.S5_LOCAL ? "local" : "cf";
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a);
export const run = Date.now().toString(36);

export async function op(tenant, name, body, base = BASE) {
  const started = performance.now();
  let response, text;
  try {
    response = await fetch(`${base}/t/${tenant}/${name}?k=${KEY}`, { method: body === undefined ? "GET" : "POST", body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(Number(process.env.S5_TIMEOUT_MS ?? 60_000)) });
    text = await response.text();
  } catch (error) {
    return { status: 0, ms: performance.now() - started, value: { error: `client: ${error?.name}: ${error?.message}` } };
  }
  const ms = performance.now() - started;
  let value;
  try { value = JSON.parse(text); } catch { value = { error: text.slice(0, 500) }; }
  return { status: response.status, ms, value };
}

/** Uploads one generation's files from .artifact/upload/<code> in 1 MB chunks. */
export async function upload(code, base = BASE) {
  const out = {};
  for (const name of ["app.js", "functions.json", "kinds.json"]) {
    const text = readFileSync(join(spike, ".artifact/upload", code, name), "utf8");
    const parts = Math.max(1, Math.ceil(text.length / 1_000_000));
    for (let part = 0; part < parts; part++) {
      const response = await fetch(`${base}/bundle/put?k=${KEY}&code=${code}&name=${name}&part=${part}`, { method: "POST", body: text.slice(part * 1_000_000, (part + 1) * 1_000_000) });
      if (!response.ok) throw new Error(`upload ${code}/${name}/${part}: ${response.status} ${await response.text()}`);
    }
    out[name] = { bytes: text.length, parts };
  }
  return out;
}

// Generation ids carry the driver run id by default, so a kernel change between runs never reuses a cached isolate.
export const gen = (code, label, nonce = run, cpuMs = null) => ({ id: `${code}:${label}:${nonce}${cpuMs ? `:cpu${cpuMs}` : ""}`, code, label, nonce, ...(cpuMs ? { cpuMs } : {}) });

export const stats = (values) => {
  const s = [...values].filter((v) => typeof v === "number").sort((a, b) => a - b);
  if (!s.length) return null;
  const q = (p) => s[Math.min(s.length - 1, Math.floor(p * s.length))];
  return { n: s.length, min: round(s[0]), p50: round(q(0.5)), p90: round(q(0.9)), max: round(s[s.length - 1]), mean: round(s.reduce((a, b) => a + b, 0) / s.length) };
};
export const round = (v) => Math.round(v * 10) / 10;

export function save(name, data) {
  mkdirSync(join(spike, "results"), { recursive: true });
  const file = join(spike, "results", `${name}-${ENV}.json`);
  writeFileSync(file, JSON.stringify(data, null, 2));
  log("wrote", file);
}

export const subset = () => JSON.parse(readFileSync(join(spike, ".artifact/schema-subset.json"), "utf8"));
