// Throwaway CPU-attribution probe (gonvex-cf-dev-cpuprobe-20261120). Which invocation is charged for work that runs
// as a continuation of another invocation's promise chain? cpu_ms = 1000 per invocation.
import { DurableObject } from "cloudflare:workers";
const burn = (ms) => { const t = performance.now(); let x = 0; for (let i = 0; i < ms * 400_000; i++) x += i % 7; return x; }; // clock-free busy work
export class Probe extends DurableObject {
  queue = Promise.resolve();
  /** sync: the step is CPU only (resolves synchronously). */
  async sync(ms) { const step = this.queue.then(() => { burn(ms); }); this.queue = step; await step; return "ok"; }
  /** io: the step burns, then awaits a timer (I/O owned by whichever context runs the step). */
  async io(ms) { const step = this.queue.then(async () => { burn(ms); await new Promise((r) => setTimeout(r, 2)); }); this.queue = step.catch(() => {}); await step; return "ok"; }
  /** loop: an engine-style loop kicked by the first caller drains every caller's work (each awaits a timer per step). */
  work = []; running;
  async loop(ms) {
    const done = new Promise((r) => this.work.push({ ms, r }));
    if (!this.running) this.running = (async () => { while (this.work.length) { const w = this.work.shift(); burn(w.ms); await new Promise((r) => setTimeout(r, 2)); w.r(); } this.running = undefined; })();
    await done; return "ok";
  }
  async calibrate(ms) { burn(ms); return "ok"; }
  /** E3: returns at once; `n` timer-chained background steps of burn(ms) follow. */
  bgStatus = "idle";
  async bgwork(ms, n = 20) {
    this.bgStatus = "running";
    void (async () => { for (let i = 0; i < n; i++) { burn(ms); await new Promise((r) => setTimeout(r, 2)); } this.bgStatus = "done"; })();
    return "started";
  }
  /** E4: the same steps, `chunk` per alarm invocation. */
  async alarmed(ms, n = 20) { await this.ctx.storage.put("work", { ms, left: n, chunk: 3 }); await this.ctx.storage.setAlarm(Date.now()); this.bgStatus = "running"; return "started"; }
  async alarm() {
    const w = await this.ctx.storage.get("work"); if (!w) return;
    for (let i = 0; i < w.chunk && w.left > 0; i++, w.left--) burn(w.ms);
    if (w.left > 0) { await this.ctx.storage.put("work", w); await this.ctx.storage.setAlarm(Date.now()); } else { await this.ctx.storage.delete("work"); this.bgStatus = "done"; }
  }
  async status() { return this.bgStatus; }
  /** E6: in flight ~`wait` ms, of which burn(ms) is CPU. */
  async light(ms, wait = 20) { burn(ms); await new Promise((r) => setTimeout(r, wait)); return "ok"; }
  /** ioh: the io chain, but every K steps the rest of the queue continues in a fresh invocation (a self-RPC). */
  pending = []; draining = false; since = 0;
  ioh(ms, k = 8) {
    const done = new Promise((resolve, reject) => this.pending.push({ fn: async () => { burn(ms); await new Promise((r) => setTimeout(r, 2)); }, resolve, reject, k }));
    if (!this.draining) void this.drain();
    return done.then(() => "ok");
  }
  async drain() {
    this.draining = true;
    while (this.pending.length) {
      if (this.since >= this.pending[0].k) { this.since = 0; void this.env.P.get(this.ctx.id).resume().catch(() => this.drain()); return; }
      const step = this.pending.shift(); this.since++;
      try { step.resolve(await step.fn()); } catch (e) { step.reject(e); }
    }
    this.draining = false; this.since = 0;
  }
  async resume() { await this.drain(); }
}
export default { async fetch(req, env) {
  const u = new URL(req.url), mode = u.searchParams.get("mode"), n = Number(u.searchParams.get("n") ?? 40), ms = Number(u.searchParams.get("ms") ?? 60);
  const obj = u.searchParams.get("obj") ?? mode, fresh = u.searchParams.get("fresh") === "1";
  const s = env.P.get(env.P.idFromName(obj));
  if (mode === "steady") { // two staggered callers keep calls overlapping for `secs`; returns calls and errors
    const until = Date.now() + Number(u.searchParams.get("secs") ?? 8) * 1000; let calls = 0; const errors = new Set();
    const worker = async (delay) => { await new Promise((r) => setTimeout(r, delay)); while (Date.now() < until) { try { await s.light(ms); calls++; } catch (e) { errors.add(String(e.message).slice(0, 100)); } } };
    await Promise.all([worker(0), worker(15)]);
    return Response.json({ mode, calls, errors: [...errors] });
  }
  let res;
  if (u.searchParams.get("seq")) { res = []; for (let i = 0; i < n; i++) { res.push(await s[mode](ms).then((v) => ({ status: "fulfilled", value: v }), (e) => ({ status: "rejected", reason: e }))); await new Promise((r) => setTimeout(r, Number(u.searchParams.get("seq")))); } }
  else res = await Promise.allSettled(Array.from({ length: n }, () => (fresh ? env.P.getByName(obj) : s)[mode](ms, Number(u.searchParams.get("k") ?? 8))));
  return Response.json({ mode, n, ms, ok: res.filter((r) => r.status === "fulfilled").length, errors: [...new Set(res.filter((r) => r.status === "rejected").map((r) => String(r.reason?.message ?? r.reason).slice(0, 120)))] });
} };
