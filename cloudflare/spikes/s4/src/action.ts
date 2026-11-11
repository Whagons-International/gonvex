// The Action body every runner executes, and the claim / renew / run loop
// around it. Each step stands in for one Whagons Action pattern.
import { tenantStub, type Env, type Job } from "./env";
import { hex } from "./bytes";

export type Step =
  /** An agent loop: SSE "turns" with pauses, for `seconds` in total. */
  | { kind: "agent"; seconds: number; turnSeconds: number; pauseSeconds: number }
  /** One OpenRouter-style SSE stream consumed to the end. */
  | { kind: "sse"; seconds: number; intervalMs?: number }
  /**
   * A download. `stream` pipes it to an upload, `hash` only hashes it (no
   * upload, so no request-body limit), `buffer` is Whagons' responseBytes.
   */
  | { kind: "download"; mb: number; mode: "stream" | "hash" | "buffer" }
  /** One request to an endpoint that answers after `seconds` (media worker). */
  | { kind: "slow"; seconds: number }
  /** Poll a status endpoint every `everyMs` until done (sandbox poll). */
  | { kind: "poll"; seconds: number; everyMs: number }
  | { kind: "wait"; seconds: number };

export interface ActionSpec {
  steps: Step[];
  /** Kill or fail attempt `onAttempt` after `afterSeconds`. */
  crash?: { onAttempt: number; afterSeconds: number; how: "oom" | "throw" };
  /** Attempt `onAttempt` stops renewing after `afterSeconds` but keeps running. */
  stall?: { onAttempt: number; afterSeconds: number };
}

export type RunOutcome =
  | { kind: "done"; attempt: number; result: Record<string, unknown> }
  | { kind: "skipped"; reason: string; retryAfterMs?: number }
  | { kind: "lost"; attempt: number; error: string }
  | { kind: "failed"; attempt: number; error: string };

const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => {
      clearTimeout(timer);
      reject(signal.reason);
    }, { once: true });
  });

class Upstream {
  constructor(private readonly env: Env, private readonly signal: AbortSignal) {}
  fetch(path: string, init: RequestInit = {}) {
    const headers = new Headers(init.headers);
    if (this.env.S4_TOKEN) headers.set("authorization", `Bearer ${this.env.S4_TOKEN}`);
    return fetch(`${this.env.UPSTREAM_URL}${path}`, { ...init, headers, signal: this.signal });
  }
}

async function consumeSse(up: Upstream, seconds: number, intervalMs: number) {
  const started = Date.now();
  const response = await up.fetch(`/sse?seconds=${seconds}&intervalMs=${intervalMs}`);
  if (!response.ok || !response.body) throw new Error(`sse HTTP ${response.status}`);
  const firstByteMs = Date.now() - started;
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = "";
  let events = 0;
  let last = 0;
  let contiguous = true;
  let lastAt = Date.now();
  let maxGapMs = 0;
  let done = false;
  for (;;) {
    const part = await reader.read();
    if (part.done) break;
    buffer += part.value;
    let cut: number;
    while ((cut = buffer.indexOf("\n\n")) >= 0) {
      const frame = buffer.slice(0, cut);
      buffer = buffer.slice(cut + 2);
      if (frame.startsWith("event: done")) {
        done = true;
        continue;
      }
      const data = frame.split("\n").find((line) => line.startsWith("data: "));
      if (!data) continue;
      const n = (JSON.parse(data.slice(6)) as { n: number }).n;
      if (n !== last + 1) contiguous = false;
      last = n;
      events += 1;
      const now = Date.now();
      maxGapMs = Math.max(maxGapMs, now - lastAt);
      lastAt = now;
    }
  }
  return { events, contiguous, sawDone: done, maxGapMs, firstByteMs, ms: Date.now() - started };
}

/** Whagons' calling/provider.ts responseBytes, verbatim in behavior. */
async function responseBytes(response: Response, maximum: number): Promise<Uint8Array> {
  if (Number(response.headers.get("content-length") ?? 0) > maximum) throw new Error("Provider response is too large");
  const reader = response.body?.getReader();
  if (!reader) return new Uint8Array();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const part = await reader.read();
      if (part.done) break;
      size += part.value.length;
      if (size > maximum) throw new Error("Provider response is too large");
      chunks.push(part.value);
    }
  } finally {
    await reader.cancel();
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  return bytes;
}

async function download(up: Upstream, mb: number, mode: "stream" | "hash" | "buffer") {
  const started = Date.now();
  const response = await up.fetch(`/download?mb=${mb}`);
  if (!response.ok || !response.body) throw new Error(`download HTTP ${response.status}`);
  const length = Number(response.headers.get("content-length"));
  if (mode === "buffer") {
    const bytes = await responseBytes(response, Math.max(64, mb) * 1024 * 1024);
    const sha256 = hex(await crypto.subtle.digest("SHA-256", bytes));
    const sink = (await (await up.fetch("/sink", { method: "PUT", body: bytes })).json()) as Record<string, unknown>;
    return { mode, bytes: bytes.byteLength, sha256, sink, ms: Date.now() - started };
  }
  const digest = new crypto.DigestStream("SHA-256");
  const digestWriter = digest.getWriter();
  let bytes = 0;
  let chunks = 0;
  let maxChunk = 0;
  const hashing = new TransformStream<Uint8Array, Uint8Array>({
    async transform(chunk, controller) {
      bytes += chunk.byteLength;
      chunks += 1;
      maxChunk = Math.max(maxChunk, chunk.byteLength);
      await digestWriter.write(chunk.slice());
      controller.enqueue(chunk);
    },
  });
  if (mode === "hash") {
    await response.body.pipeThrough(hashing).pipeTo(new WritableStream());
    await digestWriter.close();
    return { mode, contentLength: length, bytes, chunks, maxChunk, sha256: hex(await digest.digest), ms: Date.now() - started };
  }
  // Known length: a fixed-length upload. Unknown: a chunked upload.
  const { readable, writable } = length > 0 ? new FixedLengthStream(length) : new TransformStream<Uint8Array, Uint8Array>();
  const upload = up.fetch("/sink", { method: "PUT", body: readable });
  await response.body.pipeThrough(hashing).pipeTo(writable);
  await digestWriter.close();
  const sha256 = hex(await digest.digest);
  const sink = (await (await upload).json()) as Record<string, unknown>;
  return { mode, contentLength: length, bytes, chunks, maxChunk, sha256, sink, ms: Date.now() - started };
}

async function runStep(env: Env, step: Step, signal: AbortSignal) {
  const up = new Upstream(env, signal);
  const started = Date.now();
  switch (step.kind) {
    case "agent": {
      const turns: Array<{ events: number; contiguous: boolean; maxGapMs: number }> = [];
      while (Date.now() - started < step.seconds * 1000) {
        const turn = await consumeSse(up, step.turnSeconds, 200);
        turns.push({ events: turn.events, contiguous: turn.contiguous, maxGapMs: turn.maxGapMs });
        if (Date.now() - started < step.seconds * 1000) await sleep(step.pauseSeconds * 1000, signal);
      }
      return {
        kind: step.kind,
        turns: turns.length,
        events: turns.reduce((sum, turn) => sum + turn.events, 0),
        allContiguous: turns.every((turn) => turn.contiguous),
        maxGapMs: Math.max(0, ...turns.map((turn) => turn.maxGapMs)),
        ms: Date.now() - started,
      };
    }
    case "sse":
      return { kind: step.kind, ...(await consumeSse(up, step.seconds, step.intervalMs ?? 200)) };
    case "download":
      return { kind: step.kind, ...(await download(up, step.mb, step.mode)) };
    case "slow": {
      const response = await up.fetch(`/slow?seconds=${step.seconds}`);
      const body = await response.text();
      return { kind: step.kind, status: response.status, body: body.slice(0, 200), ms: Date.now() - started };
    }
    case "poll": {
      const until = started + step.seconds * 1000;
      let polls = 0;
      for (;;) {
        polls += 1;
        const state = (await (await up.fetch(`/poll?until=${until}`)).json()) as { done: boolean };
        if (state.done) break;
        await sleep(step.everyMs, signal);
      }
      return { kind: step.kind, polls, ms: Date.now() - started };
    }
    case "wait":
      await sleep(step.seconds * 1000, signal);
      return { kind: step.kind, ms: Date.now() - started };
  }
}

/** Keeps a reference so the allocation cannot be collected. */
const hog: Uint8Array[] = [];

function injectCrash(env: Env, how: "oom" | "throw"): Promise<never> {
  if (how === "throw" || env.LOCAL === "1") {
    return Promise.reject(new Error(`injected ${how} failure${env.LOCAL === "1" ? " (local)" : ""}`));
  }
  // Grow past the 128 MB isolate limit. The runtime kills the isolate.
  for (let i = 0; i < 64; i += 1) hog.push(new Uint8Array(16 << 20).fill(i + 1));
  return Promise.reject(new Error(`allocated ${hog.length * 16} MB without being killed`));
}

/**
 * Claims the outbox row, runs the Action while renewing the lease every
 * `renewMs`, and returns the outcome. The caller applies a `done` result
 * with `TenantObject.complete`.
 */
export async function runAction(env: Env, job: Job, owner: string): Promise<RunOutcome> {
  const claim = await tenantStub(env, job.tenant).claim(job.id, owner);
  if (!claim.ok) return { kind: "skipped", reason: claim.reason, retryAfterMs: claim.retryAfterMs };
  const spec = JSON.parse(claim.args) as ActionSpec;
  const started = Date.now();
  const lease = new AbortController();
  const stallAt =
    spec.stall?.onAttempt === claim.attempt ? started + spec.stall.afterSeconds * 1000 : Infinity;
  let renewals = 0;
  let renewErrors = 0;
  let maxRenewMs = 0;
  const renewer = setInterval(async () => {
    if (Date.now() >= stallAt) return;
    const sent = Date.now();
    try {
      const ok = await tenantStub(env, job.tenant).renew(job.id, claim.attempt);
      maxRenewMs = Math.max(maxRenewMs, Date.now() - sent);
      if (ok) renewals += 1;
      else lease.abort(new Error("lease lost"));
    } catch {
      renewErrors += 1;
    }
  }, claim.renewMs);
  let crashTimer: ReturnType<typeof setTimeout> | undefined;
  const crash =
    spec.crash?.onAttempt === claim.attempt
      ? new Promise<never>((_, reject) => {
          crashTimer = setTimeout(() => injectCrash(env, spec.crash!.how).catch(reject), spec.crash!.afterSeconds * 1000);
        })
      : null;
  try {
    const work = (async () => {
      const steps = [];
      for (const step of spec.steps) steps.push(await runStep(env, step, lease.signal));
      return steps;
    })();
    const steps = await (crash ? Promise.race([work, crash]) : work);
    return {
      kind: "done",
      attempt: claim.attempt,
      result: { owner, attempt: claim.attempt, wallMs: Date.now() - started, renewals, renewErrors, maxRenewMs, steps },
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (lease.signal.aborted) return { kind: "lost", attempt: claim.attempt, error: message };
    await tenantStub(env, job.tenant).fail(job.id, claim.attempt, message);
    return { kind: "failed", attempt: claim.attempt, error: message };
  } finally {
    clearInterval(renewer);
    if (crashTimer) clearTimeout(crashTimer);
  }
}
