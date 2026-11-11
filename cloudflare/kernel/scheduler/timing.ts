// Cron run times, job ids, wake keys and the outbox backoff, as the Rust
// runtime computes them (RT/scheduler.rs Stagger, stagger_key, next_run,
// next_cron_run, staggered, cron_id, wake_key; ST/lib.rs retry_delay_ms;
// CONTRACT E.b.2, E.b.4, E.c.1). test/timing.test.ts compares every function
// with the Rust functions' own output (verify/rust-vectors).
import { sha256Hex } from "../capture/digest.ts";
import { CronSchedule, CronSyntaxError, normalizeExpression } from "./cron.ts";
import type { CronSpec } from "./specs.ts";

/**
 * Per-tenant stagger of cron runs (RT/scheduler.rs `Stagger`). On by default;
 * `GONVEX_SCHEDULER_STAGGER=0|false|off` disables it and
 * `GONVEX_SCHEDULER_CRON_SPREAD_MS` sets the expression spread (default 10 min).
 */
export interface Stagger {
  readonly enabled: boolean;
  readonly spreadMs: number;
}

export const DEFAULT_SPREAD_MS = 600_000;
export const DEFAULT_STAGGER: Stagger = Object.freeze({ enabled: true, spreadMs: DEFAULT_SPREAD_MS });
export const NO_STAGGER: Stagger = Object.freeze({ enabled: false, spreadMs: 0 });

/** `Stagger::from_env` over Worker vars. */
export function staggerFromEnv(env: { GONVEX_SCHEDULER_STAGGER?: string; GONVEX_SCHEDULER_CRON_SPREAD_MS?: string }): Stagger {
  const flag = env.GONVEX_SCHEDULER_STAGGER?.trim();
  const enabled = !(flag === "0" || flag === "false" || flag === "off");
  const text = env.GONVEX_SCHEDULER_CRON_SPREAD_MS?.trim();
  const spread = text !== undefined && /^[+-]?\d+$/.test(text) ? Number(text) : Number.NaN;
  return { enabled, spreadMs: Number.isSafeInteger(spread) && spread >= 0 ? spread : DEFAULT_SPREAD_MS };
}

/** First 8 bytes, big endian, of `sha256("{project}\0{tenant}\0{cron}")`. */
export function staggerKey(project: string, tenant: string, cron: string): bigint {
  return BigInt(`0x${sha256Hex(`${project}\0${tenant}\0${cron}`).slice(0, 16)}`);
}

/** A cron's run times cannot be computed (Rust's text, returned by sync). */
export class CronSpecError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CronSpecError";
  }
}

const schedules = new Map<string, CronSchedule | CronSyntaxError>();

/** RT/scheduler.rs parse_schedule, cached per expression. */
export function parseSchedule(spec: Pick<CronSpec, "name" | "expression">): CronSchedule {
  const normalized = normalizeExpression(spec.expression ?? "");
  let parsed = schedules.get(normalized);
  if (parsed === undefined) {
    try {
      parsed = CronSchedule.parse(normalized);
    } catch (error) {
      if (!(error instanceof CronSyntaxError)) throw error;
      parsed = error;
    }
    if (schedules.size > 256) schedules.clear();
    schedules.set(normalized, parsed);
  }
  if (parsed instanceof CronSyntaxError) {
    throw new CronSpecError(`cron ${rustDebugString(spec.name)} has invalid expression: ${parsed.message}`);
  }
  return parsed;
}

function intervalOf(spec: Pick<CronSpec, "name" | "intervalMs">): number {
  const interval = spec.intervalMs as number;
  // i64::try_from(u64): only values a JS number cannot hold exactly reach this.
  if (!Number.isSafeInteger(interval)) throw new CronSpecError(`cron ${rustDebugString(spec.name)} interval is too large`);
  return interval;
}

const mod = (a: number, n: number) => ((a % n) + n) % n;

/** The next run strictly after `afterMs`, staggered per tenant (`next_run`). */
export function nextRun(spec: CronSpec, afterMs: number, key: bigint, stagger: Stagger): number | null {
  if (!stagger.enabled) return nextOccurrence(spec, afterMs);
  if (spec.intervalMs !== undefined && spec.intervalMs !== null) {
    const interval = intervalOf(spec);
    const phase = Number(key % BigInt(interval));
    return afterMs - mod(afterMs - phase, interval) + interval;
  }
  const schedule = parseSchedule(spec);
  // The latest nominal occurrence at or before `after` may still be due at its staggered time.
  const previous = schedule.latestAtOrBefore(afterMs);
  if (previous !== null) {
    const at = staggered(schedule, previous, key, stagger.spreadMs);
    if (at > afterMs) return at;
  }
  const nominal = schedule.nextAfter(afterMs);
  return nominal === null ? null : staggered(schedule, nominal, key, stagger.spreadMs);
}

/** `nominal` plus this tenant's offset inside `min(gap to the next nominal occurrence, spread)`. */
export function staggered(schedule: CronSchedule, nominal: number, key: bigint, spreadMs: number): number {
  const next = schedule.nextAfter(nominal);
  const gap = next === null ? spreadMs : next - nominal;
  const window = Math.min(gap, spreadMs);
  if (window <= 1) return nominal;
  return nominal + Number(key % BigInt(window));
}

/** Unstaggered `next_occurrence`. */
export function nextOccurrence(spec: CronSpec, afterMs: number): number | null {
  if (spec.intervalMs !== undefined && spec.intervalMs !== null) return afterMs + intervalOf(spec);
  return parseSchedule(spec).nextAfter(afterMs);
}

/** The run after `scheduledFor`, or the first one after `now` when the scheduler fell behind (missed runs are skipped). */
export function nextCronRun(spec: CronSpec, scheduledFor: number, now: number, key: bigint, stagger: Stagger): number | null {
  const next = nextRun(spec, scheduledFor, key, stagger);
  if (next !== null && next <= now) return nextRun(spec, now, key, stagger);
  return next;
}

/** chrono `DateTime<Utc>::to_rfc3339()`: fractional seconds only when non-zero. */
export function rfc3339(ms: number): string {
  const iso = new Date(ms).toISOString(); // YYYY-MM-DDTHH:MM:SS.mmmZ
  const base = iso.slice(0, 19);
  const fraction = iso.slice(20, 23);
  return `${base}${fraction === "000" ? "" : `.${fraction}`}+00:00`;
}

/** `cron_` + hex of the first 16 bytes of `sha256("{project}\0{tenant}\0{name}\0{rfc3339}")`. */
export function cronJobId(project: string, tenant: string, name: string, atMs: number): string {
  return `cron_${sha256Hex(`${project}\0${tenant}\0${name}\0${rfc3339(atMs)}`).slice(0, 32)}`;
}

/** `wake_` + hex of the first 16 bytes of `sha256("{function}\0" + canonical args)`. */
export function wakeKey(functionPath: string, args: unknown): string {
  return `wake_${sha256Hex(`${functionPath}\0${canonicalJson(args)}`).slice(0, 32)}`;
}

/** Code point order, which is the UTF-8 byte order Rust sorts `String` keys in. */
function compareCodePoints(a: string, b: string): number {
  const ai = a[Symbol.iterator]();
  const bi = b[Symbol.iterator]();
  for (;;) {
    const x = ai.next();
    const y = bi.next();
    if (x.done) return y.done ? 0 : -1;
    if (y.done) return 1;
    const cx = x.value.codePointAt(0)!;
    const cy = y.value.codePointAt(0)!;
    if (cx !== cy) return cx < cy ? -1 : 1;
  }
}

/**
 * serde_json 1.0.151's text for a JSON number that JS wrote: integers that fit
 * i64/u64 print as integers; other numbers through `zmij`, which lays out the
 * shortest digits like ryu but writes a positive exponent as `e+NN`.
 */
export function rustNumberText(value: number): string {
  if (!Number.isFinite(value)) return "null";
  if (Number.isInteger(value)) {
    const text = BigInt(value);
    if (text >= -(2n ** 63n) && text <= 2n ** 64n - 1n) return text.toString();
  }
  return shortestFloat(value);
}

/** The shortest digits (as JS finds them) laid out as serde_json prints an f64. */
function shortestFloat(value: number): string {
  if (value === 0) return Object.is(value, -0) ? "-0.0" : "0.0";
  const sign = value < 0 ? "-" : "";
  const [mantissa, exponentText] = Math.abs(value).toExponential().split("e") as [string, string];
  const digits = mantissa.replace(".", "");
  const length = digits.length;
  const kk = Number(exponentText) + 1; // 10^(kk-1) <= v < 10^kk
  const k = kk - length;
  if (k >= 0 && kk <= 16) return `${sign}${digits}${"0".repeat(kk - length)}.0`;
  if (kk > 0 && kk <= 16) return `${sign}${digits.slice(0, kk)}.${digits.slice(kk)}`;
  if (kk > -5 && kk <= 0) return `${sign}0.${"0".repeat(-kk)}${digits}`;
  const exponent = kk - 1 >= 0 ? `+${kk - 1}` : `${kk - 1}`;
  return length === 1 ? `${sign}${digits}e${exponent}` : `${sign}${digits[0]}.${digits.slice(1)}e${exponent}`;
}

/** RT/scheduler.rs wake_key `canonical`: object keys sorted, serde_json's text otherwise. */
export function canonicalJson(value: unknown): string {
  if (value === null || value === undefined) return "null";
  if (Array.isArray(value)) return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  switch (typeof value) {
    case "string":
      return JSON.stringify(value);
    case "number":
      return rustNumberText(value);
    case "boolean":
      return value ? "true" : "false";
    case "object": {
      const record = value as Record<string, unknown>;
      const keys = Object.keys(record).filter((key) => record[key] !== undefined).sort(compareCodePoints);
      return `{${keys.map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`).join(",")}}`;
    }
    default:
      return "null";
  }
}

/** Rust's `{:?}` of a string: quotes and escapes (enough for cron names). */
export function rustDebugString(text: string): string {
  let out = '"';
  for (const char of text) {
    const code = char.codePointAt(0)!;
    if (char === '"') out += '\\"';
    else if (char === "\\") out += "\\\\";
    else if (char === "\n") out += "\\n";
    else if (char === "\r") out += "\\r";
    else if (char === "\t") out += "\\t";
    else if (char === "\0") out += "\\0";
    else if (code < 0x20 || code === 0x7f) out += `\\u{${code.toString(16)}}`;
    else out += char;
  }
  return `${out}"`;
}

const RETRY_CAP_MS = 15 * 60 * 1000;

/** Outbox backoff after a failed delivery: 1 s, doubling, capped at 15 minutes (ST/lib.rs retry_delay_ms). */
export function retryDelayMs(attempts: number): number {
  const exponent = Math.min(Math.max(Math.trunc(attempts), 1), 30) - 1;
  return Math.min(1000 * 2 ** exponent, RETRY_CAP_MS);
}
