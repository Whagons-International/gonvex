/**
 * Error and performance telemetry, the pure part: a port of
 * `rust/crates/runtime/src/telemetry.rs` at `0bf3ad13` with no I/O.
 *
 * - Envelope limits (`MAX_EVENTS`, `MAX_EVENT_BYTES`, `MAX_ENVELOPE_BYTES`) and
 *   the order in which they are checked.
 * - `authoritative_event`: attribution always comes from the socket, never
 *   from the payload; secret keys are scrubbed; values are bounded.
 * - `fingerprint` and `generated_event_id` (SHA-256, truncated, lower hex).
 * - `persist_event`'s field extraction (`level`, `title`, `culprit`, …) and
 *   `DateTime::parse_from_rfc3339` for `timestamp`.
 * - `capture_performance_event`'s validation.
 * - `error.ack` in serde's key order.
 *
 * Rust holds the event as a `serde_json::Value` with `preserve_order`
 * (an IndexMap). Its `remove` is a `swap_remove`, so removing a key moves the
 * map's last key into its slot; `withoutKeys` reproduces that, so a stored
 * payload has Rust's key order. Two differences are inherent to JS values and
 * accepted (contract-changes 0041 §5): integer-like keys (`"0"`, `"42"`) are
 * ordered first by JS objects, and a float written with a zero fraction
 * (`1.0`) is held as the integer `1`.
 */
import type { Json } from "../session/decode";
import { rustTrim } from "../presence/hub";

export const LIMITS = {
  /** `MAX_EVENTS` */
  maxEvents: 20,
  /** `MAX_ENVELOPE_BYTES`: 256 KiB of serialized events. */
  maxEnvelopeBytes: 256 << 10,
  /** `MAX_EVENT_BYTES`: 32 KiB per serialized event. */
  maxEventBytes: 32 << 10,
  /** `telemetry.batch` carries at most 50 events (`lib.rs:3801`). */
  maxBatchEvents: 50,
  /** `telemetry_allowed`: per 60 s window, keyed by project and subject. */
  accountPerWindow: 120,
  connectionPerWindow: 20,
  windowMs: 60_000,
} as const;

/** `error.ack.error` texts (CONTRACT A.5). */
export const ACK_ERRORS = {
  unavailable: "error telemetry is unavailable",
  rateLimited: "error telemetry rate limit exceeded",
  store: "error store unavailable",
  invalid: "invalid error envelope",
  tooLarge: "error envelope is too large",
} as const;

const SECRET_NEEDLES = ["password", "passwd", "secret", "token", "authorization", "cookie", "apikey"];

type JsonObject = { [key: string]: Json };

const isObject = (value: Json | undefined): value is JsonObject => value !== null && typeof value === "object" && !Array.isArray(value);

/** `bounded_text`: the first `max` Unicode scalar values (`chars().take(max)`). */
export function boundedText(value: string, max: number): string {
  if (value.length <= max) return value;
  let out = "";
  let count = 0;
  for (const ch of value) {
    if (count === max) break;
    out += ch;
    count++;
  }
  return out;
}

/** Rust's `to_ascii_lowercase`: only A-Z change (a Kelvin sign stays a Kelvin sign). */
const asciiLower = (value: string) => value.replace(/[A-Z]/g, (c) => c.toLowerCase());

/** `is_secret_key` */
export function isSecretKey(key: string): boolean {
  const folded = asciiLower(key).replace(/[-_]/g, "");
  return SECRET_NEEDLES.some((needle) => folded.includes(needle));
}

/** `scrub_object`: secret keys removed, every value scrubbed one level deeper. */
function scrubObject(object: JsonObject, depth: number): JsonObject {
  // `Object.fromEntries` defines own properties, so a `__proto__` key survives as data, as in serde.
  return Object.fromEntries(
    Object.entries(object).filter(([key]) => !isSecretKey(key)).map(([key, value]) => [key, scrubValue(value, depth + 1)]),
  );
}

/** `scrub_value`: depth > 8 is truncated, strings to 4,000 chars, arrays to 50, objects to their first 100 keys. */
function scrubValue(value: Json, depth: number): Json {
  if (depth > 8) return "[Truncated]";
  if (typeof value === "string") return boundedText(value, 4_000);
  if (Array.isArray(value)) return value.slice(0, 50).map((item) => scrubValue(item, depth + 1));
  if (isObject(value)) {
    const entries = Object.entries(value);
    // `while len > 100 { remove(last key) }`: a swap_remove of the last key is a pop.
    const kept = entries.length > 100 ? Object.fromEntries(entries.slice(0, 100)) : value;
    return scrubObject(kept, depth + 1);
  }
  return value;
}

/** serde_json `Map::remove` with `preserve_order`: `swap_remove`, one key at a time. */
export function withoutKeys(object: JsonObject, keys: readonly string[]): JsonObject {
  const entries = Object.entries(object);
  for (const key of keys) {
    const index = entries.findIndex(([name]) => name === key);
    if (index < 0) continue;
    const last = entries.pop()!;
    if (index < entries.length) entries[index] = last;
  }
  return Object.fromEntries(entries);
}

/** What `authoritative_event` takes from the socket (`ControlConnection`). */
export interface Attribution {
  projectId: string;
  /** `""` without a tenant session. */
  tenantId: string;
  connectionId: string;
  /** null for an anonymous connection (no identity). */
  accountId: string | null;
}

/**
 * `authoritative_event`: null unless the event is an object with a non-blank
 * string `message`. The payload cannot choose its project, tenant, session or
 * account; an anonymous connection loses `account`, `context` and `breadcrumbs`.
 */
export function authoritativeEvent(attribution: Attribution, value: Json): JsonObject | null {
  if (!isObject(value)) return null;
  const message = value.message;
  if (typeof message !== "string" || rustTrim(message) === "") return null;
  let event = scrubObject(value, 0);
  event.project = attribution.projectId;
  event.tenant = attribution.tenantId;
  event.sessionId = attribution.connectionId;
  if (attribution.accountId !== null) event.account = { id: attribution.accountId };
  else event = withoutKeys(event, ["account", "context", "breadcrumbs"]);
  return event;
}

/** `string_field`: the string at `key`, else "". */
export function stringField(event: Json, key: string): string {
  if (!isObject(event)) return "";
  const value = event[key];
  return typeof value === "string" ? value : "";
}

const HEX = "0123456789abcdef";
function hex(bytes: Uint8Array): string {
  let out = "";
  for (const byte of bytes) out += HEX[byte >> 4]! + HEX[byte & 15]!;
  return out;
}

async function sha256(text: string): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text)));
}

/** `fingerprint`: the first 16 bytes of SHA-256(`name\nmessage\nculprit`), hex. */
export async function fingerprint(event: Json): Promise<string> {
  const input = `${stringField(event, "name")}\n${stringField(event, "message")}\n${stringField(event, "culprit")}`;
  return hex((await sha256(input)).subarray(0, 16));
}

/** `generated_event_id`: `generated-` + the first 12 bytes of SHA-256(`timestamp|message|stack|deviceId`), hex. */
export async function generatedEventId(event: Json): Promise<string> {
  const input = `${stringField(event, "timestamp")}|${stringField(event, "message")}|${stringField(event, "stack")}|${stringField(event, "deviceId")}`;
  return `generated-${hex((await sha256(input)).subarray(0, 12))}`;
}

// ---- RFC 3339 (chrono 0.4.45 `DateTime::parse_from_rfc3339`) ------------------------------------

const DIGIT = (s: string, i: number): number => {
  const c = s.charCodeAt(i);
  return c >= 48 && c <= 57 ? c - 48 : -1;
};

function daysInMonth(year: number, month: number): number {
  if (month === 2) return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0 ? 29 : 28;
  return [4, 6, 9, 11].includes(month) ? 30 : 31;
}

/**
 * Unix milliseconds of an RFC 3339 timestamp as chrono parses it, or null
 * where chrono fails (the caller then uses the current time):
 * `YYYY-MM-DD` `T`/`t`/space `hh:mm:ss`, an optional fraction of any length
 * (digits past nine are skipped), a leap second `60`, then `Z`/`z` or
 * `±hh:mm` (`−` U+2212 too) within ±23:59, and nothing after.
 */
export function parseRfc3339Millis(s: string): number | null {
  if (s.length < 19) return null;
  const d = (i: number) => DIGIT(s, i);
  const two = (i: number) => (d(i) < 0 || d(i + 1) < 0 ? -1 : d(i) * 10 + d(i + 1));
  if (d(0) < 0 || d(1) < 0 || d(2) < 0 || d(3) < 0) return null;
  const year = d(0) * 1000 + d(1) * 100 + d(2) * 10 + d(3);
  if (s[4] !== "-") return null;
  const month = two(5);
  if (month < 0 || s[7] !== "-") return null;
  const day = two(8);
  if (day < 0) return null;
  if (month < 1 || month > 12 || day < 1 || day > daysInMonth(year, month)) return null;
  if (s[10] !== "T" && s[10] !== "t" && s[10] !== " ") return null;
  const hour = two(11);
  if (hour < 0 || s[13] !== ":") return null;
  const minute = two(14);
  if (minute < 0 || s[16] !== ":") return null;
  let second = two(17);
  if (second < 0) return null;
  let nanos = 0;
  if (second === 60) {
    second = 59;
    nanos = 1_000_000_000;
  }
  let rest = s.slice(19);
  if (rest[0] === ".") {
    let i = 1;
    let value = 0;
    while (i < rest.length && i <= 9 && DIGIT(rest, i) >= 0) value = value * 10 + DIGIT(rest, i++);
    const consumed = i - 1;
    if (consumed === 0) return null;
    nanos += value * 10 ** (9 - consumed);
    while (i < rest.length && DIGIT(rest, i) >= 0) i++;
    rest = rest.slice(i);
  }
  if (hour > 23 || minute > 59 || second > 59 || nanos >= 2_000_000_000 || (nanos >= 1_000_000_000 && second !== 59)) return null;
  let offset: number;
  if (rest[0] === "Z" || rest[0] === "z") {
    offset = 0;
    rest = rest.slice(1);
  } else {
    const sign = rest[0];
    if (sign !== "+" && sign !== "-" && sign !== "−") return null;
    rest = rest.slice(1);
    const h = rest.length >= 2 && DIGIT(rest, 0) >= 0 && DIGIT(rest, 1) >= 0 ? DIGIT(rest, 0) * 10 + DIGIT(rest, 1) : -1;
    if (h < 0) return null;
    rest = rest.slice(2);
    if (rest[0] !== ":") return null;
    rest = rest.slice(1);
    if (rest.length < 2) return null;
    const m1 = DIGIT(rest, 0), m2 = DIGIT(rest, 1);
    if (m1 < 0 || m1 > 5 || m2 < 0) return null;
    rest = rest.slice(2);
    offset = (h * 3600 + (m1 * 10 + m2) * 60) * (sign === "+" ? 1 : -1);
    if (Math.abs(offset) >= 86_400) return null;
  }
  if (rest.length !== 0) return null;
  const date = new Date(0);
  date.setUTCFullYear(year, month - 1, day);
  date.setUTCHours(hour, minute, second, 0);
  return date.getTime() - offset * 1000 + Math.floor(nanos / 1_000_000);
}

// ---- envelopes --------------------------------------------------------------------------------

/** One event ready for the store: what `persist_event` binds, computed where the socket lives. */
export interface PreparedErrorEvent {
  eventId: string;
  fingerprint: string;
  /** Unix ms (`occurred_at.timestamp_millis()`). */
  occurredAt: number;
  tenant: string;
  release: string;
  environment: string;
  /** `warning`, else `error`. */
  level: string;
  account: string;
  device: string;
  /** `message`, at most 500 chars. */
  title: string;
  /** `culprit`, at most 500 chars. */
  culprit: string;
  /** The authoritative event, serialized (`payload` and `latest_event`). */
  payload: string;
}

const utf8Length = (text: string): number => {
  let bytes = 0;
  for (let i = 0; i < text.length; i++) {
    const c = text.charCodeAt(i);
    if (c < 0x80) bytes += 1;
    else if (c < 0x800) bytes += 2;
    else if (c >= 0xd800 && c <= 0xdbff && i + 1 < text.length && (text.charCodeAt(i + 1) & 0xfc00) === 0xdc00) {
      bytes += 4;
      i++;
    } else bytes += 3;
  }
  return bytes;
};

/** The envelope checks of `capture_error_envelope` after the rate limit, in Rust's order. */
export function checkEnvelope(events: readonly Json[]): typeof ACK_ERRORS.invalid | typeof ACK_ERRORS.tooLarge | null {
  if (events.length === 0 || events.length > LIMITS.maxEvents) return ACK_ERRORS.invalid;
  let total = 0;
  for (const event of events) {
    const bytes = utf8Length(JSON.stringify(event));
    if (bytes > LIMITS.maxEventBytes) return ACK_ERRORS.tooLarge;
    total += bytes;
  }
  return total > LIMITS.maxEnvelopeBytes ? ACK_ERRORS.tooLarge : null;
}

/** The events of a checked envelope that `authoritative_event` keeps, prepared for the store, in order. */
export async function prepareEvents(attribution: Attribution, events: readonly Json[], now: () => number = Date.now): Promise<PreparedErrorEvent[]> {
  const out: PreparedErrorEvent[] = [];
  for (const raw of events) {
    const event = authoritativeEvent(attribution, raw);
    if (!event) continue;
    const explicitId = stringField(event, "eventId");
    const occurredAt = parseRfc3339Millis(stringField(event, "timestamp")) ?? now();
    const account = isObject(event.account) && typeof event.account.id === "string" ? event.account.id : "";
    out.push({
      eventId: explicitId !== "" ? explicitId : await generatedEventId(event),
      fingerprint: await fingerprint(event),
      occurredAt,
      tenant: stringField(event, "tenant"),
      release: stringField(event, "release"),
      environment: stringField(event, "environment"),
      level: stringField(event, "level") === "warning" ? "warning" : "error",
      account,
      device: stringField(event, "deviceId"),
      title: boundedText(stringField(event, "message"), 500),
      culprit: boundedText(stringField(event, "culprit"), 500),
      payload: JSON.stringify(event),
    });
  }
  return out;
}

/** `error_ack`: `{"type":"error.ack","id":…}` then the flattened BTreeMap: `accepted`, `error`, `fingerprints`. */
export function errorAck(id: string, payload: { error?: string; accepted?: number; fingerprints?: readonly string[] } = {}): string {
  let out = `{"type":"error.ack","id":${JSON.stringify(id)}`;
  if (payload.accepted !== undefined) out += `,"accepted":${payload.accepted}`;
  if (payload.error !== undefined) out += `,"error":${JSON.stringify(payload.error)}`;
  if (payload.fingerprints !== undefined) out += `,"fingerprints":${JSON.stringify(payload.fingerprints)}`;
  return `${out}}`;
}

// ---- performance events -----------------------------------------------------------------------

/** One `telemetry.event` (or batch entry), as decoded. */
export interface PerformanceFrame {
  id: string;
  kind: string;
  path: string;
  reason?: string;
  outcome: string;
  error?: string;
  clientSentAtMs?: number;
  clientReceivedAtMs: number;
  clientDurationMs?: number;
  trace?: Json;
  device?: Json;
}

/** A row of `gonvex_performance_events` (without `project_id` and `created_at`). */
export interface PerformanceRow {
  eventId: string;
  tenantId: string;
  accountId: string;
  kind: string;
  path: string;
  reason: string;
  outcome: string;
  /** At most 1,000 chars. */
  error: string;
  clientSentAtMs: number | null;
  clientReceivedAtMs: number;
  clientDurationMs: number | null;
  trace: string | null;
  device: string | null;
}

/**
 * `capture_performance_event`'s checks before the rate limit: a project, a
 * known `kind` and `outcome`, `id` 1-200 bytes, `path` 1-500 bytes, finite
 * times and a non-negative duration. Invalid events are dropped silently.
 */
export function validPerformanceEvent(projectId: string, event: PerformanceFrame): boolean {
  const finite = (value: number | undefined) => value === undefined || Number.isFinite(value);
  const idBytes = utf8Length(event.id), pathBytes = utf8Length(event.path);
  return projectId !== ""
    && (event.kind === "query" || event.kind === "reducer" || event.kind === "action")
    && (event.outcome === "ok" || event.outcome === "error")
    && idBytes > 0 && idBytes <= 200
    && pathBytes > 0 && pathBytes <= 500
    && Number.isFinite(event.clientReceivedAtMs)
    && finite(event.clientSentAtMs)
    && (event.clientDurationMs === undefined || (Number.isFinite(event.clientDurationMs) && event.clientDurationMs >= 0));
}

export function performanceRow(attribution: Pick<Attribution, "tenantId" | "accountId">, event: PerformanceFrame, fallbackDevice: Json | undefined): PerformanceRow {
  const device = event.device ?? fallbackDevice;
  return {
    eventId: event.id,
    tenantId: attribution.tenantId,
    accountId: attribution.accountId ?? "",
    kind: event.kind,
    path: event.path,
    reason: event.reason ?? "",
    outcome: event.outcome,
    error: boundedText(event.error ?? "", 1_000),
    clientSentAtMs: event.clientSentAtMs ?? null,
    clientReceivedAtMs: event.clientReceivedAtMs,
    clientDurationMs: event.clientDurationMs ?? null,
    trace: event.trace === undefined ? null : JSON.stringify(event.trace),
    device: device === undefined ? null : JSON.stringify(device),
  };
}
