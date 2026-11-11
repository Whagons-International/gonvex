/**
 * Client frame decoder with exactly the accept/close boundary of the Rust
 * runtime's `serde_json::from_str::<ClientMessage>` (CONTRACT.md A.6 item 7,
 * A.1.9 row 1003). A frame this decoder refuses closes the socket with 1003.
 *
 * serde rules reproduced here (each one has a conformance vector in
 * `cloudflare/conformance/scenarios/session-frames.mjs` and a unit test):
 *
 *  - Internally tagged enum (`#[serde(tag = "type")]`): the frame is an object
 *    whose `type` names a variant, or a sequence whose first element does
 *    (serde's `TaggedContentVisitor::visit_seq`); the rest is positional.
 *  - Structs accept a map or a sequence (positional, every missing trailing
 *    field must have a default, extra elements are refused). A known field
 *    given twice is refused (`duplicate field`); unknown fields are ignored.
 *  - Integer widths are checked on the number's text: `u8`, `u32`, `u64` refuse
 *    a fraction, an exponent, a sign (serde_json reads `-0` as a float) and
 *    anything above the width. `f64` refuses only non-finite values.
 *  - `Option<T>` takes `null` as none; a non-optional field with a default
 *    (`controlOnly`, `keys`, `fullIntegrity`) still refuses `null`.
 *  - serde_json limits: nesting depth 127, no lone UTF-16 surrogate escapes,
 *    numbers must be finite. `Value` subtrees are parsed by the native
 *    `JSON.parse` after one linear scan enforces those three.
 *
 * Decoded frames mirror the wire (camelCase) with serde's defaults filled in.
 * `clientContract` is a `bigint` so a u64 compares exactly; other u64 fields
 * (revisions, sequences) are numbers.
 */

// ---- schema -------------------------------------------------------------------

type Scalar = "string" | "bool" | "u8" | "u32" | "u64" | "u64big" | "f64" | "value";
type Type =
  | Scalar
  | { opt: Type }
  | { vec: Type }
  | { map: Type }
  | { struct: Struct }
  | { oneOf: readonly string[] };
/** [wire name, type, default when missing (undefined = required)]. */
type Field = readonly [string, Type, (() => unknown)?];
type Struct = readonly Field[];

const NONE = () => undefined;
const opt = (t: Type): Type => ({ opt: t });
/** `#[serde(default)] Option<T>`: missing or null is none. */
const o = (name: string, t: Type): Field => [name, opt(t), NONE];

const BROWSER_TELEMETRY: Struct = [
  o("userAgent", "string"), o("browserName", "string"), o("browserVersion", "string"),
  o("deviceType", "string"), o("platform", "string"), o("language", "string"), o("timezone", "string"),
  o("viewportWidth", "u32"), o("viewportHeight", "u32"), o("hardwareConcurrency", "u32"),
  o("deviceMemory", "f64"), o("touchPoints", "u32"), o("connectionType", "string"),
  o("effectiveConnectionType", "string"),
];

const CLIENT_CAPABILITIES: Struct = [
  o("replicaNoOldValues", "u8"), o("replicaFieldPatches", "u8"), o("replicaSetDigest", "u8"),
  o("replicaNativeRows", "u8"), o("replicaSnapshotPages", "u8"), o("preserveSubscriptions", "u8"),
  o("tenantControl", "u8"), o("telemetryBatch", "u8"), o("replicaReadyMany", "u8"),
  o("replicaWatermark", "u8"), o("queryPagePatch", "u8"), o("queryObjectPatch", "u8"),
  o("queryOrderDelta", "u8"), o("queryFanout", "u8"), o("queryResultBatch", "u8"),
  o("replicaBatchFrames", "u8"), o("presence", "u8"),
];

const MESSAGE_TRACE: Struct = [
  o("clientSentAtMs", "f64"), o("serverReceivedAtMs", "f64"), o("serverReducerStartedAtMs", "f64"),
  o("serverReducerCommittedAtMs", "f64"), o("serverCompletedAtMs", "f64"),
  o("serverBroadcastScheduledAtMs", "f64"), o("serverChangeCommittedAtMs", "f64"),
  o("serverSubscriptionStartedAtMs", "f64"), o("serverSubscriptionSentAtMs", "f64"),
  o("serverDurationMs", "f64"), o("queryPerf", "value"),
];

const SCOPE: Type = { oneOf: ["tenant", "control"] };
const ARGS: Field = ["args", "value", () => null];
const CURSOR: Struct = [["epoch", "string"], ["revision", "u64"]];

const QUERY_SUBSCRIBE_REQUEST: Struct = [
  ["id", "string"], ["path", "string"], ARGS, o("scope", SCOPE), o("windowRevision", "string"),
];
const REDUCER_CALL_REQUEST: Struct = [
  ["id", "string"], ["path", "string"], ARGS, o("scope", SCOPE), o("trace", { struct: MESSAGE_TRACE }),
  o("idempotencyKey", "string"), o("artifactHash", "string"), o("clientContract", "u64big"),
  o("receiptPath", "string"), o("intentEntropy", "string"),
];
const REPLICA_OPEN_REQUEST: Struct = [
  ["id", "string"], ["path", "string"], ARGS, o("cursor", { struct: CURSOR }),
  ["keys", { vec: "string" }, () => []], ["hashes", { map: "string" }, () => ({})],
  o("digest", "string"), ["fullIntegrity", "bool", () => false],
];
const PERFORMANCE_EVENT: Struct = [
  ["id", "string"], ["kind", "string"], ["path", "string"], o("reason", "string"), ["outcome", "string"],
  o("error", "string"), o("clientSentAtMs", "f64"), ["clientReceivedAtMs", "f64"], o("clientDurationMs", "f64"),
  o("trace", { struct: MESSAGE_TRACE }), o("device", { struct: BROWSER_TELEMETRY }),
];

/** `ClientMessage` (rust/crates/protocol/src/lib.rs), variant name -> fields in declaration order. */
const VARIANTS: Record<string, Struct> = {
  auth: [
    ["id", "string"], o("clientContract", "u64big"), o("token", "string"), o("project", "string"),
    o("tenant", "string"), ["controlOnly", "bool", () => false], ["public", "bool", () => false],
    o("device", { struct: BROWSER_TELEMETRY }), o("capabilities", { struct: CLIENT_CAPABILITIES }),
  ],
  "query.call": [["id", "string"], ["path", "string"], ARGS, o("scope", SCOPE)],
  "query.subscribe": QUERY_SUBSCRIBE_REQUEST,
  "query.unsubscribe": [["id", "string"]],
  "query.subscribeMany": [["subscribes", { vec: { struct: QUERY_SUBSCRIBE_REQUEST } }]],
  "replica.open": REPLICA_OPEN_REQUEST,
  "replica.openMany": [["opens", { vec: { struct: REPLICA_OPEN_REQUEST } }]],
  "replica.close": [["id", "string"]],
  "replica.snapshotAck": [["id", "string"], ["snapshotId", "string"], ["sequence", "u64"]],
  "reducer.call": REDUCER_CALL_REQUEST,
  "reducer.callMany": [["calls", { vec: { struct: REDUCER_CALL_REQUEST } }]],
  "action.call": [
    ["id", "string"], ["path", "string"], ARGS, o("scope", SCOPE), o("idempotencyKey", "string"),
    o("trace", { struct: MESSAGE_TRACE }),
  ],
  "error.register": [["id", "string"], o("release", "string"), o("environment", "string")],
  "error.envelope": [["id", "string"], ["events", { vec: "value" }]],
  "error.heartbeat": [["id", "string"]],
  "telemetry.event": [
    ["id", "string"], ["kind", "string"], ["path", "string"], o("reason", "string"), ["outcome", "string"],
    o("error", "string"), o("clientSentAtMs", "f64"), ["clientReceivedAtMs", "f64"], o("clientDurationMs", "f64"),
    o("trace", { struct: MESSAGE_TRACE }), o("device", { struct: BROWSER_TELEMETRY }),
  ],
  "telemetry.batch": [["events", { vec: { struct: PERFORMANCE_EVENT } }]],
  "presence.set": [["state", "string"]],
  "presence.subscribe": [["id", "string"]],
  "presence.unsubscribe": [],
};

// ---- decoded frame types --------------------------------------------------------

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type ExecutionScope = "tenant" | "control";
export interface ClientCapabilities {
  replicaNoOldValues?: number; replicaFieldPatches?: number; replicaSetDigest?: number;
  replicaNativeRows?: number; replicaSnapshotPages?: number; preserveSubscriptions?: number;
  tenantControl?: number; telemetryBatch?: number; replicaReadyMany?: number; replicaWatermark?: number;
  queryPagePatch?: number; queryObjectPatch?: number; queryOrderDelta?: number; queryFanout?: number;
  queryResultBatch?: number; replicaBatchFrames?: number; presence?: number;
}
export type BrowserTelemetryInfo = Record<string, string | number | undefined>;
export type MessageTrace = Record<string, Json | undefined>;
export interface AuthFrame {
  type: "auth"; id: string; clientContract?: bigint; token?: string; project?: string; tenant?: string;
  controlOnly: boolean; public: boolean; device?: BrowserTelemetryInfo; capabilities?: ClientCapabilities;
}
export interface QuerySubscribeRequest { id: string; path: string; args: Json; scope?: ExecutionScope; windowRevision?: string }
export interface ReducerCallRequest {
  id: string; path: string; args: Json; scope?: ExecutionScope; trace?: MessageTrace; idempotencyKey?: string;
  artifactHash?: string; clientContract?: bigint; receiptPath?: string; intentEntropy?: string;
}
export interface ReplicaOpenRequest {
  id: string; path: string; args: Json; cursor?: { epoch: string; revision: number }; keys: string[];
  hashes: Record<string, string>; digest?: string; fullIntegrity: boolean;
}
export type ClientFrame =
  | AuthFrame
  | { type: "query.call"; id: string; path: string; args: Json; scope?: ExecutionScope }
  | ({ type: "query.subscribe" } & QuerySubscribeRequest)
  | { type: "query.unsubscribe"; id: string }
  | { type: "query.subscribeMany"; subscribes: QuerySubscribeRequest[] }
  | ({ type: "replica.open" } & ReplicaOpenRequest)
  | { type: "replica.openMany"; opens: ReplicaOpenRequest[] }
  | { type: "replica.close"; id: string }
  | { type: "replica.snapshotAck"; id: string; snapshotId: string; sequence: number }
  | ({ type: "reducer.call" } & ReducerCallRequest)
  | { type: "reducer.callMany"; calls: ReducerCallRequest[] }
  | { type: "action.call"; id: string; path: string; args: Json; scope?: ExecutionScope; idempotencyKey?: string; trace?: MessageTrace }
  | { type: "error.register"; id: string; release?: string; environment?: string }
  | { type: "error.envelope"; id: string; events: Json[] }
  | { type: "error.heartbeat"; id: string }
  | { type: "telemetry.event"; id: string; kind: string; path: string; outcome: string; clientReceivedAtMs: number; [key: string]: unknown }
  | { type: "telemetry.batch"; events: Array<Record<string, unknown>> }
  | { type: "presence.set"; state: string }
  | { type: "presence.subscribe"; id: string }
  | { type: "presence.unsubscribe" };
export type ClientFrameType = ClientFrame["type"];

// ---- scanner --------------------------------------------------------------------

/** serde_json's recursion limit: 128 levels refused, so 127 is the deepest accepted. */
export const MAX_DEPTH = 127;
const U8_MAX = 255n, U32_MAX = 4_294_967_295n, U64_MAX = 18_446_744_073_709_551_615n;

class Refused extends Error {}
const refuse = (why: string): never => { throw new Refused(why); };

const enum C { Quote = 34, Backslash = 92, LBracket = 91, RBracket = 93, LBrace = 123, RBrace = 125, Comma = 44, Colon = 58, Minus = 45, Zero = 48, Nine = 57, Dot = 46, LowerE = 101, UpperE = 69, Plus = 43, LowerU = 117 }

const isWs = (c: number) => c === 32 || c === 9 || c === 10 || c === 13;
const hex4 = (text: string, at: number) => {
  const h = text.slice(at, at + 4);
  if (!/^[0-9a-fA-F]{4}$/.test(h)) refuse("invalid escape");
  return parseInt(h, 16);
};

/** A raw JSON value: `[start, end)` in the text, and its depth (the frame object is depth 1). */
interface Span { start: number; end: number; depth: number }

class Scanner {
  constructor(readonly text: string) {}

  skipWs(i: number): number { while (i < this.text.length && isWs(this.text.charCodeAt(i))) i++; return i; }

  /** End of the string starting at `i` (a quote). Refuses lone surrogate escapes and raw control characters. */
  stringEnd(i: number): number {
    const t = this.text;
    let pendingHigh = false;
    for (let j = i + 1; j < t.length; j++) {
      const c = t.charCodeAt(j);
      if (c === C.Quote) { if (pendingHigh) refuse("lone surrogate"); return j + 1; }
      if (c < 0x20) refuse("control character in string");
      if (c === C.Backslash) {
        const e = t.charCodeAt(++j);
        if (e === C.LowerU) {
          const u = hex4(t, j + 1);
          j += 4;
          if (u >= 0xd800 && u <= 0xdbff) { if (pendingHigh) refuse("lone surrogate"); pendingHigh = true; continue; }
          if (u >= 0xdc00 && u <= 0xdfff) { if (!pendingHigh) refuse("lone surrogate"); pendingHigh = false; continue; }
        } else if (!'"\\/bfnrt'.includes(t[j] ?? "")) refuse("invalid escape");
        if (pendingHigh) refuse("lone surrogate");
        continue;
      }
      if (pendingHigh) refuse("lone surrogate");
    }
    return refuse("unterminated string");
  }

  /** End of the number starting at `i`; strict JSON grammar, finite. */
  numberEnd(i: number): number {
    const t = this.text;
    let j = i;
    if (t.charCodeAt(j) === C.Minus) j++;
    const first = t.charCodeAt(j);
    if (first === C.Zero) j++;
    else if (first > C.Zero && first <= C.Nine) while (isDigit(t.charCodeAt(j))) j++;
    else refuse("invalid number");
    let float = false;
    if (t.charCodeAt(j) === C.Dot) { float = true; j++; if (!isDigit(t.charCodeAt(j))) refuse("invalid number"); while (isDigit(t.charCodeAt(j))) j++; }
    const e = t.charCodeAt(j);
    if (e === C.LowerE || e === C.UpperE) {
      float = true; j++;
      const s = t.charCodeAt(j); if (s === C.Plus || s === C.Minus) j++;
      if (!isDigit(t.charCodeAt(j))) refuse("invalid number");
      while (isDigit(t.charCodeAt(j))) j++;
    }
    // serde_json refuses floats that overflow f64 (integers beyond u64 become floats).
    if ((float || j - i > 20) && !Number.isFinite(Number(t.slice(i, j)))) refuse("number out of range");
    return j;
  }

  /** Scans one value starting at the first non-whitespace char at or after `i`. */
  value(i: number, depth: number): Span {
    const t = this.text;
    const start = this.skipWs(i);
    const c = t.charCodeAt(start);
    if (c === C.Quote) return { start, end: this.stringEnd(start), depth };
    if (c === C.LBrace || c === C.LBracket) {
      if (depth > MAX_DEPTH) refuse("recursion limit exceeded");
      const close = c === C.LBrace ? C.RBrace : C.RBracket;
      let j = this.skipWs(start + 1);
      if (t.charCodeAt(j) === close) return { start, end: j + 1, depth };
      for (;;) {
        if (c === C.LBrace) {
          if (t.charCodeAt(j) !== C.Quote) refuse("expected key");
          j = this.skipWs(this.stringEnd(j));
          if (t.charCodeAt(j) !== C.Colon) refuse("expected colon");
          j++;
        }
        j = this.skipWs(this.value(j, depth + 1).end);
        const d = t.charCodeAt(j);
        if (d === close) return { start, end: j + 1, depth };
        if (d !== C.Comma) refuse("expected comma");
        j = this.skipWs(j + 1);
      }
    }
    if (c === C.Minus || isDigit(c)) return { start, end: this.numberEnd(start), depth };
    for (const word of ["true", "false", "null"]) if (t.startsWith(word, start)) return { start, end: start + word.length, depth };
    return refuse("invalid value");
  }

  /** Entries of the object span: [key, value span], in order, duplicates kept. */
  entries(span: Span): Array<[string, Span]> {
    const t = this.text, out: Array<[string, Span]> = [];
    let j = this.skipWs(span.start + 1);
    if (t.charCodeAt(j) === C.RBrace) return out;
    for (;;) {
      const keyEnd = this.stringEnd(j);
      const key = JSON.parse(t.slice(j, keyEnd)) as string;
      j = this.skipWs(keyEnd) + 1;
      const value = this.value(j, span.depth + 1);
      out.push([key, value]);
      j = this.skipWs(value.end);
      if (t.charCodeAt(j) === C.RBrace) return out;
      j = this.skipWs(j + 1);
    }
  }

  /** Element spans of the array span. */
  elements(span: Span): Span[] {
    const t = this.text, out: Span[] = [];
    let j = this.skipWs(span.start + 1);
    if (t.charCodeAt(j) === C.RBracket) return out;
    for (;;) {
      const value = this.value(j, span.depth + 1);
      out.push(value);
      j = this.skipWs(value.end);
      if (t.charCodeAt(j) === C.RBracket) return out;
      j = this.skipWs(j + 1);
    }
  }

  kind(span: Span): "object" | "array" | "string" | "number" | "true" | "false" | "null" {
    const c = this.text.charCodeAt(span.start);
    if (c === C.LBrace) return "object";
    if (c === C.LBracket) return "array";
    if (c === C.Quote) return "string";
    if (c === C.Minus || isDigit(c)) return "number";
    return this.text.slice(span.start, span.end) as "true" | "false" | "null";
  }

  raw(span: Span): string { return this.text.slice(span.start, span.end); }
}

function isDigit(c: number): boolean { return c >= C.Zero && c <= C.Nine; }

// ---- typed decoding ------------------------------------------------------------

function decodeType(s: Scanner, span: Span, type: Type): unknown {
  const kind = s.kind(span);
  if (typeof type === "string") {
    switch (type) {
      case "value": return JSON.parse(s.raw(span));
      case "string": return kind === "string" ? JSON.parse(s.raw(span)) : refuse("expected a string");
      case "bool": return kind === "true" ? true : kind === "false" ? false : refuse("expected a boolean");
      case "f64": return kind === "number" ? Number(s.raw(span)) : refuse("expected a number");
      default: {
        if (kind !== "number") refuse("expected an integer");
        const raw = s.raw(span);
        if (!/^(0|[1-9][0-9]*)$/.test(raw)) refuse("expected an unsigned integer");
        const n = BigInt(raw);
        if (n > (type === "u8" ? U8_MAX : type === "u32" ? U32_MAX : U64_MAX)) refuse("integer out of range");
        return type === "u64big" ? n : Number(n);
      }
    }
  }
  if ("opt" in type) return kind === "null" ? undefined : decodeType(s, span, type.opt);
  if ("vec" in type) return kind === "array" ? s.elements(span).map(e => decodeType(s, e, type.vec)) : refuse("expected a sequence");
  if ("map" in type) {
    if (kind !== "object") refuse("expected a map");
    const out: Record<string, unknown> = {};
    for (const [key, value] of s.entries(span)) out[key] = decodeType(s, value, type.map);
    return out;
  }
  if ("oneOf" in type) {
    if (kind !== "string") refuse("expected a variant name");
    const name = JSON.parse(s.raw(span)) as string;
    return type.oneOf.includes(name) ? name : refuse("unknown variant");
  }
  return decodeStruct(s, span, type.struct, {});
}

/** A struct from a map (named fields) or a sequence (positional fields). */
function decodeStruct(s: Scanner, span: Span, fields: Struct, out: Record<string, unknown>): Record<string, unknown> {
  const kind = s.kind(span);
  if (kind === "array") return decodePositional(s, s.elements(span), fields, out);
  if (kind !== "object") return refuse("expected a struct");
  return decodeNamed(s, s.entries(span), fields, out);
}

function decodeNamed(s: Scanner, entries: Array<[string, Span]>, fields: Struct, out: Record<string, unknown>): Record<string, unknown> {
  const byName = new Map<string, Field>(fields.map(f => [f[0], f]));
  const seen = new Set<string>();
  for (const [key, value] of entries) {
    const field = byName.get(key);
    if (!field) continue; // unknown fields are ignored (no deny_unknown_fields)
    if (seen.has(key)) refuse(`duplicate field ${key}`);
    seen.add(key);
    const decoded = decodeType(s, value, field[1]);
    if (decoded !== undefined) out[key] = decoded;
  }
  for (const [name, , fallback] of fields) {
    if (seen.has(name)) continue;
    if (!fallback) refuse(`missing field ${name}`);
    const value = fallback!();
    if (value !== undefined) out[name] = value;
  }
  return out;
}

function decodePositional(s: Scanner, elements: Span[], fields: Struct, out: Record<string, unknown>): Record<string, unknown> {
  if (elements.length > fields.length) refuse("invalid length");
  fields.forEach(([name, type, fallback], index) => {
    const element = elements[index];
    const value = element ? decodeType(s, element, type) : fallback ? fallback() : refuse("invalid length");
    if (value !== undefined) out[name] = value;
  });
  return out;
}

export type DecodeResult = { ok: true; frame: ClientFrame } | { ok: false; reason: string };

/** Decodes one text frame exactly as `serde_json::from_str::<ClientMessage>` would accept it. */
export function decodeClientFrame(text: string): DecodeResult {
  try {
    const s = new Scanner(text);
    const top = s.value(0, 1);
    if (s.skipWs(top.end) !== text.length) refuse("trailing characters");
    const kind = s.kind(top);
    let type: string;
    let fields: Struct | undefined;
    const frame: Record<string, unknown> = {};
    if (kind === "object") {
      const entries = s.entries(top);
      const tags = entries.filter(([key]) => key === "type");
      if (tags.length === 0) refuse("missing field type");
      if (tags.length > 1) refuse("duplicate field type");
      const tag = tags[0]![1];
      if (s.kind(tag) !== "string") refuse("type is not a string");
      type = JSON.parse(s.raw(tag)) as string;
      fields = VARIANTS[type];
      if (!fields) refuse(`unknown variant ${type}`);
      frame.type = type;
      decodeNamed(s, entries.filter(([key]) => key !== "type"), fields!, frame);
    } else if (kind === "array") {
      const [tag, ...rest] = s.elements(top);
      if (!tag) refuse("missing field type");
      if (s.kind(tag!) !== "string") refuse("type is not a string");
      type = JSON.parse(s.raw(tag!)) as string;
      fields = VARIANTS[type];
      if (!fields) refuse(`unknown variant ${type}`);
      frame.type = type;
      decodePositional(s, rest, fields!, frame);
    } else {
      refuse("expected a map or a sequence");
    }
    return { ok: true, frame: frame as unknown as ClientFrame };
  } catch (error) {
    if (error instanceof Refused) return { ok: false, reason: error.message };
    // JSON.parse on a span the scanner accepted can only fail on grammar the
    // scanner shares; treat anything unexpected as a refusal too.
    if (error instanceof SyntaxError) return { ok: false, reason: error.message };
    throw error;
  }
}
