// `ctx.fetch` for Actions (CONTRACT.md B.2a `fetch`, `fetchRead`,
// `fetchCancel`; rust/crates/runtime/src/action_calls.rs `fetch`,
// `OpenFetchBodies`). Kept from Rust:
//
// - absolute http(s) URLs only; the origin must be declared (checked by the
//   engine as a sticky denial, and again here as a host failure);
// - redirects are followed by hand, each target checked against the declared
//   origins (reqwest's custom redirect policy); an undeclared target fails the
//   request with reqwest's text;
// - responses are capped at 8 MiB, by content-length and by the bytes read;
// - bodies are decoded as lossy UTF-8 (0012 R24), with the status text taken
//   from the status code (reqwest's `canonical_reason`), not from the wire;
// - `text/event-stream` responses are not read: they are handed to the module
//   as a stream id read with `fetchRead`, in whole UTF-8 characters, with the
//   8 MiB cap applied to everything read;
// - the request budget is the rest of the Action's deadline (none for agent
//   Actions); `fetch` cannot start after the deadline.
//
// Different on purpose: a request body string is sent as bytes, so workerd
// adds no `content-type` that Rust would not send; redirects stop after 32
// hops (reqwest's custom policy has no limit; the Worker has a subrequest
// limit). The body streams through workerd's fetch, so a long event stream or
// a slow request costs no memory beyond the chunk being read (S4).
export const MAX_FETCH_RESPONSE_BYTES = 8 << 20;
export const MAX_CONCURRENT_FETCHES = 16;
export const MAX_REDIRECTS = 32;

export type Fetcher = (request: Request) => Promise<Response>;

interface FetchRequest {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: string | null;
}

/** `http::StatusCode::canonical_reason` for every code the `http` crate names. */
const REASONS: Record<number, string> = {
  100: "Continue", 101: "Switching Protocols", 102: "Processing",
  200: "OK", 201: "Created", 202: "Accepted", 203: "Non Authoritative Information", 204: "No Content", 205: "Reset Content",
  206: "Partial Content", 207: "Multi-Status", 208: "Already Reported", 226: "IM Used",
  300: "Multiple Choices", 301: "Moved Permanently", 302: "Found", 303: "See Other", 304: "Not Modified", 305: "Use Proxy",
  307: "Temporary Redirect", 308: "Permanent Redirect",
  400: "Bad Request", 401: "Unauthorized", 402: "Payment Required", 403: "Forbidden", 404: "Not Found", 405: "Method Not Allowed",
  406: "Not Acceptable", 407: "Proxy Authentication Required", 408: "Request Timeout", 409: "Conflict", 410: "Gone",
  411: "Length Required", 412: "Precondition Failed", 413: "Payload Too Large", 414: "URI Too Long", 415: "Unsupported Media Type",
  416: "Range Not Satisfiable", 417: "Expectation Failed", 418: "I'm a teapot", 421: "Misdirected Request",
  422: "Unprocessable Entity", 423: "Locked", 424: "Failed Dependency", 426: "Upgrade Required", 428: "Precondition Required",
  429: "Too Many Requests", 431: "Request Header Fields Too Large", 451: "Unavailable For Legal Reasons",
  500: "Internal Server Error", 501: "Not Implemented", 502: "Bad Gateway", 503: "Service Unavailable", 504: "Gateway Timeout",
  505: "HTTP Version Not Supported", 506: "Variant Also Negotiates", 507: "Insufficient Storage", 508: "Loop Detected",
  510: "Not Extended", 511: "Network Authentication Required",
};

export const canonicalReason = (status: number): string => REASONS[status] ?? "";

const tooLarge = () => new Error(`fetch response exceeds the ${MAX_FETCH_RESPONSE_BYTES} byte limit`);

/** `origin()` in action_calls.rs: scheme, host and an explicit non-default port. */
export function originOf(url: URL): string {
  return `${url.protocol.slice(0, -1)}://${url.hostname}${url.port ? `:${url.port}` : ""}`;
}

/** RFC 9110 token: what `http::Method::from_bytes` accepts. */
const METHOD = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;

/** Length of the prefix of `bytes` that ends on a complete UTF-8 sequence (`complete_utf8_len`). */
export function completeUtf8Length(bytes: Uint8Array): number {
  for (let back = 1; back <= Math.min(bytes.length, 3); back++) {
    const byte = bytes[bytes.length - back]!;
    if ((byte & 0xc0) === 0x80) continue;
    const needed = byte >= 0xf0 ? 4 : byte >= 0xe0 ? 3 : byte >= 0xc0 ? 2 : 1;
    return needed > back ? bytes.length - back : bytes.length;
  }
  return bytes.length;
}

/** `String::from_utf8_lossy`: WHATWG's decoder replaces the same maximal subparts. */
const lossy = (bytes: Uint8Array): string => new TextDecoder("utf-8", { fatal: false, ignoreBOM: true }).decode(bytes);

function decodeRequest(value: unknown): FetchRequest {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error("invalid fetch request: invalid type: expected struct FetchRequest");
  const r = value as Record<string, unknown>;
  if (typeof r.url !== "string") throw new Error("invalid fetch request: missing field `url`");
  const headers: Record<string, string> = {};
  if (r.headers !== undefined && r.headers !== null) {
    if (typeof r.headers !== "object" || Array.isArray(r.headers)) throw new Error("invalid fetch request: invalid type: expected a map");
    for (const [name, header] of Object.entries(r.headers)) {
      if (typeof header !== "string") throw new Error("invalid fetch request: invalid type: expected a string");
      headers[name] = header;
    }
  }
  if (r.body !== undefined && r.body !== null && typeof r.body !== "string") throw new Error("invalid fetch request: invalid type: expected a string");
  return { url: r.url, method: typeof r.method === "string" ? r.method : "", headers, body: (r.body as string | null | undefined) ?? null };
}

interface OpenBody {
  reader: ReadableStreamDefaultReader<Uint8Array>;
  pending: Uint8Array;
  received: number;
  abort: AbortController;
  timer: ReturnType<typeof setTimeout> | undefined;
  timeoutMs: number | undefined;
  timedOut: boolean;
}

export interface NetworkHostOptions {
  /** Declared `networkOrigins`. */
  origins: readonly string[];
  /** Action deadline (unix ms); `null` for agent Actions. */
  deadlineUnixMs: number | null;
  fetcher?: Fetcher | undefined;
  clock?: (() => number) | undefined;
}

/** One invocation's network: its open event streams and in-flight requests. */
export class NetworkHost {
  private readonly allowed: Set<string>;
  private readonly fetcher: Fetcher;
  private readonly clock: () => number;
  private readonly bodies = new Map<number, OpenBody>();
  private nextStream = 0;
  private inFlight = 0;
  /** Requests started and bytes received, for the PR's numbers and tests. */
  readonly stats = { requests: 0, subrequests: 0, bytes: 0, streams: 0 };

  constructor(private readonly options: NetworkHostOptions) {
    this.allowed = new Set(options.origins.map((origin) => origin.trim()));
    this.fetcher = options.fetcher ?? ((request) => fetch(request));
    this.clock = options.clock ?? (() => Date.now());
  }

  /** In-flight `fetch` calls; the engine refuses a 17th (`V8/lib.rs:650`). */
  get concurrent(): number { return this.inFlight; }

  async fetch(value: unknown): Promise<unknown> {
    if (this.inFlight >= MAX_CONCURRENT_FETCHES) throw new ConcurrencyLimit();
    this.inFlight++;
    try {
      return await this.request(decodeRequest(value));
    } finally {
      this.inFlight--;
    }
  }

  private timeout(): number | undefined {
    const deadline = this.options.deadlineUnixMs;
    if (deadline === null) return undefined;
    const now = this.clock();
    if (deadline <= now) throw new Error("fetch cannot start after the Action execution deadline");
    return deadline - now;
  }

  private async request(request: FetchRequest): Promise<unknown> {
    let url: URL;
    try { url = new URL(request.url.trim()); } catch { throw new Error("fetch only supports absolute http and https URLs"); }
    if ((url.protocol !== "http:" && url.protocol !== "https:") || !url.hostname) throw new Error("fetch only supports absolute http and https URLs");
    const requested = originOf(url);
    if (!this.allowed.has(requested)) throw new Error(`fetch origin ${JSON.stringify(requested)} is not declared for this Action`);
    const timeoutMs = this.timeout();
    const method = request.method.trim() === "" ? "GET" : request.method.trim();
    if (!METHOD.test(method)) throw new Error("invalid fetch method: invalid HTTP method");

    const abort = new AbortController();
    let timedOut = false;
    const timer = timeoutMs === undefined ? undefined : setTimeout(() => { timedOut = true; abort.abort(new Error("timeout")); }, timeoutMs);
    const timeoutText = (phase: string) => `fetch ${phase} timed out within the ${timeoutMs ?? 30_000} ms Action request budget`;
    let handedOff = false;
    try {
      let response: Response;
      try {
        response = await this.follow(url, method, request, abort.signal);
      } catch (error) {
        if (error instanceof RedirectRefused) throw new Error("fetch request failed: error following redirect");
        if (timedOut) throw new Error(timeoutText("request"));
        throw new Error("fetch request failed: error sending request");
      }
      const declared = response.headers.get("content-length");
      if (declared !== null && Number(declared) > MAX_FETCH_RESPONSE_BYTES) {
        await response.body?.cancel().catch(() => undefined);
        throw tooLarge();
      }
      const headers: Record<string, string> = {};
      for (const [name, value] of [...response.headers].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) headers[name.toLowerCase()] = value;
      const head = { status: response.status, statusText: canonicalReason(response.status), url: response.url || url.href, headers };
      if ((headers["content-type"] ?? "").trim().toLowerCase().startsWith("text/event-stream")) {
        // Read as it arrives through fetchRead; the deadline keeps running.
        const id = ++this.nextStream;
        this.stats.streams++;
        const reader = (response.body ?? new ReadableStream<Uint8Array>({ start: (c) => c.close() })).getReader();
        const open: OpenBody = { reader, pending: new Uint8Array(0), received: 0, abort, timer, timeoutMs, timedOut: false };
        if (timer !== undefined) {
          clearTimeout(timer);
          open.timer = setTimeout(() => { open.timedOut = true; abort.abort(new Error("timeout")); }, Math.max(0, (this.options.deadlineUnixMs ?? 0) - this.clock()));
        }
        this.bodies.set(id, open);
        handedOff = true;
        return { ...head, stream: id };
      }
      let bytes: Uint8Array;
      try {
        bytes = await readCapped(response);
      } catch (error) {
        if (error instanceof CapExceeded) throw tooLarge();
        if (timedOut) throw new Error(timeoutText("response body"));
        throw new Error("fetch response body failed: request or response body error");
      }
      this.stats.bytes += bytes.byteLength;
      return { ...head, body: lossy(bytes) };
    } finally {
      if (!handedOff && timer !== undefined) clearTimeout(timer);
    }
  }

  /** The redirect loop of reqwest's custom policy, each hop checked against the declared origins. */
  private async follow(start: URL, startMethod: string, request: FetchRequest, signal: AbortSignal): Promise<Response> {
    let url = start;
    let method = startMethod;
    let body: Uint8Array | null = request.body === null ? null : new TextEncoder().encode(request.body);
    const headers = new Headers();
    for (const [name, value] of Object.entries(request.headers)) headers.set(name, value);
    this.stats.requests++;
    for (let hop = 0; ; hop++) {
      this.stats.subrequests++;
      const response = await this.fetcher(new Request(url.href, { method, headers, body: method === "GET" || method === "HEAD" ? null : body, redirect: "manual", signal }));
      const location = response.headers.get("location");
      if (![301, 302, 303, 307, 308].includes(response.status) || location === null) {
        if (!response.url) Object.defineProperty(response, "url", { value: url.href });
        return response;
      }
      await response.body?.cancel().catch(() => undefined);
      let next: URL;
      try { next = new URL(location, url); } catch { return response; }
      if (hop + 1 >= MAX_REDIRECTS || (next.protocol !== "http:" && next.protocol !== "https:") || !this.allowed.has(originOf(next))) throw new RedirectRefused();
      // reqwest: 301/302 turn a POST into a GET, 303 turns anything but HEAD into a GET; the body goes with it.
      if ((response.status === 303 && method !== "HEAD") || ((response.status === 301 || response.status === 302) && method === "POST")) {
        method = "GET";
        body = null;
        headers.delete("content-type");
        headers.delete("content-length");
      }
      // reqwest drops credentials when the host or port changes.
      if (next.host !== url.host) for (const name of ["authorization", "cookie", "proxy-authorization", "www-authenticate"]) headers.delete(name);
      url = next;
    }
  }

  /** `OpenFetchBodies::read`: the next chunk of whole UTF-8 characters, or `done`. */
  async read(stream: unknown): Promise<unknown> {
    const id = Number(stream);
    const open = this.bodies.get(id);
    if (!open) return { done: true };
    for (;;) {
      let next: ReadableStreamReadResult<Uint8Array>;
      try {
        next = await open.reader.read();
      } catch {
        this.close(id);
        if (open.timedOut) throw new Error(`fetch response body timed out within the ${open.timeoutMs ?? 30_000} ms Action request budget`);
        throw new Error("fetch response body failed: request or response body error");
      }
      if (next.done) {
        const rest = lossy(open.pending);
        this.close(id);
        return rest === "" ? { done: true } : { chunk: rest, done: false };
      }
      open.received += next.value.byteLength;
      this.stats.bytes += next.value.byteLength;
      if (open.received > MAX_FETCH_RESPONSE_BYTES) {
        this.close(id);
        throw tooLarge();
      }
      const joined = new Uint8Array(open.pending.length + next.value.byteLength);
      joined.set(open.pending);
      joined.set(next.value, open.pending.length);
      const end = completeUtf8Length(joined);
      if (end === 0) { open.pending = joined; continue; }
      open.pending = joined.slice(end);
      return { chunk: lossy(joined.subarray(0, end)), done: false };
    }
  }

  /** `fetchCancel`: drops the stream (and its connection). */
  cancel(stream: unknown): null {
    this.close(Number(stream));
    return null;
  }

  private close(id: number): void {
    const open = this.bodies.get(id);
    if (!open) return;
    this.bodies.delete(id);
    if (open.timer !== undefined) clearTimeout(open.timer);
    open.reader.cancel().catch(() => undefined);
  }

  /** Ends the invocation: every open stream is dropped, as Rust drops them with the invocation. */
  closeAll(): void {
    for (const id of [...this.bodies.keys()]) this.close(id);
  }
}

/** Raised by the isolate in Rust, so it reaches the module without the capability prefix. */
export class ConcurrencyLimit extends Error {
  constructor() { super("Action concurrent network request limit exceeded"); }
}
class RedirectRefused extends Error {}
class CapExceeded extends Error {}

async function readCapped(response: Response): Promise<Uint8Array> {
  if (!response.body) return new Uint8Array(0);
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  for (;;) {
    const part = await reader.read();
    if (part.done) break;
    size += part.value.byteLength;
    if (size > MAX_FETCH_RESPONSE_BYTES) {
      await reader.cancel().catch(() => undefined);
      throw new CapExceeded();
    }
    chunks.push(part.value);
  }
  const out = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) { out.set(chunk, offset); offset += chunk.byteLength; }
  return out;
}
