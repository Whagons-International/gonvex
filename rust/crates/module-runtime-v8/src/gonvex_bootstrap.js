// Gonvex module bootstrap.
//
// Evaluated as a classic script in every isolate; the script's completion value
// is the dispatcher the Rust host calls, so the dispatcher never has to be
// parked on globalThis. Nothing about an invocation is stored globally either:
// identity, capabilities and budgets arrive as call arguments and die with the
// call, which is what makes recycling an isolate across tenants safe.
//
// The context objects built here are the `@gonvex/module-sdk` surface:
// `QueryContext` gets `db.query`, `ReducerContext` adds `db.insert/update/
// delete` and the transactional `actions.enqueue`, while `ActionContext` gets
// only its declared tools, network origins, secrets, scheduler, and storage,
// but no database handle at all. Writes
// travel as a table name, a key and a JSON object — never as SQL text a module
// interpolated values into. The Rust host quotes the identifiers and binds the
// values as parameters.
//
// Reaching `Deno.core.ops.op_gonvex_host_call` directly is not a privilege
// escalation: the Rust op re-checks the capability and the host-call budget of
// the active invocation before it forwards anything, so these context objects
// are ergonomics, not the security boundary.
((core) => {
  "use strict";

  const ops = core.ops;

  const format = (value) => {
    if (typeof value === "string") return value;
    if (value instanceof Error) return value.stack || `${value.name}: ${value.message}`;
    try {
      const encoded = JSON.stringify(value);
      return encoded === undefined ? String(value) : encoded;
    } catch {
      return String(value);
    }
  };

  // deno_core ships no console; without one, a stray console.log in a module
  // fails as a ReferenceError far from its cause.
  if (typeof globalThis.console === "undefined") {
    const write = (isError) => (...args) => core.print(`[gonvex:module] ${args.map(format).join(" ")}\n`, isError);
    globalThis.console = Object.freeze({
      log: write(false),
      info: write(false),
      debug: write(false),
      warn: write(true),
      error: write(true),
      trace: write(true),
    });
  }

  const formEncode = (value) => encodeURIComponent(String(value)).replace(/%20/g, "+").replace(/[!'()~]/g, (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`);
  const formDecode = (value) => decodeURIComponent(String(value).replace(/\+/g, " "));
  class GonvexURLSearchParams {
    constructor(init = "", changed) {
      this.items = [];
      this.changed = changed;
      if (typeof init === "string") {
        for (const pair of init.replace(/^\?/, "").split("&")) {
          if (!pair) continue;
          const [name, ...rest] = pair.split("=");
          this.items.push([formDecode(name), formDecode(rest.join("="))]);
        }
      } else if (typeof init?.[Symbol.iterator] === "function") {
        for (const pair of init) this.items.push([String(pair[0]), String(pair[1])]);
      } else if (init && typeof init === "object") {
        for (const [name, value] of Object.entries(init)) this.items.push([name, String(value)]);
      }
    }
    notify() { this.changed?.(this.toString()); }
    append(name, value) { this.items.push([String(name), String(value)]); this.notify(); }
    delete(name, value) { const key = String(name); this.items = this.items.filter((entry) => entry[0] !== key || (arguments.length > 1 && entry[1] !== String(value))); this.notify(); }
    get(name) { return this.items.find((entry) => entry[0] === String(name))?.[1] ?? null; }
    getAll(name) { return this.items.filter((entry) => entry[0] === String(name)).map((entry) => entry[1]); }
    has(name, value) { return this.items.some((entry) => entry[0] === String(name) && (arguments.length < 2 || entry[1] === String(value))); }
    set(name, value) { const key = String(name); const remaining = this.items.filter((entry) => entry[0] !== key); const first = this.items.findIndex((entry) => entry[0] === key); remaining.splice(first < 0 ? remaining.length : Math.min(first, remaining.length), 0, [key, String(value)]); this.items = remaining; this.notify(); }
    sort() { this.items = this.items.map((entry, index) => ({ entry, index })).sort((left, right) => left.entry[0] < right.entry[0] ? -1 : left.entry[0] > right.entry[0] ? 1 : left.index - right.index).map(({ entry }) => entry); this.notify(); }
    entries() { return this.items.map((entry) => [...entry])[Symbol.iterator](); }
    keys() { return this.items.map((entry) => entry[0])[Symbol.iterator](); }
    values() { return this.items.map((entry) => entry[1])[Symbol.iterator](); }
    forEach(callback, thisArg) { for (const [name, value] of this.items) callback.call(thisArg, value, name, this); }
    toString() { return this.items.map(([name, value]) => `${formEncode(name)}=${formEncode(value)}`).join("&"); }
    [Symbol.iterator]() { return this.entries(); }
  }
  class GonvexURL {
    constructor(input, base) { this.parse(String(input), base === undefined ? undefined : String(base)); }
    parse(input, base) {
      const result = JSON.parse(ops.op_gonvex_parse_url(JSON.stringify({ input, base })));
      if (result.error) throw new TypeError(`Invalid URL: ${result.error}`);
      this.parts = result;
      this.params = new GonvexURLSearchParams(result.search, (query) => { this.parts.search = query ? `?${query}` : ""; this.rebuild(); });
    }
    rebuild() { this.parse(`${this.parts.protocol}//${this.parts.username ? `${this.parts.username}${this.parts.password ? `:${this.parts.password}` : ""}@` : ""}${this.parts.host}${this.parts.pathname}${this.parts.search}${this.parts.hash}`); }
    get href() { return this.parts.href; } set href(value) { this.parse(String(value)); }
    get origin() { return this.parts.origin; }
    get protocol() { return this.parts.protocol; } set protocol(value) { this.parts.protocol = String(value).replace(/:?$/, ":"); this.rebuild(); }
    get username() { return this.parts.username; } set username(value) { this.parts.username = String(value); this.rebuild(); }
    get password() { return this.parts.password; } set password(value) { this.parts.password = String(value); this.rebuild(); }
    get host() { return this.parts.host; } set host(value) { this.parts.host = String(value); this.rebuild(); }
    get hostname() { return this.parts.hostname; }
    get port() { return this.parts.port; }
    get pathname() { return this.parts.pathname; } set pathname(value) { this.parts.pathname = String(value).startsWith("/") ? String(value) : `/${value}`; this.rebuild(); }
    get search() { return this.parts.search; } set search(value) { this.parts.search = String(value) ? (String(value).startsWith("?") ? String(value) : `?${value}`) : ""; this.rebuild(); }
    get searchParams() { return this.params; }
    get hash() { return this.parts.hash; } set hash(value) { this.parts.hash = String(value) ? (String(value).startsWith("#") ? String(value) : `#${value}`) : ""; this.rebuild(); }
    toString() { return this.href; }
    toJSON() { return this.href; }
    static canParse(input, base) { try { new GonvexURL(input, base); return true; } catch { return false; } }
  }
  globalThis.URLSearchParams ??= GonvexURLSearchParams;
  globalThis.URL ??= GonvexURL;

  const utf8Encode = (input) => {
    const bytes = [];
    for (const character of String(input)) {
      const point = character.codePointAt(0);
      if (point <= 0x7f) bytes.push(point);
      else if (point <= 0x7ff) bytes.push(0xc0 | (point >> 6), 0x80 | (point & 0x3f));
      else if (point <= 0xffff) bytes.push(0xe0 | (point >> 12), 0x80 | ((point >> 6) & 0x3f), 0x80 | (point & 0x3f));
      else bytes.push(0xf0 | (point >> 18), 0x80 | ((point >> 12) & 0x3f), 0x80 | ((point >> 6) & 0x3f), 0x80 | (point & 0x3f));
    }
    return new Uint8Array(bytes);
  };
  const utf8Decode = (input, fatal = false) => {
    const bytes = input instanceof Uint8Array ? input : new Uint8Array(input?.buffer ?? input ?? []);
    let output = "";
    for (let index = 0; index < bytes.length;) {
      const first = bytes[index++];
      let point;
      let remaining;
      if (first <= 0x7f) { point = first; remaining = 0; }
      else if ((first & 0xe0) === 0xc0) { point = first & 0x1f; remaining = 1; }
      else if ((first & 0xf0) === 0xe0) { point = first & 0x0f; remaining = 2; }
      else if ((first & 0xf8) === 0xf0) { point = first & 0x07; remaining = 3; }
      else { if (fatal) throw new TypeError("invalid UTF-8"); output += "\ufffd"; continue; }
      let valid = index + remaining <= bytes.length;
      for (let offset = 0; valid && offset < remaining; offset++) {
        const next = bytes[index++];
        if ((next & 0xc0) !== 0x80) { valid = false; index--; break; }
        point = (point << 6) | (next & 0x3f);
      }
      if (!valid || point > 0x10ffff || (point >= 0xd800 && point <= 0xdfff)) {
        if (fatal) throw new TypeError("invalid UTF-8");
        output += "\ufffd";
      } else output += String.fromCodePoint(point);
    }
    return output;
  };

  if (typeof globalThis.TextEncoder === "undefined") {
    globalThis.TextEncoder = class TextEncoder {
      get encoding() { return "utf-8"; }
      encode(input = "") { return utf8Encode(input); }
      encodeInto(input, destination) {
        const encoded = utf8Encode(input);
        const written = Math.min(encoded.length, destination.length);
        destination.set(encoded.subarray(0, written));
        return { read: String(input).length, written };
      }
    };
  }
  if (typeof globalThis.TextDecoder === "undefined") {
    globalThis.TextDecoder = class TextDecoder {
      constructor(label = "utf-8", options = {}) {
        if (!/^utf-?8$/i.test(label)) throw new RangeError("only UTF-8 is supported");
        this.fatal = Boolean(options.fatal);
      }
      get encoding() { return "utf-8"; }
      decode(input = new Uint8Array()) { return utf8Decode(input, this.fatal); }
    };
  }

  class GonvexAbortSignal {
    constructor() { this.aborted = false; this.reason = undefined; this.listeners = new Set(); }
    addEventListener(type, listener, options) {
      if (type !== "abort" || typeof listener !== "function") return;
      if (this.aborted) { listener.call(this, { type: "abort", target: this }); return; }
      this.listeners.add({ listener, once: Boolean(options?.once) });
    }
    removeEventListener(type, listener) {
      if (type !== "abort") return;
      for (const entry of this.listeners) if (entry.listener === listener) this.listeners.delete(entry);
    }
    throwIfAborted() { if (this.aborted) throw this.reason; }
    static abort(reason = new Error("This operation was aborted")) { const controller = new GonvexAbortController(); controller.abort(reason); return controller.signal; }
    static timeout(delay) { const controller = new GonvexAbortController(); setTimeout(() => controller.abort(new Error("The operation timed out")), delay); return controller.signal; }
  }
  class GonvexAbortController {
    constructor() { this.signal = new GonvexAbortSignal(); }
    abort(reason = new Error("This operation was aborted")) {
      if (this.signal.aborted) return;
      this.signal.aborted = true;
      this.signal.reason = reason;
      for (const entry of [...this.signal.listeners]) {
        try { entry.listener.call(this.signal, { type: "abort", target: this.signal }); } finally { if (entry.once) this.signal.listeners.delete(entry); }
      }
    }
  }
  globalThis.AbortSignal ??= GonvexAbortSignal;
  globalThis.AbortController ??= GonvexAbortController;

  let nextTimer = 1;
  const timers = new Map();
  const scheduleTimer = (callback, delay, repeat, args) => {
    if (typeof callback !== "function") throw new TypeError("timer callback must be a function");
    const id = nextTimer++;
    const milliseconds = Math.max(0, Math.min(0x7fffffff, Number(delay) || 0));
    const run = async () => {
      await ops.op_gonvex_sleep(milliseconds);
      if (!timers.has(id)) return;
      try { callback(...args); } finally {
        if (repeat && timers.has(id)) void run(); else timers.delete(id);
      }
    };
    timers.set(id, true);
    void run();
    return id;
  };
  globalThis.setTimeout ??= (callback, delay = 0, ...args) => scheduleTimer(callback, delay, false, args);
  globalThis.clearTimeout ??= (id) => { timers.delete(id); };
  globalThis.setInterval ??= (callback, delay = 0, ...args) => scheduleTimer(callback, delay, true, args);
  globalThis.clearInterval ??= globalThis.clearTimeout;

  class GonvexReadableStream {
    constructor(source = {}) {
      this.queue = [];
      this.waiters = [];
      this.closed = false;
      this.failure = null;
      this.locked = false;
      const controller = Object.freeze({
        enqueue: (chunk) => this.enqueue(chunk),
        close: () => this.close(),
        error: (error) => this.error(error),
      });
      try { Promise.resolve(source.start?.(controller)).catch((error) => this.error(error)); } catch (error) { this.error(error); }
    }
    enqueue(chunk) {
      if (this.closed || this.failure) throw new TypeError("stream is not readable");
      const waiter = this.waiters.shift();
      if (waiter) waiter.resolve({ value: chunk, done: false }); else this.queue.push(chunk);
    }
    close() {
      if (this.closed) return;
      this.closed = true;
      for (const waiter of this.waiters.splice(0)) waiter.resolve({ value: undefined, done: true });
    }
    error(error) {
      this.failure = error instanceof Error ? error : new Error(String(error));
      for (const waiter of this.waiters.splice(0)) waiter.reject(this.failure);
    }
    read() {
      if (this.failure) return Promise.reject(this.failure);
      if (this.queue.length) return Promise.resolve({ value: this.queue.shift(), done: false });
      if (this.closed) return Promise.resolve({ value: undefined, done: true });
      return new Promise((resolve, reject) => this.waiters.push({ resolve, reject }));
    }
    getReader() {
      if (this.locked) throw new TypeError("ReadableStream is locked");
      this.locked = true;
      let released = false;
      return Object.freeze({
        read: () => { if (released) return Promise.reject(new TypeError("reader was released")); return this.read(); },
        cancel: (reason) => { this.error(reason ?? new Error("stream cancelled")); return Promise.resolve(); },
        releaseLock: () => { released = true; this.locked = false; },
      });
    }
    async pipeTo(destination) {
      const reader = this.getReader();
      const writer = destination.getWriter();
      try { while (true) { const item = await reader.read(); if (item.done) break; await writer.write(item.value); } await writer.close(); }
      finally { reader.releaseLock(); writer.releaseLock?.(); }
    }
    pipeThrough(transform) { void this.pipeTo(transform.writable); return transform.readable; }
    [Symbol.asyncIterator]() { const reader = this.getReader(); return { next: () => reader.read(), return: async () => { reader.releaseLock(); return { done: true }; } }; }
  }
  class GonvexWritableStream {
    constructor(sink = {}) { this.sink = sink; this.locked = false; }
    getWriter() {
      if (this.locked) throw new TypeError("WritableStream is locked");
      this.locked = true;
      return { write: (chunk) => Promise.resolve(this.sink.write?.(chunk)), close: () => Promise.resolve(this.sink.close?.()), abort: (reason) => Promise.resolve(this.sink.abort?.(reason)), releaseLock: () => { this.locked = false; } };
    }
  }
  class GonvexTransformStream {
    constructor(transformer = {}) {
      let controller;
      this.readable = new GonvexReadableStream({ start: (value) => { controller = value; } });
      this.writable = new GonvexWritableStream({
        write: (chunk) => transformer.transform ? transformer.transform(chunk, controller) : controller.enqueue(chunk),
        close: async () => { await transformer.flush?.(controller); controller.close(); },
        abort: (reason) => controller.error(reason),
      });
    }
  }
  globalThis.ReadableStream ??= GonvexReadableStream;
  globalThis.WritableStream ??= GonvexWritableStream;
  globalThis.TransformStream ??= GonvexTransformStream;

  const bytesFrom = (value) => {
    if (value === undefined || value === null) return new Uint8Array();
    if (value instanceof Uint8Array) return new Uint8Array(value);
    if (value instanceof ArrayBuffer) return new Uint8Array(value.slice(0));
    if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
    return utf8Encode(String(value));
  };
  class GonvexHeaders {
    constructor(init = {}) {
      this._values = {};
      const entries = typeof init.entries === "function" ? init.entries() : Object.entries(init);
      for (const [name, value] of entries) this.set(name, value);
    }
    get(name) { return this._values[String(name).toLowerCase()] ?? null; }
    has(name) { return Object.prototype.hasOwnProperty.call(this._values, String(name).toLowerCase()); }
    set(name, value) { this._values[String(name).toLowerCase()] = String(value); }
    append(name, value) { const key = String(name).toLowerCase(); this._values[key] = this.has(key) ? `${this._values[key]}, ${value}` : String(value); }
    delete(name) { delete this._values[String(name).toLowerCase()]; }
    entries() { return Object.entries(this._values)[Symbol.iterator](); }
    keys() { return Object.keys(this._values)[Symbol.iterator](); }
    values() { return Object.values(this._values)[Symbol.iterator](); }
    forEach(callback, thisArg) { for (const [name, value] of Object.entries(this._values)) callback.call(thisArg, value, name, this); }
    [Symbol.iterator]() { return this.entries(); }
  }
  class GonvexResponse {
    constructor(body = null, init = {}) {
      this.status = init.status ?? 200;
      this.statusText = init.statusText ?? "";
      this.headers = new GonvexHeaders(init.headers);
      this.url = init.url ?? "";
      this.redirected = false;
      this.type = "basic";
      this.bodyUsed = false;
      this._bytes = bytesFrom(body);
      this.body = new GonvexReadableStream({ start: (controller) => { if (this._bytes.length) controller.enqueue(this._bytes); controller.close(); } });
    }
    get ok() { return this.status >= 200 && this.status < 300; }
    async bytes() { this.bodyUsed = true; return new Uint8Array(this._bytes); }
    async arrayBuffer() { const bytes = await this.bytes(); return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength); }
    async text() { return utf8Decode(await this.bytes()); }
    async json() { return JSON.parse(await this.text()); }
    clone() { if (this.bodyUsed) throw new TypeError("Response body has already been used"); return new GonvexResponse(this._bytes, { status: this.status, statusText: this.statusText, headers: this.headers, url: this.url }); }
    static json(value, init = {}) { const headers = new GonvexHeaders(init.headers); if (!headers.has("content-type")) headers.set("content-type", "application/json"); return new GonvexResponse(JSON.stringify(value), { ...init, headers }); }
    static error() { return new GonvexResponse(null, { status: 0, statusText: "" }); }
  }
  globalThis.Headers ??= GonvexHeaders;
  globalThis.Response ??= GonvexResponse;

  const cryptoObject = {
    getRandomValues(target) {
      if (!ArrayBuffer.isView(target) || target.byteLength > 65_536) throw new TypeError("getRandomValues requires a typed array of at most 65536 bytes");
      const random = JSON.parse(ops.op_gonvex_random_bytes(target.byteLength));
      new Uint8Array(target.buffer, target.byteOffset, target.byteLength).set(random);
      return target;
    },
    randomUUID() {
      const bytes = this.getRandomValues(new Uint8Array(16));
      bytes[6] = (bytes[6] & 0x0f) | 0x40;
      bytes[8] = (bytes[8] & 0x3f) | 0x80;
      const hex = [...bytes].map((value) => value.toString(16).padStart(2, "0")).join("");
      return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
    },
    subtle: Object.freeze({
      async digest(algorithm, data) {
        const name = typeof algorithm === "string" ? algorithm : algorithm?.name;
        if (String(name).toUpperCase().replace("-", "") !== "SHA256") throw new TypeError("only SHA-256 is supported");
        const digest = new Uint8Array(JSON.parse(ops.op_gonvex_sha256(JSON.stringify([...bytesFrom(data)]))));
        return digest.buffer;
      },
    }),
  };
  globalThis.crypto ??= Object.freeze(cryptoObject);

  class GonvexHostError extends Error {
    constructor(message, status) {
      super(message);
      this.name = status === "denied" ? "GonvexCapabilityError" : "GonvexHostError";
      this.status = status;
      this.code = String(message).includes("STALE_AGENT_CATALOG:")
        ? "STALE_AGENT_CATALOG"
        : status === "denied" ? "CAPABILITY_DENIED" : "HOST_CALL_FAILED";
    }
  }

  const hostCall = async (payload) => {
    // The op answers with a JSON envelope so a denial, a host failure and a
    // successful response are one shape the host fully controls.
    const outcome = JSON.parse(await ops.op_gonvex_host_call(JSON.stringify(payload)));
    if (outcome.status !== "ok") throw new GonvexHostError(outcome.message, outcome.status);
    if (outcome.value === "") return undefined;
    try {
      return JSON.parse(outcome.value);
    } catch {
      throw new GonvexHostError(`host operation ${payload.kind} returned a response that is not JSON`, "failed");
    }
  };

  const text = (name, value) => {
    if (typeof value !== "string" || value.length === 0) throw new TypeError(`${name} must be a non-empty string`);
    return value;
  };

  const plainObject = (name, value) => {
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
      throw new TypeError(`${name} must be an object`);
    }
    return value;
  };

  const rowKey = (value) => {
    if (typeof value === "string" || typeof value === "number") return value;
    throw new TypeError("row id must be a string or a number");
  };

  const optional = (value) => (value === undefined ? null : value);

  const nonNegativeInteger = (name, value) => {
    if (!Number.isSafeInteger(value) || value < 0) throw new TypeError(`${name} must be a non-negative safe integer`);
    return value;
  };

  const parameterList = (parameters) => {
    if (parameters === undefined || parameters === null) return [];
    if (!Array.isArray(parameters)) throw new TypeError("query parameters must be an array");
    // Values stay values: they are bound as $1..$n by the host, never spliced
    // into the statement here.
    return [...parameters];
  };

  const createResponse = (raw) => {
    return new GonvexResponse(typeof raw?.body === "string" ? raw.body : "", {
      status: raw?.status ?? 0,
      statusText: raw?.statusText ?? "",
      url: raw?.url ?? "",
      headers: raw?.headers ?? {},
    });
  };

  const requestInit = (input, init) => {
    const url = typeof input === "string" ? input : String(input?.href ?? input ?? "");
    const options = init ?? {};
    options.signal?.throwIfAborted?.();
    const headers = {};
    const source = options.headers;
    if (source && typeof source.entries === "function") {
      for (const [name, value] of source.entries()) headers[String(name).toLowerCase()] = String(value);
    } else if (source && typeof source === "object") {
      for (const [name, value] of Object.entries(source)) headers[String(name).toLowerCase()] = String(value);
    }
    let body = null;
    if (options.body !== undefined && options.body !== null) {
      if (typeof options.body === "string") body = options.body;
      else if (typeof options.body === "object") {
        body = JSON.stringify(options.body);
        if (headers["content-type"] === undefined) headers["content-type"] = "application/json";
      } else body = String(options.body);
    }
    return { url: text("fetch url", url), method: String(options.method ?? "GET").toUpperCase(), headers, body };
  };

  // Capability separation is structural: the Rust side intersects what the
  // function kind may ever reach with what the host granted this invocation,
  // and a method that is not granted is simply absent from the context. A
  // Query has no way to name a write, an Action has no database handle.
  const createContext = (request) => {
    const granted = request.capabilities;
    const identity = request.identity ?? {};
    const account = identity.account ?? null;
    const context = {
      kind: request.kind,
      function: request.function,
      now: request.now,
      auth: Object.freeze({ account }),
      tenant: identity.tenant ?? null,
      member: identity.member ?? null,
      invocation: Object.freeze({ ...(request.invocation ?? {}) }),
    };

    const db = {};
    if (granted.dbRead) {
      db.query = (statement, parameters) => hostCall({
        kind: "dbQuery",
        statement: text("statement", statement),
        parameters: parameterList(parameters),
      });
    }
    if (granted.dbWrite) {
      db.insert = (table, row) => hostCall({
        kind: "dbInsert",
        table: text("table", table),
        row: plainObject("row", row),
      });
      db.update = (table, id, patch) => hostCall({
        kind: "dbUpdate",
        table: text("table", table),
        id: rowKey(id),
        patch: plainObject("patch", patch),
      });
      db.delete = (table, id) => hostCall({
        kind: "dbDelete",
        table: text("table", table),
        id: rowKey(id),
      });
      db.deleteMany = (table, ids) => hostCall({
        kind: "dbDeleteMany",
        table: text("table", table),
        ids: Array.from(ids ?? [], rowKey),
      });
    }
    if (granted.dbRead || granted.dbWrite) context.db = Object.freeze(db);

    if (granted.actionOutbox) {
      context.actions = Object.freeze({
        enqueue: (name, args) => hostCall({ kind: "actionEnqueue", function: text("Action", name), args: optional(args) }),
      });
    }

    if (granted.actionTools) {
      const tools = {};
      for (const name of request.actionTools ?? []) {
        tools[name] = (args) => hostCall({ kind: "toolInvoke", tool: name, args: optional(args) });
      }
      context.tools = Object.freeze(tools);
    }
    if (granted.functions) {
      context.functions = Object.freeze({
        invoke: (input) => {
          const request = plainObject("function invocation", input);
          return hostCall({
            kind: "functionInvoke",
            path: text("function path", request.path),
            args: optional(request.args),
            artifactHash: text("artifact hash", request.artifactHash),
          });
        },
      });
    }
    if (granted.scheduler) {
      context.scheduler = Object.freeze({
        runAfter: (delayMs, name, args) => hostCall({
          kind: "scheduleAfter",
          delayMs: nonNegativeInteger("scheduler delayMs", delayMs),
          function: text("scheduled function", name),
          args: optional(args),
        }),
        runAt: (atUnixMs, name, args) => hostCall({
          kind: "scheduleAt",
          atUnixMs: nonNegativeInteger("scheduler atUnixMs", atUnixMs),
          function: text("scheduled function", name),
          args: optional(args),
        }),
      });
    }
    if (granted.network) {
      context.fetch = async (input, init) => createResponse(await hostCall({ kind: "fetch", request: requestInit(input, init) }));
    }
    if (granted.secrets) {
      context.secrets = Object.freeze({ ...(request.environment ?? {}) });
    }
    if (granted.storage) {
      const storage = (operation, payload) => hostCall({ kind: "storage", operation, payload: optional(payload) });
      context.storage = Object.freeze({
        generateUploadUrl: (options) => storage("generateUploadUrl", options ?? {}),
        getUrl: (fileId) => storage("getUrl", { fileId: text("fileId", fileId) }),
        generateDownloadUrl: (fileId, ttlMs) => storage("generateDownloadUrl", { fileId: text("fileId", fileId), ttlMs: ttlMs ?? 0 }),
        getMetadata: (fileId) => storage("getMetadata", { fileId: text("fileId", fileId) }),
        delete: (fileId) => storage("delete", { fileId: text("fileId", fileId) }),
        // Bytes travel base64-encoded: the op boundary is JSON text in both
        // directions, so binary payloads have to be named as such.
        store: (contentBase64, options) => storage("store", { contentBase64: text("content", contentBase64), ...(options ?? {}) }),
        call: (operation, payload) => storage(text("operation", operation), payload),
      });
    }
    if (granted.sandbox) {
      const sandbox = (operation, payload) => hostCall({ kind: "sandbox", operation, payload: optional(payload) });
      context.sandbox = Object.freeze({
        create: (options = {}) => sandbox("create", options),
        run: (sandboxId, options) => sandbox("run", { sandboxId: text("sandbox id", sandboxId), ...(options ?? {}) }),
        cancel: (sandboxId, executionId) => sandbox("cancel", { sandboxId: text("sandbox id", sandboxId), executionId: text("execution id", executionId) }),
        status: (sandboxId, executionId) => sandbox("status", { sandboxId: text("sandbox id", sandboxId), executionId: text("execution id", executionId) }),
        readFile: (sandboxId, path) => sandbox("readFile", { sandboxId: text("sandbox id", sandboxId), path: text("sandbox path", path) }),
        writeFile: (sandboxId, path, contentBase64) => sandbox("writeFile", { sandboxId: text("sandbox id", sandboxId), path: text("sandbox path", path), contentBase64: text("sandbox file contents", contentBase64) }),
        readText: (sandboxId, path) => sandbox("readText", { sandboxId: text("sandbox id", sandboxId), path: text("sandbox path", path) }),
        writeText: (sandboxId, path, content) => sandbox("writeText", { sandboxId: text("sandbox id", sandboxId), path: text("sandbox path", path), content: String(content) }),
        importFile: (sandboxId, options) => sandbox("importFile", { sandboxId: text("sandbox id", sandboxId), ...(options ?? {}) }),
        // Used only by the separate sandbox-worker process. The ordinary Rust
        // Action host rejects these operations, so this does not expand an
        // application module's authority.
        __worker: Object.freeze({
          readText: (path) => sandbox("worker.readText", { path: text("sandbox path", path) }),
          writeText: (path, content) => sandbox("worker.writeText", { path: text("sandbox path", path), content: String(content) }),
          listFiles: () => sandbox("worker.listFiles", {}),
          query: (statement, parameters) => sandbox("worker.query", { statement: text("DuckDB statement", statement), parameters: parameterList(parameters) }),
          register: (name, rows) => sandbox("worker.register", { name: text("DuckDB table", name), rows }),
        }),
      });
    }
    return Object.freeze(context);
  };

  // `export const list = query({ handler })` and `export async function list()`
  // are both legal module shapes, so unwrap one level before giving up. The
  // module SDK's declaration helpers park the executable handler on `run`.
  // A documented `createModule()` module exports its ModuleBuilder as the
  // default binding; resolve its registered handler by public path so the
  // builder form and the direct-export form share the same ABI.
  const resolveHandler = (binding, functionPath) => {
    if (typeof binding === "function") return binding;
    if (binding !== null && typeof binding === "object") {
      if (typeof binding.runtimeRegistrations === "function") {
        try {
          const registration = binding.runtimeRegistrations()
            .find((candidate) => candidate && candidate.path === functionPath);
          if (registration && typeof registration.handler === "function") return registration.handler;
        } catch {
          // Continue through the ordinary binding shapes below. The Rust
          // resolver still reports a precise dispatch failure if no handler
          // is exposed for this manifest entry.
        }
      }
      if (typeof binding.handler === "function") return binding.handler;
      if (typeof binding.run === "function") return binding.run;
      if (binding.options !== null && typeof binding.options === "object" && typeof binding.options.run === "function") {
        return binding.options.run;
      }
      if (typeof binding.default === "function") return binding.default;
    }
    return null;
  };

  const failure = (kind, message, stack) =>
    JSON.stringify(stack ? { status: "error", kind, message, stack } : { status: "error", kind, message });

  return async (binding, requestJson, argsJson) => {
    const request = JSON.parse(requestJson);

    const handler = resolveHandler(binding, request.function);
    if (handler === null) {
      return failure("dispatch", `module export for ${request.function} is not a callable ${request.kind} handler`);
    }

    let args;
    try {
      args = argsJson === "" ? undefined : JSON.parse(argsJson);
    } catch {
      return failure("dispatch", `arguments for ${request.function} are not valid JSON`);
    }

    let value;
    try {
      value = await handler(createContext(request), args);
    } catch (cause) {
      const message = cause instanceof Error ? `${cause.name}: ${cause.message}` : format(cause);
      return failure("handler", message, cause instanceof Error ? cause.stack : undefined);
    }

    let encoded;
    try {
      encoded = value === undefined ? "null" : JSON.stringify(value);
      if (encoded === undefined) encoded = "null";
    } catch (cause) {
      return failure("result", `${request.function} returned a value that cannot be encoded as JSON: ${format(cause)}`);
    }

    // A cheap pre-check in UTF-16 units; the host re-checks the exact byte
    // length, so this only keeps a runaway result from being copied twice.
    if (encoded.length > request.maxResultBytes) {
      return failure("resultSize", `${request.function} returned more than the ${request.maxResultBytes} byte result limit`);
    }
    return JSON.stringify({ status: "ok", value: encoded });
  };
})(Deno.core)
