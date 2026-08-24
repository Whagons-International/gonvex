export type ErrorAccount = { id?: string; email?: string; name?: string };
export type ErrorContext = Record<string, unknown>;

export type ErrorReporterOptions = {
  /** Native persistent-protocol sender supplied by GonvexClient. */
  transport?: (type: "register" | "envelope" | "heartbeat", payload: unknown) => Promise<void>;
  client?: {
    reportError: (type: "register" | "envelope" | "heartbeat", payload: unknown) => Promise<void>;
    connectionState?: () => { isWebSocketConnected: boolean };
    subscribeToConnectionState?: (listener: (state: { isWebSocketConnected: boolean }) => void) => () => void;
  };
  project?: string;
  tenant?: string;
  release?: string;
  environment?: string;
  account?: ErrorAccount;
  tags?: Record<string, string>;
  sampleRate?: number;
  beforeSend?: (event: ErrorEventPayload) => ErrorEventPayload | null;
  captureGlobalErrors?: boolean;
  maxQueueSize?: number;
};

export type ErrorEventPayload = {
  eventId: string;
  timestamp: string;
  level: "error" | "warning";
  message: string;
  name?: string;
  stack?: string;
  culprit?: string;
  project: string;
  tenant?: string;
  release?: string;
  environment?: string;
  account?: ErrorAccount;
  deviceId: string;
  sessionId: string;
  url?: string;
  userAgent?: string;
  language?: string;
  viewport?: string;
  online?: boolean;
  tags?: Record<string, string>;
  context?: ErrorContext;
  breadcrumbs: Array<{ timestamp: string; category: string; message: string; data?: ErrorContext }>;
};

const REDACTED = "[Filtered]";
const SECRET = /password|passwd|secret|token|authorization|cookie|api[-_]?key/i;

export class GonvexErrorReporter {
  private readonly options: ErrorReporterOptions & { transport: NonNullable<ErrorReporterOptions["transport"]>; project: string };
  private readonly breadcrumbs: ErrorEventPayload["breadcrumbs"] = [];
  private queue: ErrorEventPayload[] = [];
  private timer?: ReturnType<typeof setTimeout>;
  private heartbeatTimer?: ReturnType<typeof setTimeout>;
  private readonly deviceId = persistedId("gonvex-error-device");
  private readonly sessionId = randomId();
  private removeGlobal?: () => void;
  private removeConnectionListener?: () => void;
  private readonly queueKey: string;

  constructor(options: ErrorReporterOptions) {
    if (!options.transport && !options.client) throw new Error("GonvexErrorReporter requires client or transport");
    const transport = options.transport ?? ((type: "register" | "envelope" | "heartbeat", payload: unknown) => options.client!.reportError(type, payload));
    options = { ...options, transport, project: options.project ?? "" };
    this.options = { sampleRate: 1, captureGlobalErrors: true, maxQueueSize: 100, ...options, transport, project: options.project ?? "" };
    this.queueKey = `gonvex-error-queue:${options.project ?? ""}`;
    this.queue = readQueue(this.queueKey);
    this.registerProject();
    if (options.client?.subscribeToConnectionState) {
      let connected = options.client.connectionState?.().isWebSocketConnected ?? false;
      this.removeConnectionListener = options.client.subscribeToConnectionState((state) => {
        const restored = !connected && state.isWebSocketConnected;
        connected = state.isWebSocketConnected;
        if (restored) this.connectionRestored();
      });
    }
    if (this.options.captureGlobalErrors && typeof window !== "undefined") this.installGlobalHandlers();
    if (typeof window !== "undefined") this.scheduleHeartbeat();
    if (this.queue.length) this.scheduleFlush();
  }

  setAccount(account?: ErrorAccount) { this.options.account = account; }
  setTenant(tenant?: string) { this.options.tenant = tenant; }
  setProject(project: string) {
    if (project === this.options.project) return;
    this.options.project = project;
    this.registerProject();
  }

  addBreadcrumb(category: string, message: string, data?: ErrorContext) {
    this.breadcrumbs.push({ timestamp: new Date().toISOString(), category, message, data: scrub(data) as ErrorContext });
    if (this.breadcrumbs.length > 30) this.breadcrumbs.shift();
  }

  captureException(error: unknown, context?: ErrorContext): string | undefined {
    if (Math.random() > (this.options.sampleRate ?? 1)) return;
    const normalized = normalizeError(error);
    let event: ErrorEventPayload = {
      eventId: randomId(), timestamp: new Date().toISOString(), level: "error",
      message: normalized.message, name: normalized.name, stack: normalized.stack,
      culprit: firstAppFrame(normalized.stack), project: this.options.project,
      tenant: this.options.tenant, release: this.options.release, environment: this.options.environment,
      account: scrub(this.options.account) as ErrorAccount, deviceId: this.deviceId, sessionId: this.sessionId,
      url: typeof location === "undefined" ? undefined : stripQuery(location.href),
      userAgent: typeof navigator === "undefined" ? undefined : navigator.userAgent,
      language: typeof navigator === "undefined" ? undefined : navigator.language,
      online: typeof navigator === "undefined" ? undefined : navigator.onLine,
      viewport: typeof window === "undefined" ? undefined : `${window.innerWidth}x${window.innerHeight}`,
      tags: this.options.tags, context: scrub(context) as ErrorContext, breadcrumbs: [...this.breadcrumbs],
    };
    event = scrub(event) as ErrorEventPayload;
    const prepared = this.options.beforeSend?.(event) ?? event;
    if (!prepared) return;
    this.queue.push(prepared);
    const maxQueueSize = this.options.maxQueueSize ?? 100;
    if (this.queue.length > maxQueueSize) this.queue.splice(0, this.queue.length - maxQueueSize);
    this.persistQueue();
    this.scheduleFlush();
    return event.eventId;
  }

  async flush(): Promise<void> {
    if (!this.queue.length) return;
    const batch = this.queue.splice(0, 20);
    try {
      await this.options.transport("envelope", { events: batch });
      this.persistQueue();
      if (this.queue.length) this.scheduleFlush();
    } catch {
      this.queue = [...batch, ...this.queue].slice(0, this.options.maxQueueSize ?? 100);
      this.persistQueue();
    }
  }

  close() {
    this.removeGlobal?.();
    this.removeConnectionListener?.();
    if (this.timer) clearTimeout(this.timer);
    if (this.heartbeatTimer) clearTimeout(this.heartbeatTimer);
    void this.flush();
  }

  connectionRestored() {
    this.registerProject();
    void this.options.transport("heartbeat", {}).catch(() => undefined);
    if (this.queue.length) this.scheduleFlush();
  }

  private registerProject() {
    void this.options.transport("register", {
      release: this.options.release,
      environment: this.options.environment,
    }).catch(() => undefined);
  }

  private scheduleFlush() {
    if (this.timer) return;
    this.timer = setTimeout(() => { this.timer = undefined; void this.flush(); }, 1000);
  }

  private scheduleHeartbeat() {
    if (this.heartbeatTimer) return;
    this.heartbeatTimer = setTimeout(() => {
      this.heartbeatTimer = undefined;
      void this.options.transport("heartbeat", {}).catch(() => undefined);
      this.scheduleHeartbeat();
    }, 30_000);
  }

  private installGlobalHandlers() {
    const onError = (event: ErrorEvent) => this.captureException(event.error ?? event.message, { source: event.filename, line: event.lineno, column: event.colno });
    const onRejection = (event: PromiseRejectionEvent) => this.captureException(event.reason, { mechanism: "unhandledrejection" });
    const onPageHide = () => { void this.flush(); };
    window.addEventListener("error", onError);
    window.addEventListener("unhandledrejection", onRejection);
    window.addEventListener("pagehide", onPageHide);
    this.removeGlobal = () => { window.removeEventListener("error", onError); window.removeEventListener("unhandledrejection", onRejection); window.removeEventListener("pagehide", onPageHide); };
  }

  private persistQueue() { writeQueue(this.queueKey, this.queue.slice(0, this.options.maxQueueSize ?? 100)); }
}

function normalizeError(value: unknown): { name?: string; message: string; stack?: string } {
  if (value instanceof Error) return { name: value.name, message: value.message || value.name, stack: value.stack };
  if (typeof value === "string") return { message: value };
  try { return { message: JSON.stringify(value) }; } catch { return { message: String(value) }; }
}

function scrub(value: unknown, key = "", seen = new WeakSet<object>()): unknown {
  if (SECRET.test(key)) return REDACTED;
  if (!value || typeof value !== "object") return typeof value === "string" ? value.slice(0, 4000) : value;
  if (seen.has(value as object)) return "[Circular]";
  seen.add(value as object);
  if (Array.isArray(value)) return value.slice(0, 50).map((item) => scrub(item, key, seen));
  return Object.fromEntries(Object.entries(value as Record<string, unknown>).slice(0, 100).map(([k, v]) => [k, scrub(v, k, seen)]));
}

function firstAppFrame(stack?: string) { return stack?.split("\n").find((line) => /at\s/.test(line) && !/node_modules/.test(line))?.trim(); }
function stripQuery(url: string) { try { const parsed = new URL(url); parsed.search = ""; parsed.hash = ""; return parsed.toString(); } catch { return url.split(/[?#]/)[0]; } }
function randomId() { return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`; }
function persistedId(key: string) { try { const current = localStorage.getItem(key); if (current) return current; const next = randomId(); localStorage.setItem(key, next); return next; } catch { return randomId(); } }
function readQueue(key: string): ErrorEventPayload[] { try { const value = JSON.parse(localStorage.getItem(key) ?? "[]"); return Array.isArray(value) ? value.slice(0, 100) : []; } catch { return []; } }
function writeQueue(key: string, queue: ErrorEventPayload[]) { try { if (queue.length) localStorage.setItem(key, JSON.stringify(queue)); else localStorage.removeItem(key); } catch { /* reporting must never break the app */ } }
