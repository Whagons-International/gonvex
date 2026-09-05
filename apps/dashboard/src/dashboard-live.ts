import { GonvexClient, type FunctionReference } from "@gonvex/client";

type Next = (value: unknown) => void;
type Fail = (error: Error) => void;
type Subscribe = (key: string, next: Next, fail: Fail) => () => void;
type Entry = {
  listeners: Set<{ next: Next; fail: Fail }>;
  value?: unknown;
  error?: Error;
  stop: () => void;
  timer?: ReturnType<typeof setTimeout>;
};

/** Bounded, memory-only operator cache. Never persist browsed customer data. */
export class DashboardLiveStore {
  private entries = new Map<string, Entry>();
  constructor(private subscribe: Subscribe) {}

  watch(key: string, next: Next, fail: Fail): () => void {
    let entry = this.entries.get(key);
    if (entry?.error && entry.listeners.size === 0) {
      clearTimeout(entry.timer);
      entry.stop();
      this.entries.delete(key);
      entry = undefined;
    }
    const listener = { next, fail };
    if (!entry) {
      for (const [oldKey, oldEntry] of this.entries) {
        if (this.entries.size < 32) break;
        if (oldEntry.listeners.size) continue;
        clearTimeout(oldEntry.timer);
        oldEntry.stop();
        this.entries.delete(oldKey);
      }
      entry = { listeners: new Set(), stop: () => {} };
      this.entries.set(key, entry);
      const current = entry;
      current.stop = this.subscribe(key, (value) => {
        current.value = value;
        current.error = undefined;
        for (const target of current.listeners) target.next(value);
      }, (error) => {
        current.value = undefined;
        current.error = error;
        for (const target of current.listeners) target.fail(error);
      });
    }
    clearTimeout(entry.timer);
    entry.timer = undefined;
    entry.listeners.add(listener);
    if (entry.value !== undefined) next(entry.value);
    if (entry.error) fail(entry.error);
    const current = entry;
    return () => {
      current.listeners.delete(listener);
      if (this.entries.get(key) !== current) return;
      if (current.listeners.size || current.timer) return;
      current.timer = setTimeout(() => {
        if (this.entries.get(key) !== current) return;
        if (current.listeners.size) { current.timer = undefined; return; }
        current.stop();
        this.entries.delete(key);
      }, 30_000);
    };
  }

  failAll(error: Error) {
    for (const entry of this.entries.values()) {
      entry.value = undefined;
      entry.error = error;
      for (const listener of entry.listeners) listener.fail(error);
    }
  }

  close() {
    for (const entry of this.entries.values()) {
      clearTimeout(entry.timer);
      entry.listeners.clear();
      entry.stop();
    }
    this.entries.clear();
  }
}

const reference: FunctionReference = { path: "dashboard.read", kind: "query", scope: "control", delivery: "live" };
const connections = new Map<string, { client: GonvexClient; store: DashboardLiveStore }>();
let authScope = "";

export function closeDashboardConnections() {
  for (const connection of connections.values()) {
    connection.store.close();
    connection.client.close();
  }
  connections.clear();
  authScope = "";
}

/** Subscribe through the host-owned system connection, with REST compatibility for older runtimes. */
export function watchDashboard<T>(input: string, init: RequestInit, next: (value: T) => void, fail: Fail): () => void {
  const url = new URL(input, window.location.origin);
  const headers = new Headers(init.headers);
  const token = (headers.get("authorization") ?? "").replace(/^Bearer\s+/i, "");
  if (authScope !== token) { closeDashboardConnections(); authScope = token; }
  let cancelled = false;
  let settled = false;
  let fallbackTimer: ReturnType<typeof setTimeout> | undefined;
  const controller = new AbortController();
  // Only use HTTP if the runtime cannot establish its system connection.
  // An authorization error from that connection must never trigger a fallback.
  const fallback = () => {
    if (cancelled || settled) return;
    void fetch(input, { ...init, signal: controller.signal })
      .then(async (response) => {
        if (!response.ok) throw new Error(response.statusText || `HTTP ${response.status}`);
        return response.json() as Promise<T>;
      })
      .then((value) => { if (!cancelled && !settled) next(value); })
      .catch((error: Error) => { if (!cancelled && !settled) fail(error); });
  };
  if (typeof WebSocket === "undefined") {
    fallback();
    return () => { cancelled = true; controller.abort(); };
  }
  let connection = connections.get(url.origin);
  if (!connection) {
    const socketURL = new URL("/dev/dashboard/ws", url);
    socketURL.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    const client = new GonvexClient(socketURL.toString(), {
      token: token || undefined, telemetry: false, errorReporting: false,
      querySubscriptionRetentionMs: 0, outbox: { enabled: false },
    });
    const store = new DashboardLiveStore((key, publish, reject) => client.subscribeLiveQuery(reference, JSON.parse(key), (message) => {
      if (message.type === "query.result") publish(message.result);
      if (message.type === "query.error") reject(new Error(message.error));
    }));
    client.onAuthError((message) => store.failAll(new Error(message)));
    connection = { client, store };
    connections.set(url.origin, connection);
  }
  const key = JSON.stringify({
    resource: `${url.pathname}${url.search}`,
    project: headers.get("x-gonvex-project-id") ?? "",
    tenant: headers.get("x-gonvex-tenant-id") ?? "",
    // Legacy local development keys are memory-only, scoped with the subscription.
    projectKey: headers.get("x-gonvex-project-key") ?? headers.get("x-gonvex-key") ?? "",
  });
  fallbackTimer = setTimeout(fallback, 500);
  const stop = connection.store.watch(key, (value) => {
    settled = true;
    clearTimeout(fallbackTimer);
    controller.abort();
    if (!cancelled) next(value as T);
  }, (error) => {
    settled = true;
    clearTimeout(fallbackTimer);
    controller.abort();
    if (!cancelled) fail(error);
  });
  return () => {
    cancelled = true;
    clearTimeout(fallbackTimer);
    controller.abort();
    stop();
  };
}
