import { act, cleanup, render, renderHook } from "@testing-library/react";
import { Component, type ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { control, GonvexClientError, type ConnectionState, type FunctionReference, type GonvexClient } from "@gonvex/client";
import type { ServerMessage } from "@gonvex/protocol";
import { GonvexAuthProvider, GonvexProviderWithAuth, GonvexProvider, useAction, useGonvexAuth, useGonvexAuthState, useGonvexConnectionState, useInvitationList, useReducer, useQuery, useQueryResult, useReplicaCollection, useReplicaCollectionState, useReplicaEntities, useReplicaSelector, useRetainedLiveQuery } from "./index";

const ref: FunctionReference = { kind: "query", path: "tasks.list" };

class FakeGonvexClient {
  readonly queryListeners = new Set<(message: ServerMessage) => void>();
  readonly scopeHandlers = new Set<() => void>();
  readonly connectionHandlers = new Set<(state: ConnectionState) => void>();
  readonly authErrorHandlers = new Set<(error: string) => void>();
  readonly query = vi.fn((ref: FunctionReference, args: unknown) => {
    this.subscribedRefs.push(ref);
    this.subscribedArgs.push(args);
    return new Promise<unknown>((resolve, reject) => {
      const handler = (message: ServerMessage) => {
        if (message.type === "query.result") {
          this.queryListeners.delete(handler);
          resolve(message.result);
        }
        if (message.type === "query.error") {
          this.queryListeners.delete(handler);
          reject(new Error(message.error));
        }
      };
      this.queryListeners.add(handler);
    });
  });
  readonly reducer = vi.fn(() => Promise.resolve(null));
  readonly action = vi.fn(() => Promise.resolve(null));
  readonly setAuth = vi.fn();
  readonly authenticate = vi.fn(async (auth: unknown) => { this.setAuth(auth); });
  subscribedArgs: unknown[] = [];
  subscribedRefs: FunctionReference[] = [];
  watchedReplicaRefs: FunctionReference[] = [];
  readonly replicaRows: unknown[] = [];
  replicaState = { rows: [] as unknown[], ids: [] as string[], source: "cache", completeness: "partial", freshness: "verifying", truncated: false, computedRevision: 0 };
  replicaVersion = 0;
  readonly replicaListeners = new Set<() => void>();
  readonly entityValues = new Map<string, Record<string, unknown>>();
  retained = { rows: [] as Record<string, unknown>[], ids: [] as string[], source: "cache", completeness: "partial", freshness: "verifying" };
  readonly localReplica = {
    subscribe: (listener: () => void) => { this.replicaListeners.add(listener); return () => this.replicaListeners.delete(listener); },
    version: () => this.replicaVersion,
  };
  state: ConnectionState = {
    isWebSocketConnected: true,
    hasEverConnected: true,
    connectionCount: 1,
    connectionRetries: 0,
    hasInflightRequests: false,
    inflightReducers: 0,
    inflightActions: 0,
    inflightOneShotQueries: 0,
  };

  subscribeLiveQuery(ref: FunctionReference, args: unknown, handler: (message: ServerMessage) => void) {
    this.subscribedRefs.push(ref);
    this.subscribedArgs.push(args);
    this.queryListeners.add(handler);
    return () => {
      this.queryListeners.delete(handler);
    };
  }

  watchControlQuery(ref: FunctionReference, args: unknown) {
    let result: unknown;
    let version = 0;
    let snapshot = { result, version };
    const listeners = new Set<() => void>();
    const unsubscribe = this.subscribeLiveQuery(ref,args,(message)=>{if(message.type==="query.result"){result=message.result;version+=1;snapshot={result,version};listeners.forEach((listener)=>listener());}});
    return {getSnapshot:()=>snapshot,onUpdate:(listener:()=>void)=>{listeners.add(listener);return()=>{listeners.delete(listener);if(!listeners.size)unsubscribe();}}};
  }

  watchReplica(ref: FunctionReference) {
    this.watchedReplicaRefs.push(ref);
    return {
      localReplicaResult: () => this.replicaRows,
      localReplicaState: () => this.replicaState,
      onUpdate: (listener: () => void) => { this.replicaListeners.add(listener); return () => this.replicaListeners.delete(listener); },
    };
  }

  replicaEntities(_entity: string, ids: readonly string[]) {
    return ids.map((id) => this.entityValues.get(id));
  }

  replicaSignature(ref: FunctionReference) { return ref.path; }
  retainedLiveQuery() { return this.retained; }
  updateReplica() { this.replicaVersion += 1; for (const listener of this.replicaListeners) listener(); }

  onSessionScopeChange(handler: () => void) {
    this.scopeHandlers.add(handler);
    return () => {
      this.scopeHandlers.delete(handler);
    };
  }

  connectionState(): ConnectionState {
    return this.state;
  }

  subscribeToConnectionState(handler: (state: ConnectionState) => void) {
    this.connectionHandlers.add(handler);
    return () => {
      this.connectionHandlers.delete(handler);
    };
  }

  onAuthError(handler: (error: string) => void) {
    this.authErrorHandlers.add(handler);
    return () => this.authErrorHandlers.delete(handler);
  }

  emitAuthError(error: string) {
    for (const handler of Array.from(this.authErrorHandlers)) handler(error);
  }

  emitQuery(message: ServerMessage) {
    for (const handler of Array.from(this.queryListeners)) handler(message);
  }

  setConnected(isWebSocketConnected: boolean) {
    this.state = { ...this.state, isWebSocketConnected };
    for (const handler of Array.from(this.connectionHandlers)) handler(this.state);
  }
}

function wrapperFor(client: FakeGonvexClient) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return <GonvexProvider client={client as unknown as GonvexClient}>{children}</GonvexProvider>;
  };
}

let navigatorLocksDescriptor: PropertyDescriptor | undefined;

beforeEach(() => {
  navigatorLocksDescriptor = Object.getOwnPropertyDescriptor(navigator, "locks");
  vi.useFakeTimers();
});

afterEach(() => {
  if (navigatorLocksDescriptor) Object.defineProperty(navigator, "locks", navigatorLocksDescriptor);
  else delete (navigator as Navigator & { locks?: unknown }).locks;
  vi.useRealTimers();
});

describe("useQueryResult", () => {
  it("moves from loading to success when a one-shot result arrives", async () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useQueryResult<string[]>(ref, { status: "open" }), { wrapper: wrapperFor(client) });

    expect(result.current).toMatchObject({ status: "loading", isLoading: true, data: undefined });
    await act(async () => client.emitQuery({ type: "query.result", id: "q1", result: ["task"] }));
    expect(result.current).toMatchObject({ status: "success", isSuccess: true, data: ["task"], error: null, isStale: false });
  });

  it("reports skip status for skipped queries", () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useQueryResult(ref, "skip"), { wrapper: wrapperFor(client) });

    expect(result.current.status).toBe("skip");
    expect(client.queryListeners.size).toBe(0);
  });

  it("surfaces server query errors and executes a fresh Query on retry", async () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useQueryResult<string[]>(ref, { status: "open" }), { wrapper: wrapperFor(client) });

    await act(async () => client.emitQuery({ type: "query.error", id: "q1", error: "permission denied" }));
    expect(result.current).toMatchObject({ status: "error", isError: true });
    expect(result.current.error?.message).toBe("permission denied");

    act(() => result.current.retry());
    expect(result.current).toMatchObject({ status: "loading", error: null });
    expect(client.query).toHaveBeenCalledTimes(2);

    await act(async () => client.emitQuery({ type: "query.result", id: "q2", result: ["recovered"] }));
    expect(result.current).toMatchObject({ status: "success", data: ["recovered"] });
  });

  it("keeps the last result when a retry fails by default", async () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useQueryResult<string[]>(ref, {}), { wrapper: wrapperFor(client) });

    await act(async () => client.emitQuery({ type: "query.result", id: "q1", result: ["task"] }));
    act(() => result.current.retry());
    await act(async () => client.emitQuery({ type: "query.error", id: "q2", error: "boom" }));
    expect(result.current).toMatchObject({ status: "error", data: ["task"], isStale: true });
  });

  it("drops the last result on retry when keepPreviousData is false", async () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(
      () => useQueryResult<string[]>(ref, {}, { keepPreviousData: false }),
      { wrapper: wrapperFor(client) },
    );

    await act(async () => client.emitQuery({ type: "query.result", id: "q1", result: ["task"] }));
    act(() => result.current.retry());
    await act(async () => client.emitQuery({ type: "query.error", id: "q2", error: "boom" }));
    expect(result.current).toMatchObject({ status: "error", data: undefined, isStale: false });
  });

  it("reports a soft timeout while the one-shot Query remains pending", async () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useQueryResult<string[]>(ref, {}), { wrapper: wrapperFor(client) });

    act(() => {
      vi.advanceTimersByTime(15_000);
    });

    expect(result.current).toMatchObject({ status: "timeout", isError: true });
    expect(client.queryListeners.size).toBe(1);

    await act(async () => client.emitQuery({ type: "query.result", id: "q1", result: ["late"] }));
    expect(result.current).toMatchObject({ status: "success", data: ["late"] });
  });
});

describe("useQuery", () => {
  it("preserves generated optimistic projection metadata", () => {
    const client = new FakeGonvexClient();
    const projectedRef: FunctionReference = {
      kind: "query",
      path: "tasks.byWorkspace",
      optimistic: { projection: { entity: "tasks", key: "_id", resultPath: ["page"] } },
    };

    renderHook(() => useQuery(projectedRef, {}), { wrapper: wrapperFor(client) });

    expect(client.subscribedRefs.at(-1)).toBe(projectedRef);
  });

  it("returns undefined while loading and the result once it arrives", async () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useQuery<string[]>(ref, {}), { wrapper: wrapperFor(client) });

    expect(result.current).toBeUndefined();

    await act(async () => client.emitQuery({ type: "query.result", id: "q1", result: ["task"] }));
    expect(result.current).toEqual(["task"]);
  });

  it("reissues a Query after auth scope changes without throwing the superseded request", async () => {
    const client = new FakeGonvexClient();
    const requests: Array<{ resolve: (value: unknown) => void; reject: (error: Error) => void }> = [];
    client.query.mockImplementation((queryRef: FunctionReference, args: unknown) => {
      client.subscribedRefs.push(queryRef);
      client.subscribedArgs.push(args);
      return new Promise((resolve, reject) => requests.push({ resolve, reject }));
    });
    const { result } = renderHook(() => useQuery<string[]>(ref, {}), { wrapper: wrapperFor(client) });
    expect(client.query).toHaveBeenCalledTimes(1);

    await act(async () => {
      for (const handler of client.scopeHandlers) handler();
      requests[0]!.reject(new GonvexClientError(
        "Authentication scope changed while waiting for Query tasks.list",
        { code: "superseded", path: "tasks.list", operation: "query" },
      ));
      await Promise.resolve();
    });

    expect(result.current).toBeUndefined();
    expect(client.query).toHaveBeenCalledTimes(2);
    await act(async () => { requests[1]!.resolve(["new-scope-task"]); await Promise.resolve(); });
    expect(result.current).toEqual(["new-scope-task"]);
  });

  it("throws server query errors so error boundaries can catch them", async () => {
    const client = new FakeGonvexClient();
    const caught: Error[] = [];

    class Boundary extends Component<{ children: ReactNode }, { failed: boolean }> {
      state = { failed: false };

      static getDerivedStateFromError() {
        return { failed: true };
      }

      componentDidCatch(error: Error) {
        caught.push(error);
      }

      render() {
        return this.state.failed ? <div data-testid="failed">failed</div> : this.props.children;
      }
    }

    function QueryConsumer() {
      const value = useQuery<string[]>(ref, {});
      return <div>{JSON.stringify(value ?? null)}</div>;
    }

    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    try {
      const Wrapper = wrapperFor(client);
      const view = render(
        <Wrapper>
          <Boundary>
            <QueryConsumer />
          </Boundary>
        </Wrapper>,
      );

      await act(async () => client.emitQuery({ type: "query.error", id: "q1", error: "permission denied" }));

      expect(view.getByTestId("failed")).toBeTruthy();
      expect(caught[0]?.message).toBe("permission denied");
    } finally {
      consoleError.mockRestore();
    }
  });
});

describe("useReplicaCollection", () => {
  it("preserves generated optimistic projection metadata", () => {
    const client = new FakeGonvexClient();
    const projectedRef: FunctionReference = {
      kind: "query", delivery: "replica",
      path: "sync.recentWorkspaceTasks",
      optimistic: { projection: { entity: "tasks", key: "_id", resultPath: [] } },
    };

    renderHook(() => useReplicaCollection(projectedRef, {}), { wrapper: wrapperFor(client) });

    expect(client.watchedReplicaRefs.at(-1)).toBe(projectedRef);
  });

  it("exposes protocol-owned completeness instead of inferring from row count", () => {
    const client = new FakeGonvexClient();
    client.replicaState = { rows:[{id:"a"}],ids:["a"],source:"cache",completeness:"partial",freshness:"offline",isUpToDate:false,truncated:true,computedRevision:17 };
    const { result } = renderHook(() => useReplicaCollectionState(ref, {}), { wrapper: wrapperFor(client) });
    expect(result.current).toMatchObject({ completeness:"partial",freshness:"offline",isUpToDate:false,truncated:true,computedRevision:17 });
  });

  it("publishes a collection's own authority transition", () => {
    const client = new FakeGonvexClient();
    client.replicaState = { rows:[{id:"a"}],ids:["a"],source:"cache",completeness:"complete",freshness:"verifying",isUpToDate:false,truncated:false,computedRevision:17 };
    const { result } = renderHook(() => useReplicaCollectionState(ref, {}), { wrapper: wrapperFor(client) });

    expect(result.current).toMatchObject({ source:"cache",freshness:"verifying",isUpToDate:false });
    act(() => {
      client.replicaState = { ...client.replicaState, source:"server", freshness:"current", isUpToDate:true };
      client.updateReplica();
    });
    expect(result.current).toMatchObject({ source:"server",freshness:"current",isUpToDate:true });
  });
});

describe("normalized Replica selectors", () => {
  it("keeps a derived snapshot stable while the source rows are unchanged", () => {
    const client = new FakeGonvexClient();
    client.replicaRows.push({ id: "a", title: "A" });
    const selector = vi.fn((rows: unknown[]) => rows.map((row) => ({ ...(row as Record<string, unknown>) })));

    const { result, rerender } = renderHook(() => useReplicaSelector(ref, {}, selector), { wrapper: wrapperFor(client) });
    const initial = result.current;
    const callsAfterMount = selector.mock.calls.length;
    rerender();

    expect(result.current).toBe(initial);
    expect(selector).toHaveBeenCalledTimes(callsAfterMount);
  });

  it("updates a batched entity selector with one Replica subscription", () => {
    const client = new FakeGonvexClient();
    client.entityValues.set("a", { id: "a", title: "A" });
    const { result } = renderHook(() => useReplicaEntities<{ id: string; title: string }>("tasks", ["a", "b"]), { wrapper: wrapperFor(client) });
    expect(result.current).toEqual([{ id: "a", title: "A" }, undefined]);
    act(() => { client.entityValues.set("b", { id: "b", title: "B" }); client.updateReplica(); });
    expect(result.current).toEqual([{ id: "a", title: "A" }, { id: "b", title: "B" }]);
    expect(client.replicaListeners.size).toBe(1);
  });

  it("subscribes to retained membership without opening another query", () => {
    const client = new FakeGonvexClient();
    client.retained = { rows:[{id:"a"}],ids:["a"],source:"cache",completeness:"partial",freshness:"verifying" };
    const { result } = renderHook(() => useRetainedLiveQuery("tasks:grid"), { wrapper: wrapperFor(client) });
    expect(result.current.ids).toEqual(["a"]);
    act(() => { client.retained = {...client.retained,rows:[{id:"b"}],ids:["b"]}; client.updateReplica(); });
    expect(result.current.ids).toEqual(["b"]);
    expect(client.subscribedRefs).toHaveLength(0);
  });
});

describe("useGonvexConnectionState", () => {
  it("reflects the real client connection state and updates on changes", () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useGonvexConnectionState(), { wrapper: wrapperFor(client) });

    expect(result.current).toMatchObject({ isWebSocketConnected: true, hasEverConnected: true });

    act(() => client.setConnected(false));
    expect(result.current.isWebSocketConnected).toBe(false);

    act(() => client.setConnected(true));
    expect(result.current.isWebSocketConnected).toBe(true);
  });
});

describe("useReducer", () => {
  it("forwards per-call timeout options to the client", async () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(
      () => useReducer({ kind: "reducer", path: "tasks.create" }, { timeoutMs: 5_000 }),
      { wrapper: wrapperFor(client) },
    );

    await act(async () => {
      await result.current({ title: "Ship" });
    });

    expect(client.reducer).toHaveBeenCalledWith({ kind: "reducer", path: "tasks.create" }, { title: "Ship" }, { timeoutMs: 5_000 });
  });

  it("keeps reducer and action callbacks stable across rerenders", () => {
    const client = new FakeGonvexClient();
    const { result, rerender } = renderHook(
      () => ({
        reducer: useReducer({ kind: "reducer", path: "tasks.create" }),
        action: useAction({ kind: "action", path: "tasks.export" }),
      }),
      { wrapper: wrapperFor(client) },
    );
    const first = result.current;

    rerender();

    expect(result.current.reducer).toBe(first.reducer);
    expect(result.current.action).toBe(first.action);
  });

  it("exposes refreshed invitations before a Control Reducer follow-up runs", async () => {
    const client = new FakeGonvexClient();
    let finishReducer: ((result: { updated: boolean }) => void) | undefined;
    client.reducer.mockImplementation(() => new Promise((resolve) => {
      finishReducer = resolve;
    }));
    const { result } = renderHook(
      () => ({
        invitations: useInvitationList(),
        update: useReducer(control.invitations.update),
      }),
      { wrapper: wrapperFor(client) },
    );
    const before = [{
      id: "invitation-1", email: "person@example.test", role: "member",
      permissions: {}, teamIds: ["team-old"], allowedAuthProviders: ["firebase"],
      expiresAt: "2026-09-01T00:00:00Z", revoked: false, accepted: false,
      state: "pending", createdAt: "2026-08-01T00:00:00Z", updatedAt: "2026-08-01T00:00:00Z",
    }];
    act(() => client.emitQuery({
      type: "query.result", id: "invitations", path: "control.invitations.list",
      result: before, reason: "initial",
    }));

    const reducer = result.current.update({
      id: "invitation-1", role: "member", permissions: {}, teamIds: ["team-new"],
      allowedAuthProviders: ["firebase"], payload: {},
    });
    const after = [{ ...before[0], teamIds: ["team-new"], updatedAt: "2026-08-29T00:00:00Z" }];
    act(() => client.emitQuery({
      type: "query.result", id: "invitations", path: "control.invitations.list",
      result: after, reason: "control-change",
    }));
    expect(result.current.invitations).toEqual(after);

    let invitationsAtSettlement: unknown;
    const observed = reducer.then(() => {
      invitationsAtSettlement = result.current.invitations;
    });
    await act(async () => {
      finishReducer?.({ updated: true });
      await observed;
    });
    expect(invitationsAtSettlement).toEqual(after);
  });
});

describe("GonvexProviderWithAuth", () => {
  // This file has no global auto-cleanup (vitest globals are off), and these are
  // its only full `render` calls — unmount them so `queryByTestId` in the next
  // test doesn't find the previous test's DOM in the shared document.body.
  afterEach(cleanup);

  function authState(fetchAccessToken: (args: { forceRefreshToken: boolean }) => Promise<string | null>) {
    return { isLoading: false, isAuthenticated: true, fetchAccessToken };
  }

  it("installs the token and renders children when the fetch succeeds", async () => {
    const client = new FakeGonvexClient();
    let resolveToken!: (token: string | null) => void;
    const fetchAccessToken = vi.fn(() => new Promise<string | null>((resolve) => { resolveToken = resolve; }));

    const { queryByTestId } = render(
      <GonvexProviderWithAuth client={client as unknown as GonvexClient} useAuth={() => authState(fetchAccessToken)}>
        <div data-testid="app" />
      </GonvexProviderWithAuth>,
    );

    expect(queryByTestId("app")).toBeNull();

    await act(async () => resolveToken("jwt-token"));

    expect(client.setAuth).toHaveBeenCalledWith({ token: "jwt-token", fetchToken: fetchAccessToken });
    expect(queryByTestId("app")).not.toBeNull();
  });

  it("passes the live fetcher through so the client can refresh tokens itself", async () => {
    const client = new FakeGonvexClient();
    const fetchAccessToken = vi.fn((_args: { forceRefreshToken: boolean }) => Promise.resolve("jwt-token"));

    render(
      <GonvexProviderWithAuth client={client as unknown as GonvexClient} useAuth={() => authState(fetchAccessToken)}>
        <div data-testid="app" />
      </GonvexProviderWithAuth>,
    );
    await act(async () => {});

    const installed = client.setAuth.mock.calls[0][0] as { fetchToken: (args: { forceRefreshToken: boolean }) => Promise<string | null> };
    await installed.fetchToken({ forceRefreshToken: true });
    expect(fetchAccessToken).toHaveBeenLastCalledWith({ forceRefreshToken: true });
  });

  it("releases children without touching client auth when the fetch rejects", async () => {
    const client = new FakeGonvexClient();
    let rejectToken!: (error: Error) => void;
    const fetchAccessToken = vi.fn(() => new Promise<string | null>((_resolve, reject) => { rejectToken = reject; }));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    try {
      const { queryByTestId } = render(
        <GonvexProviderWithAuth client={client as unknown as GonvexClient} useAuth={() => authState(fetchAccessToken)}>
          <div data-testid="app" />
        </GonvexProviderWithAuth>,
      );

      expect(queryByTestId("app")).toBeNull();

      // The canonical failure: an offline load whose identity provider needs
      // the network. The app must render on whatever auth the client already
      // holds instead of staying blank forever.
      await act(async () => rejectToken(new Error("network unavailable")));

      expect(queryByTestId("app")).not.toBeNull();
      expect(client.setAuth).not.toHaveBeenCalled();
    } finally {
      warn.mockRestore();
    }
  });

  it("exposes terminal auth rejection and keeps children mounted for sign-in routing", async () => {
    const client = new FakeGonvexClient();
    const fetchAccessToken = vi.fn(() => Promise.resolve("jwt-token"));
    function AuthProbe() {
      const auth = useGonvexAuthState();
      return <output data-testid="auth-state">{JSON.stringify({ authenticated: auth.isAuthenticated, error: auth.authError?.message })}</output>;
    }

    const view = render(
      <GonvexProviderWithAuth client={client as unknown as GonvexClient} useAuth={() => authState(fetchAccessToken)}>
        <AuthProbe />
      </GonvexProviderWithAuth>,
    );
    await act(async () => {});
    expect(view.getByTestId("auth-state").textContent).toBe('{"authenticated":true}');

    act(() => client.emitAuthError("token expired"));

    expect(view.getByTestId("auth-state").textContent).toBe('{"authenticated":false,"error":"token expired"}');
  });
});

describe("GonvexAuthProvider", () => {
  afterEach(() => { cleanup(); localStorage.clear(); sessionStorage.clear(); });

  it("renders a current persisted external session before the provider identity callback", () => {
    const client = new FakeGonvexClient();
    const storageKey = "gonvex-auth:https%3A%2F%2Ffirebase-warm.test:shop";
    localStorage.setItem(storageKey, JSON.stringify({
      accessToken: "canonical-access", expiresAt: Date.now() + 900_000,
      refreshToken: "canonical-refresh", refreshExpiresAt: Date.now() + 86_400_000,
      account: { id: "acct-firebase", email: "firebase@example.test", emailVerified: true, provider: "firebase" },
      tenants: [{ id: "tenant-1", name: "Tenant", role: "admin", permissions: {}, domain: "tenant", timezone: "UTC", description: "", profile: {} }],
      activeTenantId: "tenant-1",
    }));
    let tokenListener: ((identity: { uid: string } | null) => void) | undefined;
    const externalAuth = {
      provider: "firebase" as const,
      getIdToken: vi.fn(async () => "rotated-firebase-id-token"),
      onIdTokenChanged(listener: typeof tokenListener) { tokenListener = listener; return vi.fn(); },
    };
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer() {
      auth = useGonvexAuth();
      return <div data-testid="warm-application">Application</div>;
    }

    const rendered = render(
      <GonvexAuthProvider
        client={client as unknown as GonvexClient}
        runtimeUrl="https://firebase-warm.test"
        projectId="shop"
        initialTenantId="tenant"
        externalAuth={externalAuth}
      >
        <Consumer />
      </GonvexAuthProvider>,
    );

    expect(tokenListener).toBeTypeOf("function");
    expect(rendered.queryByTestId("warm-application")).not.toBeNull();
    expect(auth).toMatchObject({
      isAuthenticated: true,
      isLoading: false,
      sessionState: "reconnecting",
    });
    expect(client.setAuth).toHaveBeenCalledWith({
      project: "shop",
      tenant: "tenant-1",
      token: "canonical-access",
      identity: { sub: "acct-firebase", iss: "shop" },
    });
    expect(client.action).not.toHaveBeenCalled();
  });

  it("warms an explicit landlord origin from a current tenant session without installing its tenant scope", async () => {
    const client = new FakeGonvexClient();
    const storageKey = "gonvex-auth:https%3A%2F%2Ffirebase-landlord.test:shop";
    const persisted = {
      accessToken: "tenant-access", expiresAt: Date.now() + 900_000,
      refreshToken: "tenant-refresh", refreshExpiresAt: Date.now() + 86_400_000,
      account: { id: "acct-firebase", email: "firebase@example.test", emailVerified: true, provider: "firebase" },
      tenants: [{ id: "tenant-1", name: "Tenant", role: "admin", permissions: {}, domain: "tenant", timezone: "UTC", description: "", profile: {} }],
      activeTenantId: "tenant-1",
    };
    localStorage.setItem(storageKey, JSON.stringify(persisted));
    client.action.mockResolvedValue({
      ...persisted,
      accessToken: "landlord-access",
      refreshToken: "landlord-refresh",
      activeTenantId: undefined,
    });
    let tokenListener: ((identity: { uid: string } | null) => void) | undefined;
    const externalAuth = {
      provider: "firebase" as const,
      getIdToken: vi.fn(async () => "firebase-id-token"),
      onIdTokenChanged(listener: typeof tokenListener) { tokenListener = listener; return vi.fn(); },
    };
    const rendered = render(
      <GonvexAuthProvider
        client={client as unknown as GonvexClient}
        runtimeUrl="https://firebase-landlord.test"
        projectId="shop"
        initialTenantId={null}
        externalAuth={externalAuth}
        loadingFallback={<div data-testid="auth-loading">Loading authentication</div>}
      >
        <div data-testid="application">Application</div>
      </GonvexAuthProvider>,
    );

    expect(rendered.queryByTestId("auth-loading")).toBeNull();
    expect(rendered.queryByTestId("application")).not.toBeNull();
    expect(client.setAuth).toHaveBeenCalledWith({
      project: "shop",
      tenant: undefined,
      token: "tenant-access",
      identity: { sub: "acct-firebase", iss: "shop" },
    });

    await act(async () => {
      tokenListener?.({ uid: "firebase-uid" });
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(client.action).toHaveBeenCalledWith(
      expect.objectContaining({ path: "control.auth.exchangeExternalToken" }),
      {
        provider: "firebase",
        token: "firebase-id-token",
        previousRefreshToken: "tenant-refresh",
      },
    );
    expect(rendered.queryByTestId("auth-loading")).toBeNull();
    expect(rendered.queryByTestId("application")).not.toBeNull();
    expect(client.setAuth).toHaveBeenCalledWith(expect.objectContaining({
      project: "shop",
      tenant: undefined,
      token: "landlord-access",
    }));
  });

  it("keeps expired, provider-mismatched, and tenant-mismatched sessions private", () => {
    const session = {
      accessToken: "canonical-access", expiresAt: Date.now() + 10_000,
      refreshToken: "canonical-refresh", refreshExpiresAt: Date.now() + 86_400_000,
      account: { id: "acct-firebase", email: "firebase@example.test", emailVerified: true, provider: "firebase" },
      tenants: [{ id: "tenant-1", name: "Tenant", role: "admin", permissions: {}, domain: "tenant", timezone: "UTC", description: "", profile: {} }],
      activeTenantId: "tenant-1",
    };
    const cases = [
      { runtime: "expired", session },
      { runtime: "provider", session: { ...session, expiresAt: Date.now() + 900_000, account: { ...session.account, provider: "password" } } },
      { runtime: "tenant", session: { ...session, expiresAt: Date.now() + 900_000 } },
    ];

    for (const testCase of cases) {
      const client = new FakeGonvexClient();
      localStorage.setItem(
        `gonvex-auth:https%3A%2F%2Ffirebase-${testCase.runtime}.test:shop`,
        JSON.stringify(testCase.session),
      );
      const externalAuth = {
        provider: "firebase" as const,
        getIdToken: vi.fn(async () => "firebase-id-token"),
        onIdTokenChanged() { return vi.fn(); },
      };
      const rendered = render(
        <GonvexAuthProvider
          client={client as unknown as GonvexClient}
          runtimeUrl={`https://firebase-${testCase.runtime}.test`}
          projectId="shop"
          initialTenantId={testCase.runtime === "tenant" ? "tenant-2" : "tenant-1"}
          externalAuth={externalAuth}
        >
          <div data-testid="private-application">Application</div>
        </GonvexAuthProvider>,
      );

      expect(rendered.queryByTestId("private-application"), testCase.runtime).toBeNull();
      expect(client.setAuth, testCase.runtime).not.toHaveBeenCalled();
      rendered.unmount();
      localStorage.clear();
    }

    const noSessionClient = new FakeGonvexClient();
    const noSession = render(
      <GonvexAuthProvider
        client={noSessionClient as unknown as GonvexClient}
        runtimeUrl="https://firebase-empty.test"
        projectId="shop"
        initialTenantId="tenant-1"
        externalAuth={{
          provider: "firebase",
          getIdToken: vi.fn(async () => "firebase-id-token"),
          onIdTokenChanged() { return vi.fn(); },
        }}
      >
        <div data-testid="private-application">Application</div>
      </GonvexAuthProvider>,
    );
    expect(noSession.queryByTestId("private-application")).toBeNull();
    expect(noSessionClient.setAuth).not.toHaveBeenCalled();
  });

  it("publishes a selected tenant only after its authoritative Replica scope is active", async () => {
    const client = new FakeGonvexClient();
    client.action.mockResolvedValue({
      accessToken: "access", expiresAt: Date.now() + 900_000, refreshToken: "refresh", refreshExpiresAt: Date.now() + 86_400_000,
      account: { id: "acct-1", email: "person@example.test", emailVerified: true, name: "Person", picture: "", provider: "password" },
      tenants: [
        { id: "tenant-1", name: "First", role: "admin", permissions: {}, domain: "first", timezone: "UTC", profile: {} },
        { id: "tenant-2", name: "Second", role: "admin", permissions: {}, domain: "second", timezone: "UTC", profile: {} },
      ],
      activeTenantId: "tenant-1",
    });
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer() { auth = useGonvexAuth(); return null; }
    render(<GonvexAuthProvider client={client as unknown as GonvexClient} runtimeUrl="https://runtime.test" projectId="shop"><Consumer /></GonvexAuthProvider>);
    await act(async () => { await Promise.resolve(); await Promise.resolve(); });
    await act(async () => { await auth!.signInWithPassword("person@example.test", "correct-password"); });

    const refreshRegistration = client.setAuth.mock.calls
      .map(([value]) => value as Record<string, unknown>)
      .findLast((value) => typeof value.fetchToken === "function");
    expect(refreshRegistration).toEqual({
      token: "access",
      fetchToken: expect.any(Function),
    });

    let acceptTenant!: () => void;
    client.authenticate.mockImplementationOnce(() => new Promise<void>((resolve) => { acceptTenant = resolve; }));
    let switching!: Promise<void>;
    act(() => { switching = auth!.setActiveTenant("tenant-2"); });

    expect(auth!.activeTenant?.id).toBe("tenant-1");
    expect(client.authenticate).toHaveBeenCalledWith(expect.objectContaining({
      project: "shop",
      tenant: "tenant-2",
      token: "access",
    }));

    await act(async () => { acceptTenant(); await switching; });
    expect(auth!.activeTenant?.id).toBe("tenant-2");
  });

  it("installs and persists a native password session through the OAuth session path", async () => {
    const client = new FakeGonvexClient();
    client.action.mockResolvedValue({
      accessToken:"access",expiresAt:Date.now()+900_000,refreshToken:"refresh",refreshExpiresAt:Date.now()+86_400_000,
      account:{id:"acct-1",email:"person@example.test",emailVerified:true,name:"Person",picture:"",provider:"password"},
      tenants:[{id:"tenant-1",name:"Tenant",role:"admin",permissions:{},domain:"tenant",timezone:"UTC",profile:{}}],activeTenantId:"tenant-1",
    });
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer(){auth=useGonvexAuth();return null;}
    render(<GonvexAuthProvider client={client as unknown as GonvexClient} runtimeUrl="https://runtime.test" projectId="shop"><Consumer/></GonvexAuthProvider>);
    await act(async()=>{await Promise.resolve();await Promise.resolve();});
    await act(async()=>{await auth!.signInWithPassword("person@example.test","correct-password");});
    expect(client.action).toHaveBeenCalledWith(expect.objectContaining({path:"control.auth.passwordLogin"}),{email:"person@example.test",password:"correct-password"});
    expect(client.setAuth).toHaveBeenCalledWith(expect.objectContaining({project:"shop",tenant:"tenant-1",token:"access"}));
    expect(JSON.parse(localStorage.getItem("gonvex-auth:https%3A%2F%2Fruntime.test:shop")!)).toMatchObject({refreshToken:"refresh",activeTenantId:"tenant-1"});
    client.reducer.mockResolvedValue({tenantId:"tenant-2",memberId:"member-2"});
    await act(async()=>{expect(await auth!.acceptInvitation("invite-token")).toEqual({tenantId:"tenant-2",memberId:"member-2"});});
    expect(client.reducer).toHaveBeenCalledWith(expect.objectContaining({path:"control.invitations.accept"}),{token:"invite-token"});
    expect(client.query).not.toHaveBeenCalled();
  });

  it("exchanges Firebase tokens, rotates them, and restores Firebase-backed auth after developer mode", async () => {
    const client = new FakeGonvexClient();
    const authLockNames: string[] = [];
    Object.defineProperty(navigator, "locks", {
      configurable: true,
      value: {
        request: async <T,>(name: string, action: () => Promise<T>) => {
          authLockNames.push(name);
          return action();
        },
      },
    });
    const now = Date.now();
    const grants = ["firebase-access-1", "firebase-access-2"].map((accessToken, index) => ({
      accessToken, expiresAt: now + 900_000, refreshToken: `firebase-refresh-${index + 1}`, refreshExpiresAt: now + 86_400_000,
      account: { id: "acct-firebase", email: "firebase@example.test", emailVerified: true, provider: "firebase" },
      tenants: [{ id: "tenant-1", name: "Tenant", role: "admin", permissions: {}, domain: "tenant", timezone: "UTC", description: "", profile: {} }],
      activeTenantId: "tenant-1",
    }));
    client.action.mockResolvedValueOnce(grants[0]).mockResolvedValueOnce(grants[1]);
    client.reducer.mockImplementation((reference: FunctionReference) => reference.path === "control.developer.enter"
      ? Promise.resolve({ id: "grant-firebase", token: "developer-secret", expiresAt: new Date(now + 300_000).toISOString() })
      : Promise.resolve({ updated: true }));
    let tokenListener: ((identity: { uid: string; issuer?: string } | null) => void) | undefined;
    const getIdToken = vi.fn(async (force: boolean) => force ? "firebase-id-token-2" : "firebase-id-token-1");
    const externalAuth = {
      provider: "firebase" as const,
      getIdToken,
      onIdTokenChanged(listener: typeof tokenListener) { tokenListener = listener; return vi.fn(); },
      signOut: vi.fn(async () => undefined),
    };
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer() { auth = useGonvexAuth(); return null; }
    render(<GonvexAuthProvider client={client as unknown as GonvexClient} runtimeUrl="https://firebase-runtime.test" projectId="shop" initialTenantId="tenant-1" externalAuth={externalAuth}><Consumer /></GonvexAuthProvider>);
    await act(async () => { tokenListener?.({ uid: "firebase-uid", issuer: "firebase-project" }); await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); });
    expect(client.action).toHaveBeenCalledWith(expect.objectContaining({ path: "control.auth.exchangeExternalToken" }), {
      provider: "firebase", token: "firebase-id-token-1", tenantId: "tenant-1",
    });
    expect(auth?.account?.provider).toBe("firebase");
    expect(localStorage.getItem("gonvex-auth:https%3A%2F%2Ffirebase-runtime.test:shop")).not.toContain("firebase-id-token");

    const installedFetcher = [...client.setAuth.mock.calls].map((call) => call[0] as { fetchToken?: (args: { forceRefreshToken: boolean }) => Promise<string | null> }).find((value) => value.fetchToken)?.fetchToken;
    expect(installedFetcher).toBeTypeOf("function");
    await act(async () => { expect(await installedFetcher!({ forceRefreshToken: true })).toBe("firebase-access-2"); });
    expect(getIdToken).toHaveBeenLastCalledWith(true);
    expect(client.action).toHaveBeenLastCalledWith(expect.objectContaining({ path: "control.auth.exchangeExternalToken" }), {
      provider: "firebase", token: "firebase-id-token-2", tenantId: "tenant-1", previousRefreshToken: "firebase-refresh-1",
    });
    expect(new Set(authLockNames)).toEqual(new Set([
      "gonvex-auth:https%3A%2F%2Ffirebase-runtime.test:shop:external-session",
    ]));

    await act(async () => { await auth!.enterDeveloperMode("tenant-1"); });
    await act(async () => { await auth!.exitDeveloperMode(); });
    expect(client.setAuth).toHaveBeenLastCalledWith(expect.objectContaining({ fetchToken: expect.any(Function) }));
  });

  it("uses the newest cross-tab Firebase session when exchanging an identity token", async () => {
    const client = new FakeGonvexClient();
    const now = Date.now();
    const storageKey = "gonvex-auth:https%3A%2F%2Ffirebase-tabs.test:shop";
    const account = { id: "acct-firebase", email: "firebase@example.test", emailVerified: true, provider: "firebase" };
    const tenants = [{ id: "tenant-1", name: "Tenant", role: "admin", permissions: {}, domain: "tenant", timezone: "UTC", description: "", profile: {} }];
    const staleSession = {
      accessToken: "access-stale", expiresAt: now + 900_000,
      refreshToken: "refresh-stale", refreshExpiresAt: now + 86_400_000,
      account, tenants, activeTenantId: "tenant-1",
    };
    const crossTabSession = {
      ...staleSession,
      accessToken: "access-cross-tab",
      refreshToken: "refresh-cross-tab",
    };
    client.action.mockResolvedValue({
      ...crossTabSession,
      accessToken: "access-current",
      refreshToken: "refresh-current",
    });
    localStorage.setItem(storageKey, JSON.stringify(staleSession));

    let tokenListener: ((identity: { uid: string } | null) => void) | undefined;
    const externalAuth = {
      provider: "firebase" as const,
      getIdToken: vi.fn(async () => "firebase-id-token"),
      onIdTokenChanged(listener: typeof tokenListener) { tokenListener = listener; return vi.fn(); },
    };
    render(<GonvexAuthProvider client={client as unknown as GonvexClient} runtimeUrl="https://firebase-tabs.test" projectId="shop" externalAuth={externalAuth}><div /></GonvexAuthProvider>);
    localStorage.setItem(storageKey, JSON.stringify(crossTabSession));

    await act(async () => {
      tokenListener?.({ uid: "firebase-uid" });
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(client.action).toHaveBeenCalledWith(expect.objectContaining({ path: "control.auth.exchangeExternalToken" }), {
      provider: "firebase",
      token: "firebase-id-token",
      tenantId: "tenant-1",
      previousRefreshToken: "refresh-cross-tab",
    });
    expect(JSON.parse(localStorage.getItem(storageKey)!)).toMatchObject({
      accessToken: "access-current",
      refreshToken: "refresh-current",
    });
  });

  it("keeps children mounted while an existing Firebase-backed session rotates", async () => {
    const client = new FakeGonvexClient();
    const now = Date.now();
    const storageKey = "gonvex-auth:https%3A%2F%2Ffirebase-background.test:shop";
    const current = {
      accessToken: "access-current", expiresAt: now + 900_000,
      refreshToken: "refresh-current", refreshExpiresAt: now + 86_400_000,
      account: { id: "acct-firebase", email: "firebase@example.test", emailVerified: true, provider: "firebase" },
      tenants: [{ id: "tenant-1", name: "Tenant", role: "admin", permissions: {}, domain: "tenant", timezone: "UTC", description: "", profile: {} }],
      activeTenantId: "tenant-1",
    };
    localStorage.setItem(storageKey, JSON.stringify(current));
    let finishExchange: ((session: typeof current) => void) | undefined;
    client.action.mockImplementation(() => new Promise((resolve) => { finishExchange = resolve; }));
    let tokenListener: ((identity: { uid: string } | null) => void) | undefined;
    const externalAuth = {
      provider: "firebase" as const,
      getIdToken: vi.fn(async () => "firebase-id-token"),
      onIdTokenChanged(listener: typeof tokenListener) { tokenListener = listener; return vi.fn(); },
    };
    const rendered = render(
      <GonvexAuthProvider
        client={client as unknown as GonvexClient}
        runtimeUrl="https://firebase-background.test"
        projectId="shop"
        externalAuth={externalAuth}
      >
        <div data-testid="application">Application</div>
      </GonvexAuthProvider>,
    );

    await act(async () => {
      tokenListener?.({ uid: "firebase-uid" });
      await Promise.resolve();
    });

    expect(rendered.queryByTestId("application")).not.toBeNull();
    expect(client.setAuth).toHaveBeenCalledWith(expect.objectContaining({
      project: "shop",
      tenant: "tenant-1",
      token: "access-current",
    }));

    await act(async () => {
      finishExchange?.({ ...current, accessToken: "access-rotated", refreshToken: "refresh-rotated" });
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(rendered.queryByTestId("application")).not.toBeNull();
  });

  it("preserves a canonical Firebase session when background exchange has a transient host failure", async () => {
    const client = new FakeGonvexClient();
    const storageKey = "gonvex-auth:https%3A%2F%2Ffirebase-degraded.test:shop";
    const current = {
      accessToken: "access-current", expiresAt: Date.now() + 900_000,
      refreshToken: "refresh-current", refreshExpiresAt: Date.now() + 86_400_000,
      account: { id: "acct-firebase", email: "firebase@example.test", emailVerified: true, provider: "firebase" },
      tenants: [{ id: "tenant-1", name: "Tenant", role: "admin", permissions: {}, domain: "tenant", timezone: "UTC", description: "", profile: {} }],
      activeTenantId: "tenant-1",
    };
    localStorage.setItem(storageKey, JSON.stringify(current));
    client.action.mockRejectedValue(new GonvexClientError(
      "Control Plane database invariant failed during Control Plane idempotency claim",
      { code: "server", path: "control.auth.exchangeExternalToken", operation: "action" },
    ));
    let tokenListener: ((identity: { uid: string } | null) => void) | undefined;
    const externalAuth = {
      provider: "firebase" as const,
      getIdToken: vi.fn(async () => "firebase-id-token"),
      onIdTokenChanged(listener: typeof tokenListener) { tokenListener = listener; return vi.fn(); },
    };
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer() { auth = useGonvexAuth(); return <div data-testid="application">Application</div>; }
    const rendered = render(
      <GonvexAuthProvider client={client as unknown as GonvexClient} runtimeUrl="https://firebase-degraded.test" projectId="shop" externalAuth={externalAuth}>
        <Consumer />
      </GonvexAuthProvider>,
    );

    expect(rendered.queryByTestId("application")).not.toBeNull();
    expect(auth).toMatchObject({ isAuthenticated: true, sessionState: "reconnecting" });

    await act(async () => {
      tokenListener?.({ uid: "firebase-uid" });
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(rendered.queryByTestId("application")).not.toBeNull();
    expect(auth).toMatchObject({ isAuthenticated: true, sessionState: "degraded" });
    expect(JSON.parse(localStorage.getItem(storageKey)!)).toMatchObject({
      accessToken: "access-current",
      refreshToken: "refresh-current",
    });
    expect(client.setAuth.mock.calls.some(([value]) => (
      (value as { token?: string }).token === undefined
    ))).toBe(false);
  });

  it("clears a canonical Firebase session after a fatal identity rejection", async () => {
    const client = new FakeGonvexClient();
    const storageKey = "gonvex-auth:https%3A%2F%2Ffirebase-rejected.test:shop";
    localStorage.setItem(storageKey, JSON.stringify({
      accessToken: "access-current", expiresAt: Date.now() + 900_000,
      refreshToken: "refresh-current", refreshExpiresAt: Date.now() + 86_400_000,
      account: { id: "acct-firebase", email: "firebase@example.test", emailVerified: true, provider: "firebase" },
      tenants: [{ id: "tenant-1", name: "Tenant", role: "admin", permissions: {}, domain: "tenant", timezone: "UTC", description: "", profile: {} }],
      activeTenantId: "tenant-1",
    }));
    client.action.mockRejectedValue(new GonvexClientError(
      "external identity token is expired",
      { code: "server", path: "control.auth.exchangeExternalToken", operation: "action" },
    ));
    let tokenListener: ((identity: { uid: string } | null) => void) | undefined;
    const externalAuth = {
      provider: "firebase" as const,
      getIdToken: vi.fn(async () => "expired-firebase-id-token"),
      onIdTokenChanged(listener: typeof tokenListener) { tokenListener = listener; return vi.fn(); },
    };
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer() { auth = useGonvexAuth(); return null; }
    render(<GonvexAuthProvider client={client as unknown as GonvexClient} runtimeUrl="https://firebase-rejected.test" projectId="shop" externalAuth={externalAuth}><Consumer /></GonvexAuthProvider>);

    expect(auth).toMatchObject({ isAuthenticated: true, sessionState: "reconnecting" });

    await act(async () => {
      tokenListener?.({ uid: "firebase-uid" });
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(auth).toMatchObject({ isAuthenticated: false, sessionState: "signedOut" });
    expect(localStorage.getItem(storageKey)).toBeNull();
    expect(client.setAuth).toHaveBeenLastCalledWith(expect.objectContaining({ token: undefined }));
  });

  it("keeps initial Firebase exchange failures signed out", async () => {
    const client = new FakeGonvexClient();
    client.action.mockRejectedValue(new GonvexClientError(
      "Control Plane database connection failed",
      { code: "server", path: "control.auth.exchangeExternalToken", operation: "action" },
    ));
    let tokenListener: ((identity: { uid: string } | null) => void) | undefined;
    const externalAuth = {
      provider: "firebase" as const,
      getIdToken: vi.fn(async () => "firebase-id-token"),
      onIdTokenChanged(listener: typeof tokenListener) { tokenListener = listener; return vi.fn(); },
    };
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer() { auth = useGonvexAuth(); return null; }
    render(<GonvexAuthProvider client={client as unknown as GonvexClient} runtimeUrl="https://firebase-initial-failure.test" projectId="shop" externalAuth={externalAuth}><Consumer /></GonvexAuthProvider>);

    await act(async () => {
      tokenListener?.({ uid: "firebase-uid" });
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(auth).toMatchObject({ isAuthenticated: false, sessionState: "signedOut" });
    expect(localStorage.getItem("gonvex-auth:https%3A%2F%2Ffirebase-initial-failure.test:shop")).toBeNull();
  });

  it("clears Firebase and local auth before remote session revocation finishes", async () => {
    const client = new FakeGonvexClient();
    const now = Date.now();
    client.action.mockResolvedValue({
      accessToken: "firebase-access", expiresAt: now + 900_000,
      refreshToken: "firebase-refresh", refreshExpiresAt: now + 86_400_000,
      account: { id: "acct-firebase", email: "firebase@example.test", emailVerified: true, provider: "firebase" },
      tenants: [{ id: "tenant-1", name: "Tenant", role: "admin", permissions: {}, domain: "tenant", timezone: "UTC", description: "", profile: {} }],
      activeTenantId: "tenant-1",
    });
    let finishRevocation: (() => void) | undefined;
    const revocation = new Promise<void>((resolve) => { finishRevocation = resolve; });
    client.reducer.mockImplementation((reference: FunctionReference) => (
      reference.path === "control.auth.logout" ? revocation : Promise.resolve({ updated: true })
    ));
    let tokenListener: ((identity: { uid: string } | null) => void) | undefined;
    const externalAuth = {
      provider: "firebase" as const,
      getIdToken: vi.fn(async () => "firebase-id-token"),
      onIdTokenChanged(listener: typeof tokenListener) { tokenListener = listener; return vi.fn(); },
      signOut: vi.fn(async () => undefined),
    };
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer() { auth = useGonvexAuth(); return null; }
    render(<GonvexAuthProvider client={client as unknown as GonvexClient} runtimeUrl="https://firebase-logout.test" projectId="shop" externalAuth={externalAuth}><Consumer /></GonvexAuthProvider>);
    await act(async () => { tokenListener?.({ uid: "firebase-uid" }); await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); });
    expect(auth?.isAuthenticated).toBe(true);

    let logout: Promise<void> | undefined;
    await act(async () => {
      logout = auth!.signOut();
      await Promise.resolve();
    });

    expect(externalAuth.signOut).toHaveBeenCalledOnce();
    expect(auth?.isAuthenticated).toBe(false);
    expect(localStorage.getItem("gonvex-auth:https%3A%2F%2Ffirebase-logout.test:shop")).toBeNull();

    finishRevocation?.();
    await act(async () => { await logout; });
  });

  it("does not auto-select a tenant from the live directory before createTenant settles", async () => {
    const client = new FakeGonvexClient();
    const now = Date.now();
    const storageKey = "gonvex-auth:https%3A%2F%2Ftenant-create.test:shop";
    localStorage.setItem(storageKey, JSON.stringify({
      accessToken: "account-access",
      expiresAt: now + 900_000,
      refreshToken: "account-refresh",
      refreshExpiresAt: now + 86_400_000,
      account: { id: "acct-1", email: "owner@example.test", emailVerified: true, provider: "password" },
      tenants: [],
    }));
    const createdTenant = {
      id: "tenant-new",
      name: "New Tenant",
      role: "owner",
      permissions: {},
      domain: "new-tenant",
      timezone: "UTC",
      description: "",
      profile: {},
    };
    let finishCreate: ((tenant: typeof createdTenant) => void) | undefined;
    client.reducer.mockImplementation((reference: FunctionReference) => (
      reference.path === "control.tenants.create"
        ? new Promise((resolve) => { finishCreate = resolve; })
        : Promise.resolve({ updated: true })
    ));
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer() { auth = useGonvexAuth(); return null; }
    render(
      <GonvexAuthProvider client={client as unknown as GonvexClient} runtimeUrl="https://tenant-create.test" projectId="shop">
        <Consumer />
      </GonvexAuthProvider>,
    );
    await act(async () => { await Promise.resolve(); await Promise.resolve(); });

    let create: Promise<unknown> | undefined;
    await act(async () => {
      create = auth!.createTenant("New Tenant", { domain: "new-tenant" });
      await Promise.resolve();
    });

    expect(client.reducer).toHaveBeenCalledWith(
      expect.objectContaining({ path: "control.tenants.create" }),
      { name: "New Tenant", domain: "new-tenant" },
    );

    await act(async () => {
      client.emitQuery({
        type: "query.result",
        id: "tenant-directory",
        path: "control.tenants.mine",
        result: [createdTenant],
        reason: "update",
      });
      await Promise.resolve();
    });

    expect(auth?.tenants).toEqual([createdTenant]);
    expect(auth?.activeTenant).toBeNull();
    expect(client.setAuth.mock.calls.some(([value]) => (
      (value as { tenant?: string }).tenant === createdTenant.id
    ))).toBe(false);

    finishCreate?.(createdTenant);
    await act(async () => { await create; });

    expect(auth?.activeTenant).toEqual(createdTenant);
    expect(client.setAuth).toHaveBeenCalledWith(expect.objectContaining({
      tenant: createdTenant.id,
      token: "account-access",
    }));
  });

  it("hands the canonical session to an allowed sibling origin before navigation", async () => {
    const client = new FakeGonvexClient();
    const now = Date.now();
    localStorage.setItem("gonvex-auth:https%3A%2F%2Fhandoff-runtime.test:shop", JSON.stringify({
      accessToken: "canonical-access",
      expiresAt: now + 900_000,
      refreshToken: "canonical-refresh",
      refreshExpiresAt: now + 86_400_000,
      account: { id: "acct-1", email: "owner@example.test", emailVerified: true, provider: "firebase" },
      tenants: [{ id: "tenant-1", name: "Tenant", role: "owner", permissions: {}, domain: "tenant", timezone: "UTC", description: "", profile: {} }],
      activeTenantId: "tenant-1",
    }));
    const externalAuth = {
      provider: "firebase" as const,
      getIdToken: vi.fn(async () => null),
      onIdTokenChanged() { return vi.fn(); },
    };
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer() { auth = useGonvexAuth(); return null; }
    render(
      <GonvexAuthProvider
        client={client as unknown as GonvexClient}
        runtimeUrl="https://handoff-runtime.test"
        projectId="shop"
        externalAuth={externalAuth}
        crossOriginHandoff={{ allowedOriginSuffix: "localhost" }}
      >
        <Consumer />
      </GonvexAuthProvider>,
    );

    let handoff: Promise<void> | undefined;
    await act(async () => {
      handoff = auth!.handoffSessionTo("http://tenant.localhost/onboarding");
      await Promise.resolve();
    });
    const frame = document.querySelector("iframe") as HTMLIFrameElement;
    expect(frame).not.toBeNull();
    const nonce = new URL(frame.src).hash.match(/gonvexSessionHandoff=([^&]+)/)?.[1];
    expect(nonce).toBeTruthy();
    const postMessage = vi.spyOn(frame.contentWindow!, "postMessage");

    act(() => window.dispatchEvent(new MessageEvent("message", {
      data: { type: "gonvex.sessionHandoff.ready", nonce },
      origin: "http://tenant.localhost",
      source: frame.contentWindow,
    })));
    expect(postMessage).toHaveBeenCalledWith(expect.objectContaining({
      type: "gonvex.sessionHandoff.offer",
      nonce,
      projectId: "shop",
      targetOrigin: "http://tenant.localhost",
      session: expect.objectContaining({ activeTenantId: "tenant-1" }),
    }), "http://tenant.localhost");

    await act(async () => {
      window.dispatchEvent(new MessageEvent("message", {
        data: { type: "gonvex.sessionHandoff.accepted", nonce },
        origin: "http://tenant.localhost",
        source: frame.contentWindow,
      }));
      await handoff;
    });
    expect(document.querySelector("iframe")).toBeNull();
  });

  it("keeps the canonical Firebase-backed session through a transient provider-null callback", async () => {
    const client = new FakeGonvexClient();
    const controlWatch = vi.spyOn(client, "watchControlQuery");
    const storageKey = "gonvex-auth:https%3A%2F%2Ffirebase-reload.test:shop";
    localStorage.setItem(storageKey, JSON.stringify({
      accessToken: "canonical-access", expiresAt: Date.now() + 900_000,
      refreshToken: "canonical-refresh", refreshExpiresAt: Date.now() + 86_400_000,
      account: { id: "acct-firebase", email: "firebase@example.test", emailVerified: true, provider: "firebase" },
      tenants: [{ id: "tenant-1", name: "Tenant", role: "admin", permissions: {}, domain: "tenant", timezone: "UTC", description: "", profile: {} }],
      activeTenantId: "tenant-1",
    }));
    let tokenListener: ((identity: { uid: string } | null) => void) | undefined;
    const externalAuth = {
      provider: "firebase" as const,
      getIdToken: vi.fn(async () => "rotated-firebase-id-token"),
      onIdTokenChanged(listener: typeof tokenListener) { tokenListener = listener; return vi.fn(); },
    };
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer() { auth = useGonvexAuth(); return null; }
    render(<GonvexAuthProvider client={client as unknown as GonvexClient} runtimeUrl="https://firebase-reload.test" projectId="shop" externalAuth={externalAuth}><Consumer /></GonvexAuthProvider>);
    await act(async () => { await Promise.resolve(); });
    // A current canonical session already owns an exact project, tenant, and
    // account scope. Its cached Replica can render while Firebase hydrates.
    expect(client.setAuth).toHaveBeenCalledWith(expect.objectContaining({
      project: "shop",
      tenant: "tenant-1",
      token: "canonical-access",
      identity: { sub: "acct-firebase", iss: "shop" },
    }));
    expect(controlWatch).toHaveBeenCalledOnce();
    expect(auth).toMatchObject({ isAuthenticated: true, sessionState: "reconnecting" });
    expect(localStorage.getItem(storageKey)).not.toContain("firebase-id-token");
    await act(async () => { tokenListener?.(null); await Promise.resolve(); });
    expect(client.setAuth).toHaveBeenCalledWith(expect.objectContaining({
      project: "shop",
      tenant: "tenant-1",
      token: "canonical-access",
    }));
    expect(client.setAuth).toHaveBeenCalledWith({ token: "canonical-access", fetchToken: expect.any(Function) });
    expect(controlWatch).toHaveBeenCalledOnce();
    expect(auth?.isAuthenticated).toBe(true);
    expect(localStorage.getItem(storageKey)).not.toBeNull();
  });

  it("does not sign out when a forced Firebase refresh runs before provider hydration", async () => {
    const client = new FakeGonvexClient();
    const storageKey = "gonvex-auth:https%3A%2F%2Ffirebase-hydration.test:shop";
    localStorage.setItem(storageKey, JSON.stringify({
      accessToken: "canonical-access", expiresAt: Date.now() + 900_000,
      refreshToken: "canonical-refresh", refreshExpiresAt: Date.now() + 86_400_000,
      account: { id: "acct-firebase", email: "firebase@example.test", emailVerified: true, provider: "firebase" },
      tenants: [{ id: "tenant-1", name: "Tenant", role: "admin", permissions: {}, domain: "tenant", timezone: "UTC", description: "", profile: {} }],
      activeTenantId: "tenant-1",
    }));
    let tokenListener: ((identity: { uid: string } | null) => void) | undefined;
    const externalAuth = {
      provider: "firebase" as const,
      getIdToken: vi.fn(async () => null),
      onIdTokenChanged(listener: typeof tokenListener) { tokenListener = listener; return vi.fn(); },
    };
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer() { auth = useGonvexAuth(); return null; }
    render(
      <GonvexAuthProvider
        client={client as unknown as GonvexClient}
        runtimeUrl="https://firebase-hydration.test"
        projectId="shop"
        externalAuth={externalAuth}
      >
        <Consumer />
      </GonvexAuthProvider>,
    );

    await act(async () => { tokenListener?.(null); await Promise.resolve(); });
    await act(async () => {
      expect(await auth!.fetchAccessToken!({ forceRefreshToken: true })).toBe("canonical-access");
    });

    expect(auth?.isAuthenticated).toBe(true);
    expect(localStorage.getItem(storageKey)).toContain("canonical-refresh");
    expect(client.action).not.toHaveBeenCalled();
  });

  it("owns the developer-mode enter/exit lifecycle without exposing or persisting its token", async () => {
    const client = new FakeGonvexClient();
    const now = Date.now();
    const normalSession = {
      accessToken:"account-access",expiresAt:now+900_000,refreshToken:"account-refresh",refreshExpiresAt:now+86_400_000,
      account:{id:"acct-1",email:"dev@example.test",emailVerified:true,name:"Developer",picture:"",provider:"password"},
      tenants:[
        {id:"tenant-home",name:"Home",role:"admin",permissions:{},domain:"home",timezone:"UTC",description:"",profile:{}},
        {id:"tenant-target",name:"Target",role:"member",permissions:{},domain:"target",timezone:"UTC",description:"",profile:{}},
      ],activeTenantId:"tenant-home",
    };
    const storageKey = "gonvex-auth:https%3A%2F%2Fdeveloper-runtime.test:shop";
    localStorage.setItem(storageKey, JSON.stringify(normalSession));
    client.reducer.mockImplementation((reference: FunctionReference) => {
      if (reference.path === "control.developer.enter") return Promise.resolve({id:"grant-1",token:"gvx_imp_secret",expiresAt:new Date(now+300_000).toISOString()});
      return Promise.resolve({updated:true});
    });
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer(){auth=useGonvexAuth();return null;}
    const originalURL = window.location.href;
    render(<GonvexAuthProvider client={client as unknown as GonvexClient} runtimeUrl="https://developer-runtime.test" projectId="shop"><Consumer/></GonvexAuthProvider>);
    await act(async()=>{await Promise.resolve();await Promise.resolve();});

    await act(async()=>{await auth!.enterDeveloperMode("tenant-target");});
    expect(auth!.developerMode).toEqual({active:true,tenantId:"tenant-target",grantId:"grant-1",expiresAt:new Date(now+300_000).toISOString()});
    expect(client.authenticate).toHaveBeenCalledWith(expect.objectContaining({tenant:"tenant-target",token:"gvx_imp_secret",fetchToken:undefined}));
    expect(localStorage.getItem(storageKey)).toBe(JSON.stringify(normalSession));
    expect(JSON.stringify(auth)).not.toContain("gvx_imp_secret");
    expect(window.location.href).toBe(originalURL);
    expect(window.location.search).not.toContain("gvx_imp_secret");

    await act(async()=>{await auth!.exitDeveloperMode();});
    expect(client.reducer).toHaveBeenCalledWith(expect.objectContaining({path:"control.developer.exit"}),{grantId:"grant-1"});
    expect(auth!.developerMode).toEqual({active:false});
    expect(client.setAuth).toHaveBeenCalledWith(expect.objectContaining({tenant:"tenant-home",token:"account-access"}));
    expect(client.setAuth).toHaveBeenLastCalledWith({token:"account-access",fetchToken:expect.any(Function)});
  });

  it("rolls back failed entry and keeps developer mode active when exit fails", async () => {
    const client = new FakeGonvexClient();
    const session = {
      accessToken:"account-access",expiresAt:Date.now()+900_000,refreshToken:"account-refresh",refreshExpiresAt:Date.now()+86_400_000,
      account:{id:"acct-1",email:"dev@example.test",emailVerified:true,provider:"password"},
      tenants:[{id:"tenant-home",name:"Home",role:"admin",domain:"home",timezone:"UTC",description:"",profile:{}},{id:"tenant-target",name:"Target",role:"member",domain:"target",timezone:"UTC",description:"",profile:{}}],activeTenantId:"tenant-home",
    };
    localStorage.setItem("gonvex-auth:https%3A%2F%2Fdeveloper-failure.test:shop",JSON.stringify(session));
    client.reducer.mockResolvedValue({id:"grant-2",token:"gvx_imp_failed",expiresAt:new Date(Date.now()+300_000).toISOString()});
    client.authenticate.mockRejectedValueOnce(new Error("grant rejected"));
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer(){auth=useGonvexAuth();return null;}
    render(<GonvexAuthProvider client={client as unknown as GonvexClient} runtimeUrl="https://developer-failure.test" projectId="shop"><Consumer/></GonvexAuthProvider>);
    await act(async()=>{await Promise.resolve();await Promise.resolve();});
    await act(async()=>{await expect(auth!.enterDeveloperMode("tenant-target")).rejects.toThrow("grant rejected");});
    expect(auth!.developerMode.active).toBe(false);
    expect(client.setAuth).toHaveBeenLastCalledWith(expect.objectContaining({tenant:"tenant-home",token:"account-access"}));

    client.authenticate.mockResolvedValueOnce(undefined);
    await act(async()=>{await auth!.enterDeveloperMode("tenant-target");});
    client.reducer.mockRejectedValueOnce(new Error("network down"));
    await act(async()=>{await expect(auth!.exitDeveloperMode()).rejects.toThrow("network down");});
    expect(auth!.developerMode.active).toBe(true);
  });

  it("restores normal authentication on grant expiry or an authentication error", async () => {
    const client = new FakeGonvexClient();
    const now = Date.now();
    const session = {accessToken:"account-access",expiresAt:now+900_000,refreshToken:"account-refresh",refreshExpiresAt:now+86_400_000,account:{id:"acct-1",email:"dev@example.test",emailVerified:true,provider:"password"},tenants:[{id:"tenant-home",name:"Home",role:"admin",domain:"home",timezone:"UTC",description:"",profile:{}},{id:"tenant-target",name:"Target",role:"member",domain:"target",timezone:"UTC",description:"",profile:{}}],activeTenantId:"tenant-home"};
    localStorage.setItem("gonvex-auth:https%3A%2F%2Fdeveloper-expiry.test:shop",JSON.stringify(session));
    client.reducer.mockResolvedValue({id:"grant-3",token:"gvx_imp_expiring",expiresAt:new Date(now+1_000).toISOString()});
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer(){auth=useGonvexAuth();return null;}
    render(<GonvexAuthProvider client={client as unknown as GonvexClient} runtimeUrl="https://developer-expiry.test" projectId="shop"><Consumer/></GonvexAuthProvider>);
    await act(async()=>{await Promise.resolve();await Promise.resolve();});
    await act(async()=>{await auth!.enterDeveloperMode("tenant-target");});
    act(()=>vi.advanceTimersByTime(1_001));
    expect(auth!.developerMode.active).toBe(false);
    expect(client.setAuth).toHaveBeenCalledWith(expect.objectContaining({tenant:"tenant-home",token:"account-access"}));
    expect(client.setAuth).toHaveBeenLastCalledWith({token:"account-access",fetchToken:expect.any(Function)});

    client.reducer.mockResolvedValue({id:"grant-4",token:"gvx_imp_auth_error",expiresAt:new Date(now+300_000).toISOString()});
    await act(async()=>{await auth!.enterDeveloperMode("tenant-target");});
    act(()=>client.emitAuthError("grant revoked"));
    expect(auth!.developerMode.active).toBe(false);
    expect(auth!.error).toBe("grant revoked");
  });

  it("keeps refreshed account credentials separate and recovers the normal session after reload", async () => {
    const client = new FakeGonvexClient();
    const now = Date.now();
    const storageKey = "gonvex-auth:https%3A%2F%2Fdeveloper-reload.test:shop";
    const session = {accessToken:"account-access",expiresAt:now+61_000,refreshToken:"account-refresh",refreshExpiresAt:now+86_400_000,account:{id:"acct-1",email:"dev@example.test",emailVerified:true,provider:"password"},tenants:[{id:"tenant-home",name:"Home",role:"admin",domain:"home",timezone:"UTC",description:"",profile:{}},{id:"tenant-target",name:"Target",role:"member",domain:"target",timezone:"UTC",description:"",profile:{}}],activeTenantId:"tenant-home"};
    localStorage.setItem(storageKey,JSON.stringify(session));
    client.reducer.mockResolvedValue({id:"grant-5",token:"gvx_imp_memory_only",expiresAt:new Date(now+300_000).toISOString()});
    client.action.mockResolvedValue({...session,accessToken:"refreshed-access",refreshToken:"refreshed-refresh",expiresAt:now+900_000});
    let auth: ReturnType<typeof useGonvexAuth> | undefined;
    function Consumer(){auth=useGonvexAuth();return null;}
    const view = render(<GonvexAuthProvider client={client as unknown as GonvexClient} runtimeUrl="https://developer-reload.test" projectId="shop"><Consumer/></GonvexAuthProvider>);
    await act(async()=>{await Promise.resolve();await Promise.resolve();});
    await act(async()=>{await auth!.enterDeveloperMode("tenant-target");});
    client.setAuth.mockClear();
    await act(async()=>{vi.advanceTimersByTime(2_000);await Promise.resolve();await Promise.resolve();});
    expect(JSON.parse(localStorage.getItem(storageKey)!)).toMatchObject({accessToken:"refreshed-access",refreshToken:"refreshed-refresh"});
    expect(client.setAuth).not.toHaveBeenCalledWith(expect.objectContaining({token:"refreshed-access"}));
    view.unmount();

    const reloadedClient = new FakeGonvexClient();
    render(<GonvexAuthProvider client={reloadedClient as unknown as GonvexClient} runtimeUrl="https://developer-reload.test" projectId="shop"><Consumer/></GonvexAuthProvider>);
    await act(async()=>{await Promise.resolve();await Promise.resolve();});
    expect(reloadedClient.setAuth).toHaveBeenCalledWith(expect.objectContaining({tenant:"tenant-home",token:"refreshed-access"}));
    expect(localStorage.getItem(storageKey)).not.toContain("gvx_imp_memory_only");
  });
});
