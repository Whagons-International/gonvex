import { act, cleanup, render, renderHook } from "@testing-library/react";
import { Component, type ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionState, FunctionReference, GonvexClient } from "@gonvex/client";
import type { ServerMessage } from "@gonvex/protocol";
import { GonvexProviderWithAuth, GonvexProvider, useGonvexAuthState, useGonvexConnectionState, useReducer, useQuery, useQueryResult, useReplicaCollection, useReplicaCollectionState, useReplicaEntities, useRetainedLiveQuery } from "./index";

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

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
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
    client.replicaState = { rows:[{id:"a"}],ids:["a"],source:"cache",completeness:"partial",freshness:"offline",truncated:true,computedRevision:17 };
    const { result } = renderHook(() => useReplicaCollectionState(ref, {}), { wrapper: wrapperFor(client) });
    expect(result.current).toMatchObject({ completeness:"partial",freshness:"offline",truncated:true,computedRevision:17 });
  });
});

describe("normalized Replica selectors", () => {
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
