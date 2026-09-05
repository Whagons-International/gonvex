import { afterEach, describe, expect, it, vi } from "vitest";
import { DashboardLiveStore } from "./dashboard-live";

describe("dashboard live cache", () => {
  afterEach(() => vi.useRealTimers());
  it("shares a subscription and immediately replays cached data during navigation", () => {
    let publish!: (value: unknown) => void;
    const stop = vi.fn();
    const subscribe = vi.fn((_key, next) => { publish = next; return stop; });
    const store = new DashboardLiveStore(subscribe);
    const first = vi.fn();
    const release = store.watch("tables", first, vi.fn());
    publish({ tables: ["tasks"] });
    release();
    const second = vi.fn();
    store.watch("tables", second, vi.fn());
    expect(subscribe).toHaveBeenCalledTimes(1);
    expect(second).toHaveBeenCalledWith({ tables: ["tasks"] });
    store.close();
    expect(stop).toHaveBeenCalledOnce();
  });
  it("expires unused subscriptions and clears cached data at session boundaries", () => {
    vi.useFakeTimers();
    const stops: ReturnType<typeof vi.fn>[] = [];
    const store = new DashboardLiveStore((_key, next) => {
      const stop = vi.fn(); stops.push(stop); next({ secret: "old session" }); return stop;
    });
    const release = store.watch("rows", vi.fn(), vi.fn());
    release();
    vi.advanceTimersByTime(30_001);
    expect(stops[0]).toHaveBeenCalledOnce();
    store.watch("rows", vi.fn(), vi.fn());
    expect(stops).toHaveLength(2);
    store.close();
    expect(stops[1]).toHaveBeenCalledOnce();
  });
  it("clears a revoked result and reports the error to every listener", () => {
    let reject!: (error: Error) => void;
    const store = new DashboardLiveStore((_key, next, fail) => {
      reject = fail; next({ rows: ["private"] }); return vi.fn();
    });
    const error = vi.fn();
    store.watch("rows", vi.fn(), error);
    reject(new Error("permission revoked"));
    const next = vi.fn();
    store.watch("rows", next, error);
    expect(next).not.toHaveBeenCalled();
    expect(error).toHaveBeenCalledTimes(2);
    store.close();
  });
  it("does not let an old unmount expire a replacement session's subscription", () => {
    vi.useFakeTimers();
    const subscribe = vi.fn(() => vi.fn());
    const store = new DashboardLiveStore(subscribe);
    const releaseOld = store.watch("rows", vi.fn(), vi.fn());
    store.close();
    store.watch("rows", vi.fn(), vi.fn());
    releaseOld();
    vi.advanceTimersByTime(30_001);
    store.watch("rows", vi.fn(), vi.fn());
    expect(subscribe).toHaveBeenCalledTimes(2);
    store.close();
  });

});
