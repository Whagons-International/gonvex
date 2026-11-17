import { act, cleanup, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { PresenceFeed, type GonvexClient } from "@gonvex/client";
import { GonvexProvider, usePresence } from "./index";

afterEach(cleanup);

function fakeClient(supported: boolean) {
  const sent: unknown[] = [];
  const feed = new PresenceFeed({ supported: () => supported, canSend: () => true, send: (m) => { sent.push(m); } });
  const client = {
    subscribePresence: vi.fn((listener: () => void) => feed.subscribe(listener)),
    presenceSnapshot: () => feed.snapshot(),
  };
  const wrapper = ({ children }: { children: ReactNode }) => <GonvexProvider client={client as unknown as GonvexClient}>{children}</GonvexProvider>;
  return { client, feed, sent, wrapper };
}

describe("usePresence", () => {
  it("renders online Members from snapshots and patches", () => {
    const { feed, sent, wrapper } = fakeClient(true);
    const { result, unmount } = renderHook(() => usePresence(), { wrapper });
    expect(result.current.supported).toBe(true);
    expect(sent).toEqual([{ type: "presence.subscribe", id: "presence-1" }]);
    act(() => { feed.handle({ type: "presence.snapshot", id: "presence-1", serverTime: 1, members: [{ memberId: "m1", state: "active", since: 1 }] }); });
    expect(result.current.ready).toBe(true);
    expect(result.current.members.get("m1")?.state).toBe("active");
    act(() => { feed.handle({ type: "presence.patch", serverTime: 2, changes: [{ memberId: "m1", state: "offline", since: 2 }] }); });
    expect(result.current.members.has("m1")).toBe(false);
    unmount();
    expect(sent.at(-1)).toEqual({ type: "presence.unsubscribe" });
  });

  it("reports unsupported runtimes and honors skip", () => {
    const { client, wrapper } = fakeClient(false);
    const { result } = renderHook(() => usePresence(), { wrapper });
    expect(result.current.supported).toBe(false);
    const skipped = renderHook(() => usePresence("skip"), { wrapper });
    expect(skipped.result.current.members.size).toBe(0);
    expect(client.subscribePresence).toHaveBeenCalledTimes(1);
  });
});
