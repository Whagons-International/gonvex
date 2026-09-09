import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { usePaintExternalStore } from "./paint-external-store.js";

let visible = true;
let nextFrame: FrameRequestCallback;
beforeEach(() => {
  visible = true;
  vi.spyOn(document, "visibilityState", "get").mockImplementation(() => visible ? "visible" : "hidden");
  vi.stubGlobal("requestAnimationFrame", vi.fn(callback => { nextFrame = callback; return 1; }));
  vi.stubGlobal("cancelAnimationFrame", vi.fn());
});
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

function store() {
  let value = 0;
  const listeners = new Set<() => void>();
  return {
    read: () => value,
    subscribe: (notify: () => void) => { listeners.add(notify); return () => { listeners.delete(notify); }; },
    write: (next: number) => { value = next; for (const listener of listeners) listener(); },
    listeners,
  };
}

it("coalesces a replica burst without delaying reads of the current snapshot", () => {
  const data = store();
  const render = vi.fn(() => usePaintExternalStore(data.subscribe, data.read));
  const hook = renderHook(render);
  act(() => { for (let i = 1; i <= 80; i++) data.write(i); });
  expect(data.read()).toBe(80);
  expect(requestAnimationFrame).toHaveBeenCalledOnce();
  expect(render).toHaveBeenCalledOnce();
  act(() => nextFrame(16));
  expect(hook.result.current).toBe(80);
  expect(render).toHaveBeenCalledTimes(2);
});

it("flushes a queued update when the tab hides and continues updates in the background", async () => {
  const data = store();
  const hook = renderHook(() => usePaintExternalStore(data.subscribe, data.read));
  act(() => data.write(1));
  act(() => { visible = false; document.dispatchEvent(new Event("visibilitychange")); });
  expect(hook.result.current).toBe(1);
  expect(cancelAnimationFrame).toHaveBeenCalledOnce();
  await act(async () => data.write(2));
  expect(hook.result.current).toBe(2);
  expect(requestAnimationFrame).toHaveBeenCalledOnce();
});

it("removes pending work when a subscriber unmounts without losing another subscriber", () => {
  const first = store();
  const second = store();
  const a = renderHook(() => usePaintExternalStore(first.subscribe, first.read));
  const b = renderHook(() => usePaintExternalStore(second.subscribe, second.read));
  act(() => { first.write(1); second.write(2); });
  a.unmount();
  expect(first.listeners.size).toBe(0);
  act(() => nextFrame(16));
  expect(b.result.current).toBe(2);
});

it("continues to publish when animation frames are unavailable", async () => {
  vi.stubGlobal("requestAnimationFrame", undefined);
  const data = store();
  const hook = renderHook(() => usePaintExternalStore(data.subscribe, data.read));
  await act(async () => data.write(4));
  expect(hook.result.current).toBe(4);
});

it("batches cold control data but publishes subsequent changes synchronously", () => {
  const data = store();
  let ready = false;
  const read = () => ready ? data.read() : undefined;
  const hook = renderHook(() => usePaintExternalStore(data.subscribe, read, undefined, true));
  act(() => { ready = true; data.write(1); data.write(2); });
  expect(hook.result.current).toBeUndefined();
  act(() => nextFrame(16));
  expect(hook.result.current).toBe(2);
  act(() => data.write(3));
  expect(hook.result.current).toBe(3);
  expect(requestAnimationFrame).toHaveBeenCalledOnce();
});
