import { useCallback, useSyncExternalStore } from "react";

const pending = new Set<() => void>();
let scheduled = false;
let frame: number | undefined;

function removeFrame() {
  if (frame === undefined) return;
  cancelAnimationFrame(frame);
  frame = undefined;
  document.removeEventListener("visibilitychange", onVisibilityChange);
}

function flush() {
  removeFrame();
  scheduled = false;
  const callbacks = [...pending];
  pending.clear();
  for (const callback of callbacks) callback();
}

function onVisibilityChange() {
  // Animation frames stop in background tabs. Auth and data subscriptions must
  // continue there, including a notification queued just before the tab hid.
  if (document.visibilityState !== "visible") flush();
}

function enqueue(callback: () => void) {
  pending.add(callback);
  if (scheduled) return;
  scheduled = true;
  if (typeof document !== "undefined" && document.visibilityState === "visible"
    && typeof requestAnimationFrame === "function") {
    document.addEventListener("visibilitychange", onVisibilityChange);
    frame = requestAnimationFrame(flush);
  } else {
    queueMicrotask(flush);
  }
}

/** Keep snapshots immediately readable, but notify React once per browser paint.
 * Initial replica delivery can update dozens of collections in separate tasks;
 * rendering every intermediate snapshot allocates discarded component trees. */
export function usePaintExternalStore<T>(
  subscribe: (notify: () => void) => () => void,
  getSnapshot: () => T,
  getServerSnapshot?: () => T,
  initialOnly = false,
): T {
  const batchedSubscribe = useCallback((notify: () => void) => {
    let active = true;
    let initial = getSnapshot() === undefined;
    const deliver = () => { initial = false; if (active) notify(); };
    const release = subscribe(() => {
      if (!active) return;
      if (initialOnly && !initial) notify();
      else enqueue(deliver);
    });
    return () => {
      active = false;
      pending.delete(deliver);
      release();
      if (!pending.size) { removeFrame(); scheduled = false; }
    };
  }, [subscribe, getSnapshot, initialOnly]);
  return useSyncExternalStore(batchedSubscribe, getSnapshot, getServerSnapshot);
}
