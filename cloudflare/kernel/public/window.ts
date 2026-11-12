/**
 * A fixed 60 s call window (`Window`, public_functions.rs:37-62). Plain data so
 * it can live in a socket attachment (the per-connection window) or in the
 * address limiter's memory (the per-address window).
 */
import { CALL_WINDOW_MS } from "./config";

export interface CallWindow {
  /** When the window started (ms). A connection's first window starts when the socket opens. */
  startedAt: number;
  count: number;
}

export const newWindow = (now: number): CallWindow => ({ startedAt: now, count: 0 });

/**
 * `Window::allow`: a window older than 60 s restarts at `now`; the call is
 * refused when `cost` exceeds what is left, and charged otherwise.
 */
export function allowCalls(window: CallWindow, cost: number, limit: number, now: number): boolean {
  if (now - window.startedAt >= CALL_WINDOW_MS) {
    window.startedAt = now;
    window.count = 0;
  }
  if (cost > Math.max(0, limit - window.count)) return false;
  window.count += cost;
  return true;
}
