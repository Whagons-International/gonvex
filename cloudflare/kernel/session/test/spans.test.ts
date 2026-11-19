/**
 * X13i: dev spans are opt-in and time-only. Off, nothing is logged; on, the CallOrigin deferral
 * line carries the waited duration and the RPC method name only.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { CallOrigin } from "../placement/origin";
import { setSpanTiming, span, spanTimingFromEnv } from "../../delivery/spans";

afterEach(() => { setSpanTiming(false); vi.restoreAllMocks(); });

function deferredLaunch(): { waited: number } {
  let now = 1_000;
  const origin = new CallOrigin(() => {}, { now: () => now, later: () => () => {} });
  const peer = origin.enter("peer");
  void origin.run(() => "ok", "dispatch");
  origin.exit(peer);
  now += 37;
  origin.exit(origin.enter("fresh"));
  return { waited: 37 };
}

describe("X13i spans", () => {
  it("are off by default and read only GONVEX_SPAN_TIMING=1", () => {
    const log = vi.spyOn(console, "log").mockImplementation(() => {});
    span("anything", { ms: 1 });
    deferredLaunch();
    expect(log).not.toHaveBeenCalled();
    expect(spanTimingFromEnv({ GONVEX_SPAN_TIMING: "1" })).toBe(true);
    expect(spanTimingFromEnv({ GONVEX_SPAN_TIMING: "true" })).toBe(false);
    expect(spanTimingFromEnv(undefined)).toBe(false);
  });

  it("log a deferred gateway call's wait for a fresh event, with the method name only", () => {
    setSpanTiming(true);
    const log = vi.spyOn(console, "log").mockImplementation(() => {});
    const { waited } = deferredLaunch();
    expect(log).toHaveBeenCalledWith("span: gateway.deferred", { call: "dispatch", waitedMs: waited });
  });

  it("do not log a call launched at once", () => {
    setSpanTiming(true);
    const log = vi.spyOn(console, "log").mockImplementation(() => {});
    const origin = new CallOrigin(() => {}, { later: () => () => {} });
    const fresh = origin.enter("fresh");
    void origin.run(() => "ok", "dispatch");
    origin.exit(fresh);
    expect(log).not.toHaveBeenCalled();
  });
});
