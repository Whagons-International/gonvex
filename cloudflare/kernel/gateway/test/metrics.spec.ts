import { expect, it, vi } from "vitest";
import { GatewayMetrics } from "../metrics";

it("counts UTF8 fan-out and preserves the completed pressure window across admission probes", () => {
  const clock = vi.spyOn(Date, "now");
  try {
    clock.mockReturnValue(1000);
    const m = new GatewayMetrics();
    for (let i = 0; i < 10; i++) m.sent("é");
    m.delivery(120, 130, 140);
    clock.mockReturnValue(6000);
    expect(m.snapshot(1, 2)).toMatchObject({ framesPerSecond: 2, bytesPerSecond: 4, deliveryMs: 120, queueAgeMs: 130, rpcMs: 140 });
    m.rotate();
    expect(m.snapshot(1, 2).rpcMs).toBe(140);
    clock.mockReturnValue(17001);
    expect(m.snapshot(1, 2).rpcMs).toBe(0);
  } finally { clock.mockRestore(); }
});
