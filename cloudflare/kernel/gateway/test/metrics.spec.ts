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

it("keeps exact completed buckets immutable across probes, forced reports and current spikes", () => {
  const clock = vi.spyOn(Date, "now");
  try {
    clock.mockReturnValue(1000); const m = new GatewayMetrics(); m.delivery(150);
    clock.mockReturnValue(6000); const first = m.snapshot(1, 1);
    expect(first.windowDeliveryMs).toBe(150); expect(first.currentPressure).toBe(0);
    clock.mockReturnValue(7000); m.delivery(900); m.rotate();
    const second = m.snapshot(1, 1);
    expect(second.deliveryMs).toBe(900); expect(second.currentPressure).toBe(1);
    expect(second.windowDeliveryMs).toBe(150); expect(second.windowSequence).toBe(first.windowSequence);
    expect(second.windowStart).toBe(first.windowStart); expect(second.windowEnd).toBe(first.windowEnd);
    second.windowDeliveryMs = 12345;
    expect(m.snapshot(1, 1).windowDeliveryMs).toBe(150);
    clock.mockReturnValue(16000); m.delivery(150);
    clock.mockReturnValue(21000); const gap = m.snapshot(1, 1);
    expect(gap.windowSequence).toBeGreaterThan(first.windowSequence! + 1);
    clock.mockReturnValue(2000); expect(m.snapshot(1, 1).windowInstance).toBeUndefined();
    clock.mockReturnValue(7000); expect(m.snapshot(1, 1).windowInstance).not.toBe(first.windowInstance);
  } finally { clock.mockRestore(); }
});

it("never assigns a newly supplied socket count to a past bucket", () => {
  const clock = vi.spyOn(Date, "now");
  try {
    clock.mockReturnValue(1000); const m = new GatewayMetrics();
    clock.mockReturnValue(8000); const late = m.snapshot(1, 125);
    expect(late.windowSocketSamples).toBe(0); // prior [1000,6000] had NO observation
    expect(late.windowSockets).toBe(0); // unknown sentinel, never evidence of emptiness
    expect(late.currentPressure).toBe(1);
    clock.mockReturnValue(9000); m.snapshot(1, 1);
    clock.mockReturnValue(11000); const next = m.snapshot(1, 125);
    expect(next.windowSocketSamples).toBe(2); expect(next.windowSockets).toBe(1);
    expect(next.currentPressure).toBe(1);
  } finally { clock.mockRestore(); }
});

it("requires real full sampled buckets and keeps later low counts out of earlier immutable groups", () => {
  const clock = vi.spyOn(Date, "now");
  try {
    clock.mockReturnValue(1000); const m = new GatewayMetrics(); m.snapshot(1, 125);
    clock.mockReturnValue(6000); const one = m.snapshot(1, 125);
    expect(one.windowSockets).toBe(125); expect(one.windowSocketSamples).toBe(1);
    clock.mockReturnValue(11000); const two = m.snapshot(1, 125);
    expect(two.windowStart).toBe(one.windowEnd); expect(two.windowEnd! - one.windowStart!).toBe(10_000);
    expect(two.windowSockets).toBe(125); expect(two.currentPressure).toBe(1);
    clock.mockReturnValue(11001); const low = m.snapshot(1, 1);
    expect(low.windowSockets).toBe(125); expect(low.currentPressure).toBe(0);
    clock.mockReturnValue(16000); const sealedLow = m.snapshot(1, 125);
    expect(sealedLow.windowSockets).toBe(1); expect(sealedLow.windowSocketSamples).toBe(2);
  } finally { clock.mockRestore(); }
});

for (const kind of ["frames", "bytes"] as const) it(`keeps ${kind} pressure across a report boundary without re-aging its source window`, () => {
  const clock = vi.spyOn(Date, "now");
  try {
    clock.mockReturnValue(1000); const m = new GatewayMetrics();
    const text = kind === "bytes" ? "x".repeat(512 * 1024) : "x";
    for (let i = 0; i < (kind === "bytes" ? 50 : 25_000); i++) m.sent(text);
    clock.mockReturnValue(6000); const first = m.snapshot(1, 1); m.rotate();
    expect(first.currentPressure).toBe(1);
    clock.mockReturnValue(10999); const repeated = m.snapshot(1, 1); m.rotate();
    expect(repeated.currentPressure).toBe(1);
    expect(repeated.windowSequence).toBe(first.windowSequence);
    expect(repeated.windowStart).toBe(first.windowStart);
    expect(repeated.windowEnd).toBe(first.windowEnd);
    clock.mockReturnValue(11000); const quiet = m.snapshot(1, 1);
    expect(quiet.currentPressure).toBe(0);
    expect(quiet.windowSequence).toBe(first.windowSequence! + 1);
    expect(quiet.windowFramesPerSecond).toBe(0);
    expect(quiet.windowBytesPerSecond).toBe(0);
  } finally { clock.mockRestore(); }
});

it("does not carry completed latency or socket pressure into the current reporting horizon", () => {
  const clock = vi.spyOn(Date, "now");
  try {
    clock.mockReturnValue(1000); const m = new GatewayMetrics();
    m.delivery(150, 150, 150); m.snapshot(1, 125);
    clock.mockReturnValue(6000); const current = m.snapshot(1, 1);
    expect(current.windowDeliveryMs).toBe(150);
    expect(current.windowSockets).toBe(125);
    expect(current.currentPressure).toBe(0);
  } finally { clock.mockRestore(); }
});
