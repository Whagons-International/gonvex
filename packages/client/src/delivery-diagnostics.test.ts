import { afterEach, describe, expect, it, vi } from 'vitest';
import { DeliveryDiagnostics } from './delivery-diagnostics';

afterEach(() => vi.unstubAllGlobals());

describe('delivery diagnostics', () => {
  it('separates handler, decoding and processing delays on the monotonic clock', () => {
    let now = 100;
    vi.stubGlobal('performance', { now: () => now });
    vi.stubGlobal('PerformanceObserver', undefined);
    vi.stubGlobal('document', { visibilityState: 'hidden' });
    const diagnostics = new DeliveryDiagnostics();
    diagnostics.begin(75);
    now = 108;
    diagnostics.decodedMessage();
    now = 120;
    expect(diagnostics.snapshot(128)).toMatchObject({
      messageHandlerDelayMs: 25, messageDecodeMs: 8, messageProcessingMs: 12,
      longTaskSupported: false, visibilityState: 'hidden', bufferedAmount: 128,
    });
    diagnostics.begin(1788715158149);
    expect(diagnostics.snapshot(0).messageHandlerDelayMs).toBeUndefined();
  });

  it('drains pending long tasks, bounds retention and disconnects the observer', () => {
    let now = 1000;
    const disconnect = vi.fn();
    let pending = Array.from({ length: 40 }, (_, i) => ({ startTime: i, duration: 100 }));
    vi.stubGlobal('performance', { now: () => now });
    vi.stubGlobal('PerformanceObserver', class {
      static supportedEntryTypes = ['longtask'];
      observe() {}
      takeRecords() { const entries = pending; pending = []; return entries; }
      disconnect = disconnect;
    });
    const diagnostics = new DeliveryDiagnostics();
    diagnostics.begin(990);
    expect(diagnostics.snapshot(0)).toMatchObject({ longTaskSupported: true, longTaskCount: 32, longTaskTotalMs: 3200, longTaskMaxMs: 100 });
    now = 32_000;
    expect(diagnostics.snapshot(0).longTaskCount).toBe(0);
    diagnostics.close();
    expect(disconnect).toHaveBeenCalledOnce();
  });
});
