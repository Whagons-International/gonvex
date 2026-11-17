import { Refusal } from './binding.mjs';

/** One owned worker realm. SDK direct socket.send, reconnect and fetch all cross this boundary.
 * Not a sandbox for untrusted modules; reviewed SDK/refs dependency closure is required. */
export function installEffects({ WebSocket: NativeSocket, fetch: nativeFetch, socketUrl, httpUrls, deadline, budget, signal, now = () => performance.now() }) {
  let stopped = false, opened = 0, sent = 0, received = 0, frames = 0, receivedFrames = 0, nextOpen = 0;
  const sockets = new Set(), requests = new Set();
  const check = () => { if (now() >= deadline || signal?.aborted) stopped = true; if (stopped) throw new Refusal('STOPPED', 'effect-lifetime'); };
  const bytes = value => typeof value === 'string' ? Buffer.byteLength(value) : value?.byteLength ?? value?.size ?? 0;
  const account = (direction, count) => {
    check(); if (direction === 'send') sent += count; else received += count;
    if (sent > budget.maxSentBytes || received > budget.maxReceivedBytes) { stopped = true; throw new Refusal('STOPPED', 'wire-byte-budget'); }
  };
  class Socket extends NativeSocket {
    constructor(url, protocols) {
      check(); if (String(url) !== socketUrl || now() < nextOpen || sockets.size >= budget.maxConnections) throw new Refusal('STOPPED', 'socket-admission');
      nextOpen = now() + 1000 / budget.opensPerSecond;
      super(url, protocols); opened++; sockets.add(this);
      this.addEventListener('close', () => sockets.delete(this), { once: true });
      this.addEventListener('message', e => { try { receivedFrames++; account('receive', bytes(e.data)); } catch { this.close(); } });
    }
    send(data) { account('send', bytes(data)); frames++; return super.send(data); }
  }
  const fetch = async (url, options = {}) => {
    check(); const target = String(url instanceof Request ? url.url : url);
    if (!httpUrls.includes(target)) throw new Refusal('STOPPED', 'fetch-origin');
    const controller = new AbortController(); requests.add(controller);
    try { return await nativeFetch(url, { ...options, signal: AbortSignal.any([controller.signal, ...(options.signal ? [options.signal] : [])]) }); }
    finally { requests.delete(controller); }
  };
  return { WebSocket: Socket, fetch, check,
    limitLifetime(ms) { check(); deadline = Math.min(deadline, now() + ms); },
    async settle(ms) {
      const end = performance.now() + ms;
      while (sockets.size && performance.now() < end) await new Promise(resolve => setTimeout(resolve, Math.min(20, end - performance.now())));
      return { localTransportObjectsRemaining: sockets.size, remoteQuiescence: 'UNKNOWN' };
    },
    snapshot: () => ({ opened, connectedTransports: sockets.size, sentBytes: sent, receivedBytes: received, sentFrames: frames, receivedFrames }),
    stop() { stopped = true; for (const c of requests) c.abort(); for (const s of sockets) { try { s.close(); } catch {} } },
  };
}
