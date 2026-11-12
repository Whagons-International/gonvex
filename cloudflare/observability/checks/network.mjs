import { lookup } from 'node:dns/promises';
import tls from 'node:tls';
import { requireCheck } from './contract.mjs';

/** No default endpoint. HTTPS allowlist is explicit; redirects cannot escape it. No auth headers. */
export function networkAdapter({ origin, allowedOrigins, bootstrapMarker }) {
  const url = new URL(origin);
  requireCheck(url.protocol === 'https:' && !url.username && !url.password && !url.search && !url.hash && url.pathname === '/' && allowedOrigins.includes(url.origin), 'target-blocked', 'blocked');
  requireCheck(typeof bootstrapMarker === 'string' && bootstrapMarker.length > 0 && bootstrapMarker.length < 128, 'bootstrap-marker-required', 'blocked');
  return {
    async dns(c) {
      const answers = await lookup(url.hostname, { all:true });
      requireCheck(!c.signal.aborted && answers.length > 0, 'dns');
    },
    async tls(c) {
      await new Promise((resolve, reject) => {
        const socket = tls.connect({ host:url.hostname, port:Number(url.port || 443), servername:url.hostname, rejectUnauthorized:true, signal:c.signal });
        socket.once('secureConnect', () => { const ok = socket.authorized; socket.destroy(); ok ? resolve() : reject(new Error('tls')); });
        socket.once('error', reject);
      });
    },
    async http(c) {
      const response = await fetch(url, { redirect:'error', signal:c.signal });
      try { requireCheck(response.status === 200, 'http'); } finally { await response.body?.cancel(); }
    },
    async bootstrap(c) {
      const response = await fetch(url, { redirect:'error', signal:c.signal });
      requireCheck(response.status === 200, 'bootstrap-http');
      // Streaming cap: no unbounded text() allocation from an unhealthy endpoint.
      const reader = response.body.getReader(); let bytes = 0, text = '';
      try {
        while (true) {
          const { value, done } = await reader.read(); if (done) break;
          bytes += value.byteLength; requireCheck(bytes <= 262144, 'bootstrap-byte-cap');
          text += new TextDecoder().decode(value);
        }
      } finally { await reader.cancel(); }
      requireCheck(text.includes(bootstrapMarker), 'bootstrap-marker');
      // HTML bootstrap marker is not proof JS initialized; browser-bootstrap is a distinct check.
    },
  };
}
