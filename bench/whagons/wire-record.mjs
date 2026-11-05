/** Diagnostic only: records every WebSocket frame a load client sends and
 * receives, so the thin client can be compared with the real SDK.
 * Enabled with LOAD_WIRE_RECORD=/absolute/path.jsonl. Large payloads keep
 * their structure (types, keys, counts), not their row contents. */
import { appendFileSync } from 'node:fs';

const authorities=new WeakMap();
export const transportReplicaAuthority=client=>authorities.get(client.socket);

const shape = (value, depth = 0) => {
  if (Array.isArray(value)) return depth > 3 ? `[${value.length}]` : value.length > 3 ? [`len:${value.length}`, shape(value[0], depth + 1)] : value.map(v => shape(v, depth + 1));
  if (value && typeof value === 'object') {
    if (depth > 3) return '{…}';
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, shape(v, depth + 1)]));
  }
  if (typeof value === 'string' && value.length > 80) return `str:${value.length}`;
  return value;
};

export function installWireRecorder(file, { full = false, observe } = {}) {
  if (!file && !observe) return;
  const Native = globalThis.WebSocket;
  let next = 0;
  const write = (record) => { if(file) appendFileSync(file, JSON.stringify(record) + '\n'); };
  const frame = (socket, direction, data, transport) => {
    try {
      const m=JSON.parse(typeof data==='string'?data:new TextDecoder().decode(data));
      if((direction==='send'&&m.type==='auth')||(direction==='recv'&&(m.type==='auth.error'||m.type==='auth.result')))authorities.delete(transport);
      if(direction==='recv'&&m.type==='auth.result'&&m.result?.replica){const d=m.result.replica;authorities.set(transport,Object.freeze({protocolVersion:d.protocolVersion,scope:d.scope,visibilityScope:d.visibilityScope,epoch:d.epoch}));}
      observe?.(socket,direction,m);
    } catch {}
  };
  const describe = (data) => {
    if (typeof data === 'string') { try { const m = JSON.parse(data); return { kind: 'text', bytes: data.length, message: full ? m : shape(m) }; } catch { return { kind: 'text', bytes: data.length }; } }
    const bytes = data.byteLength ?? data.length;
    try { const m = JSON.parse(new TextDecoder().decode(data)); return { kind: 'binary', bytes, message: full ? m : shape(m) }; } catch { return { kind: 'binary', bytes }; }
  };
  class RecordedWebSocket extends Native {
    constructor(url, protocols) {
      super(url, protocols);
      const socket = ++next;
      write({ at: Date.now(), socket, event: 'open-request', url: String(url), protocols: protocols ?? null });
      this.addEventListener('open', () => write({ at: Date.now(), socket, event: 'open', extensions: this.extensions, protocol: this.protocol }));
      this.addEventListener('close', (e) => {authorities.delete(this);write({ at: Date.now(), socket, event: 'close', code: e.code, reason: e.reason });});
      this.addEventListener('message', (e) => { frame(socket,'recv',e.data,this); if(file)write({ at: Date.now(), socket, event: 'recv', ...describe(e.data) }); });
      const send = this.send.bind(this);
      this.send = (data) => { frame(socket,'send',data,this); if(file)write({ at: Date.now(), socket, event: 'send', ...describe(data) }); return send(data); };
    }
  }
  globalThis.WebSocket = RecordedWebSocket;
}
