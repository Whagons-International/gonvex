#!/usr/bin/env node
/**
 * One gateway hop, measured two ways against a running kernel (local
 * `wrangler dev` or a `gonvex-cf-dev-*` deploy) with the stub routes enabled:
 *
 *  1. Client-observed: the round trip of a `query.call` the gateway answers
 *     itself (unauthenticated socket, no hop) against the same frame on a
 *     tenant socket, which the gateway forwards to the tenant object and back
 *     (one gateway -> tenant RPC). The difference is one hop as a client sees it.
 *  2. In-object: `GET /_stub/session/hop` times n sequential RPCs inside the
 *     objects, gateway -> tenant and tenant -> gateway (the commit delivery hop).
 *
 * Usage: node hop.mjs <base-url> <stub-admin-key> [rounds=300]
 * Prints one JSON object. Uses only its own project, tenant and member.
 */
const [base, key, roundsArg] = process.argv.slice(2);
if (!base || !key) { console.error('usage: node hop.mjs <base-url> <stub-admin-key> [rounds]'); process.exit(2); }
const rounds = Number(roundsArg ?? 300);
const wsBase = base.replace(/^http/, 'ws');

async function stub(method, route, body) {
  const r = await fetch(`${base}/_stub/session/${route}`, { method, headers: { authorization: `Bearer ${key}`, ...(body ? { 'content-type': 'application/json' } : {}) }, body: body ? JSON.stringify(body) : undefined });
  if (!r.ok) throw new Error(`${method} ${route}: ${r.status} ${await r.text()}`);
  return r.json();
}

function open() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`${wsBase}/ws`);
    const inbox = [], waiters = [];
    ws.onmessage = (e) => { inbox.push(JSON.parse(String(e.data))); waiters.splice(0).forEach(w => w()); };
    ws.onerror = () => reject(new Error('socket failed'));
    ws.onopen = () => resolve({
      ws,
      send: (f) => ws.send(JSON.stringify(f)),
      async next(match) {
        for (;;) {
          const i = inbox.findIndex(match);
          if (i >= 0) return inbox.splice(i, 1)[0];
          await new Promise(r => waiters.push(r));
        }
      },
    });
  });
}

const stats = (xs) => {
  const s = [...xs].sort((a, b) => a - b), q = (p) => s[Math.min(s.length - 1, Math.floor(p * s.length))];
  const r = (v) => Math.round(v * 100) / 100;
  return { n: s.length, min: r(s[0]), p50: r(q(0.5)), p90: r(q(0.9)), p99: r(q(0.99)), max: r(s.at(-1)) };
};

async function rtts(client, label) {
  const out = [];
  for (let i = 0; i < rounds + 20; i++) {
    const id = `${label}-${i}`, t = performance.now();
    client.send({ type: 'query.call', id, path: 'bench:noop', args: {} });
    await client.next(m => m.id === id);
    if (i >= 20) out.push(performance.now() - t); // first 20 warm the path
  }
  return out;
}

const suffix = Date.now().toString(36);
const { projectId } = await stub('POST', 'projects', { name: `hop-${suffix}` });
await stub('POST', 'sync', { project: projectId, module: { hash: 'hop' } });
const tenant = `hop-${suffix}`;
await stub('POST', 'tenants', { projectId, id: tenant, name: 'Hop' });
await stub('POST', 'members', { projectId, tenantId: tenant, email: 'hop@k05.invalid', password: 'hop-password', name: 'hop' });

const control = await open();
control.send({ type: 'auth', id: 'c', controlOnly: true, project: projectId, clientContract: 1 });
await control.next(m => m.id === 'c');
control.send({ type: 'action.call', id: 'login', path: 'control.auth.passwordLogin', scope: 'control', args: { email: 'hop@k05.invalid', password: 'hop-password' }, idempotencyKey: 'login' });
const token = (await control.next(m => m.id === 'login')).result.accessToken;

const noHop = await open();
const hop = await open();
hop.send({ type: 'auth', id: 'a', project: projectId, tenant, token, clientContract: 1 });
const auth = await hop.next(m => m.id === 'a');
if (auth.type !== 'auth.result') throw new Error(`auth failed: ${JSON.stringify(auth)}`);
const [gateway] = await stub('GET', `gateways?tenant=${tenant}`);

const gatewayOnly = await rtts(noHop, 'g');
const viaTenant = await rtts(hop, 't');
const inObject = await stub('GET', `hop?n=${rounds}&tenant=${tenant}&gateway=${gateway}`);
for (const c of [control, noHop, hop]) c.ws.close(1000);

const a = stats(gatewayOnly), b = stats(viaTenant);
console.log(JSON.stringify({
  base: base.replace(/\/\/[^.]+\./, '//…'), rounds, gateway,
  clientRtt: { gatewayOnly: a, viaTenant: b, hopP50: Math.round((b.p50 - a.p50) * 100) / 100, hopP90: Math.round((b.p90 - a.p90) * 100) / 100 },
  inObject: { gatewayToTenant: inObject.gatewayToTenant, tenantToGateway: inObject.tenantToGateway, colo: inObject.colo },
}, null, 1));
process.exit(0);
