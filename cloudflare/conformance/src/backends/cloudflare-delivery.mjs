/**
 * The kernel with replica delivery (K10) on local workerd: the K05 socket path
 * (router, gateways, the control stand-in) and the K10 tenant object, which is
 * K07's Reducer path plus Replica Collections, from
 * `cloudflare/kernel/delivery/harness/wrangler.jsonc`.
 *
 * It is the K07 adapter (`cloudflare-reducer`) pointed at the K10 harness: the
 * fixture bundles are built with the CLI and written by K07's generator, then
 * `wrangler dev` starts. `restart({ crash })` kills workerd and starts it again
 * on the same port and state directory.
 */
import { join } from 'node:path';
import { CloudflareReducerBackend } from './cloudflare-reducer.mjs';
import { REPO } from '../paths.mjs';

export class CloudflareDeliveryBackend extends CloudflareReducerBackend {
  get harness() { return join(REPO, 'cloudflare/kernel/delivery/harness'); }

  /**
   * `orphanReveal`: intended behavior IB5 (0032), which Rust does not have.
   * `epochReset`: `resetEpoch` below. `tenantRestart`: the tenant object alone
   * restarts while gateways keep their sockets (Rust has no such object).
   */
  get capabilities() {
    // A deployed Worker (GONVEX_CONFORMANCE_CF_URL, `kernel/delivery/harness/platform.mjs`) cannot be restarted from here.
    const remote = !!process.env.GONVEX_CONFORMANCE_CF_URL;
    return { ...super.capabilities, restart: !remote, crash: !remote, orphanReveal: true, epochReset: true, tenantRestart: true };
  }

  async start() {
    if (!process.env.GONVEX_CONFORMANCE_CF_URL) return super.start();
    this.httpUrl = process.env.GONVEX_CONFORMANCE_CF_URL.replace(/\/$/, '');
    this.wsUrl = this.httpUrl.replace(/^http/, 'ws') + '/ws';
    this.adminKey = process.env.GONVEX_CONFORMANCE_CF_KEY;
    if (!this.adminKey) throw new Error('GONVEX_CONFORMANCE_CF_KEY is required with GONVEX_CONFORMANCE_CF_URL');
  }

  async stop() {
    if (!process.env.GONVEX_CONFORMANCE_CF_URL) return super.stop();
  }

  async post(route, body) {
    const r = await fetch(`${this.httpUrl}${route}`, { method: 'POST', headers: { authorization: `Bearer ${this.adminKey}`, 'content-type': 'application/json' }, body: JSON.stringify(body) });
    if (!r.ok) throw new Error(`POST ${route} -> ${r.status}: ${(await r.text()).slice(0, 300)}`);
    return r.json();
  }

  /** A new replica epoch for the tenant database (as the Rust adapter writes it into the file). */
  resetEpoch(project, tenantId, epoch) { return this.post('/_stub/delivery/epoch', { tenant: tenantId, epoch }); }

  /** Restarts the tenant object only (its memory is lost; sockets stay on their gateways). */
  restartTenant(project, tenantId) { return this.post('/_stub/delivery/restart', { tenant: tenantId }); }

  /** The engine's counters for a tenant (opens, commits, payloads, statements, rows read). */
  async deliveryStats(tenantId) {
    const r = await fetch(`${this.httpUrl}/_stub/delivery/stats?tenant=${encodeURIComponent(tenantId)}`, { headers: { authorization: `Bearer ${this.adminKey}` } });
    if (!r.ok) throw new Error(`GET /_stub/delivery/stats -> ${r.status}: ${(await r.text()).slice(0, 300)}`);
    return r.json();
  }
}
