// HTTP/SDK fixture driver for the ONE serving kernel, locally or deployed.
// Inherits only the reference adapter's HTTP fixture commands; no Rust process
// or package harness is used by this backend.
import { RustBackend, CONFORMANCE_SECRETS, firebaseEmulatorEnv } from './rust.mjs';
import { startLocalAssembly } from '../../../kernel/assembly/local.mjs';
import { setTimeout as delay } from 'node:timers/promises';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
export class CloudflareAssemblyBackend extends RustBackend {
  constructor(options) { super({ ...options, binary: process.execPath }); }
  get capabilities() { return { ...super.capabilities, orphanReveal: true, tenantRestart: true, receiptExpiry: true }; }
  async start() {
    if (process.env.GONVEX_ASSEMBLY_URL) {
      this.httpUrl = process.env.GONVEX_ASSEMBLY_URL.replace(/\/$/, '');
      this.adminKey = process.env.GONVEX_ASSEMBLY_KEY;
      if (!this.adminKey) throw new Error('GONVEX_ASSEMBLY_KEY is required');
    } else {
      this.runtime = await startLocalAssembly({ state: this.workDir, port: this.port, placement: process.env.GONVEX_ASSEMBLY_PLACEMENT ?? 'automatic', vars: {
        GONVEX_ADMIN_KEY: this.adminKey, ...CONFORMANCE_SECRETS, ...firebaseEmulatorEnv(),
        ...(this.servicePrincipals ? { GONVEX_SERVICE_PRINCIPALS: this.servicePrincipals } : {}),
      } });
      this.httpUrl = this.runtime.url;
      this.port = Number(new URL(this.httpUrl).port);
    }
    this.wsUrl = this.httpUrl.replace(/^http/, 'ws') + '/ws';
  }
  async stop() { await this.runtime?.stop(); this.runtime = undefined; }
  inspectHolders(project) { return this.request('GET', `/__assembly/holders?${new URLSearchParams({project})}`); }
  async captureEvidence({ scenario, project }) {
    const holders = await this.inspectHolders(project), identity = await this.request('GET', '/__assembly/version');
    const configs = [];
    if (this.runtime) for (const kind of ['kernel', 'actions', 'sandbox']) {
      const bytes = await readFile(join(this.workDir, `${kind}.json`)), config = JSON.parse(bytes);
      configs.push({kind, sha256:createHash('sha256').update(bytes).digest('hex'), name:config.name, compatibilityDate:config.compatibility_date,
        bindings:config.durable_objects?.bindings, services:config.services, limits:config.limits, shards:config.vars.GONVEX_GATEWAY_SHARDS ?? null});
    }
    const directory = join(this.workDir, 'evidence'); await mkdir(directory, {recursive:true,mode:0o700});
    await writeFile(join(directory, `${scenario}.json`), JSON.stringify({scenario, project, identity, configs, ...holders}, null, 2), {mode:0o600});
    if (process.env.GONVEX_ASSEMBLY_PLACEMENT === 'static-one-holder' && scenario === 'error-telemetry') {
      if (holders.placement !== 'static' || holders.holders.length !== 1 || holders.holders[0].holder !== 'gw-0') throw new Error('canonical telemetry did not use exactly the actual static gw-0 holder');
    }
  }
  async restart() {
    if (process.env.GONVEX_ASSEMBLY_URL) throw new Error('remote assembly restart must deploy a recorded real Worker version');
    await this.stop(); this.restarts++; await this.start();
  }
  async deploy(project, manifest) {
    const outcome = await super.deploy(project, manifest);
    const deadline = Date.now() + 120_000;
    for (;;) {
      try {
        const identity = await this.request('GET', '/__assembly/version');
        if (identity.releases?.[project] === manifest.module.hash) return outcome;
      } catch {}
      if (Date.now() > deadline) throw new Error('serving Worker did not activate the deployed artifact');
      await delay(100);
    }
  }
  resetEpoch(_project, tenant, epoch) { return this.request('POST', '/__assembly/epoch', { body: { tenant, epoch } }); }
  restartTenant(_project, tenant) { return this.request('POST', '/__assembly/tenant-restart', { body: { tenant } }); }
  async readChangeLog(_project, tenant) {
    const changes = [], transactions = []; let clock;
    for (let offset = 0; ; offset += 1000) {
      const page = await this.request('GET', `/__assembly/changelog?${new URLSearchParams({tenant,offset:String(offset)})}`);
      clock = page.clock; changes.push(...page.changes); transactions.push(...page.transactions);
      if (page.changes.length < 1000 && page.transactions.length < 1000) return { clock, changes, transactions };
    }
  }
  pruneReceipts(_project, tenant, now, offlineMaxAgeMs) { return this.request('POST', '/__assembly/receipt-prune', { body: { tenant, now, offlineMaxAgeMs } }); }
  expireReceipts(project, tenant, { now, offlineMaxAgeMs }) { return this.pruneReceipts(project, tenant, now, offlineMaxAgeMs); }
}
export default CloudflareAssemblyBackend;
