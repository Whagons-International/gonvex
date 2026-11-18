// HTTP/SDK fixture driver for the ONE serving kernel, locally or deployed.
// Inherits only the reference adapter's HTTP fixture commands; no Rust process
// or package harness is used by this backend.
import { RustBackend, CONFORMANCE_SECRETS, firebaseEmulatorEnv } from './rust.mjs';
import { startLocalAssembly } from '../../../kernel/assembly/local.mjs';
import { setTimeout as delay } from 'node:timers/promises';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { postDevSync } from '../../../../packages/gonvex/src/sync-request.ts';
import { registerAddressHeaders } from '../client-address.mjs';
export class CloudflareAssemblyBackend extends RustBackend {
  constructor(options) { super({ ...options, binary: process.execPath }); }
  get capabilities() {
    // 0062: one telemetry holder only under the verified static placement (captureEvidence checks gw-0).
    const oneHolder = process.env.GONVEX_ASSEMBLY_PLACEMENT === 'static-one-holder';
    return { ...super.capabilities, orphanReveal: true, tenantRestart: true, receiptExpiry: true, singleTelemetryHolder: oneHolder, multiHolderTelemetry: !oneHolder };
  }
  async start() {
    if (process.env.GONVEX_ASSEMBLY_URL) {
      this.httpUrl = process.env.GONVEX_ASSEMBLY_URL.replace(/\/$/, '');
      this.adminKey = process.env.GONVEX_ASSEMBLY_KEY;
      if (!this.adminKey) throw new Error('GONVEX_ASSEMBLY_KEY is required');
    } else {
      this.runtime = await startLocalAssembly({ state: this.workDir, port: this.port, placement: process.env.GONVEX_ASSEMBLY_PLACEMENT ?? 'automatic', vars: {
        GONVEX_ADMIN_KEY: this.adminKey, ...CONFORMANCE_SECRETS, ...firebaseEmulatorEnv(),
        // `ctx.storage` on local R2, as the deployed assembly and the Rust reference's S3 stub have storage.
        GONVEX_STORAGE_SECRET: 'conformance-local-storage-secret', GONVEX_STORAGE_BUCKET: 'conformance',
        ...(this.servicePrincipals ? { GONVEX_SERVICE_PRINCIPALS: this.servicePrincipals } : {}),
      } });
      this.httpUrl = this.runtime.url;
      this.port = Number(new URL(this.httpUrl).port);
    }
    this.wsUrl = this.httpUrl.replace(/^http/, 'ws') + '/ws';
    // The edge refuses a client-supplied CF-Connecting-IP; the fixture key gates the dev/test override.
    const fixtureKey = process.env.GONVEX_ASSEMBLY_URL ? (process.env.GONVEX_ASSEMBLY_FIXTURE_KEY || this.adminKey) : this.adminKey;
    registerAddressHeaders(this.wsUrl, (address) => ({ 'x-gonvex-fixture-key': fixtureKey, 'x-gonvex-fixture-client-ip': address }));
  }
  async stop() { await this.runtime?.stop(); this.runtime = undefined; }
  inspectHolders(project) { return this.request('GET', `/__assembly/holders?${new URLSearchParams({project})}`); }
  async captureEvidence({ scenario, project }) {
    const holders = await this.inspectHolders(project), identity = await this.request('GET', '/__assembly/version');
    const configs = [];
    if (this.runtime) for (const kind of ['kernel', 'runtime', 'actions', 'sandbox']) {
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
  async createProject(name) { const r = await super.createProject(name); this.scenarioProject = r.projectId; this.scenarioTenants = []; return r; }
  async createTenant(project, tenantId, name) { const r = await super.createTenant(project, tenantId, name); if (project === this.scenarioProject) this.scenarioTenants.push(tenantId); return r; }
  /**
   * Rust restarts its process (`crash` = SIGKILL). On a deployed Worker the same observable restart is the
   * dev/test fixture abort (X03): every gateway holder with a socket of the scenario's project and every one of
   * its tenants is reset. Sockets drop abruptly, nothing in memory survives, durable storage stays.
   */
  async restart({ crash = false } = {}) {
    if (!process.env.GONVEX_ASSEMBLY_URL) { await this.stop(); this.restarts++; await this.start(); return; }
    const project = this.scenarioProject;
    if (!project) throw new Error('remote assembly restart needs the scenario project');
    const { holders } = await this.inspectHolders(project);
    for (const { holder } of holders) await this.request('POST', '/__assembly/gateway-restart', { body: { holder } });
    for (const tenant of this.scenarioTenants ?? []) await this.restartTenant(project, tenant);
    this.restarts++;
    this.lastRestart = { crash, holders: holders.length, tenants: this.scenarioTenants?.length ?? 0 };
  }
  /** Rust and local workerd restart with the new `GONVEX_SERVICE_PRINCIPALS`; a deployed Worker takes the dev/test fixture override, then restarts. */
  async configureServicePrincipals(principals) {
    if (!process.env.GONVEX_ASSEMBLY_URL) return super.configureServicePrincipals(principals);
    await this.request('POST', '/__assembly/service-principals', { body: { principals: JSON.stringify(principals) } });
    await this.restart();
  }
  async deploy(project, manifest) {
    const reply = await postDevSync(this.httpUrl + '/dev/sync', {headers:{authorization:`Bearer ${this.keys.get(project)}`,'content-type':'application/json','x-gonvex-project-id':project},body:JSON.stringify({...manifest,project})});
    if (!reply.ok) throw new Error(`POST /dev/sync -> ${reply.status}: ${(await reply.text()).slice(0,1500)}`);
    const result=await reply.json(),outcome={ok:result.ok===true,functionCount:result.functionCount};
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
  /** cleanup.mjs: the ordinary dev delete path; it also deletes the project's runtime-created tenants. */
  deleteProject(project) { return this.request('DELETE', `/dev/projects/${encodeURIComponent(project)}`); }
  /** cleanup.mjs: the open whole-catalog deployment fence, or null. */
  async openDeployment() { return (await this.request('GET', '/_gonvex/k19/topology')).deployment ?? null; }
  /**
   * operator-routes creates an admin-operator account token ('ci') and lists them all, revoked ones included
   * (as Rust does). A persistent deployment keeps every run's token, so around that scenario the adapter
   * revokes the operator's active 'ci' tokens through the normal DELETE /dev/auth/tokens/{id} path and drops
   * the revoked ones through the dev/test fixture (X03). Never compared.
   */
  async beforeScenario(name) { if (name === 'operator-routes') await this.cleanOperatorTokens(); }
  async afterScenario(name) { if (name === 'operator-routes') await this.cleanOperatorTokens(); }
  async cleanOperatorTokens() {
    const { tokens = [] } = await this.request('GET', '/dev/auth/tokens');
    for (const t of tokens) if (t.name === 'ci' && !t.revokedAt) await this.request('DELETE', `/dev/auth/tokens/${encodeURIComponent(t.id)}`);
    return this.request('POST', '/__assembly/purge-revoked-tokens', { body: {} });
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
