/**
 * The K19 local kernel (`cloudflare/router/dev/local`): router `/dev/*`, the
 * control object with the operator surface, the local tenant object, R2, the
 * rollout queue and the release builder, under `wrangler dev`. It serves the
 * deploy and operator routes; sockets for application tenants are the wave-2
 * packages', so run only the operator scenarios against it
 * (`--scenario operator-routes`), with `tenants: {}`.
 */
import { join } from 'node:path';
import { Backend } from '../backend.mjs';
import { REPO } from '../paths.mjs';

export class CloudflareKernelBackend extends Backend {
  constructor(options) {
    super(options);
    this.keys = new Map();
  }

  get capabilities() {
    return { restart: false, crash: false, operator: true };
  }

  async start() {
    const { startLocalKernel } = await import(join(REPO, 'cloudflare/router/dev/local/start.mjs'));
    this.kernel = await startLocalKernel({ state: join(this.workDir, 'state'), logs: this.workDir, deferMs: 300 });
    this.adminKey = this.kernel.adminKey;
    this.httpUrl = this.kernel.url;
    this.wsUrl = `${this.kernel.url.replace(/^http/, 'ws')}/ws`;
  }

  async stop() {
    await this.kernel?.stop();
  }

  async request(method, path, { key = this.adminKey, body } = {}) {
    const r = await fetch(this.httpUrl + path, { method, headers: { authorization: `Bearer ${key}`, ...(body ? { 'content-type': 'application/json' } : {}) }, body: body ? JSON.stringify(body) : undefined });
    const text = await r.text();
    let json;
    try {
      json = JSON.parse(text);
    } catch {
      json = { raw: text };
    }
    if (!r.ok) throw new Error(`${method} ${path} -> ${r.status}: ${text.slice(0, 300)}`);
    return json;
  }

  async createProject(name) {
    const r = await this.request('POST', '/dev/projects', { body: { name, databaseMode: 'multiTenant' } });
    this.keys.set(r.project.id, r.projectKey);
    return { projectId: r.project.id, projectKey: r.projectKey };
  }

  async deploy(project, manifest) {
    const before = await this.workerVersion();
    const r = await this.request('POST', '/dev/sync', { key: this.keys.get(project), body: { ...manifest, project } });
    await this.waitForWorkerVersion(before);
    return { ok: r.ok === true, functionCount: r.functionCount };
  }

  /** The live Worker version's id (the release builder's generated module). */
  async workerVersion() {
    try {
      return (await (await fetch(`${this.httpUrl}/_gonvex/k19/version`)).json()).version;
    } catch {
      return undefined;
    }
  }

  /**
   * Local only: the release builder swaps the Worker version shortly after a
   * sync answers, and `wrangler dev` restarts workerd, failing non-GET requests
   * in that window. Waits until a version other than `before` serves.
   */
  async waitForWorkerVersion(before, timeoutMs = 30_000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const now = await this.workerVersion();
      if (now !== undefined && now !== before) return now;
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
    throw new Error('the local kernel did not switch to the new Worker version');
  }

  async createTenant(project, tenantId, name) {
    const r = await this.request('POST', '/dev/tenants', { body: { projectId: project, id: tenantId, name } });
    return { tenantId: r.id };
  }

  async createMember() {
    throw new Error('the K19 local kernel does not seed Members (K06 tenant port is served by the wave-2 tenant object); use tenants: {}');
  }

  async readTable(project, tenantId, table) {
    const q = new URLSearchParams({ project, tenant: tenantId, limit: '1000' });
    return (await this.request('GET', `/dev/data/tables/${encodeURIComponent(table)}/rows?${q}`)).rows;
  }
}

export default CloudflareKernelBackend;
