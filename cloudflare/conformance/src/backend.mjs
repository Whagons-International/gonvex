/**
 * Backend adapter interface.
 *
 * A backend is anything that serves the Gonvex contract: the Rust runtime in
 * this repo today, the Cloudflare kernel later. The harness talks to it in two
 * ways only:
 *
 *  1. Clients (real SDK, thin client, raw frames) over `wsUrl`. This is the
 *     contract under test; adapters never touch it.
 *  2. This adapter, for the out-of-band work a test needs: process lifecycle,
 *     seeding, deploying a bundle and reading tables back.
 *
 * Seeding goes through the adapter because the routes that create projects,
 * tenants and password Members for tests are not part of the contract. The
 * session token, in contrast, always comes from `control.auth.passwordLogin`
 * over the socket (see runner.mjs), because that is contract.
 *
 * To plug in a backend, subclass Backend, implement every method marked
 * "required", and register it in backends/index.mjs. README.md walks through it.
 *
 * Every identifier the adapter returns (project id, account id, member id) is
 * registered with the run as an alias, so frames and observations show
 * `<member:alice>` instead of a backend-specific value.
 */
export class Backend {
  /** @param {{ name: string, workDir: string, log?: (line: string) => void }} options */
  constructor({ name, workDir, log = () => {} }) {
    /** Label used in diffs, e.g. "rust-a". */
    this.name = name;
    this.workDir = workDir;
    this.log = log;
    /** ws://host/ws */
    this.wsUrl = undefined;
    /** http://host */
    this.httpUrl = undefined;
  }

  /** What this backend can do; scenarios that need a missing capability are skipped and reported. */
  get capabilities() {
    return { restart: false, crash: false };
  }

  /** required. Starts with empty state. Resolves once the backend serves requests. */
  async start() { throw new Error('start() not implemented'); }

  /** required. Stops and releases everything start() created. */
  async stop() { throw new Error('stop() not implemented'); }

  /**
   * Optional (`capabilities.restart` / `capabilities.crash`). Restarts on the
   * same durable state. `crash: true` must lose everything not yet durable:
   * SIGKILL for a process, an abrupt eviction for a Durable Object.
   */
  async restart({ crash = false } = {}) { throw new Error(`restart(crash=${crash}) not supported by ${this.name}`); }

  /**
   * required. Creates an empty project.
   * @returns {Promise<{ projectId: string, projectKey: string }>}
   */
  async createProject(name) { throw new Error('createProject() not implemented'); }

  /**
   * required. Deploys a module bundle exactly as `POST /dev/sync` receives it.
   * `manifest` is the CLI's manifest.json; the adapter sets `manifest.project`.
   * @returns {Promise<{ ok: boolean, functionCount?: number }>}
   */
  async deploy(project, manifest) { throw new Error('deploy() not implemented'); }

  /**
   * required. Creates a tenant with the given id (after the first deploy).
   * @returns {Promise<{ tenantId: string }>}
   */
  async createTenant(project, tenantId, name) { throw new Error('createTenant() not implemented'); }

  /**
   * required. Creates (or reuses, by email) an account with a password login
   * and makes it an active Member of the tenant.
   * @returns {Promise<{ accountId: string, memberId: string }>}
   */
  async createMember(project, tenantId, { email, password, name }) { throw new Error('createMember() not implemented'); }

  /**
   * required. Every row of one tenant table, any order. The harness sorts,
   * masks aliases and checksums them (checksum.mjs), so all backends are
   * checksummed by the same code.
   * @returns {Promise<Array<Record<string, unknown>>>}
   */
  async readTable(project, tenantId, table) { throw new Error('readTable() not implemented'); }

  /**
   * Optional (`capabilities.changeLog`). The tenant's `_gonvex_sync_clock` row and
   * every `_gonvex_sync_changes` and `_gonvex_sync_transactions` row, as plain
   * objects with SQLite's column names. Used by the `change-capture` scenario.
   * @returns {Promise<{ clock: { epoch: string, revision: number, retained_revision: number }, changes: object[], transactions: object[] }>}
   */
  async readChangeLog(project, tenantId) { throw new Error(`readChangeLog() not supported by ${this.name}`); }
}
