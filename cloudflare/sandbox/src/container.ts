// One sandbox Container per tenant (CONTRACT B.2a: adapt, Container; 0048 §2.1).
//
// A Durable Object with a Container: the image (container/Dockerfile) holds
// Rust's `gonvex-sandbox-worker` with DuckDB, Rust's `data_ingest.rs` and the
// manager (container/manager.mjs) that runs them as Rust's `SandboxManager`
// does. The object starts the Container on demand with no Internet access,
// forwards each operation to the manager, and stops the Container once no
// workspace of the tenant is alive. Workspaces live on the Container's disk:
// a Container that stops loses them, as a Rust runtime restart does.
//
// `SANDBOX_CONTAINER_URL` replaces the Container with a manager at that URL
// (tests and local development).
import { DurableObject } from "cloudflare:workers";
import { answer, type Answer } from "./answer";
import { MANAGER_PORT } from "./constants";

export interface SandboxContainerEnv {
  SANDBOX_CONTAINER_URL?: string;
  /** "true" only for local development without root (sandbox-worker main.rs:729-731). */
  GONVEX_SANDBOX_ALLOW_UNCONFINED?: string;
}

/** How long a stopped tenant waits for its Container's manager to listen. */
const STARTUP_BUDGET_MS = 60_000;
/** Kept after the last workspace expires, so the next sandbox of a busy tenant starts warm. */
const IDLE_GRACE_MS = 60_000;
const KEEP_ALIVE = "keepAliveUntil";

type ManagerAnswer = { ok: true; value: unknown } | { ok: false; error: string };

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export class SandboxContainer extends DurableObject<SandboxContainerEnv> {
  /** Cold start of the last start, for the numbers PRs state. */
  private lastStartMs: number | null = null;

  /** Starts the Container if it is not running and waits for the manager. */
  private async ready(): Promise<Fetcher | null> {
    if (this.env.SANDBOX_CONTAINER_URL) return null;
    const container = this.ctx.container;
    if (!container) throw new Error("sandbox container is not configured");
    const port = container.getTcpPort(MANAGER_PORT);
    if (container.running) return port;
    const started = Date.now();
    container.start({
      enableInternet: false,
      env: { GONVEX_SANDBOX_ALLOW_UNCONFINED: this.env.GONVEX_SANDBOX_ALLOW_UNCONFINED ?? "false" },
    });
    container.monitor().catch(() => undefined);
    for (;;) {
      try {
        const response = await port.fetch("http://manager/healthz");
        if (response.ok) {
          this.lastStartMs = Date.now() - started;
          return port;
        }
      } catch (error) {
        if (Date.now() - started > STARTUP_BUDGET_MS) throw new Error(`sandbox container did not start: ${(error as Error).message}`);
      }
      await sleep(100);
    }
  }

  private async manager(path: string, init: RequestInit): Promise<ManagerAnswer> {
    const port = await this.ready();
    const response = port ? await port.fetch(`http://manager${path}`, init) : await fetch(`${this.env.SANDBOX_CONTAINER_URL}${path}`, init);
    if (!response.ok) throw new Error(`sandbox manager answered ${response.status}`);
    return (await response.json()) as ManagerAnswer;
  }

  /** Keeps the Container until the latest workspace has expired. */
  private async keepAlive(until: number): Promise<void> {
    const current = (await this.ctx.storage.get<number>(KEEP_ALIVE)) ?? 0;
    if (until <= current) return;
    await this.ctx.storage.put(KEEP_ALIVE, until);
    await this.ctx.storage.setAlarm(until + IDLE_GRACE_MS);
  }

  private async checkSoon(): Promise<void> {
    const due = Date.now() + IDLE_GRACE_MS;
    const current = await this.ctx.storage.getAlarm();
    if (current === null || current > due) await this.ctx.storage.setAlarm(due);
  }

  /** One manager operation (`create`, `destroy`, `run`, `cancel`, `status`, `read`, `write`), answered as JSON text. */
  async call(operation: string, args: Record<string, unknown>): Promise<Answer<string>> {
    return answer(async () => {
      const reply = await this.manager(`/op/${operation}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(args) });
      if (!reply.ok) throw new Error(reply.error);
      if (operation === "create") await this.keepAlive((reply.value as { expiresAt: number }).expiresAt);
      // Whagons destroys its sandbox when a turn ends: check soon whether the Container can stop.
      if (operation === "destroy") await this.checkSoon();
      return JSON.stringify(reply.value);
    });
  }

  /** `importFile`: the uploaded bytes go to Rust's ingest in the Container. */
  async importFile(args: Record<string, unknown>, body: ReadableStream<Uint8Array>): Promise<Answer<string>> {
    return answer(async () => {
      const header = btoa(String.fromCharCode(...new TextEncoder().encode(JSON.stringify(args))));
      // Streamed: an import may be 64 MiB, more than one RPC argument carries.
      const reply = await this.manager("/op/import", { method: "POST", headers: { "x-sandbox-import": header, "content-type": "application/octet-stream" }, body });
      if (!reply.ok) throw new Error(reply.error);
      return JSON.stringify(reply.value);
    });
  }

  /** The manager's counters and memory, and the last cold start. */
  async stats(): Promise<Answer<{ manager: unknown; lastStartMs: number | null; running: boolean }>> {
    return answer(async () => {
      const reply = await this.manager("/op/stats", { method: "POST" });
      return { manager: reply.ok ? reply.value : reply.error, lastStartMs: this.lastStartMs, running: this.ctx.container?.running ?? false };
    });
  }

  /** Stops the Container when no workspace is alive; otherwise checks again later. */
  override async alarm(): Promise<void> {
    if (this.env.SANDBOX_CONTAINER_URL || !this.ctx.container?.running) {
      await this.ctx.storage.delete(KEEP_ALIVE);
      return;
    }
    const reply = await this.manager("/op/stats", { method: "POST" }).catch(() => null);
    const stats = reply?.ok ? (reply.value as { live: number; active: number; latestExpiry: number }) : null;
    if (stats && (stats.live > 0 || stats.active > 0)) {
      const next = Math.max(stats.latestExpiry, Date.now()) + IDLE_GRACE_MS;
      await this.ctx.storage.put(KEEP_ALIVE, next - IDLE_GRACE_MS);
      await this.ctx.storage.setAlarm(next);
      return;
    }
    await this.ctx.storage.delete(KEEP_ALIVE);
    await this.ctx.container.destroy();
  }
}
