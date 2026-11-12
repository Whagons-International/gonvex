// Node (vitest globalSetup), not workerd. Starts the sandbox manager (container/manager.mjs) the tests' Container
// object talks to through SANDBOX_CONTAINER_URL:
// - K20_SANDBOX_URL set: a manager already running there (the image under
//   `docker run`, as root with chroot): nothing is started;
// - K20_SANDBOX_BIN set: the manager here with the Rust binaries from that
//   directory (gonvex-sandbox-worker with DuckDB, k20-ingest), unconfined;
// - neither (CI): the manager with the protocol stubs in test/stub/.
import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export const MANAGER_PORT = 47820;

export default async function setup() {
  if (process.env.K20_SANDBOX_URL) return () => undefined;
  const here = dirname(fileURLToPath(import.meta.url));
  const bin = process.env.K20_SANDBOX_BIN;
  const real = Boolean(bin && existsSync(join(bin, "gonvex-sandbox-worker")) && existsSync(join(bin, "k20-ingest")));
  const root = mkdtempSync(join(tmpdir(), "k20-sandboxes-"));
  const child = spawn(process.execPath, [join(here, "../container/manager.mjs")], {
    stdio: "ignore",
    env: {
      ...process.env,
      PORT: String(MANAGER_PORT),
      GONVEX_SANDBOX_ROOT: root,
      GONVEX_SANDBOX_WORKER_BINARY: real ? join(bin, "gonvex-sandbox-worker") : join(here, "stub/worker.mjs"),
      K20_INGEST_BINARY: real ? join(bin, "k20-ingest") : join(here, "stub/ingest.mjs"),
      K20_WORKER_KIND: real ? "rust" : "stub",
      GONVEX_SANDBOX_ALLOW_UNCONFINED: "true",
    },
  });
  for (let i = 0; i < 100; i++) {
    const up = await fetch(`http://127.0.0.1:${MANAGER_PORT}/healthz`).then((r) => r.ok, () => false);
    if (up) break;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  return () => {
    child.kill("SIGTERM");
    rmSync(root, { recursive: true, force: true });
  };
}
