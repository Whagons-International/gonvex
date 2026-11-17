// Node globalSetup: each local fixture owns its manager, ephemeral port and
// workspace until teardown has awaited process exit. An external image URL is
// borrowed without starting or stopping it. Rust/stub selection is unchanged.
import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));

export async function startManager({
  env = process.env,
  executable = process.execPath,
  managerPath = join(here, "../container/manager.mjs"),
  tempRoot = tmpdir(),
  startupTimeoutMs = 10_000,
} = {}) {
  const bin = env.K20_SANDBOX_BIN;
  const real = Boolean(bin && existsSync(join(bin, "gonvex-sandbox-worker")) && existsSync(join(bin, "k20-ingest")));
  const root = mkdtempSync(join(tempRoot, "k20-sandboxes-"));
  const child = spawn(executable, [managerPath], {
    stdio: ["ignore", "ignore", "pipe", "ipc"],
    env: {
      ...env,
      PORT: "0",
      GONVEX_SANDBOX_ROOT: root,
      GONVEX_SANDBOX_WORKER_BINARY: real ? join(bin, "gonvex-sandbox-worker") : join(here, "stub/worker.mjs"),
      K20_INGEST_BINARY: real ? join(bin, "k20-ingest") : join(here, "stub/ingest.mjs"),
      K20_WORKER_KIND: real ? "rust" : "stub",
      GONVEX_SANDBOX_ALLOW_UNCONFINED: "true",
    },
  });
  let stderr = "", spawnError;
  child.stderr.on("data", chunk => { stderr = (stderr + chunk).slice(-16_384); });
  child.on("error", error => { spawnError = error; });
  const closed = new Promise(resolve => child.once("close", (code, signal) => resolve({ code, signal })));
  let stopping;
  const stop = () => stopping ??= (async () => {
    const timer = setTimeout(() => child.kill("SIGKILL"), 5_000);
    try {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
      await closed;
    } finally {
      clearTimeout(timer);
      rmSync(root, { recursive: true, force: true });
    }
  })();

  let timer, onMessage;
  try {
    const url = await Promise.race([
      new Promise(resolve => {
        onMessage = message => {
          if (message?.type === "sandbox-manager-ready" && Number.isInteger(message.port) && message.port > 0 && message.port <= 65535) {
            resolve(`http://127.0.0.1:${message.port}`);
          }
        };
        child.on("message", onMessage);
      }),
      closed.then(({ code, signal }) => { throw new Error(`manager exited before readiness (code ${code}, signal ${signal})${spawnError ? `: ${spawnError.message}` : ""}`); }),
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`manager readiness timed out after ${startupTimeoutMs} ms`)), startupTimeoutMs); }),
    ]);
    const response = await fetch(`${url}/healthz`, { signal: AbortSignal.timeout(1_000) });
    const health = await response.json();
    if (!response.ok || !health.ok || health.worker !== (real ? "rust" : "stub") || child.exitCode !== null || child.signalCode !== null) {
      throw new Error("manager health check did not match the started fixture");
    }
    return { url, stop, pid: child.pid, root };
  } catch (error) {
    await stop();
    throw new Error(`sandbox fixture startup failed: ${error.message}${stderr ? `\nManager stderr (tail):\n${stderr}` : ""}`, { cause: error });
  } finally {
    clearTimeout(timer);
    if (onMessage) child.off("message", onMessage);
  }
}

export default async function setup(project) {
  if (process.env.K20_SANDBOX_URL) {
    project.provide("k20SandboxUrl", process.env.K20_SANDBOX_URL);
    return;
  }
  const fixture = await startManager();
  try {
    project.provide("k20SandboxUrl", fixture.url);
  } catch (error) {
    await fixture.stop();
    throw error;
  }
  return fixture.stop;
}
