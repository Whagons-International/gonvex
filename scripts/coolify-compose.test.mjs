import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import test from "node:test";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const composePath = path.join(root, "docker-compose.coolify.yml");

function resolvedCompose() {
  const result = spawnSync(
    "docker",
    ["compose", "-f", composePath, "config", "--format", "json"],
    {
      cwd: root,
      env: process.env,
      encoding: "utf8",
    },
  );
  assert.equal(result.status, 0, result.stderr || result.stdout);
  return JSON.parse(result.stdout);
}

test("Coolify Compose is a stable data plane only", () => {
  const compose = resolvedCompose();
  assert.deepEqual(Object.keys(compose.services).sort(), ["gonvex-sqlite-backup"]);
  assert.equal(compose.services["gonvex-runtime"], undefined);
  assert.equal(compose.services["gonvex-dashboard"], undefined);
  assert.equal(compose.volumes["gonvex-runtime-data"], undefined);
  assert.equal(compose.volumes["gonvex-postgres-data"], undefined);
  // The runtime Application owns the SQLite volume; this stack only reads it.
  assert.equal(compose.volumes["gonvex-sqlite-data"].external, true);
  assert.ok(compose.volumes["gonvex-sqlite-backups"]);
  assert.equal(compose.volumes["gonvex-valkey-data"], undefined);
  for (const service of Object.values(compose.services)) {
    assert.equal(service.ports, undefined, "durable dependencies must not publish host ports");
  }
});
