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
      env: { ...process.env, GONVEX_POSTGRES_PASSWORD: "compose-postgres" },
      encoding: "utf8",
    },
  );
  assert.equal(result.status, 0, result.stderr || result.stdout);
  return JSON.parse(result.stdout);
}

test("Coolify Compose is a stable data plane only", () => {
  const compose = resolvedCompose();
  assert.deepEqual(Object.keys(compose.services).sort(), ["gonvex-postgres", "gonvex-valkey"]);
  assert.equal(compose.services["gonvex-runtime"], undefined);
  assert.equal(compose.services["gonvex-dashboard"], undefined);
  assert.equal(compose.volumes["gonvex-runtime-data"], undefined);
  assert.ok(compose.volumes["gonvex-postgres-data"]);
  assert.ok(compose.volumes["gonvex-valkey-data"]);
  for (const service of Object.values(compose.services)) {
    assert.equal(service.ports, undefined, "durable dependencies must not publish host ports");
  }

  const postgresNetworks = compose.services["gonvex-postgres"].networks;
  const valkeyNetworks = compose.services["gonvex-valkey"].networks;
  assert.deepEqual(Object.keys(postgresNetworks).sort(), ["coolify-edge", "default"]);
  assert.deepEqual(Object.keys(valkeyNetworks).sort(), ["coolify-edge", "default"]);
  assert.deepEqual(postgresNetworks["coolify-edge"].aliases, ["gonvex-postgres"]);
  assert.deepEqual(valkeyNetworks["coolify-edge"].aliases, ["gonvex-valkey"]);
  assert.equal(compose.networks["coolify-edge"].external, true);
  assert.equal(compose.networks["coolify-edge"].name, "coolify");
});
