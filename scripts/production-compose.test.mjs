import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import test from "node:test";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const composePath = path.join(root, "docker-compose.production.yml");
const secretEnvironment = {
  ...process.env,
  SERVICE_PASSWORD_64_GONVEX_MINIO: "compose-contract-minio",
  SERVICE_PASSWORD_64_GONVEX_DASHBOARD: "compose-contract-dashboard",
  SERVICE_REALBASE64_64_GONVEX_DASHBOARD_SESSION: "compose-contract-session",
};

function resolvedCompose(overrides = {}) {
  const result = spawnSync(
    "docker",
    ["compose", "-f", composePath, "config", "--format", "json"],
    { cwd: root, env: { ...secretEnvironment, GONVEX_DUCKDB: "false", ...overrides }, encoding: "utf8" },
  );
  assert.equal(result.status, 0, result.stderr || result.stdout);
  return JSON.parse(result.stdout);
}

test("production compose keeps stateful dependencies private and pinned", () => {
  const compose = resolvedCompose();
  const expected = [
    "gonvex-maker-dashboard",
    "gonvex-maker-minio",
    "gonvex-maker-minio-init",
    "gonvex-maker-runtime",
    "gonvex-maker-sqlite-backup",
  ];
  assert.deepEqual(Object.keys(compose.services).sort(), expected);

  for (const service of Object.values(compose.services)) {
    assert.equal(service.ports, undefined, "production services must not publish host ports");
  }
  for (const name of [
    "gonvex-maker-sqlite-backup",
    "gonvex-maker-minio",
    "gonvex-maker-minio-init",
  ]) {
    if (compose.services[name].image)
      assert.match(compose.services[name].image, /@sha256:[a-f0-9]{64}$/);
    else
      assert.match(
        readFileSync(path.resolve(root, compose.services[name].build.context, "Dockerfile"), "utf8"),
        /^FROM \S+@sha256:[a-f0-9]{64}$/m,
        `${name} must build from a pinned base image`,
      );
    assert.deepEqual(
      Object.keys(compose.services[name].networks ?? {}),
      ["default"],
      `${name} must remain isolated from Coolify's shared edge network`,
    );
  }

  const runtimeNetworks = compose.services["gonvex-maker-runtime"].networks;
  assert.deepEqual(Object.keys(runtimeNetworks).sort(), ["coolify-edge", "default"]);
  assert.deepEqual(runtimeNetworks["coolify-edge"].aliases, ["gonvex-maker-runtime-edge"]);
  assert.equal(compose.networks["coolify-edge"].external, true);
  assert.equal(compose.networks["coolify-edge"].name, "coolify");

  assert.equal(compose.volumes["gonvex-sqlite-data"].name, "gonvex-maker-sqlite-data");
  assert.equal(compose.volumes["gonvex-sqlite-backups"].name, "gonvex-maker-sqlite-backups");
  assert.equal(compose.volumes["gonvex-runtime-data"].name, "gonvex-maker-runtime-data");
  for (const removed of ["gonvex-postgres-data", "gonvex-postgres-backups"])
    assert.equal(compose.volumes[removed], undefined);
});

test("production runtime and dashboard require protected durable configuration", () => {
  const { services } = resolvedCompose();
  const runtime = services["gonvex-maker-runtime"];
  const dashboard = services["gonvex-maker-dashboard"];
  assert.equal(runtime.build.args.GONVEX_DUCKDB, "false");
  assert.equal(runtime.environment.GONVEX_ENVIRONMENT, "production");
  assert.equal(runtime.environment.GONVEX_REQUIRE_AUTH, "true");
  assert.equal(runtime.working_dir, "/var/lib/gonvex");
  assert.equal(runtime.user, "0:0");
  assert.equal(
    runtime.environment.GONVEX_PUBLIC_URL,
    "https://gonvex-maker.whagons.com",
  );
  assert.equal(runtime.environment.GONVEX_ADMIN_KEY, undefined);
  assert.equal(runtime.environment.DATABASE_URL, undefined);
  assert.equal(runtime.environment.GONVEX_CONTROL_PLANE_DATABASE_URL, undefined);
  assert.equal(runtime.environment.GONVEX_DATA_DIR, "/data");
  assert.ok(
    runtime.volumes.some((volume) => volume.source === "gonvex-sqlite-data" && volume.target === "/data"),
    "runtime must persist its SQLite data directory",
  );
  assert.equal(runtime.environment.VALKEY_URL, undefined);
  assert.equal(runtime.environment.GONVEX_DB_MAX_TOTAL_CONNS, undefined);
  assert.equal(runtime.environment.GOCACHE, undefined);
  assert.equal(runtime.environment.S3_ENDPOINT, "http://gonvex-maker-minio:9000");
  assert.deepEqual(runtime.cap_drop, ["ALL"]);
  assert.equal(dashboard.environment.DASHBOARD_AUTH_ENABLED, "true");
  assert.equal(dashboard.environment.DASHBOARD_COOKIE_SECURE, "true");
  assert.equal(dashboard.environment.GONVEX_RUNTIME_URL, "http://gonvex-maker-runtime:8080");
  assert.equal(dashboard.read_only, true);
  assert.equal(runtime.depends_on["gonvex-maker-postgres"], undefined);
  assert.equal(runtime.depends_on["gonvex-maker-valkey"], undefined);
  assert.equal(runtime.depends_on["gonvex-maker-minio-init"].condition, "service_healthy");
  assert.equal(services["gonvex-maker-runtime-permissions"], undefined);
});

test("production image can explicitly include DuckDB analysis", () => {
  const { services } = resolvedCompose({ GONVEX_DUCKDB: "true" });
  assert.equal(services["gonvex-maker-runtime"].build.args.GONVEX_DUCKDB, "true");
});

test("production public routes use the Gabriel server HTTP challenge resolver", () => {
  const { services } = resolvedCompose();
  const runtimeLabels = services["gonvex-maker-runtime"].labels;
  const dashboardLabels = services["gonvex-maker-dashboard"].labels;
  assert.equal(
    runtimeLabels["traefik.http.routers.gonvex-maker-runtime-lehttp.tls.certresolver"],
    "lehttp",
  );
  assert.equal(
    runtimeLabels["traefik.http.services.gonvex-maker-runtime-lehttp.loadbalancer.server.port"],
    "8080",
  );
  assert.match(
    runtimeLabels["traefik.http.routers.gonvex-maker-runtime-lehttp.rule"],
    /gonvex-maker\.whagons\.com/,
  );
  assert.equal(
    dashboardLabels["traefik.http.routers.gonvex-maker-dashboard-lehttp.tls.certresolver"],
    "lehttp",
  );
  assert.equal(
    dashboardLabels["traefik.http.services.gonvex-maker-dashboard-lehttp.loadbalancer.server.port"],
    "80",
  );
  assert.match(
    dashboardLabels["traefik.http.routers.gonvex-maker-dashboard-lehttp.rule"],
    /gonvex-maker-dashboard\.whagons\.com/,
  );
});

test("SQLite backups snapshot the runtime data volume and expose restore-tested health", () => {
  const { services } = resolvedCompose();
  const backup = services["gonvex-maker-sqlite-backup"];
  assert.equal(backup.depends_on["gonvex-maker-runtime"].condition, "service_healthy");
  assert.equal(backup.environment.GONVEX_DATA_DIR, "/data");
  assert.ok(backup.volumes.some((volume) => volume.source === "gonvex-sqlite-data" && volume.target === "/data"));
  assert.ok(backup.volumes.some((volume) => volume.source === "gonvex-sqlite-backups" && volume.target === "/backups"));
  assert.equal(backup.environment.BACKUP_RETENTION_DAYS, "14");
  assert.deepEqual(backup.healthcheck.test, ["CMD", "/usr/local/bin/gonvex-sqlite-backup", "check"]);
  assert.equal(backup.healthcheck.start_period, "2m0s");
  const script = readFileSync(path.join(root, "infra/sqlite-backup/backup.sh"), "utf8");
  assert.match(script, /\.backup '/);
  assert.match(script, /PRAGMA integrity_check/);
  assert.match(script, /\.partial/);
  assert.match(script, /PRAGMA quick_check/);
});
