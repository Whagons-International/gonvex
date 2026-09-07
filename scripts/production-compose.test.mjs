import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import test from "node:test";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const composePath = path.join(root, "docker-compose.production.yml");
const secretEnvironment = {
  ...process.env,
  SERVICE_PASSWORD_64_GONVEX_POSTGRES: "compose-contract-postgres",
  SERVICE_PASSWORD_64_GONVEX_MINIO: "compose-contract-minio",
  SERVICE_PASSWORD_64_GONVEX_DASHBOARD: "compose-contract-dashboard",
  SERVICE_REALBASE64_64_GONVEX_DASHBOARD_SESSION: "compose-contract-session",
};

function resolvedCompose(overrides = {}) {
  const result = spawnSync(
    "docker",
    ["compose", "-f", composePath, "config", "--format", "json"],
    { cwd: root, env: { ...secretEnvironment, ...overrides }, encoding: "utf8" },
  );
  assert.equal(result.status, 0, result.stderr || result.stdout);
  return JSON.parse(result.stdout);
}

test("production runtime receives the configured Telegram destination and thresholds", () => {
  const configured = {
    GONVEX_TELEGRAM_BOT_TOKEN: "test-bot",
    GONVEX_TELEGRAM_CHAT_ID: "test-chat",
    GONVEX_ALERT_CPU_PERCENT: "250",
    GONVEX_ALERT_TTLU: "6s",
    GONVEX_ALERT_OPERATION_DURATION: "4s",
    GONVEX_ALERT_COOLDOWN: "10m",
  };
  const { services } = resolvedCompose(configured);
  for (const [key, value] of Object.entries(configured)) {
    assert.equal(services["gonvex-maker-runtime"].environment[key], value);
    assert.equal(services["gonvex-maker-dashboard"].environment[key], undefined);
  }
});

test("production compose keeps stateful dependencies private and pinned", () => {
  const compose = resolvedCompose();
  const expected = [
    "gonvex-maker-dashboard",
    "gonvex-maker-minio",
    "gonvex-maker-minio-init",
    "gonvex-maker-postgres",
    "gonvex-maker-postgres-backup",
    "gonvex-maker-runtime",
    "gonvex-maker-valkey",
  ];
  assert.deepEqual(Object.keys(compose.services).sort(), expected);

  for (const service of Object.values(compose.services)) {
    assert.equal(service.ports, undefined, "production services must not publish host ports");
  }
  for (const name of [
    "gonvex-maker-postgres",
    "gonvex-maker-postgres-backup",
    "gonvex-maker-valkey",
    "gonvex-maker-minio",
    "gonvex-maker-minio-init",
  ]) {
    assert.match(compose.services[name].image, /@sha256:[a-f0-9]{64}$/);
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

  assert.equal(compose.volumes["gonvex-postgres-data"].name, "gonvex-maker-postgres-data");
  assert.equal(compose.volumes["gonvex-runtime-data"].name, "gonvex-maker-runtime-data");
});

test("production runtime and dashboard require protected durable configuration", () => {
  const { services } = resolvedCompose();
  const runtime = services["gonvex-maker-runtime"];
  const dashboard = services["gonvex-maker-dashboard"];
  assert.equal(runtime.environment.GONVEX_ENVIRONMENT, "production");
  assert.equal(runtime.environment.GONVEX_REQUIRE_AUTH, "true");
  assert.equal(runtime.working_dir, "/var/lib/gonvex");
  assert.equal(runtime.user, "0:0");
  assert.equal(
    runtime.environment.GONVEX_PUBLIC_URL,
    "https://gonvex-maker.whagons.com",
  );
  assert.equal(runtime.environment.GONVEX_ADMIN_KEY, undefined);
  assert.equal(
    runtime.environment.DATABASE_URL,
    "postgres://gonvex:compose-contract-postgres@gonvex-maker-postgres:5432/gonvex?sslmode=disable",
  );
  assert.equal(runtime.environment.VALKEY_URL, "redis://gonvex-maker-valkey:6379/0");
  assert.equal(runtime.environment.GONVEX_DB_MAX_TOTAL_CONNS, "20");
  assert.equal(runtime.environment.GONVEX_DB_MAX_OPEN_CONNS, "2");
  assert.equal(runtime.environment.GONVEX_DB_MAX_IDLE_CONNS, "1");
  assert.equal(runtime.environment.GOCACHE, "/var/cache/gonvex/go-build");
  assert.equal(runtime.environment.S3_ENDPOINT, "http://gonvex-maker-minio:9000");
  assert.deepEqual(runtime.cap_drop, ["ALL"]);
  assert.equal(dashboard.environment.DASHBOARD_AUTH_ENABLED, "true");
  assert.equal(dashboard.environment.DASHBOARD_COOKIE_SECURE, "true");
  assert.equal(dashboard.environment.GONVEX_RUNTIME_URL, "http://gonvex-maker-runtime:8080");
  assert.equal(dashboard.read_only, true);
  assert.equal(runtime.depends_on["gonvex-maker-postgres"].condition, "service_healthy");
  assert.equal(runtime.depends_on["gonvex-maker-valkey"].condition, "service_healthy");
  assert.equal(runtime.depends_on["gonvex-maker-minio-init"].condition, "service_healthy");
  assert.equal(services["gonvex-maker-runtime-permissions"], undefined);
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

test("first retained backup waits for runtime and exposes restore-tested health", () => {
  const { services } = resolvedCompose();
  const backup = services["gonvex-maker-postgres-backup"];
  assert.equal(backup.depends_on["gonvex-maker-runtime"].condition, "service_healthy");
  assert.equal(backup.environment.PGHOST, "gonvex-maker-postgres");
  assert.match(backup.entrypoint.join("\n"), /pg_dump/);
  assert.match(backup.entrypoint.join("\n"), /\.dump\.partial/);
  assert.match(backup.entrypoint.join("\n"), /-mtime \+14 -delete/);
  assert.match(backup.healthcheck.test.join("\n"), /-ge 20000/);
  assert.match(backup.healthcheck.test.join("\n"), /pg_restore --list/);
  assert.equal(backup.healthcheck.start_period, "2m0s");
});
