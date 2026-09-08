import assert from "node:assert/strict";
import test from "node:test";

import {
  deployRollingApplications,
  waitForHealthyApplication,
  verifyApplicationUpdateAcknowledgement,
  verifyDashboardEnvironment,
  verifyRollingApplication,
  verifyRuntimeEnvironment,
} from "./deploy-coolify.mjs";

const sha = "b".repeat(40);

test("requires Coolify to acknowledge the exact application settings update", () => {
  assert.doesNotThrow(() => verifyApplicationUpdateAcknowledgement(
    { uuid: "runtime-uuid" },
    "runtime",
    "runtime-uuid",
  ));
  assert.throws(
    () => verifyApplicationUpdateAcknowledgement({}, "runtime", "runtime-uuid"),
    /settings update was not acknowledged/,
  );
  assert.throws(
    () => verifyApplicationUpdateAcknowledgement(
      { uuid: "different-uuid" },
      "runtime",
      "runtime-uuid",
    ),
    /settings update was not acknowledged/,
  );
});

function application(role, overrides = {}) {
  const runtime = role === "runtime";
  return {
    uuid: `${role}-uuid`,
    build_pack: "dockerfile",
    dockerfile_location: runtime ? "/Dockerfile.runtime" : "/Dockerfile.dashboard",
    ports_exposes: runtime ? "8080" : "80",
    ports_mappings: null,
    git_commit_sha: sha,
    health_check_enabled: true,
    health_check_path: "/healthz",
    status: "running:healthy",
    ...overrides,
  };
}

test("requires Dockerfile applications with readiness and no host port mapping", () => {
  assert.doesNotThrow(() => verifyRollingApplication(application("runtime"), "runtime", sha));
  assert.doesNotThrow(() => verifyRollingApplication(application("dashboard"), "dashboard", sha));
  assert.throws(
    () => verifyRollingApplication(application("runtime", { build_pack: "dockercompose" }), "runtime", sha),
    /Dockerfile build pack/,
  );
  assert.throws(
    () => verifyRollingApplication(application("runtime", { health_check_enabled: false }), "runtime", sha),
    /readiness check/,
  );
  assert.throws(
    () => verifyRollingApplication(application("runtime", { ports_mappings: "8080:8080" }), "runtime", sha),
    /must not publish a host port/,
  );
});

test("requires the selected runtime auth policy, loopback proxy, and exact advertised version", () => {
  const environment = [
    { key: "GONVEX_REQUIRE_AUTH", real_value: "false", is_buildtime: false },
    { key: "GONVEX_RUNTIME_VERSION", real_value: sha, is_buildtime: false },
    { key: "GONVEX_TRUSTED_PROXY_CIDRS", real_value: "10.0.0.0/8,127.0.0.1/32", is_buildtime: false },
  ];
  assert.doesNotThrow(() => verifyRuntimeEnvironment(environment, sha, false));
  assert.throws(
    () => verifyRuntimeEnvironment(environment, sha, true),
    /GONVEX_REQUIRE_AUTH=true/,
  );
  assert.doesNotThrow(() => verifyRuntimeEnvironment([
    { key: "GONVEX_REQUIRE_AUTH", real_value: "true", is_preview: true, is_buildtime: false },
    { key: "GONVEX_RUNTIME_VERSION", real_value: "preview", is_preview: true, is_buildtime: false },
    { key: "GONVEX_TRUSTED_PROXY_CIDRS", real_value: "192.0.2.0/24", is_preview: true, is_buildtime: false },
    ...environment,
  ], sha, false));
  assert.throws(
    () => verifyRuntimeEnvironment(environment, "c".repeat(40), false),
    /advertise exact version/,
  );
  assert.throws(
    () => verifyRuntimeEnvironment(
      [...environment, { key: "S3_SECRET_ACCESS_KEY", value: "secret", is_buildtime: true }],
      sha,
      false,
    ),
    /must not be build-time variables/,
  );
  assert.throws(
    () => verifyRuntimeEnvironment(
      environment.filter((entry) => entry.key !== "GONVEX_TRUSTED_PROXY_CIDRS"),
      sha,
      false,
    ),
    /127\.0\.0\.1\/32/,
  );
  assert.doesNotThrow(() => verifyDashboardEnvironment([
    { key: "VITE_GONVEX_URL", value: "https://runtime.test", is_buildtime: true },
    { key: "DASHBOARD_SESSION_SECRET", value: "secret", is_buildtime: false },
  ]));
  assert.throws(
    () => verifyDashboardEnvironment([{ key: "DASHBOARD_SESSION_SECRET", value: "secret", is_buildtime: true }]),
    /private variables must not be build-time variables/,
  );
});

test("waits for propagated application health before deploying dashboard", async () => {
  const events = [];
  const healthReads = {};
  const states = {
    "runtime-uuid": application("runtime", { git_commit_sha: "HEAD" }),
    "dashboard-uuid": application("dashboard", { git_commit_sha: "HEAD" }),
  };
  const environments = {
    "runtime-uuid": [
      { key: "GONVEX_REQUIRE_AUTH", real_value: "true", is_preview: true, is_buildtime: false },
      { key: "GONVEX_RUNTIME_VERSION", real_value: "preview", is_preview: true, is_buildtime: false },
      { key: "GONVEX_TRUSTED_PROXY_CIDRS", real_value: "192.0.2.0/24", is_preview: true, is_buildtime: false },
      { key: "GONVEX_REQUIRE_AUTH", real_value: "false", is_buildtime: false },
      { key: "GONVEX_TRUSTED_PROXY_CIDRS", real_value: "10.0.0.0/8", is_buildtime: false },
    ],
    "dashboard-uuid": [],
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    const target = new URL(url);
    const method = init.method ?? "GET";
    const environmentMatch = target.pathname.match(/\/applications\/([^/]+)\/envs$/);
    if (environmentMatch) {
      const uuid = decodeURIComponent(environmentMatch[1]);
      if (method === "GET") return Response.json(environments[uuid]);
      const update = JSON.parse(init.body);
      const existing = environments[uuid].find(
        (entry) => entry.key === update.key && entry.is_preview !== true,
      );
      if (existing) {
        existing.value = update.value;
        existing.real_value = update.value;
      } else {
        environments[uuid].push({
          key: update.key,
          value: update.value,
          real_value: update.value,
          is_preview: update.is_preview,
        });
      }
      events.push(`env:${method}:${uuid}:${update.key}`);
      return Response.json({});
    }
    const applicationMatch = target.pathname.match(/\/applications\/([^/]+)$/);
    if (applicationMatch) {
      const uuid = decodeURIComponent(applicationMatch[1]);
      if (method === "PATCH") {
        events.push(`patch:${uuid}`);
        Object.assign(states[uuid], JSON.parse(init.body));
        return Response.json({ uuid });
      }
      if (healthReads[uuid] !== undefined) {
        healthReads[uuid]++;
        if (healthReads[uuid] >= 3) states[uuid].status = "running:healthy";
      }
      return Response.json(states[uuid]);
    }
    if (target.pathname.endsWith("/deploy")) {
      if (method !== "POST") return Response.json({ message: "This endpoint has changed to a POST request." }, { status: 405 });
      const uuid = target.searchParams.get("uuid");
      events.push(`deploy:${uuid}`);
      return Response.json({ deployments: [{ deployment_uuid: `${uuid}-deployment` }] });
    }
    const deploymentMatch = target.pathname.match(/\/deployments\/(.+)$/);
    if (deploymentMatch) {
      const uuid = decodeURIComponent(deploymentMatch[1]).replace(/-deployment$/, "");
      events.push(`finished:${uuid}`);
      states[uuid].status = "running:unhealthy";
      healthReads[uuid] = 0;
      return Response.json({ status: "finished" });
    }
    return new Response("not found", { status: 404 });
  };

  try {
    await deployRollingApplications({
      base: "https://coolify.example.test/api/v1",
      token: "test-token",
      sha,
      applications: { runtime: "runtime-uuid", dashboard: "dashboard-uuid" },
      expectedRequireAuth: false,
      autoDeploy: false,
      waitOptions: { timeoutMS: 100, intervalMS: 0 },
    });
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.deepEqual(events, [
    "env:POST:runtime-uuid:GONVEX_RUNTIME_VERSION",
    "env:PATCH:runtime-uuid:GONVEX_TRUSTED_PROXY_CIDRS",
    "patch:runtime-uuid",
    "deploy:runtime-uuid",
    "finished:runtime-uuid",
    "patch:dashboard-uuid",
    "deploy:dashboard-uuid",
    "finished:dashboard-uuid",
  ]);
  assert.equal(states["runtime-uuid"].is_auto_deploy_enabled, false);
  assert.equal(states["dashboard-uuid"].is_auto_deploy_enabled, false);
  assert.equal(
    environments["runtime-uuid"].find(
      (entry) => entry.key === "GONVEX_REQUIRE_AUTH" && entry.is_preview !== true,
    ).real_value,
    "false",
  );
  assert.equal(
    environments["runtime-uuid"].find(
      (entry) => entry.key === "GONVEX_TRUSTED_PROXY_CIDRS" && entry.is_preview !== true,
    ).real_value,
    "10.0.0.0/8,127.0.0.1/32",
  );
  assert.equal(
    environments["runtime-uuid"].find(
      (entry) => entry.key === "GONVEX_RUNTIME_VERSION" && entry.is_preview === true,
    ).real_value,
    "preview",
  );
});

test("health propagation wait stays bounded and rejects concurrent pins", async () => {
 const originalFetch=globalThis.fetch;
 try {
  globalThis.fetch=async()=>Response.json(application("runtime",{status:"running:unhealthy"}));
  await assert.rejects(waitForHealthyApplication("https://coolify.test","test","runtime-uuid","runtime",sha,{healthTimeoutMS:5,intervalMS:1}),/before timeout/);
  globalThis.fetch=async()=>Response.json(application("runtime",{git_commit_sha:"c".repeat(40)}));
  await assert.rejects(waitForHealthyApplication("https://coolify.test","test","runtime-uuid","runtime",sha,{healthTimeoutMS:5,intervalMS:1}),/not pinned/);
 } finally { globalThis.fetch=originalFetch; }
});
