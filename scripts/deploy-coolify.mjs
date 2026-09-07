import { pathToFileURL } from "node:url";

const finishedDeploymentStates = new Set(["finished", "success"]);
const failedDeploymentStates = new Set([
  "cancelled",
  "cancelled-by-user",
  "failed",
  "error",
]);

export const applicationContracts = Object.freeze({
  runtime: Object.freeze({
    dockerfile: "/Dockerfile.runtime",
    port: "8080",
    healthPath: "/healthz",
  }),
  dashboard: Object.freeze({
    dockerfile: "/Dockerfile.dashboard",
    port: "80",
    healthPath: "/healthz",
  }),
});

function requiredEnvironment(name) {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function requiredBooleanEnvironment(name) {
  const value = requiredEnvironment(name);
  if (value !== "true" && value !== "false") {
    throw new Error(`${name} must be true or false`);
  }
  return value === "true";
}

function apiBase(value) {
  const base = value.replace(/\/$/, "");
  return base.endsWith("/api/v1") ? base : `${base}/api/v1`;
}

async function coolifyRequest(base, token, path, init = {}) {
  const response = await fetch(`${base}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
      ...(init.body ? { "Content-Type": "application/json" } : {}),
      ...init.headers,
    },
  });
  if (!response.ok) {
    const detail = (await response.text()).slice(0, 500);
    throw new Error(`Coolify ${init.method ?? "GET"} ${path} failed (${response.status}): ${detail}`);
  }
  const text = await response.text();
  return text ? JSON.parse(text) : undefined;
}

export function verifyRollingApplication(application, role, sha) {
  const contract = applicationContracts[role];
  if (!contract) throw new Error(`unknown Gonvex application role ${role}`);
  if (application?.build_pack !== "dockerfile") {
    throw new Error(`${role} must use the Dockerfile build pack`);
  }
  if (application.dockerfile_location !== contract.dockerfile) {
    throw new Error(`${role} must build ${contract.dockerfile}`);
  }
  if (String(application.ports_exposes) !== contract.port) {
    throw new Error(`${role} must expose container port ${contract.port}`);
  }
  if (application.git_commit_sha !== sha) {
    throw new Error(`${role} is not pinned to ${sha}`);
  }
  if (!application.health_check_enabled || application.health_check_path !== contract.healthPath) {
    throw new Error(`${role} must use the ${contract.healthPath} readiness check`);
  }
  if (String(application.ports_mappings ?? "").trim() !== "") {
    throw new Error(`${role} must not publish a host port; rolling replicas need the same container port`);
  }
}

export function verifyApplicationUpdateAcknowledgement(update, role, uuid) {
  if (update?.uuid !== uuid) {
    throw new Error(`${role} settings update was not acknowledged for ${uuid}`);
  }
}

function productionEnvironmentEntry(environment, key) {
  return environment?.find(
    (candidate) => candidate?.key === key && candidate?.is_preview !== true,
  );
}

function environmentValue(environment, key) {
  const entry = productionEnvironmentEntry(environment, key);
  const value = String(entry?.value ?? entry?.real_value ?? "").trim();
  if (
    value.length >= 2
    && ((value.startsWith("'") && value.endsWith("'"))
      || (value.startsWith('"') && value.endsWith('"')))
  ) {
    return value.slice(1, -1);
  }
  return value;
}

function verifyRuntimePolicyEnvironment(environment, expectedRequireAuth) {
  const expected = String(expectedRequireAuth);
  if (environmentValue(environment, "GONVEX_REQUIRE_AUTH") !== expected) {
    throw new Error(`runtime application must set GONVEX_REQUIRE_AUTH=${expected}`);
  }
  const buildTimeKeys = environment
    ?.filter((entry) => entry?.is_buildtime === true && entry?.is_preview !== true)
    .map((entry) => entry.key) ?? [];
  if (buildTimeKeys.length > 0) {
    throw new Error(`runtime secrets must not be build-time variables: ${buildTimeKeys.join(", ")}`);
  }
}

export function verifyRuntimeEnvironment(environment, sha, expectedRequireAuth) {
  verifyRuntimePolicyEnvironment(environment, expectedRequireAuth);
  if (environmentValue(environment, "GONVEX_RUNTIME_VERSION") !== sha) {
    throw new Error(`runtime application must advertise exact version ${sha}`);
  }
  const trustedProxies = environmentValue(environment, "GONVEX_TRUSTED_PROXY_CIDRS")
    .split(",")
    .map((value) => value.trim());
  if (!trustedProxies.includes("127.0.0.1/32")) {
    throw new Error("runtime application must trust supervisor proxy 127.0.0.1/32");
  }
}

export function verifyDashboardEnvironment(environment) {
  const privateBuildTimeKeys = environment
    ?.filter(
      (entry) => entry?.is_buildtime === true
        && entry?.is_preview !== true
        && !String(entry?.key ?? "").startsWith("VITE_"),
    )
    .map((entry) => entry.key) ?? [];
  if (privateBuildTimeKeys.length > 0) {
    throw new Error(`dashboard private variables must not be build-time variables: ${privateBuildTimeKeys.join(", ")}`);
  }
}

async function upsertApplicationEnvironment(base, token, uuid, key, value) {
  const environment = await coolifyRequest(
    base,
    token,
    `/applications/${encodeURIComponent(uuid)}/envs`,
  );
  const exists = productionEnvironmentEntry(environment, key) !== undefined;
  await coolifyRequest(base, token, `/applications/${encodeURIComponent(uuid)}/envs`, {
    method: exists ? "PATCH" : "POST",
    body: JSON.stringify({ key, value, is_literal: true, is_preview: false }),
  });
}

function withTrustedProxy(environment, cidr) {
  const configured = environmentValue(environment, "GONVEX_TRUSTED_PROXY_CIDRS")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  return [...new Set([...configured, cidr])].join(",");
}

async function waitForDeployment(base, token, deploymentUUID, options = {}) {
  const timeoutMS = options.timeoutMS ?? 12 * 60 * 1000;
  const intervalMS = options.intervalMS ?? 5_000;
  const deadline = Date.now() + timeoutMS;
  while (Date.now() < deadline) {
    const deployment = await coolifyRequest(
      base,
      token,
      `/deployments/${encodeURIComponent(deploymentUUID)}`,
    );
    const status = String(deployment?.status ?? "").toLowerCase();
    if (finishedDeploymentStates.has(status)) return deployment;
    if (failedDeploymentStates.has(status)) {
      throw new Error(`Coolify deployment ${deploymentUUID} ended as ${status}`);
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMS));
  }
  throw new Error(`Coolify deployment ${deploymentUUID} did not finish before timeout`);
}

export async function deployRollingApplications({
  base,
  token,
  applications,
  sha,
  expectedRequireAuth,
  autoDeploy,
  waitOptions,
}) {
  if (!/^[0-9a-f]{40}$/.test(sha)) {
    throw new Error("GONVEX_DEPLOY_SHA must be a full lowercase Git commit SHA");
  }
  for (const role of ["runtime", "dashboard"]) {
    if (!applications?.[role]) throw new Error(`missing Coolify ${role} application UUID`);
  }
  if (typeof expectedRequireAuth !== "boolean") {
    throw new Error("expectedRequireAuth must be explicitly true or false");
  }
  if (typeof autoDeploy !== "boolean") {
    throw new Error("autoDeploy must be explicitly true or false");
  }

  // Fail before changing a source pin or starting a deployment if the selected
  // auth policy does not match the application. Promotion must never silently
  // turn authentication on or off.
  const initialRuntimeEnvironment = await coolifyRequest(
    base,
    token,
    `/applications/${encodeURIComponent(applications.runtime)}/envs`,
  );
  verifyRuntimePolicyEnvironment(initialRuntimeEnvironment, expectedRequireAuth);
  const initialDashboardEnvironment = await coolifyRequest(
    base,
    token,
    `/applications/${encodeURIComponent(applications.dashboard)}/envs`,
  );
  verifyDashboardEnvironment(initialDashboardEnvironment);

  await upsertApplicationEnvironment(
    base,
    token,
    applications.runtime,
    "GONVEX_RUNTIME_VERSION",
    sha,
  );
  await upsertApplicationEnvironment(
    base,
    token,
    applications.runtime,
    "GONVEX_TRUSTED_PROXY_CIDRS",
    withTrustedProxy(initialRuntimeEnvironment, "127.0.0.1/32"),
  );

  // Runtime first: the dashboard may proxy session/API traffic to it. Each
  // application is pinned to the exact tested commit before Coolify starts the
  // health-first rolling replacement.
  for (const role of ["runtime", "dashboard"]) {
    const uuid = applications[role];
    const contract = applicationContracts[role];
    const updated = await coolifyRequest(base, token, `/applications/${encodeURIComponent(uuid)}`, {
      method: "PATCH",
      body: JSON.stringify({
        git_commit_sha: sha,
        is_auto_deploy_enabled: autoDeploy,
        health_check_enabled: true,
        health_check_path: contract.healthPath,
        health_check_port: contract.port,
        health_check_method: "GET",
        health_check_return_code: 200,
        health_check_interval: 10,
        health_check_timeout: 5,
        health_check_retries: 30,
        health_check_start_period: role === "runtime" ? 30 : 15,
      }),
    });
    // Coolify 4.1.2 stores this flag on the application_settings relation but
    // omits that relation from GET /applications/{uuid}. Its PATCH endpoint
    // saves the relation before returning the updated application's UUID, so
    // validate that write acknowledgement instead of pretending an omitted
    // GET field is a read-back verification.
    verifyApplicationUpdateAcknowledgement(updated, role, uuid);
    const saved = await coolifyRequest(base, token, `/applications/${encodeURIComponent(uuid)}`);
    verifyRollingApplication(saved, role, sha);
    const savedEnvironment = await coolifyRequest(
      base,
      token,
      `/applications/${encodeURIComponent(uuid)}/envs`,
    );
    if (role === "runtime") verifyRuntimeEnvironment(savedEnvironment, sha, expectedRequireAuth);
    else verifyDashboardEnvironment(savedEnvironment);

    const queued = await coolifyRequest(
      base,
      token,
      `/deploy?uuid=${encodeURIComponent(uuid)}&force=true`,
      { method: "POST" },
    );
    const deploymentUUID = queued?.deployments?.[0]?.deployment_uuid;
    if (!deploymentUUID) throw new Error(`Coolify did not return a ${role} deployment UUID`);
    await waitForDeployment(base, token, deploymentUUID, waitOptions);

    const deployed = await coolifyRequest(base, token, `/applications/${encodeURIComponent(uuid)}`);
    verifyRollingApplication(deployed, role, sha);
    const deployedEnvironment = await coolifyRequest(
      base,
      token,
      `/applications/${encodeURIComponent(uuid)}/envs`,
    );
    if (role === "runtime") verifyRuntimeEnvironment(deployedEnvironment, sha, expectedRequireAuth);
    else verifyDashboardEnvironment(deployedEnvironment);
    if (deployed.status !== "running:healthy") {
      throw new Error(`${role} ended deployment as ${deployed.status ?? "unknown"}`);
    }
    console.log(`Deployed rolling Gonvex ${role} ${uuid} at ${sha}`);
  }
}

async function main() {
  const base = apiBase(requiredEnvironment("COOLIFY_API_URL"));
  const token = requiredEnvironment("COOLIFY_API_TOKEN");
  const sha = requiredEnvironment("GONVEX_DEPLOY_SHA");
  await deployRollingApplications({
    base,
    token,
    sha,
    applications: {
      runtime: requiredEnvironment("COOLIFY_RUNTIME_APPLICATION_UUID"),
      dashboard: requiredEnvironment("COOLIFY_DASHBOARD_APPLICATION_UUID"),
    },
    expectedRequireAuth: requiredBooleanEnvironment("GONVEX_EXPECT_REQUIRE_AUTH"),
    autoDeploy: requiredBooleanEnvironment("GONVEX_COOLIFY_AUTO_DEPLOY"),
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
