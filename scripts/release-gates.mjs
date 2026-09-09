import { pathToFileURL } from "node:url";

const fullSHA = /^[0-9a-f]{40}$/;

function requiredEnvironment(name) {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function apiBase(value) {
  const base = value.replace(/\/$/, "");
  return base.endsWith("/api/v1") ? base : `${base}/api/v1`;
}

function assertAncestorComparison(comparison, label) {
  const validStatus = comparison?.status === "ahead" || comparison?.status === "identical";
  if (!validStatus || Number(comparison?.behind_by ?? 0) !== 0) {
    throw new Error(`${label} is not a fast-forward`);
  }
}

export function assertPromotionCandidate({
  sha,
  productionSha,
  mainComparison,
  productionComparison,
  checkRuns,
  devApplications,
  devRuntimeHealth,
}) {
  if (!fullSHA.test(sha)) throw new Error("promotion SHA must be a full lowercase Git SHA");
  if (!fullSHA.test(productionSha)) throw new Error("production SHA must be a full lowercase Git SHA");
  assertAncestorComparison(mainComparison, `${sha} from main`);
  assertAncestorComparison(productionComparison, `production to ${sha}`);

  const ci = checkRuns?.find((run) => run?.name === "CI");
  if (ci?.status !== "completed" || ci?.conclusion !== "success") {
    throw new Error(`${sha} does not have a successful CI check`);
  }
  const compatibility = checkRuns?.find((run) => run?.name === "Whagons compatibility");
  if (compatibility?.status !== "completed" || compatibility?.conclusion !== "success") {
    throw new Error(`${sha} does not have a successful Whagons compatibility check`);
  }
  assertRuntimeHealth(devRuntimeHealth, sha);
  for (const role of ["runtime", "dashboard"]) {
    const application = devApplications?.[role];
    if (application?.status !== "running:healthy") {
      throw new Error(`dev ${role} is not healthy`);
    }
  }
}

export function assertRuntimeHealth(payload, sha) {
  if (payload?.ok !== true) throw new Error("production runtime is not healthy");
  if (payload?.version !== sha) {
    throw new Error(`production runtime version ${payload?.version ?? "missing"} does not match ${sha}`);
  }
}

export function assertDashboardHealth(body) {
  if (String(body).trim() !== "ok") throw new Error("production dashboard is not healthy");
}

async function getJSON(fetchImpl, url, headers) {
  const response = await fetchImpl(url, { headers });
  if (!response.ok) throw new Error(`GET ${url} failed (${response.status})`);
  return response.json();
}

export async function verifyPromotionCandidate({
  githubAPI = "https://api.github.com",
  githubRepository,
  githubToken,
  sha,
  productionSha,
  coolifyAPI,
  coolifyToken,
  runtimeApplicationUUID,
  dashboardApplicationUUID,
  runtimeHealthURL,
  fetchImpl = fetch,
}) {
  const githubHeaders = {
    Accept: "application/vnd.github+json",
    Authorization: `Bearer ${githubToken}`,
    "X-GitHub-Api-Version": "2022-11-28",
  };
  const coolifyHeaders = {
    Accept: "application/json",
    Authorization: `Bearer ${coolifyToken}`,
  };
  const repository = githubRepository.split("/").map(encodeURIComponent).join("/");
  const coolify = apiBase(coolifyAPI);
  const [mainComparison, productionComparison, checks, runtime, dashboard, runtimeHealth] = await Promise.all([
    getJSON(fetchImpl, `${githubAPI}/repos/${repository}/compare/${sha}...main`, githubHeaders),
    getJSON(fetchImpl, `${githubAPI}/repos/${repository}/compare/${productionSha}...${sha}`, githubHeaders),
    getJSON(fetchImpl, `${githubAPI}/repos/${repository}/commits/${sha}/check-runs?filter=latest&per_page=100`, githubHeaders),
    getJSON(fetchImpl, `${coolify}/applications/${encodeURIComponent(runtimeApplicationUUID)}`, coolifyHeaders),
    getJSON(fetchImpl, `${coolify}/applications/${encodeURIComponent(dashboardApplicationUUID)}`, coolifyHeaders),
    getJSON(fetchImpl, new URL("/healthz", runtimeHealthURL), {}),
  ]);
  assertPromotionCandidate({
    sha,
    productionSha,
    mainComparison,
    productionComparison,
    checkRuns: checks.check_runs,
    devApplications: { runtime, dashboard },
    devRuntimeHealth: runtimeHealth,
  });
}

export async function verifyDeployedEndpoints({ runtimeURL, dashboardURL, sha, fetchImpl = fetch }) {
  const [runtimeResponse, dashboardResponse] = await Promise.all([
    fetchImpl(new URL("/healthz", runtimeURL)),
    fetchImpl(new URL("/healthz", dashboardURL)),
  ]);
  if (!runtimeResponse.ok) throw new Error(`runtime health request failed (${runtimeResponse.status})`);
  if (!dashboardResponse.ok) throw new Error(`dashboard health request failed (${dashboardResponse.status})`);
  assertRuntimeHealth(await runtimeResponse.json(), sha);
  assertDashboardHealth(await dashboardResponse.text());
}

async function main() {
  const mode = process.argv[2];
  if (mode === "candidate") {
    await verifyPromotionCandidate({
      githubRepository: requiredEnvironment("GITHUB_REPOSITORY"),
      githubToken: requiredEnvironment("GITHUB_TOKEN"),
      sha: requiredEnvironment("GONVEX_DEPLOY_SHA"),
      productionSha: requiredEnvironment("GONVEX_PRODUCTION_SHA"),
      coolifyAPI: requiredEnvironment("COOLIFY_API_URL"),
      coolifyToken: requiredEnvironment("COOLIFY_API_TOKEN"),
      runtimeApplicationUUID: requiredEnvironment("COOLIFY_RUNTIME_APPLICATION_UUID"),
      dashboardApplicationUUID: requiredEnvironment("COOLIFY_DASHBOARD_APPLICATION_UUID"),
      runtimeHealthURL: requiredEnvironment("GONVEX_RUNTIME_HEALTH_URL"),
    });
    console.log(`Promotion candidate ${process.env.GONVEX_DEPLOY_SHA} passed CI, ancestry, and dev gates`);
    return;
  }
  if (mode === "deployment") {
    await verifyDeployedEndpoints({
      runtimeURL: requiredEnvironment("GONVEX_RUNTIME_HEALTH_URL"),
      dashboardURL: requiredEnvironment("GONVEX_DASHBOARD_HEALTH_URL"),
      sha: requiredEnvironment("GONVEX_DEPLOY_SHA"),
    });
    console.log(`Production endpoints are healthy at ${process.env.GONVEX_DEPLOY_SHA}`);
    return;
  }
  throw new Error("usage: node scripts/release-gates.mjs candidate|deployment");
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
