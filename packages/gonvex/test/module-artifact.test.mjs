import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { execFileSync } from "node:child_process";
import { pathToFileURL, fileURLToPath } from "node:url";
import test from "node:test";

import { buildModuleArtifact, moduleManifestFunctions } from "../dist/module-artifact.js";
import { renderFunctionCatalog } from "../dist/function-catalog.js";

async function moduleProject(t, source, supportingFiles = {}) {
  const root = await mkdtemp(join(tmpdir(), "gonvex-module-artifact-"));
  const backendDir = join(root, "gonvex");
  await mkdir(backendDir);
  const entrypoint = join(backendDir, "index.ts");
  await writeFile(entrypoint, source);
  for (const [name, contents] of Object.entries(supportingFiles)) {
    await writeFile(join(backendDir, name), contents);
  }
  t.after(() => rm(root, { recursive: true, force: true }));
  return { root, backendDir, entrypoint };
}

test("TypeScript artifacts are self-contained ESM and preserve reducer and Live Query contracts", async (t) => {
  const project = await moduleProject(t, `
import { suffix } from "./shared.ts";
const schema = {
  string: (options = {}) => ({ kind: "string", ...options }),
  integer: (options = {}) => ({ kind: "number", integer: true, ...options }),
  boolean: () => ({ kind: "boolean" }),
  id: (entity) => ({ kind: "id", entity }),
  array: (items) => ({ kind: "array", items }),
  object: (fields, options = {}) => ({ kind: "object", fields, ...options }),
};

const liveQuery = (definition: unknown) => definition;
const query = (definition: unknown) => definition;
const reducer = (definition: unknown) => definition;
const visibility = (definition: unknown) => definition;
type GridArgs = { workspaceId: string };
type GridRow = { id: string };
type RenameArgs = { taskId: string; title: string };
type RenameResult = { ok: boolean };

export const taskVisibility = visibility({
  table: "tasks",
  key: "id",
  sets: {},
  where: { operator: "public" },
});

export const grid = liveQuery<GridArgs, GridRow[]>({
  args: schema.object({
    workspaceId: schema.string(),
    offset: schema.integer(),
    limit: schema.integer(),
  }),
  result: schema.array(schema.object({ id: schema.id("tasks") })),
  liveQueryPlan: {
    table: "tasks",
    key: "id",
    columns: ["id", "title", "workspaceId"],
    where: { operator: "eq", column: "workspaceId", value: { argument: "workspaceId" } },
    window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 100, maxLimit: 200 },
  },
  run: async (_ctx: unknown, args: { workspaceId: string }) => [{ id: args.workspaceId + suffix }],
});

export const oneShot = query<GridArgs, GridRow[]>({
  args: schema.object({ workspaceId: schema.string() }),
  result: schema.array(schema.object({ id: schema.id("tasks") })),
  liveQueryPlan: {
    table: "tasks",
    key: "id",
    columns: ["id", "workspaceId", "deletedAt"],
    where: { operator: "eq", column: "deletedAt", value: { literal: null } },
  },
  run: async () => [],
});

export const rename = reducer<RenameArgs, RenameResult>({
  args: schema.object({ taskId: schema.id("tasks"), title: schema.string({ minLength: 1 }) }),
  result: schema.object({ ok: schema.boolean() }),
  offline: { mode: "allowed", conflict: "expectedVersion" },
  optimistic: {
    effects: [{ operation: "patch", entity: "tasks", id: ["taskId"], fields: { title: "pending" } }],
  },
  run: async () => ({ ok: true }),
});
`, { "shared.ts": "export const suffix = '-bundled';\n" });

  const artifact = await buildModuleArtifact({
    root: project.root,
    backendDir: project.backendDir,
    files: [project.entrypoint, join(project.backendDir, "shared.ts")],
    migrations: [],
  });

  const artifactFile = join(project.root, "artifact.json");
  await writeFile(artifactFile, JSON.stringify(artifact));
  const repositoryRoot = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
  const verifiedHash = execFileSync("go", ["run", "./cmd/gonvex", "internal", "verify-module-artifact", "--file", artifactFile], {
    cwd: repositoryRoot,
    encoding: "utf8",
  }).trim();
  assert.equal(verifiedHash, artifact.hash, "TypeScript CLI and migration verifier must hash the same artifact contract");
  let rustVerifiedHash;
  try {
    rustVerifiedHash = execFileSync("cargo", [
      "run", "--quiet", "--manifest-path", join(repositoryRoot, "rust", "Cargo.toml"),
      "-p", "gonvex-runtime", "--", "verify-module-artifact", "--file", artifactFile,
    ], { cwd: repositoryRoot, encoding: "utf8" }).trim();
  } catch (error) {
    throw new Error(`Rust artifact verifier failed (status ${String(error?.status)}): ${String(error?.stdout ?? "")} ${String(error?.stderr ?? error)}`);
  }
  assert.equal(rustVerifiedHash, artifact.hash, "TypeScript CLI and Rust runtime must hash the same artifact contract");
  const tamperedArtifacts = [
    { ...artifact, entrypoint: "gonvex/tampered.ts" },
    { ...artifact, files: { ...artifact.files, "gonvex/extra.ts": "export {};" } },
    { ...artifact, functions: { ...artifact.functions, grid: { ...artifact.functions.grid, handler: "differentHandler" } } },
    { ...artifact, visibility: { ...artifact.visibility, tasks: { ...artifact.visibility.tasks, key: "anotherId" } } },
    { ...artifact, crons: [{ name: "tampered", function: "rename", scope: "project", intervalMs: 60_000 }] },
    { ...artifact, javascript: { ...artifact.javascript, path: "gonvex/_build/another.js" } },
    { ...artifact, invitationAcceptanceReducer: "rename" },
  ];
  for (const tampered of tamperedArtifacts) {
    await writeFile(artifactFile, JSON.stringify(tampered));
    assert.throws(() => execFileSync("go", ["run", "./cmd/gonvex", "internal", "verify-module-artifact", "--file", artifactFile], {
      cwd: repositoryRoot,
      stdio: "pipe",
    }), /Command failed/);
  }
  await writeFile(artifactFile, JSON.stringify(tamperedArtifacts[0]));
  assert.throws(() => execFileSync("cargo", [
    "run", "--quiet", "--manifest-path", join(repositoryRoot, "rust", "Cargo.toml"),
    "-p", "gonvex-runtime", "--", "verify-module-artifact", "--file", artifactFile,
  ], { cwd: repositoryRoot, stdio: "pipe" }), /Command failed/);

  assert.equal(artifact.javascript?.path, "gonvex/_build/module.js");
  const bundled = Buffer.from(artifact.javascript.code, "base64").toString("utf8");
  assert.match(bundled, /-bundled/);
  assert.doesNotMatch(bundled, /from\s+["']\.\/shared/);
  assert.equal(await readFile(join(project.backendDir, "_build", "module.js"), "utf8"), bundled);

  const functions = moduleManifestFunctions(artifact);
  assert.equal(functions.grid.kind, "query");
  assert.equal(functions.grid.delivery, "live");
  assert.equal(functions.grid.dependencies.liveQueryPlan.table, "tasks");
  assert.equal(functions.oneShot.delivery, "oneShot");
  assert.equal(functions.oneShot.dependencies.liveQueryPlan.table, "tasks");
  assert.deepEqual(functions.oneShot.dependencies.liveQueryPlan.where.value, { literal: null });
  assert.deepEqual(artifact.visibility.tasks, {
    table: "tasks",
    key: "id",
    sets: {},
    where: { operator: "public" },
  });
  assert.deepEqual(functions.rename.offline, { mode: "allowed", conflict: "expectedVersion" });
  assert.deepEqual(functions.rename.optimistic.effects[0], {
    operation: "patch",
    entity: "tasks",
    id: ["taskId"],
    fields: { title: "pending" },
  });
  assert.deepEqual(functions.grid.args, {
    kind: "object",
    fields: {
      workspaceId: { kind: "string" },
      offset: { kind: "number", integer: true },
      limit: { kind: "number", integer: true },
    },
  });
  assert.deepEqual(functions.grid.result, {
    kind: "array",
    items: { kind: "object", fields: { id: { kind: "id", entity: "tasks" } } },
  });
  assert.deepEqual(functions.rename.args, {
    kind: "object",
    fields: {
      taskId: { kind: "id", entity: "tasks" },
      title: { kind: "string", minLength: 1 },
    },
  });
  assert.deepEqual(functions.rename.result, { kind: "object", fields: { ok: { kind: "boolean" } } });
});

test("literal agent metadata is signed and checkout-path independent", async (t) => {
  const source = `
const schema = { object: (fields) => ({ kind: "object", fields }), string: () => ({ kind: "string" }) };
const reducer = (definition) => definition;
const action = (definition) => definition;
export const start = reducer({
  interactive: true,
  description: "Start a task",
  agent: { tags: ["workflow", "tasks"], confirmation: "required" },
  args: schema.object({ taskId: schema.string() }),
  result: schema.object({ taskId: schema.string() }),
  offline: { mode: "onlineOnly", reason: "test" },
  nonOptimisticReason: "test",
  run: async (_ctx, args) => args,
});
export const callback = action({
  args: schema.object({}),
  result: schema.object({}),
  run: async () => ({}),
});
`;
  const left = await moduleProject(t, source);
  const right = await moduleProject(t, source);
  const leftArtifact = await buildModuleArtifact({ root: left.root, backendDir: left.backendDir, files: [left.entrypoint], migrations: [] });
  const rightArtifact = await buildModuleArtifact({ root: right.root, backendDir: right.backendDir, files: [right.entrypoint], migrations: [] });
  assert.equal(leftArtifact.hash, rightArtifact.hash);
  assert.equal(renderFunctionCatalog(leftArtifact, "ndjson"), renderFunctionCatalog(rightArtifact, "ndjson"));
  assert.equal(renderFunctionCatalog(leftArtifact, "typescript"), renderFunctionCatalog(rightArtifact, "typescript"));
  assert.deepEqual(leftArtifact.functions.start.agent, { tags: ["tasks", "workflow"], confirmation: "required" });
  assert.equal(leftArtifact.functions.start.classification, "interactive");
  assert.equal(leftArtifact.functions.callback.classification, "system");
  await writeFile(left.entrypoint, source.replace("Start a task", "Begin a task"));
  const changed = await buildModuleArtifact({ root: left.root, backendDir: left.backendDir, files: [left.entrypoint], migrations: [] });
  assert.notEqual(changed.hash, leftArtifact.hash);
});

test("agent classification metadata rejects values that static extraction cannot prove", async (t) => {
  for (const declaration of [
    "interactive: runtimeChoice",
    "internal: runtimeChoice",
    "description: runtimeDescription",
    "agent: runtimeAgentMetadata",
  ]) {
    const project = await moduleProject(t, `
const schema = { object: (fields) => ({ kind: "object", fields }) };
const reducer = (definition) => definition;
const runtimeChoice = true;
const runtimeDescription = "unsafe";
const runtimeAgentMetadata = { tags: ["unsafe"], confirmation: "none" };
export const update = reducer({
  ${declaration},
  args: schema.object({}),
  result: schema.object({}),
  offline: { mode: "onlineOnly", reason: "test" },
  nonOptimisticReason: "test",
  run: async () => ({}),
});
`);
    await assert.rejects(
      buildModuleArtifact({ root: project.root, backendDir: project.backendDir, files: [project.entrypoint], migrations: [] }),
      /must be (?:a boolean literal|a string literal|an object literal)/,
    );
  }
});

test("Replica delivery requires the canonical replica definition", async (t) => {
  const project = await moduleProject(t, `
const replicaCollection = (definition) => definition;
const schema = {
  object: (fields) => ({ kind: "object", fields }),
  array: (items) => ({ kind: "array", items }),
};
export const broken = replicaCollection({
  args: schema.object({}),
  result: schema.array(schema.object({})),
  run: async () => [],
});
`);
  await assert.rejects(
    buildModuleArtifact({ root: project.root, backendDir: project.backendDir, files: [project.entrypoint], migrations: [] }),
    /requires a replica definition/,
  );
});

test("TypeScript artifacts retain explicit visibility self-join aliases", async (t) => {
  const project = await moduleProject(t, `
const visibility = (definition) => definition;
export const locationVisibility = visibility({
  table: "userLiveLocations",
  key: "id",
  sets: {
    teammates: {
      table: "memberTeams",
      alias: "viewerTeams",
      select: "memberId",
      selectFrom: "peerTeams",
      joins: [{
        table: "memberTeams",
        alias: "peerTeams",
        leftAlias: "viewerTeams",
        leftColumn: "teamId",
        rightColumn: "teamId",
      }],
      where: [{ table: "viewerTeams", column: "memberId", context: "member.id" }],
    },
  },
  where: { operator: "inSet", column: "memberId", set: "teammates" },
});
`);
  const artifact = await buildModuleArtifact({ root: project.root, backendDir: project.backendDir, files: [project.entrypoint], migrations: [] });
  assert.deepEqual(artifact.visibility.userLiveLocations.sets.teammates, {
    table: "memberTeams",
    alias: "viewerTeams",
    select: "memberId",
    selectFrom: "peerTeams",
    joins: [{
      table: "memberTeams",
      alias: "peerTeams",
      leftAlias: "viewerTeams",
      leftColumn: "teamId",
      rightColumn: "teamId",
    }],
    where: [{ table: "viewerTeams", column: "memberId", context: "member.id" }],
  });
});

test("TypeScript artifacts reject missing and non-static function schemas", async (t) => {
  const missing = await moduleProject(t, `
const query = (definition: unknown) => definition;
export const list = query({ run: async () => [] });
`);
  await assert.rejects(
    buildModuleArtifact({ root: missing.root, backendDir: missing.backendDir, files: [missing.entrypoint], migrations: [] }),
    /must declare literal args and result schemas|must declare args: schema/,
  );

  const dynamic = await moduleProject(t, `
const query = (definition: unknown) => definition;
const args = { kind: "object" };
export const list = query({ args, result: schema.string(), run: async () => "ok" });
`);
  await assert.rejects(
    buildModuleArtifact({ root: dynamic.root, backendDir: dynamic.backendDir, files: [dynamic.entrypoint], migrations: [] }),
    /must use a static schema/,
  );
});

test("TypeScript artifacts reject Node built-ins", async (t) => {
  const project = await moduleProject(t, `
import { readFile } from "node:fs/promises";
export const unsafe = async () => readFile("secret");
`);

  await assert.rejects(
    buildModuleArtifact({
      root: project.root,
      backendDir: project.backendDir,
      files: [project.entrypoint],
      migrations: [],
    }),
    /Node runtime module.*node:fs\/promises.*unavailable/,
  );
});

test("TypeScript artifacts extract literal project and tenant cron declarations", async (t) => {
  const project = await moduleProject(t, `
const schema = { object: (fields) => ({ kind: "object", fields }), boolean: () => ({ kind: "boolean" }) };
const internalReducer = (definition) => definition;
const cron = (definition) => definition;
const tenantCron = (definition) => definition;
export const heartbeat = internalReducer({
  args: schema.object({}),
  result: schema.object({ ok: schema.boolean() }),
  run: async () => ({ ok: true }),
});

export const heartbeatSchedule = cron({ name: "heartbeat", function: "heartbeat", intervalMs: 15000 });
export const tenantSchedule = tenantCron({ name: "tenant-heartbeat", function: "heartbeat", args: { reason: "test" }, expression: "*/5 * * * *" });
`);

  const artifact = await buildModuleArtifact({
    root: project.root,
    backendDir: project.backendDir,
    files: [project.entrypoint],
    migrations: [],
  });

  assert.deepEqual(artifact.crons, [
    { name: "heartbeat", function: "heartbeat", scope: "project", intervalMs: 15000 },
    { name: "tenant-heartbeat", function: "heartbeat", scope: "tenant", args: { reason: "test" }, expression: "*/5 * * * *" },
  ]);
});

test("TypeScript artifacts declare one internal invitation acceptance Reducer", async (t) => {
  const project = await moduleProject(t, `
const schema = { object: (fields) => ({ kind: "object", fields }), string: () => ({ kind: "string" }) };
const internalReducer = (definition) => definition;
const invitationAcceptance = (path) => ({ reducer: path });
export const acceptInvitation = internalReducer({ args: schema.object({}), result: schema.string(), run: async () => "ok" });
export const invitationLifecycle = invitationAcceptance("acceptInvitation");
`);
  const artifact = await buildModuleArtifact({root:project.root,backendDir:project.backendDir,files:[project.entrypoint],migrations:[]});
  assert.equal(artifact.invitationAcceptanceReducer,"acceptInvitation");
});

test("ModuleBuilder artifacts retain executable default registrations and builder crons", async (t) => {
  const sdk = resolve(fileURLToPath(new URL("../../module-sdk/dist/index.js", import.meta.url)));
  const project = await moduleProject(t, `
import { createModule, schema } from ${JSON.stringify(pathToFileURL(sdk).href)};
const app = createModule({ name: "builder-app", version: "1" });
app.action("reports.daily", {
  args: schema.object({}),
  result: schema.object({ ok: schema.boolean() }),
  run: async () => ({ ok: true }),
});
app.internalReducer("tasks.expire", {
  args: schema.object({}),
  result: schema.object({ ok: schema.boolean() }),
  run: async () => ({ ok: true }),
});
app.cron({ name: "daily-report", intervalMs: 60_000, function: "reports.daily" });
app.tenantCron({ name: "tenant-expiry", expression: "*/5 * * * *", function: "tasks.expire", args: { source: "cron" } });
export default app;
`);
  const artifact = await buildModuleArtifact({
    root: project.root,
    backendDir: project.backendDir,
    files: [project.entrypoint],
    migrations: [],
  });

  assert.deepEqual(artifact.crons, [
    { name: "daily-report", function: "reports.daily", scope: "project", intervalMs: 60_000 },
    { name: "tenant-expiry", function: "tasks.expire", scope: "tenant", args: { source: "cron" }, expression: "*/5 * * * *" },
  ]);
  assert.deepEqual(Object.keys(artifact.functions), ["reports.daily", "tasks.expire"]);

  const bundled = await import(`${pathToFileURL(join(project.backendDir, "_build", "module.js"))}?test=${Date.now()}`);
  const registrations = bundled.default.runtimeRegistrations();
  assert.equal(registrations.length, 2);
  assert.equal(registrations.find((entry) => entry.path === "reports.daily").definition.kind, "action");
  assert.deepEqual(await registrations.find((entry) => entry.path === "reports.daily").handler({}, {}), { ok: true });
});

test("TypeScript artifacts reject optional schemas outside object fields", async (t) => {
  const project = await moduleProject(t, `
const schema = {
  optional: (value) => ({ kind: "optional", value }),
  string: () => ({ kind: "string" }),
};
const action = (definition) => definition;
export const broken = action({
  args: schema.optional(schema.string()),
  result: schema.string(),
  run: async () => "ok",
});
`);
  await assert.rejects(
    buildModuleArtifact({ root: project.root, backendDir: project.backendDir, files: [project.entrypoint], migrations: [] }),
    /uses schema\.optional outside an object field/,
  );
});

test("TypeScript artifacts sign internal Query and agent Action capabilities", async (t) => {
  const project = await moduleProject(t, `
const schema = { string: () => ({ kind: "string" }), any: () => ({ kind: "any" }), object: (fields) => ({ kind: "object", fields }) };
const internalQuery = (definition) => definition;
const action = (definition) => definition;
export const searchTasks = internalQuery({
  args: schema.object({ search: schema.string() }), result: schema.any(),
  liveQueryPlan: { table: "tasks", key: "id", columns: ["id", "title"] },
  run: async () => [],
});
export const run = action({
  profile: "agent",
  capabilities: {
    networkOrigins: ["https://api.openai.com"],
    secrets: ["OPENAI_API_KEY"],
    storage: true,
    sandbox: { duckdb: true },
    tools: { searchTasks: { kind: "query", function: "searchTasks" } },
  },
  args: schema.object({ prompt: schema.string() }), result: schema.any(), run: async () => null,
});
`);
  const artifact = await buildModuleArtifact({ root: project.root, backendDir: project.backendDir, files: [project.entrypoint], migrations: [] });
  assert.equal(artifact.generation, 8);
  assert.equal(artifact.functions.searchTasks.internal, true);
  assert.equal(artifact.functions.run.actionProfile, "agent");
  assert.deepEqual(artifact.functions.run.actionCapabilities.tools.searchTasks, { kind: "query", function: "searchTasks" });
  assert.deepEqual(moduleManifestFunctions(artifact).run.actionCapabilities.networkOrigins, ["https://api.openai.com"]);
  assert.deepEqual(moduleManifestFunctions(artifact).run.actionCapabilities.sandbox, { duckdb: true });
});
