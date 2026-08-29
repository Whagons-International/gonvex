#!/usr/bin/env node
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { existsSync, readFileSync, realpathSync } from "node:fs";
import { chmod, mkdir, readFile, readdir, rm, stat, writeFile, copyFile } from "node:fs/promises";
import { homedir } from "node:os";
import { createInterface } from "node:readline/promises";
import { dirname, join, relative, resolve } from "node:path";
import { Writable } from "node:stream";
import { fileURLToPath } from "node:url";

import {
  buildModuleArtifact,
  detectProjectLanguage,
  moduleManifestFunctions,
  moduleSourceFiles,
  isModuleSchema,
  type ProjectLanguage,
} from "./module-artifact.js";
import { renderFunctionCatalog, type FunctionCatalogFormat } from "./function-catalog.js";
import type {
  FunctionEntry,
  JsonValue,
  Manifest,
  ModuleSchema,
  SchemaDefinition,
  Table,
} from "./manifest-types.js";

export type * from "./manifest-types.js";
export type { ProjectLanguage } from "./module-artifact.js";
export * from "./function-catalog.js";

type BackendSources = {
  moduleFiles: string[];
  config: ProjectConfig;
};

type Settings = {
  projectID: string;
  runtimeURL: string;
  key: string;
};

type ProjectConfig = {
  project?: string;
  runtime?: string;
  backendDir?: string;
  generatedDir?: string;
  tenantMode?: string;
  /** Gonvex v2 accepts only "typescript". */
  language?: string;
  module?: {
    /** Project-relative module entrypoint. */
    entrypoint?: string;
    /** Project-relative ESM output path under gonvex/_build. */
    bundle?: string;
  };
  auth?: {
    providers?: {
      google?: {
        enabled?: boolean;
        callbackPath?: string;
        redirectUris?: string[];
        signupMode?: "personal" | "inviteOnly";
      };
    };
  };
  [key: string]: unknown;
};

type ProjectGoogleAuth = {
  provider: "google";
  enabled: boolean;
  redirectUris?: string[];
  runtimeConfigured?: boolean;
  ready?: boolean;
  issues?: string[];
  brokerCallbackUrl?: string;
  signupMode?: "personal" | "inviteOnly";
  tenantCount?: number;
  membershipCount?: number;
  invitationCount?: number;
  databaseMode?: string;
};

type AppAuthAccount = {
  id: string;
  email?: string;
  name?: string;
  provider: string;
  createdAt?: string;
  lastSignedInAt?: string;
};

type RuntimeProject = {
  id: string;
  name: string;
  environment?: string;
  database?: string;
  databaseMode?: string;
  storageBucket?: string;
  status?: string;
  description?: string;
  provisioned?: boolean;
  runtimeCreated?: boolean;
  ownerEmail?: string;
  role?: string;
};

type CreatedRuntimeProject = {
  project: RuntimeProject;
  projectKey: string;
};

type AccountIdentity = {
  account: {
    email: string;
    name: string;
    role: string;
  };
  authentication: string;
  permissions: string[];
};

type AccountProfile = {
  runtimeURL: string;
  accessToken: string;
  email: string;
  name: string;
  authentication: string;
  permissions: string[];
  expiresAt?: number;
};

type CLIConfig = {
  version: 1;
  currentRuntime?: string;
  runtimes: Record<string, AccountProfile>;
};

type AccountAccessToken = {
  id: string;
  name: string;
  prefix: string;
  permissions: string[];
  createdAt: string;
  expiresAt?: string;
  lastUsedAt?: string;
  revokedAt?: string;
};

type RuntimeEnvVariable = {
  name: string;
  value?: string;
  masked: string;
  source: string;
  sensitive: boolean;
};

type RuntimeLogEntry = {
  time?: string;
  project?: string;
  path?: string;
  kind?: string;
  outcome?: string;
  durationMs?: number;
  error?: string;
  cache?: string;
};

type RuntimeLogOptions = {
  verbose: boolean;
};

type WatchState = {
  lastFingerprint: string;
  lastManifest: Manifest | null;
  lastSyncAttempt: number;
  lastSyncSucceeded: boolean;
  lastRuntimeCheck: number;
};

type BindingWriteResult = {
  changedFiles: number;
};

type FunctionDiff = {
  added: string[];
  removed: string[];
  edited: string[];
};

const defaultRuntimeURL = "http://localhost:8080";
const runtimeSyncRetryMs = 5000;
const runtimeStateCheckMs = 2500;
const supportsColor = process.env.NO_COLOR === undefined && (process.env.FORCE_COLOR !== undefined || process.stdout.isTTY);
const defaultAccountTokenPermissions = ["projects:read", "projects:create", "projects:keys:read"];
const accountTokenPermissions = [
  "projects:read",
  "projects:create",
  "projects:update",
  "projects:delete",
  "projects:keys:read",
  "projects:keys:write",
  "projects:members:read",
  "projects:members:write",
  "projects:env:read",
  "projects:env:write",
  "projects:*",
  "admin:projects",
  "tokens:read",
  "tokens:create",
  "tokens:revoke",
  "tokens:*",
  "*",
];

function ansi(code: string, value: string) {
  return supportsColor ? `\x1b[${code}m${value}\x1b[0m` : value;
}

const color = {
  dim: (value: string) => ansi("2", value),
  green: (value: string) => ansi("32", value),
  orange: (value: string) => ansi("38;5;208", value),
  red: (value: string) => ansi("31", value),
  yellow: (value: string) => ansi("33", value),
};

export async function main(argv = process.argv.slice(2)) {
  const command = argv[0];
  if (!command || command === "help" || command === "--help") {
    printHelp();
    return;
  }

  if (command === "dev") {
    await runDev(argv.slice(1));
    return;
  }
  if (command === "codegen") {
    await runCodegen(argv.slice(1));
    return;
  }
  if (command === "functions") {
    await runFunctions(argv.slice(1));
    return;
  }
  if (command === "init") {
    await runInit(argv.slice(1));
    return;
  }
  if (command === "create") {
    await runCreate(argv.slice(1));
    return;
  }
  if (command === "env") {
    await runEnv(argv.slice(1));
    return;
  }
  if (command === "auth") {
    await runAuth(argv.slice(1));
    return;
  }
  if (command === "login") {
    await runLogin(argv.slice(1));
    return;
  }
  if (command === "logout") {
    await runLogout(argv.slice(1));
    return;
  }
  if (command === "whoami") {
    await runWhoAmI(argv.slice(1));
    return;
  }
  if (command === "token" || command === "tokens") {
    await runToken(argv.slice(1));
    return;
  }
  if (command === "project") {
    await runProject(argv.slice(1));
    return;
  }

  printHelp();
  throw new Error(`unknown command ${command}`);
}

async function runFunctions(argv: string[]) {
  const action = argv[0];
  if (!action || !["emit", "check"].includes(action)) {
    throw new Error("usage: gonvex functions <emit|check> [--format ndjson|typescript] [--output FILE] [--project PATH]");
  }
  const root = resolve(valueFor(argv, "--project") ?? ".");
  const sources = await collectBackendSources(root);
  const settings = await loadSettings(root, {});
  const manifest = await buildManifest(root, sources, settings.projectID);
  const targets: Array<{ format: FunctionCatalogFormat; output: string }> = action === "check"
    ? [
      { format: "ndjson", output: resolve(root, "agent-api.ndjson") },
      { format: "typescript", output: resolve(root, "agent-api.d.ts") },
    ]
    : [{
      format: normalizeCatalogFormat(valueFor(argv, "--format") ?? "ndjson"),
      output: resolve(root, valueFor(argv, "--output") ?? (valueFor(argv, "--format") === "typescript" ? "agent-api.d.ts" : "agent-api.ndjson")),
    }];
  for (const target of targets) {
    const expected = renderFunctionCatalog(manifest.module, target.format);
    if (action === "check") {
      const actual = await readFile(target.output, "utf8").catch(() => "");
      if (actual !== expected) throw new Error(`${relative(root, target.output)} is stale; run gonvex functions emit --format ${target.format} --output ${relative(root, target.output)}`);
      continue;
    }
    await mkdir(dirname(target.output), { recursive: true });
    await writeFile(target.output, expected);
    console.log(`[gonvex] wrote ${relative(root, target.output)} for artifact ${manifest.module.hash}`);
  }
}

function normalizeCatalogFormat(value: string): FunctionCatalogFormat {
  if (value === "ndjson" || value === "typescript") return value;
  throw new Error("--format must be ndjson or typescript");
}

export async function runCreate(argv: string[]) {
  const optionNames = ["--template", "--runtime-url", "--runtime", "--database-mode", "--origin", "--callback-path", "--signup-mode", "--owner"];
  const target = positionalArgs(argv, optionNames)[0] ?? "my-gonvex-app";
  const appName = basename(target);
  const template = valueFor(argv, "--template") ?? "vite-react";
  const runtimeURL = (valueFor(argv, "--runtime-url") ?? valueFor(argv, "--runtime") ?? defaultRuntimeURL).replace(/\/$/, "");
  const databaseMode = valueFor(argv, "--database-mode") ?? "single";
  if (databaseMode !== "single" && databaseMode !== "multiTenant") throw new Error("--database-mode must be single or multiTenant");
  const googleAuth = argv.includes("--google-auth");
  const signupMode = normalizeAuthSignupMode(valueFor(argv, "--signup-mode") ?? "personal");
  const ownerEmail = valueFor(argv, "--owner")?.trim() ?? "";
  if (googleAuth && signupMode === "inviteOnly" && !ownerEmail) {
    throw new Error("--owner <verified-google-email> is required when creating an invite-only Google app");
  }
  const shouldProvision = googleAuth || argv.includes("--provision") || Boolean(valueFor(argv, "--runtime-url") ?? valueFor(argv, "--runtime"));
  const root = resolve(target);
  if (existsSync(root)) throw new Error(`${target} already exists`);

  let created: CreatedRuntimeProject | null = null;
  let provisioningAccountToken: string | undefined;
  try {
    await copyTemplate(template, root);
    await rewritePackageName(root, appName);
    await rewriteGonvexConfig(root, appName, runtimeURL);
    if (shouldProvision) {
      provisioningAccountToken = await accountAccessTokenForRuntime(runtimeURL);
      created = await createRuntimeProject(runtimeURL, appName, provisioningAccountToken, databaseMode);
      await rewriteGonvexConfig(root, created.project.id, runtimeURL);
      await writeProjectEnv(root, runtimeURL, created.project.id, created.projectKey, true);
      if (googleAuth) {
        const authArgs = ["add", "google", "--project", root, "--signup-mode", signupMode];
        const callbackPath = valueFor(argv, "--callback-path");
        if (callbackPath) authArgs.push("--callback-path", callbackPath);
        const suppliedOrigins = valuesFor(argv, "--origin");
        const origins = suppliedOrigins.length > 0 ? suppliedOrigins : [
          process.env.GONVEX_APP_ORIGIN ?? process.env.VITE_APP_URL ?? "http://localhost:5173",
        ];
        for (const origin of origins) authArgs.push("--origin", origin);
        await runAuth(authArgs);
        if (signupMode === "inviteOnly") {
          if (databaseMode === "multiTenant") {
            await runAuth(["tenants", "create", `${appName} workspace`, "--owner", ownerEmail, "--project", root]);
          } else {
            await runAuth(["memberships", "add", "--tenant", created.project.id, "--email", ownerEmail, "--role", "owner", "--project", root]);
          }
        }
        const hasProductionOrigin = origins.some((origin) => normalizeAppOrigin(origin).startsWith("https://"));
        if (hasProductionOrigin && !argv.includes("--allow-unready")) {
          await runAuth(["doctor", "--project", root]);
        }
      }
    } else {
      await writeEnvLocal(root, appName, runtimeURL);
    }
  } catch (error) {
    if (created) {
      try {
        await runtimeJSON(await fetch(`${runtimeURL}/dev/projects/${encodeURIComponent(created.project.id)}`, {
          method: "DELETE",
          headers: provisioningAccountToken
            ? accountHeaders(provisioningAccountToken)
            : projectAuthHeaders({ runtimeURL, projectID: created.project.id, key: created.projectKey }),
        }));
      } catch (cleanupError) {
        console.warn(`[gonvex] could not roll back runtime project ${created.project.id}: ${cleanupError instanceof Error ? cleanupError.message : String(cleanupError)}`);
      }
    }
    await rm(root, { recursive: true, force: true });
    throw error;
  }
  console.log(`[gonvex] created ${target} from ${template} template`);
  console.log(`[gonvex] next: cd ${target} && npm install && npm run dev`);
}

async function runInit(argv: string[]) {
  const template = valueFor(argv, "--template") ?? "vite-react";
  const project = valueFor(argv, "--project") ?? basename(process.cwd());
  const runtime = valueFor(argv, "--runtime") ?? defaultRuntimeURL;
  await copyTemplate(template, process.cwd(), { overwrite: false });
  await writeEnvLocal(process.cwd(), project, runtime);
  console.log(`[gonvex] initialized ${project}`);
}

async function runLogin(argv: string[]) {
  const runtimeURL = await accountRuntimeForArgs(argv);
  const suppliedToken = valueFor(argv, "--token") ?? process.env.GONVEX_ACCOUNT_TOKEN;
  let accessToken: string;
  let expiresAt: number | undefined;

  if (suppliedToken) {
    accessToken = suppliedToken.trim();
    if (!accessToken) throw new Error("--token requires a non-empty personal access token");
  } else {
    const positional = positionalArgs(argv, ["--runtime-url", "--runtime", "--email", "--password", "--token"]);
    let email = valueFor(argv, "--email") ?? positional[0] ?? "";
    let password = valueFor(argv, "--password") ?? process.env.GONVEX_PASSWORD ?? "";
    if ((!email || !password) && (!process.stdin.isTTY || !process.stdout.isTTY)) {
      throw new Error("non-interactive login requires --email and --password (or GONVEX_PASSWORD), or --token");
    }
    const rl = createInterface({ input: process.stdin, output: process.stdout });
    try {
      if (!email) email = (await rl.question("Email: ")).trim();
    } finally {
      rl.close();
    }
    if (!password) password = await promptHidden("Password: ");
    if (!email || !password) throw new Error("email and password are required");

    const response = await fetch(`${runtimeURL}/dev/auth/login`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ email, password }),
    });
    const payload = await runtimeJSON<{ session?: { accessToken?: string; expiresAt?: number } }>(response);
    accessToken = payload.session?.accessToken ?? "";
    expiresAt = payload.session?.expiresAt;
    if (!accessToken) throw new Error("runtime login did not return an account session");
  }

  const identity = await fetchAccountIdentity(runtimeURL, accessToken);
  await saveAccountProfile(runtimeURL, accessToken, identity, expiresAt);
  console.log(`[gonvex] logged in as ${identity.account.email} to ${runtimeURL}`);
}

async function runLogout(argv: string[]) {
  const runtimeURL = await accountRuntimeForArgs(argv);
  const removed = await removeAccountProfile(runtimeURL);
  if (removed) console.log(`[gonvex] logged out of ${runtimeURL}`);
  else console.log(`[gonvex] no saved login for ${runtimeURL}`);
}

async function runWhoAmI(argv: string[]) {
  const runtimeURL = await accountRuntimeForArgs(argv);
  const accessToken = await requireAccountAccessToken(runtimeURL);
  const identity = await fetchAccountIdentity(runtimeURL, accessToken);
  if (argv.includes("--json")) {
    console.log(JSON.stringify({ runtimeURL, ...identity }, null, 2));
    return;
  }
  console.log(`${identity.account.email} (${identity.account.role})`);
  console.log(`Runtime: ${runtimeURL}`);
  console.log(`Authentication: ${identity.authentication}`);
  console.log(`Permissions: ${identity.permissions.join(", ")}`);
}

async function runToken(argv: string[]) {
  const commandArgs = positionalArgs(argv, ["--runtime-url", "--runtime", "--permission", "--expires-at"]);
  const action = commandArgs[0];
  if (!action || action === "help" || action === "--help") {
    printTokenHelp();
    return;
  }
  if (action === "permissions") {
    for (const permission of accountTokenPermissions) console.log(permission);
    return;
  }

  const runtimeURL = await accountRuntimeForArgs(argv);
  const accessToken = await requireAccountAccessToken(runtimeURL);
  if (action === "list" || action === "ls") {
    const response = await fetch(`${runtimeURL}/dev/auth/tokens`, { headers: accountHeaders(accessToken) });
    const payload = await runtimeJSON<{ tokens?: AccountAccessToken[] }>(response);
    const tokens = payload.tokens ?? [];
    if (argv.includes("--json")) {
      console.log(JSON.stringify(tokens, null, 2));
      return;
    }
    if (tokens.length === 0) {
      console.log("[gonvex] no account tokens");
      return;
    }
    for (const token of tokens) {
      const state = token.revokedAt ? "revoked" : token.expiresAt && Date.parse(token.expiresAt) <= Date.now() ? "expired" : "active";
      console.log(`${token.id}\t${token.name}\t${state}\t${token.permissions.join(",")}`);
    }
    return;
  }

  if (action === "create") {
    const positional = positionalArgs(argv, ["--runtime-url", "--runtime", "--permission", "--expires-at"]);
    const name = positional[1] ?? "CLI provisioning";
    let permissions = valuesFor(argv, "--permission").flatMap((value) => value.split(",")).map((value) => value.trim()).filter(Boolean);
    if (argv.includes("--full")) permissions = ["*"];
    if (permissions.length === 0) permissions = defaultAccountTokenPermissions;
    const unknown = permissions.find((permission) => !accountTokenPermissions.includes(permission));
    if (unknown) throw new Error(`unknown account token permission ${unknown}; run 'gonvex token permissions' to list valid values`);
    const response = await fetch(`${runtimeURL}/dev/auth/tokens`, {
      method: "POST",
      headers: { ...accountHeaders(accessToken), "content-type": "application/json" },
      body: JSON.stringify({ name, permissions, expiresAt: valueFor(argv, "--expires-at") }),
    });
    const payload = await runtimeJSON<{ token: AccountAccessToken; accessToken: string }>(response);
    if (argv.includes("--json")) {
      console.log(JSON.stringify(payload, null, 2));
      return;
    }
    console.error(`[gonvex] created account token ${payload.token.id}; copy it now because it will not be shown again:`);
    console.log(payload.accessToken);
    return;
  }

  if (action === "revoke" || action === "delete" || action === "rm") {
    const positional = positionalArgs(argv, ["--runtime-url", "--runtime"]);
    const id = positional[1];
    if (!id) throw new Error("usage: gonvex token revoke <token-id>");
    const response = await fetch(`${runtimeURL}/dev/auth/tokens/${encodeURIComponent(id)}`, {
      method: "DELETE",
      headers: accountHeaders(accessToken),
    });
    await runtimeJSON(response);
    console.log(`[gonvex] revoked account token ${id}`);
    return;
  }

  printTokenHelp();
  throw new Error(`unknown token command ${action}`);
}

async function runProject(argv: string[]) {
  const commandArgs = positionalArgs(argv, ["--runtime-url", "--runtime", "--database-mode", "--project-root"]);
  const action = commandArgs[0];
  if (!action || action === "help" || action === "--help") {
    printProjectHelp();
    return;
  }
  const runtimeURL = await accountRuntimeForArgs(argv);
  const accountToken = await accountAccessTokenForRuntime(runtimeURL);

  if (action === "list" || action === "ls") {
    const projects = await fetchRuntimeProjects(runtimeURL, accountToken);
    if (argv.includes("--json")) {
      console.log(JSON.stringify(projects, null, 2));
      return;
    }
    if (projects.length === 0) {
      console.log("[gonvex] no projects");
      return;
    }
    for (const project of projects) console.log(`${project.id}\t${project.name}\t${project.environment ?? ""}\t${project.database ?? ""}`);
    return;
  }

  if (action === "create") {
    const positional = positionalArgs(argv, ["--runtime-url", "--runtime", "--database-mode", "--project-root"]);
    const projectRoot = resolve(valueFor(argv, "--project-root") ?? ".");
    if (!existsSync(projectRoot)) throw new Error(`project root ${projectRoot} does not exist`);
    const name = positional[1] ?? basename(projectRoot);
    const databaseMode = valueFor(argv, "--database-mode") ?? "single";
    if (databaseMode !== "single" && databaseMode !== "multiTenant") throw new Error("--database-mode must be single or multiTenant");
    const created = await createRuntimeProject(runtimeURL, name, accountToken, databaseMode);
    await writeProjectEnv(projectRoot, runtimeURL, created.project.id, created.projectKey, true);
    if (argv.includes("--json")) {
      console.log(JSON.stringify(created, null, 2));
      return;
    }
    console.log(`[gonvex] created and configured ${created.project.name} (${created.project.id})`);
    return;
  }

  if (action === "select" || action === "link") {
    const positional = positionalArgs(argv, ["--runtime-url", "--runtime", "--project-root"]);
    const id = positional[1];
    if (!id) throw new Error("usage: gonvex project select <project-id>");
    const projectRoot = resolve(valueFor(argv, "--project-root") ?? ".");
    if (!existsSync(projectRoot)) throw new Error(`project root ${projectRoot} does not exist`);
    const projects = await fetchRuntimeProjects(runtimeURL, accountToken);
    const project = projects.find((candidate) => candidate.id === id);
    if (!project) throw new Error(`project ${id} was not found or is not accessible`);
    const projectKey = await fetchRuntimeProjectKey(runtimeURL, id, accountToken);
    await writeProjectEnv(projectRoot, runtimeURL, id, projectKey, true);
    console.log(`[gonvex] configured ${project.name} (${id})`);
    return;
  }

  printProjectHelp();
  throw new Error(`unknown project command ${action}`);
}

async function runDev(argv: string[]) {
  const split = argv.indexOf("--");
  const flagArgs = split === -1 ? argv : argv.slice(0, split);
  const childCommand = split === -1 ? [] : argv.slice(split + 1);
  const projectRoot = resolve(valueFor(flagArgs, "--project") ?? ".");
  const once = flagArgs.includes("--once");
  const verboseLogs = flagArgs.includes("--verbose-logs");
  if (once && childCommand.length > 0) throw new Error("--once cannot be used with a child command");

  let settings = await loadSettings(projectRoot, {
    runtimeURL: valueFor(flagArgs, "--runtime-url"),
    projectID: valueFor(flagArgs, "--project-id"),
    key: valueFor(flagArgs, "--key"),
  });
  settings = await ensureProjectSettings(projectRoot, settings, {
    keyWasExplicit: Boolean(valueFor(flagArgs, "--key")),
    runtimeWasExplicit: Boolean(valueFor(flagArgs, "--runtime-url")),
  });

  if (childCommand.length > 0) {
    const initialState = await watchProject(projectRoot, settings, true);
    const controller = new AbortController();
    const logStream = startRuntimeLogStream(settings, controller.signal, { verbose: verboseLogs });
    const watcher = watchProject(projectRoot, settings, false, controller.signal, initialState);
    const child = spawn(childCommand[0]!, childCommand.slice(1), {
      cwd: projectRoot,
      stdio: "inherit",
      signal: controller.signal,
      shell: process.platform === "win32",
    });
    const code = await new Promise<number | null>((resolve) => child.on("exit", resolve));
    controller.abort();
    await logStream.catch(() => {});
    await watcher.catch((error) => {
      if (error?.name !== "AbortError") throw error;
    });
    process.exitCode = code ?? 0;
    return;
  }

  const controller = new AbortController();
  const logStream = once ? Promise.resolve() : startRuntimeLogStream(settings, controller.signal, { verbose: verboseLogs });
  const result = await watchProject(projectRoot, settings, once);
  controller.abort();
  await logStream.catch(() => {});
  // In one-shot mode (CI/Docker `gonvex dev --once`), a failed sync must fail
  // the process so the build doesn't silently ship un-deployed functions.
  if (once && !result.lastSyncSucceeded) {
    console.error("[gonvex] one-shot sync did not succeed; exiting non-zero");
    process.exitCode = 1;
  }
}

async function runCodegen(argv: string[]) {
  const projectRoot = resolve(valueFor(argv, "--project") ?? ".");
  const settings = await loadSettings(projectRoot, {
    projectID: valueFor(argv, "--project-id"),
  });
  const backendDir = join(projectRoot, "gonvex");
  await mkdir(backendDir, { recursive: true });
  const sources = await collectBackendSources(projectRoot);
  const manifest = await buildManifest(projectRoot, sources, settings.projectID);
  await writeBindings(projectRoot, manifest);
  console.log(`[gonvex] generated ${Object.keys(manifest.functions).length} TypeScript function binding(s)`);
  if (manifest.module) {
    const fileCount = Object.keys(manifest.module.files).length;
    console.log(`[gonvex] built ${manifest.module.language} module artifact ${manifest.module.hash.slice(0, 12)} from ${fileCount} file(s)`);
  }
}

async function runEnv(argv: string[]) {
  const parsedArgs = parseEnvCommandArgs(argv);
  const action = parsedArgs.action;
  if (!action || action === "help" || action === "--help") {
    printEnvHelp();
    return;
  }
  const projectRoot = resolve(parsedArgs.options["--project"] ?? ".");
  const settings = await loadSettings(projectRoot, {
    runtimeURL: parsedArgs.options["--runtime-url"],
    projectID: parsedArgs.options["--project-id"],
    key: parsedArgs.options["--key"],
  });
  if (!settings.key) throw new Error("GONVEX_PROJECT_KEY is required for project env commands");

  if (action === "list" || action === "ls") {
    const variables = await fetchProjectEnv(settings);
    if (variables.length === 0) {
      console.log(`[gonvex] no env vars set for ${settings.projectID}`);
      return;
    }
    for (const variable of variables) {
      console.log(`${variable.name}=${variable.sensitive ? variable.masked : variable.value ?? variable.masked}`);
    }
    return;
  }

  if (action === "get") {
    const name = parsedArgs.positional[0];
    if (!name) throw new Error("usage: gonvex env get NAME");
    const variables = await fetchProjectEnv(settings);
    const variable = variables.find((item) => item.name === name);
    if (!variable) throw new Error(`${name} is not set for ${settings.projectID}`);
    console.log(variable.sensitive ? variable.masked : variable.value ?? variable.masked);
    return;
  }

  if (action === "set") {
    const parsed = parseEnvSetArgs(parsedArgs.positional);
    await saveProjectEnv(settings, parsed.name, parsed.value);
    console.log(`[gonvex] saved ${parsed.name} for ${settings.projectID}`);
    return;
  }

  if (action === "push" || action === "upload") {
    const optionPath = parsedArgs.options["--file"];
    if (optionPath && parsedArgs.positional.length > 0) {
      throw new Error("pass the env file either as FILE or --file, not both");
    }
    if (parsedArgs.positional.length > 1) throw new Error("usage: gonvex env push FILE");
    const file = optionPath ?? parsedArgs.positional[0];
    if (!file) throw new Error("usage: gonvex env push FILE");
    const envPath = resolve(projectRoot, file);
    let content: string;
    try {
      content = await readFile(envPath, "utf8");
    } catch (error) {
      throw new Error(`could not read env file ${file}: ${error instanceof Error ? error.message : String(error)}`);
    }
    const names = dotEnvVariableNames(content);
    if (names.length === 0) throw new Error(`env file ${file} contains no variables; refusing to replace the project env with an empty set`);
    const credentialName = names.find((name) => ["GONVEX_PROJECT_KEY", "GONVEX_DEPLOY_KEY", "GONVEX_KEY"].includes(name));
    if (credentialName) {
      throw new Error(`env file ${file} contains ${credentialName}; keep CLI project credentials out of uploaded function environment variables`);
    }
    const count = await replaceProjectEnv(settings, content);
    const displayPath = relative(projectRoot, envPath) || file;
    console.log(`[gonvex] replaced project env for ${settings.projectID} with ${count} variable(s) from ${displayPath}`);
    return;
  }

  if (action === "remove" || action === "rm" || action === "unset" || action === "delete") {
    const name = parsedArgs.positional[0];
    if (!name) throw new Error("usage: gonvex env remove NAME");
    await deleteProjectEnv(settings, name);
    console.log(`[gonvex] removed ${name} from ${settings.projectID}`);
    return;
  }

  printEnvHelp();
  throw new Error(`unknown env command ${action}`);
}

export async function runAuth(argv: string[]) {
  const optionsWithValues = ["--project", "--runtime-url", "--project-id", "--key", "--origin", "--callback-path", "--signup-mode", "--tenant", "--email", "--owner", "--role", "--member", "--account", "--mode", "--firebase-project-id", "--firebase-tenant-id", "--issuer", "--audience", "--jwks-url", "--admin-credentials-file"];
  const positional = positionalArgs(argv, optionsWithValues);
  const action = positional[0];
  if (!action || action === "help") {
    printAuthHelp();
    return;
  }
  const projectRoot = resolve(valueFor(argv, "--project") ?? ".");
  const settings = await loadSettings(projectRoot, {
    runtimeURL: valueFor(argv, "--runtime-url"),
    projectID: valueFor(argv, "--project-id"),
    key: valueFor(argv, "--key"),
  });
  if (!settings.key) throw new Error("GONVEX_PROJECT_KEY is required for project auth commands");
  const endpoint = `${settings.runtimeURL}/dev/projects/${encodeURIComponent(settings.projectID)}/auth/google`;

  if (action === "configure") {
    const provider = positional[1];
    if (provider !== "firebase" && provider !== "external-oidc") {
      throw new Error("usage: gonvex auth configure firebase|external-oidc [options]");
    }
    const providerEndpoint = `${settings.runtimeURL}/dev/projects/${encodeURIComponent(settings.projectID)}/auth/providers/${provider}`;
    const firebaseProjectId = valueFor(argv, "--firebase-project-id") ?? "";
    if (provider === "firebase" && !firebaseProjectId) throw new Error("--firebase-project-id is required");
    const issuer = valueFor(argv, "--issuer") ?? "";
    const audience = valueFor(argv, "--audience") ?? "";
    const jwksUrl = valueFor(argv, "--jwks-url") ?? "";
    if (provider === "external-oidc" && (!issuer || !audience || !jwksUrl)) {
      throw new Error("external-oidc requires --issuer, --audience, and --jwks-url");
    }
    let adminCredentials = "";
    const credentialsFile = valueFor(argv, "--admin-credentials-file");
    if (credentialsFile) {
      adminCredentials = await readFile(resolve(projectRoot, credentialsFile), "utf8");
      if (Buffer.byteLength(adminCredentials) > 1 << 20) throw new Error("admin credentials file exceeds 1 MiB");
    }
    await runtimeJSON(await fetch(providerEndpoint, {
      method: "PUT",
      headers: { ...projectAuthHeaders(settings), "content-type": "application/json" },
      body: JSON.stringify({
        authMode: valueFor(argv, "--mode") ?? provider,
        enabled: true,
        signupMode: normalizeAuthSignupMode(valueFor(argv, "--signup-mode") ?? "inviteOnly"),
        ...(firebaseProjectId ? { firebaseProjectId } : {}),
        ...(valueFor(argv, "--firebase-tenant-id") ? { firebaseTenantId: valueFor(argv, "--firebase-tenant-id") } : {}),
        ...(issuer ? { issuer } : {}),
        ...(audience ? { audience } : {}),
        ...(jwksUrl ? { jwksUrl } : {}),
        ...(adminCredentials ? { adminCredentials } : {}),
      }),
    }));
    console.log(`[gonvex] configured ${provider} authentication for ${settings.projectID}`);
    return;
  }

  if (action === "add" || action === "enable") {
    const provider = positional[1];
    if (provider !== "google") throw new Error("usage: gonvex auth add google [--origin URL]");
    const callbackPath = normalizeAuthCallbackPath(valueFor(argv, "--callback-path") ?? "/");
    const signupMode = normalizeAuthSignupMode(valueFor(argv, "--signup-mode") ?? "personal");
    const suppliedOrigins = valuesFor(argv, "--origin");
    const rawOrigins = suppliedOrigins.length > 0 ? suppliedOrigins : [
      process.env.GONVEX_APP_ORIGIN ?? process.env.VITE_APP_URL ?? "http://localhost:5173",
    ];
    const redirectUris = [...new Set(rawOrigins.map((raw) => new URL(callbackPath, `${normalizeAppOrigin(raw)}/`).toString()))];
    let configured: ProjectGoogleAuth | null = null;
    for (const redirectUri of redirectUris) {
      configured = await runtimeJSON<ProjectGoogleAuth>(await fetch(endpoint, {
        method: "PUT",
        headers: { ...projectAuthHeaders(settings), "content-type": "application/json" },
        body: JSON.stringify({ redirectUri, signupMode }),
      }));
    }
    await saveGoogleAuthProjectConfig(projectRoot, callbackPath, redirectUris, signupMode);
    await writeGonvexAuthModule(projectRoot, settings, callbackPath);
    const wired = await wireViteReactGoogleAuth(projectRoot);
    console.log(`[gonvex] enabled Google auth for ${settings.projectID}`);
    for (const redirectUri of redirectUris) console.log(`[gonvex] registered callback ${redirectUri}`);
    console.log(`[gonvex] wrote gonvex/auth.tsx; wrap your app with GonvexAuthProvider and render GoogleSignInButton`);
    if (wired) console.log("[gonvex] wired the Vite React starter with a production-ready sign-in screen");
    if (!configured?.ready) {
      console.warn(`[gonvex] Google auth is registered but the runtime is not production-ready: ${(configured?.issues ?? ["runtime broker configuration is incomplete"]).join("; ")}`);
    }
    return;
  }

  if (action === "remove" || action === "disable") {
    const provider = positional[1];
    if (provider !== "google") throw new Error("usage: gonvex auth remove google");
    const suppliedOrigins = valuesFor(argv, "--origin");
    if (suppliedOrigins.length > 0) {
      const localConfig = await loadConfig(projectRoot);
      const callbackPath = normalizeAuthCallbackPath(valueFor(argv, "--callback-path") ?? localConfig.auth?.providers?.google?.callbackPath ?? "/");
      const redirectUris = [...new Set(suppliedOrigins.map((raw) => new URL(callbackPath, `${normalizeAppOrigin(raw)}/`).toString()))];
      for (const redirectUri of redirectUris) {
        const url = new URL(endpoint);
        url.searchParams.set("redirect_uri", redirectUri);
        await runtimeJSON(await fetch(url, { method: "DELETE", headers: projectAuthHeaders(settings) }));
      }
      await removeGoogleAuthProjectRedirects(projectRoot, redirectUris);
      for (const redirectUri of redirectUris) console.log(`[gonvex] removed Google callback ${redirectUri}`);
      return;
    }
    await runtimeJSON(await fetch(endpoint, { method: "DELETE", headers: projectAuthHeaders(settings) }));
    await setGoogleAuthProjectEnabled(projectRoot, false);
    console.log(`[gonvex] disabled Google auth for ${settings.projectID}`);
    return;
  }

  if (action === "status" && (positional[1] === "firebase" || positional[1] === "external-oidc")) {
    const provider = positional[1]!;
    const providerEndpoint = `${settings.runtimeURL}/dev/projects/${encodeURIComponent(settings.projectID)}/auth/providers/${provider}`;
    const configured = await runtimeJSON<Record<string, unknown>>(await fetch(providerEndpoint, { headers: projectAuthHeaders(settings) }));
    console.log(JSON.stringify(configured, null, 2));
    return;
  }

  if (action === "status") {
    const configured = await runtimeJSON<ProjectGoogleAuth>(await fetch(endpoint, { headers: projectAuthHeaders(settings) }));
    if (argv.includes("--json")) {
      console.log(JSON.stringify(configured, null, 2));
      return;
    }
    console.log(`Google: ${configured.enabled ? "enabled" : "disabled"}`);
    console.log(`Broker: ${configured.ready ? "ready" : "not ready"}`);
    console.log(`Signup: ${configured.signupMode ?? "personal"}`);
    for (const redirectUri of configured.redirectUris ?? []) console.log(`Callback: ${redirectUri}`);
    if (configured.brokerCallbackUrl) console.log(`Google Cloud redirect URI: ${configured.brokerCallbackUrl}`);
    for (const issue of configured.issues ?? []) console.log(`Issue: ${issue}`);
    return;
  }

  if (action === "doctor") {
    const configured = await runtimeJSON<ProjectGoogleAuth>(await fetch(endpoint, { headers: projectAuthHeaders(settings) }));
    const localConfig = await loadConfig(projectRoot);
    const localRedirects = localConfig.auth?.providers?.google?.redirectUris ?? [];
    const missing = localRedirects.filter((redirectUri) => !(configured.redirectUris ?? []).includes(redirectUri));
    const untracked = (configured.redirectUris ?? []).filter((redirectUri) => !localRedirects.includes(redirectUri));
    const issues = [...(configured.issues ?? [])];
    if (!configured.enabled) issues.push("Google provider is disabled for this project");
    for (const redirectUri of missing) issues.push(`local callback is not registered: ${redirectUri}`);
    for (const redirectUri of untracked) issues.push(`runtime callback is not tracked in gonvex.json: ${redirectUri}`);
    if ((configured.redirectUris ?? []).length === 0) issues.push("no app callback is registered");
    if (configured.signupMode === "inviteOnly" && (configured.tenantCount ?? 0) === 0) issues.push("invite-only signup needs at least one tenant scope");
    if (configured.signupMode === "inviteOnly" && (configured.membershipCount ?? 0) + (configured.invitationCount ?? 0) === 0) issues.push("invite-only signup needs at least one member or invitation");
    if (argv.includes("--json")) {
      console.log(JSON.stringify({ ...configured, issues, ready: configured.ready && issues.length === 0 }, null, 2));
    } else if (configured.ready && issues.length === 0) {
      console.log(`[gonvex] Google auth is ready for production (${configured.brokerCallbackUrl})`);
      for (const redirectUri of configured.redirectUris ?? []) console.log(`Callback: ${redirectUri}`);
    } else {
      for (const issue of issues) console.error(`[gonvex] ${issue}`);
    }
    if (!configured.ready || issues.length > 0) throw new Error("Google auth production readiness check failed");
    return;
  }

  if (action === "accounts") {
    const accountsEndpoint = `${settings.runtimeURL}/dev/projects/${encodeURIComponent(settings.projectID)}/auth/accounts`;
    const payload = await runtimeJSON<{ accounts?: AppAuthAccount[] }>(await fetch(accountsEndpoint, { headers: projectAuthHeaders(settings) }));
    const accounts = payload.accounts ?? [];
    if (argv.includes("--json")) {
      console.log(JSON.stringify(accounts, null, 2));
      return;
    }
    if (accounts.length === 0) {
      console.log(`[gonvex] no app accounts for ${settings.projectID}`);
      return;
    }
    for (const account of accounts) {
      console.log(`${account.id}\t${account.email ?? ""}\t${account.name ?? ""}\t${account.provider}`);
    }
    return;
  }

  if (action === "tenants" || action === "tenant") {
    const operation = positional[1] ?? "list";
    const tenantsEndpoint = `${settings.runtimeURL}/dev/projects/${encodeURIComponent(settings.projectID)}/auth/tenants`;
    if (operation === "list" || operation === "ls") {
      const payload = await runtimeJSON<{ tenants?: Array<{ id: string; name: string; memberCount: number }> }>(
        await fetch(tenantsEndpoint, { headers: projectAuthHeaders(settings) }),
      );
      if (argv.includes("--json")) {
        console.log(JSON.stringify(payload.tenants ?? [], null, 2));
        return;
      }
      for (const tenant of payload.tenants ?? []) console.log(`${tenant.id}\t${tenant.name}\t${tenant.memberCount} member(s)`);
      return;
    }
    if (operation === "create") {
      const name = positional[2];
      if (!name) throw new Error("usage: gonvex auth tenants create <name> [--owner email]");
      const payload = await runtimeJSON<{ tenant: { id: string; name: string } }>(await fetch(tenantsEndpoint, {
        method: "POST", headers: { ...projectAuthHeaders(settings), "content-type": "application/json" },
        body: JSON.stringify({ name, ownerEmail: valueFor(argv, "--owner") ?? "" }),
      }));
      console.log(`[gonvex] created tenant ${payload.tenant.name} (${payload.tenant.id})`);
      return;
    }
    throw new Error(`unknown auth tenants command ${operation}`);
  }

  if (action === "members" || action === "memberships") {
    const operation = positional[1] ?? "list";
    const tenant = valueFor(argv, "--tenant") ?? positional[2];
    if (!tenant) throw new Error("--tenant is required for membership commands");
    const membershipEndpoint = `${settings.runtimeURL}/dev/projects/${encodeURIComponent(settings.projectID)}/auth/memberships?tenant=${encodeURIComponent(tenant)}`;
    if (operation === "list" || operation === "ls") {
      const payload = await runtimeJSON<{ members?: Array<{ memberId: string; email: string; name: string; role: string }>; invitations?: Array<{ email: string; role: string; expiresAt?: string }> }>(
        await fetch(membershipEndpoint, { headers: projectAuthHeaders(settings) }),
      );
      if (argv.includes("--json")) {
        console.log(JSON.stringify(payload, null, 2));
        return;
      }
      for (const member of payload.members ?? []) console.log(`${member.memberId}\t${member.email}\t${member.role}\t${member.name}`);
      for (const invitation of payload.invitations ?? []) console.log(`invited\t${invitation.email}\t${invitation.role}\t${invitation.expiresAt ?? ""}`);
      return;
    }
    if (operation === "add" || operation === "invite") {
      const email = valueFor(argv, "--email") ?? positional[2];
      if (!email) throw new Error("--email is required when adding a membership");
      const role = valueFor(argv, "--role") ?? "member";
      await runtimeJSON(await fetch(membershipEndpoint, {
        method: "PUT", headers: { ...projectAuthHeaders(settings), "content-type": "application/json" },
        body: JSON.stringify({ email, role }),
      }));
      console.log(`[gonvex] granted ${role} access to ${email} for tenant ${tenant}`);
      return;
    }
    if (operation === "remove" || operation === "rm") {
      const member = valueFor(argv, "--member");
      const email = valueFor(argv, "--email");
      if (!member && !email) throw new Error("--member is required to remove a member, or --email to revoke an invitation");
      const target = member ? `member=${encodeURIComponent(member)}` : `email=${encodeURIComponent(email!)}`;
      await runtimeJSON(await fetch(`${membershipEndpoint}&${target}`, {
        method: "DELETE", headers: projectAuthHeaders(settings),
      }));
      console.log(member
        ? `[gonvex] removed ${member} from tenant ${tenant}`
        : `[gonvex] revoked the invitation for ${email} from tenant ${tenant}`);
      return;
    }
    throw new Error(`unknown auth memberships command ${operation}`);
  }

  if (action === "account") {
    const operation = positional[1];
    const account = valueFor(argv, "--account") ?? positional[2];
    if (!operation || !account) throw new Error("usage: gonvex auth account <disable|enable|delete> <account-id>");
    const accountEndpoint = `${settings.runtimeURL}/dev/projects/${encodeURIComponent(settings.projectID)}/auth/accounts/${encodeURIComponent(account)}`;
    if (operation === "disable" || operation === "enable") {
      await runtimeJSON(await fetch(accountEndpoint, {
        method: "PATCH", headers: { ...projectAuthHeaders(settings), "content-type": "application/json" },
        body: JSON.stringify({ disabled: operation === "disable" }),
      }));
      console.log(`[gonvex] ${operation}d app account ${account}`);
      return;
    }
    if (operation === "delete" || operation === "remove") {
      await runtimeJSON(await fetch(accountEndpoint, { method: "DELETE", headers: projectAuthHeaders(settings) }));
      console.log(`[gonvex] deleted app account ${account}`);
      return;
    }
    throw new Error(`unknown auth account command ${operation}`);
  }

  printAuthHelp();
  throw new Error(`unknown auth command ${action}`);
}

function normalizeAppOrigin(raw: string) {
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    throw new Error(`invalid app origin ${raw}`);
  }
  if (parsed.username || parsed.password || parsed.pathname !== "/" || parsed.search || parsed.hash) {
    throw new Error("--origin must contain only scheme, host, and optional port");
  }
  const local = ["localhost", "127.0.0.1", "[::1]", "::1"].includes(parsed.hostname);
  if (parsed.protocol !== "https:" && !(parsed.protocol === "http:" && local)) {
    throw new Error("--origin must use https (http is allowed only for localhost)");
  }
  return parsed.origin;
}

function normalizeAuthCallbackPath(raw: string) {
  const value = raw.trim();
  if (!value.startsWith("/") || value.startsWith("//") || value.includes("?") || value.includes("#")) {
    throw new Error("--callback-path must be an absolute pathname without a query or fragment");
  }
  return value;
}

function normalizeAuthSignupMode(raw: string): "personal" | "inviteOnly" {
  if (raw === "personal") return raw;
  if (raw === "inviteOnly" || raw === "invite-only" || raw === "invited") return "inviteOnly";
  throw new Error("--signup-mode must be personal or inviteOnly");
}

async function saveGoogleAuthProjectConfig(root: string, callbackPath: string, addedRedirectUris: string[], signupMode: "personal" | "inviteOnly") {
  const configPath = join(root, "gonvex.json");
  const config = await loadConfig(root);
  const existing = config.auth?.providers?.google;
  const redirectUris = [...new Set([...(existing?.redirectUris ?? []), ...addedRedirectUris])].sort();
  config.auth = {
    ...(config.auth ?? {}),
    providers: {
      ...(config.auth?.providers ?? {}),
      google: { ...(existing ?? {}), enabled: true, callbackPath, redirectUris, signupMode },
    },
  };
  await writeFile(configPath, `${JSON.stringify(config, null, 2)}\n`);
}

async function setGoogleAuthProjectEnabled(root: string, enabled: boolean) {
  const configPath = join(root, "gonvex.json");
  const config = await loadConfig(root);
  const existing = config.auth?.providers?.google ?? {};
  config.auth = {
    ...(config.auth ?? {}),
    providers: { ...(config.auth?.providers ?? {}), google: { ...existing, enabled } },
  };
  await writeFile(configPath, `${JSON.stringify(config, null, 2)}\n`);
}

async function removeGoogleAuthProjectRedirects(root: string, removedRedirectUris: string[]) {
  const configPath = join(root, "gonvex.json");
  const config = await loadConfig(root);
  const existing = config.auth?.providers?.google ?? {};
  const removed = new Set(removedRedirectUris);
  config.auth = {
    ...(config.auth ?? {}),
    providers: {
      ...(config.auth?.providers ?? {}),
      google: { ...existing, redirectUris: (existing.redirectUris ?? []).filter((redirectUri) => !removed.has(redirectUri)) },
    },
  };
  await writeFile(configPath, `${JSON.stringify(config, null, 2)}\n`);
}

async function writeGonvexAuthModule(root: string, settings: Settings, callbackPath: string) {
  const source = [
    "// Generated by `gonvex auth add google`. Re-running the command replaces this file.",
    'import { createGonvexAuth } from "./_generated/react";',
    "",
    "const env = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env ?? {};",
    "",
    "export const gonvexAuth = createGonvexAuth({",
    `  runtimeUrl: env.VITE_GONVEX_URL ?? env.VITE_GONVEX_RUNTIME_URL ?? ${JSON.stringify(settings.runtimeURL)},`,
    `  projectId: env.VITE_GONVEX_PROJECT_ID ?? ${JSON.stringify(settings.projectID)},`,
    `  callbackPath: ${JSON.stringify(callbackPath)},`,
    "});",
    "",
    "export const GonvexAuthProvider = gonvexAuth.GonvexAuthProvider;",
    "export const GoogleSignInButton = gonvexAuth.GoogleSignInButton;",
    "export const useGonvexAuth = gonvexAuth.useGonvexAuth;",
    "",
  ].join("\n");
  await writeFileIfChanged(join(root, "gonvex", "auth.tsx"), source);
}

async function wireViteReactGoogleAuth(root: string) {
  const mainPath = join(root, "src", "main.tsx");
  const appPath = join(root, "src", "App.tsx");
  if (!existsSync(mainPath) || !existsSync(appPath)) return false;
  let main = await readFile(mainPath, "utf8");
  let app = await readFile(appPath, "utf8");
  if (main.includes('from "../gonvex/auth"') && app.includes("GoogleSignInButton")) return true;
  const providerImport = 'import { GonvexProvider } from "../gonvex/_generated/react";';
  const reactImport = 'import { useLiveQuery, useReducer } from "../gonvex/_generated/react";';
  const appStart = 'export default function App(props: { runtimeURL: string }) {\n  const messages = useLiveQuery<Message[]>(api.messages.list, {}) ?? [];';
  if (!main.includes(providerImport) || !main.includes("<GonvexProvider client={gonvex}>") || !app.includes(reactImport) || !app.includes(appStart)) {
    return false;
  }
  main = main
    .replace(providerImport, 'import { GonvexAuthProvider } from "../gonvex/auth";')
    .replace("<GonvexProvider client={gonvex}>", "<GonvexAuthProvider client={gonvex}>")
    .replace("</GonvexProvider>", "</GonvexAuthProvider>");
  app = app
    .replace(reactImport, `${reactImport}\nimport { GoogleSignInButton, useGonvexAuth } from "../gonvex/auth";`)
    .replace(appStart, [
      "export default function App(props: { runtimeURL: string }) {",
      "  const auth = useGonvexAuth();",
      "  if (!auth.isAuthenticated) {",
      '    return <main className="shell"><section className="hero"><div className="status">Secure Gonvex account</div><h1>Sign in to continue.</h1><p>Your app uses Gonvex-native Google authentication—no Firebase SDK or per-app Google Cloud project.</p><GoogleSignInButton />{auth.error ? <p role="alert">{auth.error}</p> : null}</section></main>;',
      "  }",
      "  return <AuthenticatedApp runtimeURL={props.runtimeURL} />;",
      "}",
      "",
      "function AuthenticatedApp(props: { runtimeURL: string }) {",
      "  const auth = useGonvexAuth();",
      '  const messages = useLiveQuery<Message[]>(api.messages.list, {}) ?? [];',
    ].join("\n"))
    .replace('<div className="status">Connected to {props.runtimeURL}</div>', '<div className="status"><span>Connected to {props.runtimeURL} as {auth.account?.email}</span><GoogleSignInButton /></div>');
  await writeFile(mainPath, main);
  await writeFile(appPath, app);
  return true;
}

async function watchProject(root: string, settings: Settings, once: boolean, signal?: AbortSignal, initialState?: WatchState): Promise<WatchState> {
  const backendDir = join(root, "gonvex");
  await mkdir(backendDir, { recursive: true });
  let lastFingerprint = initialState?.lastFingerprint ?? "";
  let lastManifest: Manifest | null = initialState?.lastManifest ?? null;
  let lastSyncAttempt = initialState?.lastSyncAttempt ?? 0;
  let lastSyncSucceeded = initialState?.lastSyncSucceeded ?? false;
  let lastRuntimeCheck = initialState?.lastRuntimeCheck ?? 0;

  while (!signal?.aborted) {
    const sources = await collectBackendSources(root);
    // Watch migrations too: editing or adding one must trigger a re-sync,
    // otherwise a new migration sits unapplied until an unrelated module edit.
    // gonvex.json selects the TypeScript entrypoint and bundle destination. It
    // belongs in the fingerprint, while gonvex/_build is excluded from source
    // collection so writing the generated ESM cannot trigger a rebuild loop.
    const configPath = join(root, "gonvex.json");
    const fingerprintFiles = [...sources.moduleFiles, ...await migrationFiles(join(root, "migrations"))];
    if (existsSync(configPath)) fingerprintFiles.push(configPath);
    const fingerprint = await filesFingerprint(fingerprintFiles);
    const now = Date.now();
    const shouldBuild = fingerprint !== lastFingerprint;
    const shouldRetryRuntimeSync = !once && !lastSyncSucceeded && lastManifest !== null && now - lastSyncAttempt > runtimeSyncRetryMs;
    const shouldVerifyRuntimeState = !once && lastSyncSucceeded && lastManifest !== null && now - lastRuntimeCheck > runtimeStateCheckMs;

    if (shouldBuild || shouldRetryRuntimeSync) {
      lastFingerprint = fingerprint;
      let manifest: Manifest;
      if (shouldBuild) {
        manifest = await buildManifest(root, sources, settings.projectID);
      } else {
        manifest = lastManifest!;
      }
      const isInitialBuild = lastManifest === null;
      const previousManifest = lastManifest ?? await readGeneratedManifest(root);
      if (shouldBuild) {
        lastManifest = manifest;
        const writeResult = await writeBindings(root, manifest);
        logFunctionDiff(previousManifest, manifest, writeResult, isInitialBuild);
      }
      lastSyncAttempt = now;
      try {
        const observedSchema = await syncRuntime(settings, manifest);
        if (observedSchema) {
          manifest.schema = observedSchema;
          lastManifest = manifest;
          await writeBindings(root, manifest);
        }
        lastSyncSucceeded = true;
        lastRuntimeCheck = now;
        console.log(`[gonvex] synced project ${settings.projectID || "(key-inferred)"} to ${settings.runtimeURL}`);
      } catch (error) {
        lastSyncSucceeded = false;
        const detail = error instanceof Error ? error.message : String(error);
        const valkeyHint = isLocalRuntimeURL(settings.runtimeURL)
          ? " Local runtimes require a reachable VALKEY_URL, for example VALKEY_URL=redis://127.0.0.1:6380/0."
          : "";
        console.error(`[gonvex] runtime sync failed: ${detail}.${valkeyHint}`);
      }
    } else if (shouldVerifyRuntimeState) {
      const manifest = lastManifest;
      if (manifest === null) continue;
      lastRuntimeCheck = now;
      try {
        const inSync = await runtimeHasManifest(settings, manifest);
        if (!inSync) {
          lastSyncAttempt = now;
          const observedSchema = await syncRuntime(settings, manifest);
          if (observedSchema) {
            manifest.schema = observedSchema;
            lastManifest = manifest;
            await writeBindings(root, manifest);
          }
          lastSyncSucceeded = true;
          console.log(`[gonvex] runtime state was missing; re-synced project ${settings.projectID || "(key-inferred)"}`);
        }
      } catch {
        lastSyncSucceeded = false;
      }
    }

    if (once) {
      return { lastFingerprint, lastManifest, lastSyncAttempt, lastSyncSucceeded, lastRuntimeCheck };
    }
    await sleep(500, signal);
  }
  return { lastFingerprint, lastManifest, lastSyncAttempt, lastSyncSucceeded, lastRuntimeCheck };
}

// Backend sources are collected once per build so the fingerprint, the
// manifest, and the shipped artifact all agree on the same file set.
async function collectBackendSources(root: string): Promise<BackendSources> {
  const backendDir = join(root, "gonvex");
  const config = await loadConfig(root);
  const module = await moduleSourceFiles(backendDir);
  await detectProjectLanguage(backendDir, config.language);
  return {
    moduleFiles: module,
    config,
  };
}

async function buildManifest(root: string, sources: BackendSources, projectID: string): Promise<Manifest> {
  const module = await buildModuleArtifact({
      root,
      backendDir: join(root, "gonvex"),
      files: sources.moduleFiles,
      migrations: await migrationFiles(join(root, "migrations")),
      entrypoint: sources.config.module?.entrypoint,
      bundle: sources.config.module?.bundle,
    });
  return {
    project: projectID,
    generatedAt: new Date().toISOString(),
    functions: moduleManifestFunctions(module),
    schema: emptySchemaDefinition(),
    module,
    ...(Object.keys(module.visibility).length > 0 ? { visibility: module.visibility } : {}),
  };
}

async function writeBindings(root: string, manifest: Manifest): Promise<BindingWriteResult> {
  const dir = join(root, "gonvex", "_generated");
  await mkdir(dir, { recursive: true });
  await rm(join(dir, "landlord"), { recursive: true, force: true });
  let changedFiles = 0;
  if (await writeManifestIfChanged(join(dir, "manifest.json"), manifest)) changedFiles += 1;
  const outputs: Record<string, string> = {
    "api.ts": renderAPI(manifest),
    "client.ts": '// Generated by gonvex dev. Do not edit.\nexport { GonvexClient, createFirebaseAuthAdapter } from "@gonvex/client";\nexport type { GonvexExternalAuthAdapter, GonvexExternalIdentityHint, GonvexFirebaseAuthAdapterOptions } from "@gonvex/client";\n',
    "react.ts": '// Generated by gonvex dev. Do not edit.\nexport { createFirebaseAuthAdapter, createGonvexAuth, GonvexAuthProvider, GonvexGoogleAuthButton, GonvexProvider, GonvexProviderWithAuth, useAction, useControlQuery, useCurrentTenantProfile, useEntity, useGonvexAuth, useGonvexAuthState, useGonvexClient, useGonvexConnectionState, useInvitationList, useLiveQuery, useLiveQueryState, useQuery, useReducer, useReplicaCollection, useReplicaCollectionState, useReplicaEntities, useReplicaSelector, useRetainedLiveQuery } from "@gonvex/react";\nexport type { GonvexAuthAccount, GonvexAuthConfig, GonvexAuthProviderName, GonvexAuthTenant, GonvexAuthValue, GonvexDeveloperModeState, GonvexExternalAuthAdapter, GonvexExternalIdentityHint, GonvexFirebaseAuthAdapterOptions } from "@gonvex/react";\n',
    "types.ts": "// Generated by gonvex dev. Do not edit.\nexport type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };\n",
    "schema.ts": renderSchemaIndex(manifest),
    "control-plane/schema.ts": renderScopedSchemaModule("control-plane", manifest.schema.controlPlaneTables),
    "control-plane/tables.ts": renderScopedTablesModule("control-plane", manifest.schema.controlPlaneTables),
    "tenant/schema.ts": renderScopedSchemaModule("tenant", manifest.schema.tenantTables),
    "tenant/tables.ts": renderScopedTablesModule("tenant", manifest.schema.tenantTables),
    // Module-artifact projects emit the artifact next to the manifest so the
    // deploy payload is inspectable without re-running codegen.
    ...(manifest.module ? { "module.json": `${JSON.stringify(manifest.module, null, 2)}\n` } : {}),
  };
  for (const [name, contents] of Object.entries(outputs)) {
    if (await writeFileIfChanged(join(dir, name), contents)) changedFiles += 1;
  }
  return { changedFiles };
}

async function writeManifestIfChanged(path: string, manifest: Manifest) {
  try {
    const existing = JSON.parse(await readFile(path, "utf8")) as Manifest;
    if (JSON.stringify(manifestWithoutGeneratedAt(existing)) === JSON.stringify(manifestWithoutGeneratedAt(manifest))) {
      return false;
    }
  } catch {
    // Missing, stale, or invalid generated manifests are repaired by writing them.
  }
  await writeFile(path, `${JSON.stringify(manifest, null, 2)}\n`);
  return true;
}

async function writeFileIfChanged(path: string, contents: string) {
  try {
    if ((await readFile(path, "utf8")) === contents) return false;
  } catch {
    // Missing or unreadable generated files are repaired by writing them.
  }
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, contents);
  return true;
}

async function readGeneratedManifest(root: string): Promise<Manifest | null> {
  try {
    return JSON.parse(await readFile(join(root, "gonvex", "_generated", "manifest.json"), "utf8")) as Manifest;
  } catch {
    return null;
  }
}

function manifestWithoutGeneratedAt(manifest: Manifest) {
  const { generatedAt: _generatedAt, ...rest } = manifest;
  return rest;
}

function logFunctionDiff(previous: Manifest | null, current: Manifest, writeResult: BindingWriteResult, isInitialBuild: boolean) {
  const functionCount = Object.keys(current.functions).length;
  if (!previous) {
    console.log(`[gonvex] uploaded ${functionCount} runtime function(s)`);
    return;
  }

  const diff = diffFunctions(previous, current);
  if (diff.added.length === 0 && diff.removed.length === 0 && diff.edited.length === 0) {
    if (isInitialBuild && writeResult.changedFiles === 0) {
      return;
    }
    if (isInitialBuild) {
      return;
    }
    console.log("[gonvex] ~ uploaded runtime source changes (no function API changes)");
    return;
  }

  for (const path of diff.added) console.log(color.green(`[gonvex] + added function ${path}`));
  for (const path of diff.edited) console.log(color.yellow(`[gonvex] ~ edited function ${path}`));
  for (const path of diff.removed) console.log(color.red(`[gonvex] - removed function ${path}`));
}

function diffFunctions(previous: Manifest, current: Manifest): FunctionDiff {
  const previousPaths = new Set(Object.keys(previous.functions));
  const currentPaths = new Set(Object.keys(current.functions));
  const added = [...currentPaths].filter((path) => !previousPaths.has(path)).sort();
  const removed = [...previousPaths].filter((path) => !currentPaths.has(path)).sort();
  const edited = [...currentPaths]
    .filter((path) => previousPaths.has(path))
    .filter((path) => functionEntryChanged(previous, current, path))
    .sort();
  return { added, removed, edited };
}

function functionEntryChanged(previous: Manifest, current: Manifest, path: string) {
  const previousEntry = previous.functions[path]!;
  const currentEntry = current.functions[path]!;
  if (JSON.stringify(previousEntry) !== JSON.stringify(currentEntry)) return true;
  return sourceForFunction(previous, previousEntry) !== sourceForFunction(current, currentEntry);
}

function sourceForFunction(manifest: Manifest, entry: FunctionEntry) {
  return manifest.module.files[normalizedFunctionFile(entry)];
}

function normalizedFunctionFile(entry: FunctionEntry) {
  return entry.file.replace(/\\/g, "/").replace(/^\.?\//, "");
}

function renderAPI(manifest: Manifest) {
  const publicRoot: Record<string, any> = {};
  const internalRoot: Record<string, any> = {};
  const optimisticTransactions: Record<string, JsonValue> = {};
  const functionTypes: Array<{ path: string; args: string; result: string }> = [];
  for (const [path, entry] of Object.entries(manifest.functions).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)) {
    const parts = path.split(".").filter(Boolean);
    if (parts.length === 0) continue;
    let target = entry.internal ? internalRoot : publicRoot;
    for (const part of parts.slice(0, -1)) {
      target = target[part] ??= {};
    }
    const reference: Record<string, unknown> = {
      kind: entry.kind,
      path,
      __argsType: functionTypeName(path, "Args"),
      __resultType: functionTypeName(path, "Result"),
      ...(entry.delivery ? { delivery: entry.delivery } : {}),
      ...(entry.delivery === "replica" && entry.replica ? { replica: entry.replica } : {}),
      ...(entry.offline !== undefined ? { offline: entry.offline } : {}),
      ...(isModuleSchema(entry.args) ? { args: entry.args } : {}),
      ...(isModuleSchema(entry.result) ? { result: entry.result } : {}),
    };
    if (entry.delivery === "live" && entry.dependencies?.liveQueryPlan) {
      const plan = entry.dependencies.liveQueryPlan;
      reference.live = { entity: plan.table, key: plan.key, resultPath: plan.resultPath ?? [], plan };
    }
    const transaction = entry.kind === "reducer" ? entry.optimistic : undefined;
    if (transaction !== undefined) {
      reference.optimistic = { transaction };
    }
    target[parts[parts.length - 1]!] = reference;
    if (entry.kind === "reducer" && transaction !== undefined) optimisticTransactions[path] = transaction;
    functionTypes.push({
      path,
      args: functionTypeName(path, "Args"),
      result: functionTypeName(path, "Result"),
    });
  }
  const lines = [
    "// Generated by gonvex dev. Do not edit.",
    "",
    "import { control as gonvexControl, type LiveQueryPlan } from \"@gonvex/client\";",
    "",
    "export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };",
    "export type FunctionKind = \"query\" | \"reducer\" | \"action\";",
    "export type OptimisticID = string | readonly string[];",
    "export type OptimisticValue = JsonValue | { readonly $arg: string | readonly string[] } | readonly OptimisticValue[] | { readonly [key: string]: OptimisticValue };",
    "export type OptimisticEffectDefinition =",
    "  | { readonly operation: \"patch\"; readonly entity: string; readonly id: OptimisticID; readonly fields: Readonly<Record<string, OptimisticValue>> }",
    "  | { readonly operation: \"upsert\"; readonly entity: string; readonly id: OptimisticID; readonly value: Readonly<Record<string, OptimisticValue>> }",
    "  | { readonly operation: \"delete\"; readonly entity: string; readonly id: OptimisticID };",
    "export type OptimisticTransactionDefinition = { readonly effects: readonly OptimisticEffectDefinition[]; readonly expectedRevision?: number };",
    "export type FunctionReference<Kind extends FunctionKind = FunctionKind, Args = JsonValue, Result = JsonValue> = {",
    "  readonly kind: Kind;",
    "  readonly path: string;",
    "  readonly delivery?: \"oneShot\" | \"live\" | \"replica\";",
    "  readonly replica?: { readonly table: string; readonly key: string; readonly columns?: readonly string[] };",
    "  readonly offline?: { readonly mode: \"forbidden\" | \"allowed\" | \"onlineOnly\"; readonly conflict?: \"reject\" | \"expectedVersion\" | \"merge\"; readonly reason?: string };",
    "  readonly args?: Args;",
    "  readonly result?: Result;",
    "  readonly live?: { readonly entity: string; readonly key: string; readonly resultPath?: readonly string[]; readonly plan: LiveQueryPlan };",
    "  readonly optimistic?: { readonly transaction?: OptimisticTransactionDefinition };",
    "};",
    "",
    ...functionTypes.flatMap(({ path, args, result }) => [
      `export type ${args} = ${renderSchemaType(manifest.functions[path]?.args)};`,
      `export type ${result} = ${renderSchemaType(manifest.functions[path]?.result)};`,
    ]),
    "",
    `export const api = ${renderObject(publicRoot, 0)} as const;`,
    "",
    "export const control = gonvexControl;",
    "",
    `export const internal = ${renderObject(internalRoot, 0)} as const;`,
    "export type Api = typeof api;",
    "",
    `export const optimisticTransactions: Record<string, OptimisticTransactionDefinition> = ${renderObject(optimisticTransactions, 0)};`,
    "",
    "export type ApiArgs = {",
    ...functionTypes.map(({ path, args }) => `  ${JSON.stringify(path)}: ${args};`),
    "};",
    "",
    "export type ApiResults = {",
    ...functionTypes.map(({ path, result }) => `  ${JSON.stringify(path)}: ${result};`),
    "};",
    "",
    "export function optimisticPatchesFor(",
    "  path: string,",
    "  args: Record<string, unknown>,",
    "): Array<{ entity?: string; collection?: string; rowId: string; op: \"patch\" | \"insert\" | \"upsert\" | \"delete\"; fields?: Record<string, unknown> }> {",
    "  const transaction = Object.prototype.hasOwnProperty.call(optimisticTransactions, path)",
    "    ? optimisticTransactions[path]",
    "    : undefined;",
    "  if (transaction && Array.isArray(transaction.effects) && transaction.effects.length > 0) {",
    "    const readPath = (value: unknown, segments: readonly string[]): unknown =>",
    "      segments.reduce<unknown>((current, segment) =>",
    "        current && typeof current === \"object\" ? (current as Record<string, unknown>)[segment] : undefined, value);",
    "    const resolveValue = (value: unknown): unknown => {",
    "      if (Array.isArray(value)) return value.map(resolveValue);",
    "      if (!value || typeof value !== \"object\") return value;",
    "      const record = value as Record<string, unknown>;",
    "      if (Object.keys(record).length === 1 && Object.prototype.hasOwnProperty.call(record, \"$arg\")) {",
    "        const path = record.$arg;",
    "        const segments = Array.isArray(path)",
    "          ? path.filter((part): part is string => typeof part === \"string\" && part.trim().length > 0)",
    "          : typeof path === \"string\" ? path.split(\".\").map((part) => part.trim()).filter(Boolean) : [];",
    "        return segments.length > 0 ? readPath(args, segments) : undefined;",
    "      }",
    "      return Object.fromEntries(Object.entries(record).map(([key, item]) => [key, resolveValue(item)]));",
    "    };",
    "    const containsUndefined = (value: unknown): boolean =>",
    "      value === undefined || (Array.isArray(value)",
    "        ? value.some(containsUndefined)",
    "        : !!value && typeof value === \"object\" && Object.values(value as Record<string, unknown>).some(containsUndefined));",
    "    const patches: Array<{ entity?: string; collection?: string; rowId: string; op: \"patch\" | \"insert\" | \"upsert\" | \"delete\"; fields?: Record<string, unknown> }> = [];",
    "    for (const effect of transaction.effects) {",
    "      const rawId = Array.isArray(effect.id) ? readPath(args, effect.id) : effect.id;",
    "      const rowId = String(rawId ?? \"\").trim();",
    "      if (!rowId || !effect.entity) return [];",
    "      if (effect.operation === \"delete\") {",
    "        patches.push({ entity: effect.entity, rowId, op: \"delete\" });",
    "        continue;",
    "      }",
    "      const template = effect.operation === \"patch\" ? effect.fields : effect.value;",
    "      if (!template || typeof template !== \"object\" || Array.isArray(template)) return [];",
    "      const fields = resolveValue(template);",
    "      if (!fields || typeof fields !== \"object\" || Array.isArray(fields) || containsUndefined(fields)) return [];",
    "      patches.push({ entity: effect.entity, rowId, op: effect.operation === \"upsert\" ? \"upsert\" : \"patch\", fields: { ...(fields as Record<string, unknown>) } });",
    "    }",
    "    return patches;",
    "  }",
    "  return [];",
    "}",
    "",
  ];
  return lines.join("\n");
}

function functionTypeName(path: string, suffix: "Args" | "Result"): string {
  const parts = path.split(/[^A-Za-z0-9_$]+/).filter(Boolean);
  const stem = parts.map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join("") || "Function";
  return `${/^[0-9]/.test(stem) ? "Fn" : ""}${stem}${suffix}`;
}

function renderSchemaType(schema: ModuleSchema | undefined): string {
  if (!schema) return "JsonValue";
  switch (schema.kind) {
    case "string": return "string";
    case "number": return "number";
    case "boolean": return "boolean";
    case "null": return "null";
    case "any": return "JsonValue";
    case "id": return "string";
    case "literal": return renderLiteralType(schema.value);
    case "array": return `Array<${renderSchemaType(schema.items)}>`;
    case "record": return `Record<string, ${renderSchemaType(schema.values)}>`;
    case "optional": return `${renderSchemaType(schema.value)} | undefined`;
    case "object": {
      const fields = Object.entries(schema.fields).map(([key, value]) => {
        const property = /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(key) ? key : JSON.stringify(key);
        const optional = value.kind === "optional" ? "?" : "";
        return `  ${property}${optional}: ${renderSchemaType(value)};`;
      });
      if (schema.allowUnknown) fields.push("  [key: string]: JsonValue;");
      return fields.length === 0 ? "{ }" : `{\n${fields.join("\n")}\n}`;
    }
  }
}

function renderLiteralType(value: JsonValue): string {
  if (value === null) return "null";
  if (typeof value === "string") return JSON.stringify(value);
  if (typeof value === "boolean" || typeof value === "number") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(renderLiteralType).join(", ")}]`;
  const fields = Object.entries(value).map(([key, nested]) => `${JSON.stringify(key)}: ${renderLiteralType(nested)}`);
  return fields.length === 0 ? "{ }" : `{ ${fields.join(", ")} }`;
}

function renderSchemaIndex(manifest: Manifest) {
  const controlPlane = renderSchemaObject("control-plane", manifest.schema.controlPlaneTables, 0);
  const tenant = renderSchemaObject("tenant", manifest.schema.tenantTables, 0);
  return [
    "// Generated by gonvex dev. Do not edit.",
    "",
    `export const controlPlane = ${controlPlane} as const;`,
    "",
    `export const tenant = ${tenant} as const;`,
    "",
    "export const tables = tenant.tables;",
    "",
    "export const schema = {",
    "  controlPlane,",
    "  tenant,",
    "  tables,",
    "} as const;",
    "",
    "export type ControlPlaneTableName = keyof typeof controlPlane.tables;",
    "export type TenantTableName = keyof typeof tenant.tables;",
    "export type TableName = TenantTableName;",
    "",
  ].join("\n");
}

function renderScopedSchemaModule(scope: "control-plane" | "tenant", tables: Record<string, Table>) {
  return [
    "// Generated by gonvex dev. Do not edit.",
    "",
    `export const schema = ${renderSchemaObject(scope, tables, 0)} as const;`,
    "",
    ...(scope === "control-plane" ? ["export const controlPlane = schema;"] : ["export const tenant = schema;"]),
    "export const tables = schema.tables;",
    "",
    "export type TableName = keyof typeof tables;",
    "",
  ].join("\n");
}

function renderScopedTablesModule(scope: "control-plane" | "tenant", tables: Record<string, Table>) {
  return [
    "// Generated by gonvex dev. Do not edit.",
    "",
    `export const scope = ${JSON.stringify(scope)} as const;`,
    "",
    `export const tables = ${renderObject(tables, 0)} as const;`,
    "",
    "export type TableName = keyof typeof tables;",
    "",
  ].join("\n");
}

function renderSchemaObject(scope: "control-plane" | "tenant", tables: Record<string, Table>, depth: number) {
  return renderObject({ scope, tables }, depth);
}

function renderObject(value: any, depth: number): string {
  if (!value || typeof value !== "object" || Array.isArray(value)) return JSON.stringify(value);
  const entries = Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
  if (isFunctionRef(value)) {
    const visible = Object.fromEntries(entries.filter(([key]) => !key.startsWith("__")));
    return `${renderObject(visible, depth)} as unknown as FunctionReference<${JSON.stringify(value.kind)}, ${value.__argsType}, ${value.__resultType}>`;
  }
  const indent = "  ".repeat(depth);
  const childIndent = "  ".repeat(depth + 1);
  const lines = ["{"];
  for (const [key, child] of entries) {
    lines.push(`${childIndent}${propertyKey(key)}: ${renderObject(child, depth + 1)},`);
  }
  lines.push(`${indent}}`);
  return lines.join("\n");
}

function isFunctionRef(value: any) {
  return typeof value.kind === "string"
    && typeof value.path === "string"
    && typeof value.__argsType === "string"
    && typeof value.__resultType === "string";
}

function propertyKey(key: string) {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(key) ? key : JSON.stringify(key);
}

function emptySchemaDefinition(): SchemaDefinition {
  return {
    tables: {},
    controlPlaneTables: {},
    tenantTables: {},
  };
}

function mergeSchemaDefinition(target: SchemaDefinition, source: SchemaDefinition) {
  Object.assign(target.controlPlaneTables, source.controlPlaneTables);
  Object.assign(target.tenantTables, source.tenantTables);
  target.tables = target.tenantTables;
}

async function syncRuntime(settings: Settings, manifest: Manifest): Promise<SchemaDefinition | undefined> {
  const response = await fetch(`${settings.runtimeURL.replace(/\/$/, "")}/dev/sync`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(settings.projectID ? { "x-gonvex-project-id": settings.projectID } : {}),
      ...(settings.key ? { authorization: `Bearer ${settings.key}`, "x-gonvex-key": settings.key } : {}),
    },
    body: JSON.stringify(manifest),
  });
  if (!response.ok) throw new Error(`runtime returned ${response.status} ${response.statusText}: ${await response.text()}`);
  const payload = await response.json() as { schemaDefinition?: SchemaDefinition };
  return payload.schemaDefinition;
}

async function fetchProjectEnv(settings: Settings): Promise<RuntimeEnvVariable[]> {
  const response = await fetch(projectEnvURL(settings), {
    headers: projectAuthHeaders(settings),
  });
  if (!response.ok) throw new Error(`runtime returned ${response.status} ${response.statusText}: ${await response.text()}`);
  const payload = await response.json() as { variables?: RuntimeEnvVariable[] };
  return payload.variables ?? [];
}

async function saveProjectEnv(settings: Settings, name: string, value: string) {
  const response = await fetch(projectEnvURL(settings), {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...projectAuthHeaders(settings),
    },
    body: JSON.stringify({ name, value }),
  });
  if (!response.ok) throw new Error(`runtime returned ${response.status} ${response.statusText}: ${await response.text()}`);
}

async function replaceProjectEnv(settings: Settings, content: string): Promise<number> {
  const response = await fetch(projectEnvURL(settings), {
    method: "PUT",
    headers: {
      "content-type": "application/json",
      ...projectAuthHeaders(settings),
    },
    body: JSON.stringify({ content }),
  });
  if (!response.ok) throw new Error(`runtime returned ${response.status} ${response.statusText}: ${await response.text()}`);
  const payload = await response.json() as { count?: number };
  return typeof payload.count === "number" ? payload.count : 0;
}

async function deleteProjectEnv(settings: Settings, name: string) {
  const response = await fetch(projectEnvURL(settings), {
    method: "DELETE",
    headers: {
      "content-type": "application/json",
      ...projectAuthHeaders(settings),
    },
    body: JSON.stringify({ name }),
  });
  if (!response.ok) throw new Error(`runtime returned ${response.status} ${response.statusText}: ${await response.text()}`);
}

function projectEnvURL(settings: Settings) {
  return `${settings.runtimeURL.replace(/\/$/, "")}/dev/projects/${encodeURIComponent(settings.projectID)}/env`;
}

function projectAuthHeaders(settings: Settings): Record<string, string> {
  return settings.key ? { authorization: `Bearer ${settings.key}`, "x-gonvex-key": settings.key } : {};
}

function parseEnvSetArgs(positional: string[]) {
  const first = positional[0];
  if (!first) throw new Error("usage: gonvex env set NAME VALUE");
  const equalsIndex = first.indexOf("=");
  if (equalsIndex > 0 && positional.length === 1) {
    return { name: first.slice(0, equalsIndex), value: first.slice(equalsIndex + 1) };
  }
  const value = positional.slice(1).join(" ");
  if (!value) throw new Error("usage: gonvex env set NAME VALUE");
  return { name: first, value };
}

function dotEnvVariableNames(content: string): string[] {
  const names = new Set<string>();
  for (const rawLine of content.split(/\r?\n/)) {
    let line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    if (line.startsWith("export ")) line = line.slice("export ".length).trimStart();
    const equalsIndex = line.indexOf("=");
    if (equalsIndex < 1) continue;
    const name = line.slice(0, equalsIndex).trim();
    if (name) names.add(name);
  }
  return [...names];
}

function parseEnvCommandArgs(argv: string[]) {
  const options: Record<string, string> = {};
  const positional: string[] = [];
  let action = "";
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]!;
    if (["--project", "--runtime-url", "--project-id", "--key", "--file"].includes(arg)) {
      const value = argv[index + 1];
      if (value === undefined) throw new Error(`${arg} requires a value`);
      options[arg] = value;
      index += 1;
      continue;
    }
    if (!action && !arg.startsWith("-")) {
      action = arg;
      continue;
    }
    positional.push(arg);
  }
  return { action, options, positional };
}

async function runtimeHasManifest(settings: Settings, manifest: Manifest) {
  if (!manifest.project) return true;
  const url = new URL(`${settings.runtimeURL.replace(/\/$/, "")}/dev/manifest`);
  url.searchParams.set("project", manifest.project);
  const response = await fetch(url, {
    headers: {
      ...(settings.projectID ? { "x-gonvex-project-id": settings.projectID } : {}),
      ...(settings.key ? { authorization: `Bearer ${settings.key}`, "x-gonvex-key": settings.key } : {}),
    },
  });
  if (!response.ok) return false;
  const current = await response.json() as Manifest;
  return current.project === manifest.project
    && deployedSourceHash(current) === deployedSourceHash(manifest)
    && Object.keys(current.functions ?? {}).length === Object.keys(manifest.functions ?? {}).length;
}

function deployedSourceHash(manifest: Manifest) {
  return manifest.module.hash;
}

async function ensureProjectSettings(root: string, settings: Settings, options: { keyWasExplicit: boolean; runtimeWasExplicit: boolean }): Promise<Settings> {
  if (settings.key) return settings;
  if (options.keyWasExplicit) return settings;
  let accountToken = await accountAccessTokenForRuntime(settings.runtimeURL);
  const configuredProject = await findRuntimeProject(settings.runtimeURL, settings.projectID, accountToken).catch(() => null);
  if (configuredProject) {
    if (accountToken) {
      const projectKey = await fetchRuntimeProjectKey(settings.runtimeURL, configuredProject.id, accountToken);
      await writeProjectEnv(root, settings.runtimeURL, configuredProject.id, projectKey);
      console.log(`[gonvex] configured ${configuredProject.id} in .env.local`);
      return { ...settings, projectID: configuredProject.id, key: projectKey };
    }
    await writeProjectEnv(root, settings.runtimeURL, configuredProject.id, settings.key);
    console.log(`[gonvex] configured ${configuredProject.id} in .env.local`);
    console.warn("[gonvex] GONVEX_PROJECT_KEY is not configured; run 'gonvex login' or provide the project key.");
    return { ...settings, projectID: configuredProject.id };
  }
  if (!process.stdin.isTTY || !process.stdout.isTTY) {
    console.warn("[gonvex] GONVEX_PROJECT_KEY is not configured; runtime sync will fail if the runtime requires project keys.");
    return settings;
  }

  const rl = createInterface({ input: process.stdin, output: process.stdout });
  try {
    console.log("[gonvex] No Gonvex project is configured for this app.");
    const runtimeURL = await promptDefault(rl, "Runtime URL", settings.runtimeURL || defaultRuntimeURL);
    settings = { ...settings, runtimeURL };
    accountToken = await accountAccessTokenForRuntime(runtimeURL);

    const projects = await fetchRuntimeProjects(runtimeURL, accountToken);
    let project: RuntimeProject;
    if (projects.length > 0) {
      console.log("[gonvex] Choose a project:");
      projects.forEach((item, index) => {
        console.log(`  ${index + 1}. ${item.name} (${item.id})`);
      });
      console.log(`  ${projects.length + 1}. Create a new project`);
      const choice = await promptDefault(rl, "Project", "1");
      const index = Number.parseInt(choice, 10);
      if (Number.isFinite(index) && index >= 1 && index <= projects.length) {
        project = projects[index - 1]!;
        const projectKey = accountToken
          ? await fetchRuntimeProjectKey(runtimeURL, project.id, accountToken)
          : await promptDefault(rl, "Project key", "");
        if (!projectKey) throw new Error("Gonvex project key is required for existing projects");
        const next = { ...settings, projectID: project.id, key: projectKey };
        await writeProjectEnv(root, runtimeURL, project.id, projectKey);
        console.log(`[gonvex] configured ${project.id} in .env.local`);
        return next;
      } else {
        const created = await createRuntimeProject(runtimeURL, await promptDefault(rl, "New project name", basename(root)), accountToken);
        project = created.project;
        const next = { ...settings, projectID: project.id, key: created.projectKey };
        await writeProjectEnv(root, runtimeURL, project.id, created.projectKey);
        console.log(`[gonvex] configured ${project.id} in .env.local`);
        return next;
      }
    } else {
      const created = await createRuntimeProject(runtimeURL, await promptDefault(rl, "New project name", basename(root)), accountToken);
      project = created.project;
      const next = { ...settings, projectID: project.id, key: created.projectKey };
      await writeProjectEnv(root, runtimeURL, project.id, created.projectKey);
      console.log(`[gonvex] configured ${project.id} in .env.local`);
      return next;
    }
  } finally {
    rl.close();
  }
}

async function findRuntimeProject(runtimeURL: string, projectID: string, accountToken?: string): Promise<RuntimeProject | null> {
  const wanted = projectID.trim();
  if (!wanted) return null;
  const projects = await fetchRuntimeProjects(runtimeURL, accountToken);
  return projects.find((project) => project.id === wanted) ?? null;
}

async function fetchRuntimeProjects(runtimeURL: string, accountToken?: string): Promise<RuntimeProject[]> {
  const response = await fetch(`${runtimeURL.replace(/\/$/, "")}/dev/projects`, {
    headers: accountToken ? accountHeaders(accountToken) : undefined,
  });
  const payload = await runtimeJSON<{ projects?: RuntimeProject[] }>(response);
  return payload.projects ?? [];
}

async function createRuntimeProject(runtimeURL: string, name: string, accountToken?: string, databaseMode = "single"): Promise<CreatedRuntimeProject> {
  const response = await fetch(`${runtimeURL.replace(/\/$/, "")}/dev/projects`, {
    method: "POST",
    headers: { ...(accountToken ? accountHeaders(accountToken) : {}), "content-type": "application/json" },
    body: JSON.stringify({ name, databaseMode }),
  });
  const payload = await runtimeJSON<{ project: RuntimeProject; projectKey?: string }>(response);
  if (!payload.project?.id) throw new Error("runtime did not return project details");
  if (!payload.projectKey) throw new Error("runtime did not return a project key");
  return { project: payload.project, projectKey: payload.projectKey };
}

async function fetchRuntimeProjectKey(runtimeURL: string, projectID: string, accountToken?: string): Promise<string> {
  const response = await fetch(`${runtimeURL.replace(/\/$/, "")}/dev/projects/${encodeURIComponent(projectID)}/key`, {
    method: "POST",
    headers: accountToken ? accountHeaders(accountToken) : undefined,
  });
  const payload = await runtimeJSON<{ projectKey?: string }>(response);
  if (!payload.projectKey) throw new Error("runtime did not return a project key");
  return payload.projectKey;
}

async function promptDefault(rl: ReturnType<typeof createInterface>, label: string, fallback: string) {
  const answer = (await rl.question(`${label} (${fallback}): `)).trim();
  return answer || fallback;
}

async function writeProjectEnv(root: string, runtimeURL: string, projectID: string, projectKey: string, overwrite = false) {
  await upsertEnvLocal(root, {
    GONVEX_PROJECT_ID: projectID,
    GONVEX_RUNTIME_URL: runtimeURL,
    GONVEX_PROJECT_KEY: projectKey,
    VITE_GONVEX_PROJECT_ID: projectID,
    VITE_GONVEX_URL: runtimeURL,
    VITE_GONVEX_WS_URL: webSocketURL(runtimeURL),
    ...(isLocalRuntimeURL(runtimeURL) ? { VALKEY_URL: "redis://127.0.0.1:6380/0" } : {}),
  }, overwrite);
}

async function upsertEnvLocal(root: string, values: Record<string, string>, overwrite = false) {
  const envPath = join(root, ".env.local");
  const existing = existsSync(envPath) ? await readFile(envPath, "utf8") : "";
  const seen = new Set<string>();
  const lines = existing.split(/\r?\n/).filter((line, index, array) => index < array.length - 1 || line !== "");
  const next = lines.flatMap((line) => {
    const match = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=/);
    if (!match) return [line];
    const key = match[1]!;
    if (!(key in values)) return [line];
    if (seen.has(key)) return [];
    seen.add(key);
    if (!overwrite && envLineValue(line) !== "") return [line];
    return [`${key}=${values[key]}`];
  });
  for (const [key, value] of Object.entries(values)) {
    if (!seen.has(key)) next.push(`${key}=${value}`);
  }
  await writeFile(envPath, `${next.join("\n")}\n`, { mode: 0o600 });
  await chmod(envPath, 0o600).catch(() => {});
}

function envLineValue(line: string) {
  const index = line.indexOf("=");
  if (index === -1) return "";
  return line.slice(index + 1).trim().replace(/^['"]|['"]$/g, "");
}

function webSocketURL(runtimeURL: string) {
  return runtimeURL.replace(/^http:/, "ws:").replace(/^https:/, "wss:").replace(/\/$/, "") + "/ws";
}

function logStreamURL(settings: Settings) {
  const url = new URL(settings.runtimeURL.replace(/^http:/, "ws:").replace(/^https:/, "wss:").replace(/\/$/, "") + "/dev/logs/stream");
  if (settings.projectID) url.searchParams.set("project", settings.projectID);
  if (settings.key) url.searchParams.set("key", settings.key);
  url.searchParams.set("replay", "0");
  return url.toString();
}

async function startRuntimeLogStream(settings: Settings, signal: AbortSignal, options: RuntimeLogOptions) {
  if (!settings.projectID || typeof globalThis.WebSocket !== "function") return;
  let warned = false;
  let retryDelay = 500;
  let stopped = false;
  signal.addEventListener("abort", () => {
    stopped = true;
  }, { once: true });

  while (!stopped && !signal.aborted) {
    const socket = new globalThis.WebSocket(logStreamURL(settings));
    const closed = new Promise<void>((resolve) => {
      const close = () => resolve();
      socket.addEventListener("open", () => {
        retryDelay = 500;
      });
      socket.addEventListener("message", (event) => {
        printRuntimeLogMessage(event.data, options);
      });
      socket.addEventListener("error", () => {
        if (!warned) {
          warned = true;
          console.warn(`[gonvex] runtime log stream unavailable for ${settings.projectID}; continuing without streamed logs`);
        }
      });
      socket.addEventListener("close", close, { once: true });
      signal.addEventListener("abort", () => {
        try {
          socket.close();
        } catch {
          // ignore close races
        }
        close();
      }, { once: true });
    });
    await closed;
    if (stopped || signal.aborted) return;
    await sleep(retryDelay, signal).catch(() => {});
    retryDelay = Math.min(retryDelay * 2, 5000);
  }
}

function printRuntimeLogMessage(data: unknown, options: RuntimeLogOptions) {
  let payload: { type?: string; log?: RuntimeLogEntry };
  try {
    payload = JSON.parse(typeof data === "string" ? data : Buffer.from(data as ArrayBuffer).toString("utf8"));
  } catch {
    return;
  }
  if (payload.type !== "log" || !payload.log) return;
  const entry = payload.log;
  if (!shouldPrintRuntimeLogEntry(entry, options)) return;
  const duration = typeof entry.durationMs === "number" ? ` ${entry.durationMs.toFixed(1)}ms` : "";
  const cache = entry.cache ? ` cache=${entry.cache}` : "";
  const error = entry.error ? ` ${color.red(entry.error)}` : "";
  const outcome = colorRuntimeOutcome(entry.outcome ?? "ok");
  console.log(`${color.dim("[gonvex runtime]")} ${entry.kind ?? "event"} ${entry.path ?? ""} ${outcome}${duration}${cache}${error}`.trimEnd());
}

function colorRuntimeOutcome(outcome: string) {
  const normalized = outcome.toLowerCase();
  if (normalized === "error" || normalized === "failed" || normalized === "failure") return color.red(outcome);
  if (normalized === "warn" || normalized === "warning") return color.orange(outcome);
  if (normalized === "ok" || normalized === "success" || normalized === "succeeded") return color.green(outcome);
  return color.yellow(outcome);
}

function shouldPrintRuntimeLogEntry(entry: RuntimeLogEntry, options: RuntimeLogOptions) {
  if (options.verbose) return true;
  const outcome = String(entry.outcome ?? "").toLowerCase();
  return Boolean(entry.error) || outcome === "error" || outcome === "warn" || outcome === "warning";
}

function normalizeRuntimeURL(value: string) {
  const trimmed = value.trim().replace(/\/+$/, "");
  if (!trimmed) return defaultRuntimeURL;
  let parsed: URL;
  try {
    parsed = new URL(trimmed);
  } catch {
    throw new Error(`invalid runtime URL ${value}`);
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") throw new Error("runtime URL must use http or https");
  return parsed.toString().replace(/\/+$/, "");
}

function cliConfigPath() {
  if (process.env.GONVEX_CONFIG_PATH) return resolve(process.env.GONVEX_CONFIG_PATH);
  const configRoot = process.env.XDG_CONFIG_HOME ? resolve(process.env.XDG_CONFIG_HOME) : join(homedir(), ".config");
  return join(configRoot, "gonvex", "config.json");
}

function emptyCLIConfig(): CLIConfig {
  return { version: 1, runtimes: {} };
}

async function readCLIConfig(): Promise<CLIConfig> {
  const path = cliConfigPath();
  if (!existsSync(path)) return emptyCLIConfig();
  let parsed: Partial<CLIConfig>;
  try {
    parsed = JSON.parse(await readFile(path, "utf8")) as Partial<CLIConfig>;
  } catch (error) {
    throw new Error(`could not read Gonvex CLI config ${path}: ${error instanceof Error ? error.message : String(error)}`);
  }
  return {
    version: 1,
    currentRuntime: parsed.currentRuntime,
    runtimes: parsed.runtimes && typeof parsed.runtimes === "object" ? parsed.runtimes : {},
  };
}

async function writeCLIConfig(config: CLIConfig) {
  const path = cliConfigPath();
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  await writeFile(path, `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600 });
  await chmod(path, 0o600).catch(() => {});
}

async function saveAccountProfile(runtimeURL: string, accessToken: string, identity: AccountIdentity, expiresAt?: number) {
  runtimeURL = normalizeRuntimeURL(runtimeURL);
  const config = await readCLIConfig();
  config.currentRuntime = runtimeURL;
  config.runtimes[runtimeURL] = {
    runtimeURL,
    accessToken,
    email: identity.account.email,
    name: identity.account.name,
    authentication: identity.authentication,
    permissions: identity.permissions,
    ...(expiresAt ? { expiresAt } : {}),
  };
  await writeCLIConfig(config);
}

async function removeAccountProfile(runtimeURL: string) {
  runtimeURL = normalizeRuntimeURL(runtimeURL);
  const config = await readCLIConfig();
  if (!config.runtimes[runtimeURL]) return false;
  delete config.runtimes[runtimeURL];
  if (config.currentRuntime === runtimeURL) config.currentRuntime = Object.keys(config.runtimes)[0];
  await writeCLIConfig(config);
  return true;
}

async function accountRuntimeForArgs(argv: string[]) {
  const explicit = valueFor(argv, "--runtime-url") ?? valueFor(argv, "--runtime") ?? process.env.GONVEX_RUNTIME_URL;
  if (explicit) return normalizeRuntimeURL(explicit);
  const config = await readCLIConfig();
  return normalizeRuntimeURL(config.currentRuntime ?? defaultRuntimeURL);
}

async function accountAccessTokenForRuntime(runtimeURL: string): Promise<string | undefined> {
  if (process.env.GONVEX_ACCOUNT_TOKEN?.trim()) return process.env.GONVEX_ACCOUNT_TOKEN.trim();
  const config = await readCLIConfig();
  const profile = config.runtimes[normalizeRuntimeURL(runtimeURL)];
  if (profile?.expiresAt && profile.expiresAt <= Date.now()) {
    throw new Error(`saved login for ${normalizeRuntimeURL(runtimeURL)} has expired; run 'gonvex login' again`);
  }
  return profile?.accessToken;
}

async function requireAccountAccessToken(runtimeURL: string) {
  const token = await accountAccessTokenForRuntime(runtimeURL);
  if (!token) throw new Error(`not logged in to ${normalizeRuntimeURL(runtimeURL)}; run 'gonvex login --runtime-url ${normalizeRuntimeURL(runtimeURL)}'`);
  return token;
}

function accountHeaders(accessToken: string): Record<string, string> {
  return { authorization: `Bearer ${accessToken}` };
}

async function runtimeJSON<T = Record<string, unknown>>(response: Response): Promise<T> {
  const text = await response.text();
  let payload: unknown = {};
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = { error: text };
    }
  }
  if (!response.ok) {
    const error = payload && typeof payload === "object" && "error" in payload ? String((payload as { error: unknown }).error) : response.statusText;
    const permission = payload && typeof payload === "object" && "permission" in payload ? ` (${String((payload as { permission: unknown }).permission)})` : "";
    throw new Error(`runtime returned ${response.status}: ${error}${permission}`);
  }
  return payload as T;
}

async function fetchAccountIdentity(runtimeURL: string, accessToken: string): Promise<AccountIdentity> {
  const response = await fetch(`${normalizeRuntimeURL(runtimeURL)}/dev/auth/me`, { headers: accountHeaders(accessToken) });
  return runtimeJSON<AccountIdentity>(response);
}

async function promptHidden(label: string): Promise<string> {
  if (!process.stdin.isTTY || !process.stdout.isTTY) throw new Error("password must be provided non-interactively with --password or GONVEX_PASSWORD");
  let hidden = false;
  const output = new Writable({
    write(chunk, encoding, callback) {
      if (!hidden) process.stdout.write(chunk, encoding as BufferEncoding);
      callback();
    },
  });
  const rl = createInterface({ input: process.stdin, output, terminal: true });
  try {
    const pending = rl.question(label);
    hidden = true;
    const answer = await pending;
    hidden = false;
    process.stdout.write("\n");
    return answer;
  } finally {
    rl.close();
  }
}

async function loadSettings(root: string, overrides: Partial<Settings>): Promise<Settings> {
  loadDotEnv(join(root, ".env.local"));
  loadDotEnv(join(root, ".env"));
  const config = await loadConfig(root);
  const cliConfig = await readCLIConfig();
  const key = overrides.key ?? process.env.GONVEX_PROJECT_KEY ?? process.env.GONVEX_DEPLOY_KEY ?? process.env.GONVEX_KEY ?? "";
  const explicitProjectID = overrides.projectID ?? process.env.GONVEX_PROJECT_ID ?? process.env.GONVEX_PROJECT ?? config.project;
  const overriddenProjectID = overrides.projectID?.trim() || undefined;
  const keyedProjectID = projectIDFromKey(key);
  if (overriddenProjectID && keyedProjectID && overriddenProjectID !== keyedProjectID) {
    throw new Error(`project id ${JSON.stringify(overriddenProjectID)} does not match the project key for ${JSON.stringify(keyedProjectID)}`);
  }
  return {
    projectID: overriddenProjectID ?? keyedProjectID ?? (explicitProjectID?.trim() || basename(root)),
    runtimeURL: normalizeRuntimeURL(overrides.runtimeURL ?? process.env.GONVEX_RUNTIME_URL ?? config.runtime ?? cliConfig.currentRuntime ?? defaultRuntimeURL),
    key,
  };
}

function projectIDFromKey(key: string) {
  const trimmed = key.trim();
  if (!trimmed.startsWith("gvx_") || trimmed.startsWith("gvx_pat_")) return undefined;
  const payload = trimmed.slice("gvx_".length);
  let encodedProject = payload.split(".", 1)[0];
  if (encodedProject === payload) {
    const parts = trimmed.split("_");
    if (parts.length !== 3 || parts[0] !== "gvx" || !parts[1]) return undefined;
    encodedProject = parts[1]!;
  }
  try {
    return Buffer.from(encodedProject, "base64url").toString("utf8").trim() || undefined;
  } catch {
    return undefined;
  }
}

async function loadConfig(root: string): Promise<ProjectConfig> {
  try {
    return JSON.parse(await readFile(join(root, "gonvex.json"), "utf8"));
  } catch {
    return {};
  }
}

function loadDotEnv(path: string) {
  if (!existsSync(path)) return;
  const content = readFileSyncText(path);
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const index = line.indexOf("=");
    if (index === -1) continue;
    const key = line.slice(0, index).trim();
    if (!key || process.env[key] !== undefined) continue;
    process.env[key] = line.slice(index + 1).trim().replace(/^['"]|['"]$/g, "");
  }
}

function readFileSyncText(path: string) {
  return existsSync(path) ? readFileSync(path, "utf8") : "";
}

async function migrationFiles(root: string): Promise<string[]> {
  if (!existsSync(root)) return [];
  const entries = await readdir(root, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      files.push(...await migrationFiles(path));
    } else if (entry.isFile() && entry.name.endsWith(".sql")) {
      files.push(path);
    }
  }
  return files.sort();
}

async function filesFingerprint(files: string[]) {
  const hash = createHash("sha256");
  for (const file of files) {
    const info = await stat(file);
    hash.update(`${file}:${info.mtimeMs}:${info.size};`);
  }
  return hash.digest("hex");
}

async function copyTemplate(template: string, target: string, options: { overwrite?: boolean } = {}) {
  const source = templateDir(template);
  if (!existsSync(source)) throw new Error(`unknown template ${template}`);
  await copyDir(source, target, options.overwrite ?? true);
}

async function copyDir(source: string, target: string, overwrite: boolean) {
  await mkdir(target, { recursive: true });
  for (const entry of await readdir(source, { withFileTypes: true })) {
    if (entry.name === "node_modules" || entry.name === "dist" || entry.name === "_build") continue;
    const sourcePath = join(source, entry.name);
    const targetPath = join(target, entry.name === "_gitignore" ? ".gitignore" : entry.name);
    if (entry.isDirectory()) {
      await copyDir(sourcePath, targetPath, overwrite);
    } else if (overwrite || !existsSync(targetPath)) {
      await mkdir(dirname(targetPath), { recursive: true });
      await copyFile(sourcePath, targetPath);
    }
  }
}

async function rewritePackageName(root: string, name: string) {
  const packagePath = join(root, "package.json");
  const packageJSON = JSON.parse(await readFile(packagePath, "utf8"));
  packageJSON.name = name;
  await writeFile(packagePath, `${JSON.stringify(packageJSON, null, 2)}\n`);
}

async function rewriteGonvexConfig(root: string, project: string, runtime: string) {
  const configPath = join(root, "gonvex.json");
  if (!existsSync(configPath)) return;
  const config = JSON.parse(await readFile(configPath, "utf8"));
  config.project = project;
  config.runtime = runtime;
  await writeFile(configPath, `${JSON.stringify(config, null, 2)}\n`);
}

async function writeEnvLocal(root: string, project: string, runtime: string) {
  const envPath = join(root, ".env.local");
  if (existsSync(envPath)) return;
  const wsURL = runtime.replace(/^http:/, "ws:").replace(/^https:/, "wss:").replace(/\/$/, "") + "/ws";
  const valkey = isLocalRuntimeURL(runtime) ? "VALKEY_URL=redis://127.0.0.1:6380/0\n" : "";
  await writeFile(envPath, `GONVEX_PROJECT_ID=${project}\nGONVEX_RUNTIME_URL=${runtime}\nGONVEX_PROJECT_KEY=\n${valkey}VITE_GONVEX_PROJECT_ID=${project}\nVITE_GONVEX_URL=${runtime}\nVITE_GONVEX_WS_URL=${wsURL}\n`);
}

function isLocalRuntimeURL(runtime: string) {
  try {
    const hostname = new URL(runtime).hostname;
    return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "::1";
  } catch {
    return false;
  }
}

function templateDir(template: string) {
  const packageTemplate = resolve(dirname(fileURLToPath(import.meta.url)), "templates", template);
  if (existsSync(packageTemplate)) return packageTemplate;
  return resolve(dirname(fileURLToPath(import.meta.url)), "..", "..", "..", "templates", template);
}

function valueFor(args: string[], key: string) {
  const index = args.indexOf(key);
  if (index === -1) return undefined;
  return args[index + 1];
}

function valuesFor(args: string[], key: string) {
  const values: string[] = [];
  for (let index = 0; index < args.length; index += 1) {
    if (args[index] === key && args[index + 1] !== undefined) {
      values.push(args[index + 1]!);
      index += 1;
    }
  }
  return values;
}

function positionalArgs(args: string[], optionsWithValues: string[]) {
  const positional: string[] = [];
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (optionsWithValues.includes(arg)) {
      index += 1;
      continue;
    }
    if (!arg.startsWith("-")) positional.push(arg);
  }
  return positional;
}

function basename(path: string) {
  const parts = path.split(/[\\/]/).filter(Boolean);
  return parts.at(-1) ?? "app";
}

function sleep(ms: number, signal?: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => {
      clearTimeout(timeout);
      reject(new DOMException("aborted", "AbortError"));
    }, { once: true });
  });
}

function printHelp() {
  console.log("Usage: gonvex <dev|codegen|functions|init|create|env|auth|login|logout|whoami|project|token> [options]");
  console.log("  gonvex dev [--project <path>] [--runtime-url <url>] [--project-id <id>] [--key <key>] [--once] [--verbose-logs] [-- <command>]");
  console.log("  gonvex codegen [--project <path>] [--project-id <id>]");
  console.log("  gonvex functions emit --format ndjson|typescript [--output FILE] [--project <path>]");
  console.log("  gonvex functions check [--project <path>]");
  console.log("  gonvex init [--template vite-react] [--project <id>] [--runtime <url>]");
  console.log("  gonvex create <app-name> [--runtime-url <url>] [--provision] [--database-mode single|multiTenant] [--google-auth] [--origin <url>]... [--signup-mode personal|inviteOnly] [--owner <email>]");
  console.log("  gonvex env <list|get|set|push|remove> [--project <path>] [--runtime-url <url>] [--project-id <id>] [--key <key>]");
  console.log("  gonvex auth <add|remove|status|users> [google] [options]");
  console.log("  gonvex login [--runtime-url <url>] [--email <email> | --token <personal-access-token>]");
  console.log("  gonvex logout [--runtime-url <url>]");
  console.log("  gonvex whoami [--runtime-url <url>] [--json]");
  console.log("  gonvex project <list|create|select> [options]");
  console.log("  gonvex token <list|create|revoke|permissions> [options]");
}

function printAuthHelp() {
  console.log("Usage: gonvex auth <command> [options]");
  console.log("  gonvex auth add google [--origin URL]... [--callback-path /] [--signup-mode personal|inviteOnly]");
  console.log("  gonvex auth configure firebase --firebase-project-id ID [--firebase-tenant-id ID] [--mode firebase|hybrid] [--admin-credentials-file FILE]");
  console.log("  gonvex auth configure external-oidc --issuer URL --audience VALUE --jwks-url URL [--mode external-oidc|hybrid]");
  console.log("  gonvex auth remove google [--origin URL]... [--callback-path /]");
  console.log("  gonvex auth status [--json]");
  console.log("  gonvex auth doctor [--json]");
  console.log("  gonvex auth accounts [--json]");
  console.log("  gonvex auth tenants list [--json]");
  console.log("  gonvex auth tenants create <name> [--owner <email>]");
  console.log("  gonvex auth memberships list --tenant <tenant-id> [--json]");
  console.log("  gonvex auth memberships add --tenant <tenant-id> --email <email> [--role member]");
  console.log("  gonvex auth memberships remove --tenant <tenant-id> (--member <member-id> | --email <invited-email>)");
  console.log("  gonvex auth account <disable|enable|delete> <account-id>");
}

function printEnvHelp() {
  console.log("Usage: gonvex env <command> [options]");
  console.log("  gonvex env list");
  console.log("  gonvex env get NAME");
  console.log("  gonvex env set NAME VALUE");
  console.log("  gonvex env set NAME=VALUE");
  console.log("  gonvex env push FILE");
  console.log("  gonvex env push --file FILE");
  console.log("  gonvex env remove NAME");
}

function printProjectHelp() {
  console.log("Usage: gonvex project <command> [options]");
  console.log("  gonvex project list [--runtime-url <url>] [--json]");
  console.log("  gonvex project create [name] [--runtime-url <url>] [--database-mode single|multiTenant] [--project-root <path>] [--json]");
  console.log("  gonvex project select <project-id> [--runtime-url <url>] [--project-root <path>]");
}

function printTokenHelp() {
  console.log("Usage: gonvex token <command> [options]");
  console.log("  gonvex token list [--runtime-url <url>] [--json]");
  console.log("  gonvex token create [name] [--permission <permission>]... [--full] [--expires-at <RFC3339>] [--json]");
  console.log("  gonvex token revoke <token-id> [--runtime-url <url>]");
  console.log("  gonvex token permissions");
}

function isCliEntrypoint() {
  const invokedPath = process.argv[1];
  if (!invokedPath) return false;
  try {
    return realpathSync(fileURLToPath(import.meta.url)) === realpathSync(resolve(invokedPath));
  } catch {
    return fileURLToPath(import.meta.url) === resolve(invokedPath);
  }
}

if (isCliEntrypoint()) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
