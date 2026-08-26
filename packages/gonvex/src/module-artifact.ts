// TypeScript server modules ship as a language-neutral module artifact: the
// runtime receives declarative function metadata plus a required,
// self-contained JavaScript bundle.
//
// Parsing stays regex- and scanner-based. Pulling the TypeScript compiler into
// the CLI at runtime
// would cost more than the declarative metadata this pipeline needs.
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { builtinModules } from "node:module";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";

import { rolldown, type OutputChunk, type RolldownPlugin } from "rolldown";

import type {
  ActionCapabilities,
  FunctionDependencies,
  FunctionEntry,
  FunctionKind,
  FilterOperator,
  JsonValue,
  LiveExpression,
  LiveQueryPlan,
  LiveValue,
  ModuleArtifact,
  ModuleCron,
  ModuleFunction,
  ModuleJavaScript,
  ModuleLanguage,
  ModuleSchema,
  ReplicaCollectionDefinition,
  VisibilityExpression,
  VisibilityPlan,
  VisibilitySet,
} from "./manifest-types.js";

/** Bumped whenever the artifact layout changes; mixed into the hash. */
export const moduleArtifactGeneration = 8;

/** Deterministic ESM output when gonvex.json does not name one. */
const defaultBundlePath = join("_build", "module.js");

const moduleSourceExtensions = [".ts", ".tsx", ".mts", ".cts"];
const skippedDirectories = new Set(["_build", "_generated", "node_modules", "dist", "build"]);
const defaultEntrypoints = ["index.ts", "index.mts", "index.tsx", "main.ts", "module.ts"];
const nodeBuiltinImports = new Set(
  builtinModules.flatMap((name) => name.startsWith("node:") ? [name, name.slice(5)] : [name, `node:${name}`]),
);
// `gonvex auth add google` writes gonvex/auth.tsx for the browser; it is not a
// server module and must not be treated as an executable backend module.
const skippedSourceFiles = new Set(["auth.tsx", "auth.ts"]);

type ModuleFunctionRegistration = {
  kind: FunctionKind;
  internal?: boolean;
  delivery?: ModuleFunction["delivery"];
};

const moduleFunctionKinds = new Map<string, ModuleFunctionRegistration>([
  ["query", { kind: "query", delivery: "oneShot" }],
  ["internalquery", { kind: "query", internal: true, delivery: "oneShot" }],
  ["livequery", { kind: "query", delivery: "live" }],
  ["replicacollection", { kind: "query", delivery: "replica" }],
  ["reducer", { kind: "reducer" }],
  ["internalreducer", { kind: "reducer", internal: true }],
  ["action", { kind: "action" }],
]);

const kindAlternation = "query|internalQuery|liveQuery|replicaCollection|reducer|internalReducer|action";
const definitionPattern = new RegExp(
  `export\\s+(?:const|let|var)\\s+([A-Za-z_$][A-Za-z0-9_$]*)\\s*(?::[^=;]+)?=\\s*(?:await\\s+)?(${kindAlternation})\\s*(<[^(){};]*>)?\\s*\\(`,
  "gi",
);
const registrationPattern = new RegExp(
  `\\b(?:app|server|gonvex)\\s*\\.\\s*(${kindAlternation})\\s*(<[^(){};]*>)?\\s*\\(`,
  "gi",
);
const visibilityDefinitionPattern = /export\s+(?:const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*(?::[^=;]+)?=\s*visibility\s*\(/gi;
const visibilityRegistrationPattern = /\b(?:app|server|gonvex)\s*\.\s*visibility\s*\(/gi;
const cronDefinitionPattern = /export\s+(?:const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*(?::[^=;]+)?=\s*(cron|tenantCron)\s*\(/gi;
const cronRegistrationPattern = /\b(?:app|server|gonvex)\s*\.\s*(cron|tenantCron)\s*\(/gi;
const invitationAcceptancePattern = /\binvitationAcceptance\s*\(\s*(["'`])([^"'`]+)\1\s*\)/g;
const identifierPattern = /[A-Za-z_$][A-Za-z0-9_$]*/y;
const keywordPattern = /(?:true|false|null|undefined)\b/y;
const numberPattern = /-?(?:0[xX][0-9a-fA-F_]+|\d[\d_]*(?:\.[\d_]*)?(?:[eE][+-]?\d+)?)/y;

export type ProjectLanguage = ModuleLanguage;

export type ModuleArtifactOptions = {
  root: string;
  backendDir: string;
  /** Module sources, absolute paths. */
  files: string[];
  /** Versioned SQL migrations, absolute paths. */
  migrations: string[];
  /** gonvex.json `module.entrypoint`, project-relative. */
  entrypoint?: string;
  /** gonvex.json `module.bundle`, project-relative output under gonvex/_build. */
  bundle?: string;
};

/**
 * Gonvex v2 application modules are TypeScript-only.
 */
export async function detectProjectLanguage(backendDir: string, declared?: string): Promise<ProjectLanguage> {
  const normalized = declared?.trim().toLowerCase();
  if (normalized === "ts" || normalized === "typescript") return "typescript";
  if (normalized) throw new Error(`unknown gonvex.json language ${JSON.stringify(declared)}; expected "typescript"`);
  if (!existsSync(backendDir)) return "typescript";
  const moduleSources = await moduleSourceFiles(backendDir);
  if (moduleSources.length === 0) throw new Error("Gonvex backend has no TypeScript module sources");
  return "typescript";
}

export async function moduleSourceFiles(backendDir: string): Promise<string[]> {
  return walkFiles(backendDir, isModuleSourceFile);
}

export async function buildModuleArtifact(options: ModuleArtifactOptions): Promise<ModuleArtifact> {
  const sources = [...options.files].sort();
  const entrypoint = resolveEntrypoint(options.root, options.backendDir, sources, options.entrypoint);
  const javascript = await bundleModuleJavaScript(options, entrypoint.absolute);
  const files: Record<string, string> = {};
  const functions: Record<string, ModuleFunction> = {};
  const visibilityPlans: Record<string, VisibilityPlan> = {};
  const crons: ModuleCron[] = [];
  let invitationAcceptanceReducer = "";
  for (const file of sources) {
    const contents = await readFile(file);
    files[projectPath(options.root, file)] = contents.toString("base64");
    for (const [path, entry] of parseModuleFunctions(options.root, options.backendDir, file, contents.toString("utf8"))) {
      if (path === "control" || path.startsWith("control.")) {
        throw new Error(`module function path ${JSON.stringify(path)} uses the host-reserved Control Plane namespace`);
      }
      if (functions[path]) throw new Error(`duplicate module function path ${JSON.stringify(path)}`);
      functions[path] = entry;
    }
    for (const plan of parseVisibilityDefinitions(contents.toString("utf8"))) {
      if (visibilityPlans[plan.table]) throw new Error(`duplicate visibility plan for table ${JSON.stringify(plan.table)}`);
      visibilityPlans[plan.table] = plan;
    }
    crons.push(...parseCronDefinitions(contents.toString("utf8")));
    invitationAcceptancePattern.lastIndex = 0;
    for (const match of contents.toString("utf8").matchAll(invitationAcceptancePattern)) {
      if (invitationAcceptanceReducer && invitationAcceptanceReducer !== match[2]) throw new Error("module declares more than one invitation acceptance Reducer");
      invitationAcceptanceReducer = match[2];
    }
  }
  // Versioned SQL migrations travel with the artifact so the runtime applies
  // the same schema changes as the source module.
  for (const file of [...options.migrations].sort()) {
    files[projectPath(options.root, file)] = (await readFile(file)).toString("base64");
  }
  const cronNames = new Set<string>();
  for (const cron of crons) {
    if (cronNames.has(cron.name)) throw new Error(`duplicate cron: ${cron.name}`);
    cronNames.add(cron.name);
    const target = functions[cron.function];
    if (!target) throw new Error(`cron ${JSON.stringify(cron.name)} targets unknown function ${JSON.stringify(cron.function)}`);
    if (target.kind === "query") throw new Error(`cron ${JSON.stringify(cron.name)} must target a reducer or action`);
  }
  for (const [path, definition] of Object.entries(functions)) {
    for (const [name, binding] of Object.entries(definition.actionCapabilities?.tools ?? {})) {
      const target = functions[binding.function];
      if (!target) throw new Error(`action ${JSON.stringify(path)} tool ${JSON.stringify(name)} targets unknown function ${JSON.stringify(binding.function)}`);
      if (target.kind !== binding.kind) throw new Error(`action ${JSON.stringify(path)} tool ${JSON.stringify(name)} kind does not match ${JSON.stringify(binding.function)}`);
      if (binding.kind === "query" && (!target.internal || (target.delivery ?? "oneShot") !== "oneShot")) throw new Error(`action ${JSON.stringify(path)} tool ${JSON.stringify(name)} must target an internal one-shot Query`);
      if (binding.kind === "reducer" && target.internal) throw new Error(`action ${JSON.stringify(path)} tool ${JSON.stringify(name)} must target a public business-intent Reducer`);
    }
  }
  const sortedFiles = sortedRecord(files);
  const sortedFunctions = sortedRecord(functions);
  const sortedVisibility = sortedRecord(visibilityPlans);
  const sortedCrons = crons.sort((left, right) => left.name < right.name ? -1 : left.name > right.name ? 1 : 0);
  if (invitationAcceptanceReducer) {
    const target = sortedFunctions[invitationAcceptanceReducer];
    if (!target || target.kind !== "reducer" || !target.internal) throw new Error("invitationAcceptance must target an internal Reducer");
  }
  return {
    language: "typescript",
    generation: moduleArtifactGeneration,
    hash: artifactHash({
      entrypoint: entrypoint.projectPath,
      files: sortedFiles,
      functions: sortedFunctions,
      visibility: sortedVisibility,
      crons: sortedCrons,
      javascript,
      invitationAcceptanceReducer,
    }),
    entrypoint: entrypoint.projectPath,
    functions: sortedFunctions,
    visibility: sortedVisibility,
    files: sortedFiles,
    javascript,
    ...(sortedCrons.length > 0 ? { crons: sortedCrons } : {}),
    ...(invitationAcceptanceReducer ? { invitationAcceptanceReducer } : {}),
  };
}

function parseCronDefinitions(source: string): ModuleCron[] {
  const crons: ModuleCron[] = [];
  cronDefinitionPattern.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = cronDefinitionPattern.exec(source)) !== null) {
    const openParen = match.index + match[0].length - 1;
    const call = readCallArguments(source, openParen);
    cronDefinitionPattern.lastIndex = Math.max(call.end, openParen + 1);
    const value = call.args[0]?.value;
    const name = stringMember(value, "name");
    const functionPath = stringMember(value, "function");
    if (!isJsonObject(value) || !name || !functionPath) {
      throw new Error(`cron export ${JSON.stringify(match[1])} must use a literal name and function`);
    }
    const intervalMs = numberMember(value, "intervalMs");
    const expression = stringMember(value, "expression");
    if ((intervalMs === undefined) === (expression === undefined)) {
      throw new Error(`cron ${JSON.stringify(name)} requires exactly one intervalMs or expression`);
    }
    if (intervalMs !== undefined && (!Number.isSafeInteger(intervalMs) || intervalMs <= 0)) {
      throw new Error(`cron ${JSON.stringify(name)} intervalMs must be a positive safe integer`);
    }
    if (expression !== undefined && !expression.trim()) {
      throw new Error(`cron ${JSON.stringify(name)} expression must be non-empty`);
    }
    const args = readMember(value, "args");
    crons.push({
      name,
      function: functionPath,
      scope: match[2] === "tenantCron" ? "tenant" : "project",
      ...(args === undefined ? {} : { args }),
      ...(intervalMs === undefined ? { expression } : { intervalMs }),
    });
  }

  // ModuleBuilder is the documented registration form (`app.cron(...)` and
  // `app.tenantCron(...)`). Keep these declarations in the language-neutral
  // artifact just like the exported helper form above.
  cronRegistrationPattern.lastIndex = 0;
  while ((match = cronRegistrationPattern.exec(source)) !== null) {
    const openParen = match.index + match[0].length - 1;
    const call = readCallArguments(source, openParen);
    cronRegistrationPattern.lastIndex = Math.max(call.end, openParen + 1);
    const value = call.args[0]?.value;
    const name = stringMember(value, "name");
    const functionPath = stringMember(value, "function");
    if (!isJsonObject(value) || !name || !functionPath) {
      throw new Error(`cron registration must use a literal name and function`);
    }
    const intervalMs = numberMember(value, "intervalMs");
    const expression = stringMember(value, "expression");
    if ((intervalMs === undefined) === (expression === undefined)) {
      throw new Error(`cron ${JSON.stringify(name)} requires exactly one intervalMs or expression`);
    }
    if (intervalMs !== undefined && (!Number.isSafeInteger(intervalMs) || intervalMs <= 0)) {
      throw new Error(`cron ${JSON.stringify(name)} intervalMs must be a positive safe integer`);
    }
    if (expression !== undefined && !expression.trim()) {
      throw new Error(`cron ${JSON.stringify(name)} expression must be non-empty`);
    }
    const args = readMember(value, "args");
    crons.push({
      name,
      function: functionPath,
      scope: match[1] === "tenantCron" ? "tenant" : "project",
      ...(args === undefined ? {} : { args }),
      ...(intervalMs === undefined ? { expression } : { intervalMs }),
    });
  }
  return crons;
}

function parseVisibilityDefinitions(source: string): VisibilityPlan[] {
  const plans: VisibilityPlan[] = [];
  visibilityDefinitionPattern.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = visibilityDefinitionPattern.exec(source)) !== null) {
    const openParen = match.index + match[0].length - 1;
    const call = readCallArguments(source, openParen);
    visibilityDefinitionPattern.lastIndex = Math.max(call.end, openParen + 1);
    const value = call.args[0]?.value;
    const plan = parseVisibilityPlan(value);
    if (!plan) throw new Error(`visibility export ${JSON.stringify(match[1])} must use a literal visibility plan`);
    plans.push(plan);
  }

  visibilityRegistrationPattern.lastIndex = 0;
  while ((match = visibilityRegistrationPattern.exec(source)) !== null) {
    const openParen = match.index + match[0].length - 1;
    const call = readCallArguments(source, openParen);
    visibilityRegistrationPattern.lastIndex = Math.max(call.end, openParen + 1);
    const plan = parseVisibilityPlan(call.args[0]?.value);
    if (!plan) throw new Error("module visibility registration must use a literal visibility plan");
    plans.push(plan);
  }
  return plans;
}

function parseVisibilityPlan(value: JsonValue | undefined): VisibilityPlan | undefined {
  const table = stringMember(value, "table");
  const key = stringMember(value, "key");
  const rawSets = readMember(value, "sets");
  const where = parseVisibilityExpression(readMember(value, "where"));
  if (!table || !key || !isJsonObject(rawSets) || !where) return undefined;
  const sets: Record<string, VisibilitySet> = {};
  for (const name of Object.keys(rawSets).sort()) {
    const candidate = rawSets[name];
    const setTable = stringMember(candidate, "table");
    const alias = stringMember(candidate, "alias");
    const select = stringMember(candidate, "select");
    const selectFrom = stringMember(candidate, "selectFrom");
    const rawJoins = readMember(candidate, "joins");
    const rawWhere = readMember(candidate, "where");
    if (!setTable || !select || !Array.isArray(rawJoins) || !Array.isArray(rawWhere)) return undefined;
    const joins = rawJoins.map((join) => ({
      table: stringMember(join, "table") ?? "",
      ...(stringMember(join, "alias") ? { alias: stringMember(join, "alias")! } : {}),
      ...(stringMember(join, "leftAlias") ? { leftAlias: stringMember(join, "leftAlias")! } : {}),
      leftColumn: stringMember(join, "leftColumn") ?? "",
      rightColumn: stringMember(join, "rightColumn") ?? "",
    }));
    const constraints = rawWhere.map((constraint) => ({
      table: stringMember(constraint, "table") ?? "",
      column: stringMember(constraint, "column") ?? "",
      context: stringMember(constraint, "context") as "account.id" | "member.id" | "tenant.id",
    }));
    if (joins.some((join) => !join.table || !join.leftColumn || !join.rightColumn) ||
      constraints.some((constraint) => !constraint.table || !constraint.column || !["account.id", "member.id", "tenant.id"].includes(constraint.context))) {
      return undefined;
    }
    sets[name] = {
      table: setTable,
      ...(alias ? { alias } : {}),
      select,
      ...(selectFrom ? { selectFrom } : {}),
      joins,
      where: constraints,
    };
  }
  if (!visibilityExpressionSetsExist(where, sets)) return undefined;
  return { table, key, sets, where };
}

function parseVisibilityExpression(value: JsonValue | undefined): VisibilityExpression | undefined {
  const operator = stringMember(value, "operator");
  if (!operator || !["public", "permission", "role", "eqContext", "inSet", "and", "or", "not"].includes(operator)) return undefined;
  const result: VisibilityExpression = { operator: operator as VisibilityExpression["operator"] };
  const column = stringMember(value, "column");
  const context = stringMember(value, "context");
  const set = stringMember(value, "set");
  const expressionValue = stringMember(value, "value");
  if (column) result.column = column;
  if (context === "account.id" || context === "member.id" || context === "tenant.id") result.context = context;
  if (set) result.set = set;
  if (expressionValue) result.value = expressionValue;
  const rawChildren = readMember(value, "children");
  if (Array.isArray(rawChildren)) {
    const children = rawChildren.map(parseVisibilityExpression);
    if (children.some((child) => child === undefined)) return undefined;
    result.children = children as VisibilityExpression[];
  }
  switch (result.operator) {
    case "public": return Object.keys(result).length === 1 ? result : undefined;
    case "permission":
    case "role": return result.value ? result : undefined;
    case "eqContext": return result.column && result.context ? result : undefined;
    case "inSet": return result.column && result.set ? result : undefined;
    case "and":
    case "or": return result.children?.length ? result : undefined;
    case "not": return result.children?.length === 1 ? result : undefined;
  }
}

function visibilityExpressionSetsExist(expression: VisibilityExpression, sets: Record<string, VisibilitySet>): boolean {
  if (expression.operator === "inSet" && (!expression.set || !(expression.set in sets))) return false;
  return (expression.children ?? []).every((child) => visibilityExpressionSetsExist(child, sets));
}

/** Projects the artifact functions onto the language-neutral manifest shape. */
export function moduleManifestFunctions(artifact: ModuleArtifact): Record<string, FunctionEntry> {
  const functions: Record<string, FunctionEntry> = {};
  for (const [path, entry] of Object.entries(artifact.functions)) {
    functions[path] = {
      kind: entry.kind,
      handler: entry.handler,
      file: entry.file,
      ...(isModuleSchema(entry.args) ? { args: entry.args } : {}),
      ...(isModuleSchema(entry.result) ? { result: entry.result } : {}),
      ...(entry.internal ? { internal: true } : {}),
      ...(entry.delivery ? { delivery: entry.delivery } : {}),
      ...(entry.dependencies ? { dependencies: entry.dependencies } : {}),
      ...(entry.replica ? { replica: entry.replica } : {}),
      ...(entry.offline === undefined ? {} : { offline: entry.offline }),
      ...(entry.optimistic === undefined ? {} : { optimistic: entry.optimistic }),
      ...(entry.actionProfile === undefined ? {} : { actionProfile: entry.actionProfile }),
      ...(entry.actionCapabilities === undefined ? {} : { actionCapabilities: entry.actionCapabilities }),
      ...(entry.interactive === undefined ? {} : { interactive: entry.interactive }),
      ...(entry.classification === undefined ? {} : { classification: entry.classification }),
      ...(entry.description === undefined ? {} : { description: entry.description }),
      ...(entry.agent === undefined ? {} : { agent: entry.agent }),
    };
  }
  return functions;
}

function artifactHash(input: {
  entrypoint: string;
  files: Record<string, string>;
  functions: Record<string, ModuleFunction>;
  visibility: Record<string, VisibilityPlan>;
  crons: ModuleCron[];
  javascript: ModuleJavaScript;
  invitationAcceptanceReducer?: string;
}) {
  const contract = {
    generation: moduleArtifactGeneration,
    language: "typescript",
    entrypoint: input.entrypoint,
    files: input.files,
    functions: input.functions,
    visibility: input.visibility,
    crons: input.crons,
    javascript: { path: input.javascript.path, hash: input.javascript.hash },
    invitationAcceptanceReducer: input.invitationAcceptanceReducer ?? "",
  };
  return createHash("sha256").update(canonicalJson(contract)).digest("hex");
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  return `{${Object.entries(value as Record<string, unknown>)
    .filter(([, child]) => child !== undefined)
    .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
    .map(([key, child]) => `${JSON.stringify(key)}:${canonicalJson(child)}`)
    .join(",")}}`;
}

async function bundleModuleJavaScript(options: ModuleArtifactOptions, entrypoint: string): Promise<ModuleJavaScript> {
  const buildDir = resolve(options.backendDir, "_build");
  const declaredOutput = options.bundle?.trim();
  if (declaredOutput && isAbsolute(declaredOutput)) {
    throw new Error("gonvex.json module.bundle must be a project-relative path under gonvex/_build");
  }
  const outputPath = declaredOutput ? resolve(options.root, declaredOutput) : resolve(options.backendDir, defaultBundlePath);
  assertInside(buildDir, outputPath, "gonvex.json module.bundle must resolve under gonvex/_build");

  await mkdir(dirname(outputPath), { recursive: true });
  let bundle: Awaited<ReturnType<typeof rolldown>>;
  try {
    bundle = await rolldown({
      input: entrypoint,
      cwd: options.root,
      platform: "neutral",
      tsconfig: false,
      external: () => false,
      resolve: {
        // The isolate implements Web APIs, not Node. Prefer packages' browser
        // branches so optional Node-only helpers never enter the signed bundle.
        conditionNames: ["browser", "import", "default"],
        mainFields: ["browser", "module", "main"],
      },
      plugins: [rejectNodeBuiltinsPlugin()],
    });
  } catch (error) {
    throw moduleBundleError(entrypoint, error);
  }

  try {
    const result = await bundle.generate({
      file: outputPath,
      format: "esm",
      codeSplitting: false,
      sourcemap: false,
    });
    const chunks = result.output.filter((item): item is OutputChunk => item.type === "chunk");
    const assets = result.output.filter((item) => item.type === "asset");
    if (chunks.length !== 1 || assets.length !== 0) {
      throw new Error(`expected one self-contained ESM chunk, received ${chunks.length} chunks and ${assets.length} assets`);
    }
    const chunk = chunks[0]!;
    const externalImports = [...chunk.imports, ...chunk.dynamicImports];
    if (externalImports.length > 0) {
      throw new Error(`module bundle contains unbundled imports: ${externalImports.join(", ")}`);
    }
    if (!chunk.code.trim()) throw new Error("module bundle is empty");

    await writeFile(outputPath, chunk.code, "utf8");
    const code = Buffer.from(chunk.code, "utf8");
    return {
      path: projectPath(options.root, outputPath),
      hash: createHash("sha256").update(code).digest("hex"),
      code: code.toString("base64"),
    };
  } catch (error) {
    throw moduleBundleError(entrypoint, error);
  } finally {
    await bundle.close();
  }
}

function rejectNodeBuiltinsPlugin(): RolldownPlugin {
  return {
    name: "gonvex-reject-node-builtins",
    resolveId(source, importer) {
      if (!source.startsWith("node:") && !nodeBuiltinImports.has(source)) return null;
      const importedBy = importer ? ` imported by ${importer}` : "";
      this.error(`Node runtime module ${JSON.stringify(source)} is unavailable in Gonvex modules${importedBy}`);
    },
  };
}

function moduleBundleError(entrypoint: string, error: unknown) {
  const detail = error instanceof Error ? error.message : String(error);
  return new Error(`failed to bundle TypeScript module ${entrypoint}: ${detail}`, { cause: error });
}

function resolveEntrypoint(root: string, backendDir: string, sources: string[], configured?: string) {
  const declared = configured?.trim();
  if (declared && isAbsolute(declared)) {
    throw new Error("gonvex.json module.entrypoint must be a project-relative path");
  }
  const absolute = declared ? resolve(root, declared) : defaultEntrypoints
    .map((candidate) => resolve(backendDir, candidate))
    .find((candidate) => sources.includes(candidate));
  if (!absolute) {
    throw new Error(`TypeScript modules require gonvex.json module.entrypoint or one of: ${defaultEntrypoints.map((name) => `gonvex/${name}`).join(", ")}`);
  }
  assertInside(root, absolute, "gonvex.json module.entrypoint must resolve inside the project");
  if (!sources.includes(absolute)) {
    throw new Error(`TypeScript module entrypoint ${projectPath(root, absolute)} is missing or is not a server module source`);
  }
  return { absolute, projectPath: projectPath(root, absolute) };
}

function assertInside(parent: string, candidate: string, message: string) {
  const nested = relative(resolve(parent), resolve(candidate));
  if (nested === ".." || nested.startsWith(`..${sep}`) || isAbsolute(nested)) throw new Error(message);
}

function parseModuleFunctions(root: string, backendDir: string, file: string, source: string): Array<[string, ModuleFunction]> {
  const relativeFile = projectPath(root, file);
  const prefix = functionPathPrefix(backendDir, file);
  const entries: Array<[string, ModuleFunction]> = [];

  // `export const list = query({ ... })` names the function after its module
  // path and exported binding, the way the generated api.ts addresses it.
  definitionPattern.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = definitionPattern.exec(source)) !== null) {
    const registration = moduleFunctionKinds.get((match[2] ?? "").toLowerCase());
    const openParen = match.index + match[0].length - 1;
    const call = readCallArguments(source, openParen);
    definitionPattern.lastIndex = Math.max(call.end, openParen + 1);
    if (!registration) continue;
    const exportName = match[1]!;
    const options = call.args.find((argument) => argument.entries)?.entries;
    // `query(listMessages)` passes the handler directly instead of options.
    const firstArgument = call.args[0];
    const inlineHandler = firstArgument && !firstArgument.entries ? identifierText(firstArgument.text) : undefined;
    const handlerEntry = options?.get("handler");
    const declaredPath = stringEntry(options, "name");
    entries.push([
      declaredPath ?? (prefix ? `${prefix}.${exportName}` : exportName),
      moduleFunction({
        ...registration,
        path: declaredPath ?? (prefix ? `${prefix}.${exportName}` : exportName),
        file: relativeFile,
        handler: identifierText(handlerEntry?.text) ?? inlineHandler ?? exportName,
        exportName,
        signature: handlerEntry?.text,
        options,
      }),
    ]);
  }

  // The explicit registration form lets a module assign stable public paths
  // independently from its exported binding names.
  registrationPattern.lastIndex = 0;
  while ((match = registrationPattern.exec(source)) !== null) {
    const registration = moduleFunctionKinds.get((match[1] ?? "").toLowerCase());
    const openParen = match.index + match[0].length - 1;
    const call = readCallArguments(source, openParen);
    registrationPattern.lastIndex = Math.max(call.end, openParen + 1);
    if (!registration) continue;
    const declaredPath = call.args[0]?.value;
    const path = typeof declaredPath === "string" ? declaredPath.trim() : "";
    if (!path) continue;
    const options = call.args.find((argument) => argument.entries)?.entries;
    entries.push([
      path,
      moduleFunction({
        ...registration,
        path,
        file: relativeFile,
        handler: identifierText(call.args[1]?.text) ?? path.split(".").pop() ?? path,
        signature: options?.get("handler")?.text,
        options,
      }),
    ]);
  }

  return entries;
}

function moduleFunction(input: {
  kind: FunctionKind;
  internal?: boolean;
  delivery?: ModuleFunction["delivery"];
  file: string;
  handler: string;
  path: string;
  exportName?: string;
  signature?: string;
  options?: ObjectEntries;
}): ModuleFunction {
  const schemas = callSchemas(input.options, input.path);
  const configuredDelivery = input.options?.get("delivery")?.value;
  const delivery = normalizeDelivery(configuredDelivery) ?? input.delivery;
  const dependencies = dependenciesFromOptions(input.options);
  const replica = delivery === "replica" ? replicaFromOptions(input.options) : undefined;
  if (input.internal && input.kind === "query" && delivery !== "oneShot") {
    throw new Error(`internal Query ${input.path} must use one-shot delivery`);
  }
  if (delivery === "replica" && !replica) {
    throw new Error(`Replica Collection ${input.path} requires a replica definition`);
  }
  if (input.kind === "query" && (delivery ?? "oneShot") === "oneShot") {
    const plan = dependencies?.liveQueryPlan;
    if (!plan) throw new Error(`one-shot query ${input.path} requires a structured live query plan`);
    if (!plan.table.trim() || !plan.key.trim() || !plan.columns?.length || !plan.columns.includes(plan.key)) {
      throw new Error(`one-shot query ${input.path} requires a structured live query plan with a table, key, and columns including the key`);
    }
  }
  const offline = input.options?.get("offline")?.value;
  const optimistic = input.options?.get("optimistic")?.value;
  const internalEntry = input.options?.get("internal");
  if (internalEntry && typeof internalEntry.value !== "boolean") {
    throw new Error(`${input.kind} ${input.path} internal must be a boolean literal`);
  }
  const internal = input.internal || internalEntry?.value === true;
  const interactiveEntry = input.options?.get("interactive");
  const interactiveValue = interactiveEntry?.value;
  if (interactiveEntry && typeof interactiveValue !== "boolean") {
    throw new Error(`${input.kind} ${input.path} interactive must be a boolean literal`);
  }
  const interactive = !internal && (interactiveValue === true || (interactiveValue === undefined && input.kind !== "action"));
  const classification = internal ? "internal" : interactive ? "interactive" : "system";
  const descriptionEntry = input.options?.get("description");
  let description: string | undefined;
  if (descriptionEntry) {
    if (typeof descriptionEntry.value !== "string") {
      throw new Error(`${input.kind} ${input.path} description must be a string literal`);
    }
    description = descriptionEntry.value;
  }
  const agentEntry = input.options?.get("agent");
  if (agentEntry && agentEntry.value === undefined) {
    throw new Error(`function ${input.path} agent metadata must be an object literal`);
  }
  const agent = parseAgentMetadata(agentEntry?.value, input.path);
  const actionProfileValue = input.options?.get("profile")?.value;
  const actionProfile: "standard" | "agent" = actionProfileValue === "agent" ? "agent" : "standard";
  const actionCapabilities = input.options?.get("capabilities")?.value;
  if (input.kind === "reducer" && optimistic !== undefined) {
    validateOptimisticTransaction(optimistic);
  }
  if (input.kind === "action") {
    if (actionProfileValue !== undefined && actionProfileValue !== "standard" && actionProfileValue !== "agent") {
      throw new Error(`action ${input.path} profile must be "standard" or "agent"`);
    }
    validateActionCapabilities(actionProfile, actionCapabilities, input.path);
  } else if (actionProfileValue !== undefined || actionCapabilities !== undefined) {
    throw new Error(`${input.kind} ${input.path} cannot declare Action capabilities`);
  }
  return {
    kind: input.kind,
    handler: input.handler,
    file: input.file,
    ...(internal ? { internal: true } : {}),
    ...(input.exportName ? { export: input.exportName } : {}),
    args: schemas.args,
    result: schemas.result,
    ...(dependencies ? { dependencies } : {}),
    ...(delivery === undefined ? {} : { delivery }),
    ...(replica ? { replica } : {}),
    ...(offline === undefined ? {} : { offline }),
    ...(optimistic === undefined ? {} : { optimistic }),
    ...(interactiveEntry ? { interactive } : interactive ? { interactive: true } : {}),
    classification,
    ...(description === undefined ? {} : { description }),
    ...(agent === undefined ? {} : { agent }),
    ...(input.kind === "action" ? { actionProfile, ...(actionCapabilities === undefined ? {} : { actionCapabilities: actionCapabilities as ActionCapabilities }) } : {}),
  };
}

function parseAgentMetadata(value: JsonValue | undefined, path: string) {
  if (value === undefined) return undefined;
  if (!isJsonObject(value)) throw new Error(`function ${path} agent metadata must be an object literal`);
  const allowed = new Set(["tags", "confirmation"]);
  for (const field of Object.keys(value)) {
    if (!allowed.has(field)) throw new Error(`function ${path} agent metadata has unsupported field ${field}`);
  }
  const tags = value.tags;
  if (tags !== undefined && (!Array.isArray(tags) || tags.some((tag) => typeof tag !== "string" || !tag.trim()))) {
    throw new Error(`function ${path} agent tags must be non-empty string literals`);
  }
  const confirmation = value.confirmation;
  if (confirmation !== undefined && !["none", "required", "destructive"].includes(String(confirmation))) {
    throw new Error(`function ${path} agent confirmation is invalid`);
  }
  return {
    ...(tags === undefined ? {} : { tags: [...new Set(tags as string[])].sort() }),
    ...(confirmation === undefined ? {} : { confirmation: confirmation as "none" | "required" | "destructive" }),
  };
}

function validateActionCapabilities(profile: "standard" | "agent", value: JsonValue | undefined, path: string): void {
  if (value === undefined) return;
  if (!isJsonObject(value)) throw new Error(`action ${path} capabilities must be an object literal`);
  const allowed = new Set(["networkOrigins", "secrets", "tools", "scheduler", "storage", "sandbox", "functions"]);
  for (const field of Object.keys(value)) {
    if (!allowed.has(field)) throw new Error(`action ${path} capabilities has unsupported field ${field}`);
  }
  const origins = value.networkOrigins;
  if (origins !== undefined) {
    if (!Array.isArray(origins) || origins.length === 0) throw new Error(`action ${path} networkOrigins must be a non-empty array`);
    const seen = new Set<string>();
    for (const origin of origins) {
      if (typeof origin !== "string") throw new Error(`action ${path} networkOrigins must contain strings`);
      let parsed: URL;
      try { parsed = new URL(origin); } catch { throw new Error(`action ${path} network origin ${JSON.stringify(origin)} is invalid`); }
      if ((parsed.protocol !== "https:" && parsed.protocol !== "http:") || parsed.origin !== origin || parsed.username || parsed.password) {
        throw new Error(`action ${path} network origin ${JSON.stringify(origin)} must be an exact HTTP(S) origin`);
      }
      if (seen.has(origin)) throw new Error(`action ${path} declares duplicate network origin ${origin}`);
      seen.add(origin);
    }
  }
  const secrets = value.secrets;
  if (secrets !== undefined && (!Array.isArray(secrets) || secrets.some((name) => typeof name !== "string" || !/^[A-Z][A-Z0-9_]*$/.test(name)))) {
    throw new Error(`action ${path} secrets must be uppercase environment names`);
  }
  const tools = value.tools;
  if (tools !== undefined) {
    if (profile !== "agent") throw new Error(`action ${path} tools require profile "agent"`);
    if (!isJsonObject(tools) || Object.keys(tools).length === 0) throw new Error(`agent action ${path} tools must be a non-empty object`);
    for (const [name, binding] of Object.entries(tools)) {
      if (!/^[A-Za-z_$][A-Za-z0-9_$]*$/.test(name) || !isJsonObject(binding) ||
        (binding.kind !== "query" && binding.kind !== "reducer") || typeof binding.function !== "string" || !binding.function.trim()) {
        throw new Error(`agent action ${path} has an invalid tool binding ${JSON.stringify(name)}`);
      }
    }
  }
  if (value.scheduler !== undefined && value.scheduler !== true) throw new Error(`action ${path} scheduler must be true when declared`);
  if (value.storage !== undefined && value.storage !== true) throw new Error(`action ${path} storage must be true when declared`);
  if (value.functions !== undefined) {
    if (profile !== "agent") throw new Error(`action ${path} functions require profile "agent"`);
    if (value.functions !== true) throw new Error(`action ${path} functions must be true when declared`);
  }
  if (value.sandbox !== undefined) {
    if (profile !== "agent") throw new Error(`action ${path} sandbox requires profile "agent"`);
    if (!isJsonObject(value.sandbox)) throw new Error(`action ${path} sandbox must be an object literal`);
    for (const field of Object.keys(value.sandbox)) {
      if (field !== "duckdb") throw new Error(`action ${path} sandbox has unsupported field ${field}`);
    }
    if (value.sandbox.duckdb !== undefined && value.sandbox.duckdb !== true) {
      throw new Error(`action ${path} sandbox.duckdb must be true when declared`);
    }
  }
}

/**
 * Validate the literal optimistic contract while producing the artifact. The
 * module itself is still validated by @gonvex/module-sdk at load time, but a
 * malformed literal must not be silently copied into a client manifest.
 */
function validateOptimisticTransaction(value: JsonValue): void {
  if (!isJsonObject(value) || !Array.isArray(value.effects) || value.effects.length === 0) {
    throw new Error("reducer optimistic metadata must contain a non-empty effects array");
  }
  if (value.expectedRevision !== undefined && (
    typeof value.expectedRevision !== "number"
    || !Number.isSafeInteger(value.expectedRevision)
    || value.expectedRevision < 0
  )) {
    throw new Error("reducer optimistic expectedRevision must be a non-negative integer");
  }
  for (const effect of value.effects) {
    if (!isJsonObject(effect) || (effect.operation !== "patch" && effect.operation !== "upsert" && effect.operation !== "delete")) {
      throw new Error("reducer has an invalid optimistic effect");
    }
    if (typeof effect.entity !== "string" || !effect.entity.trim()) {
      throw new Error("reducer optimistic effects require an entity");
    }
    if (typeof effect.id !== "string" && (
      !Array.isArray(effect.id)
      || effect.id.length === 0
      || effect.id.some((part) => typeof part !== "string" || !part.trim())
    )) {
      throw new Error("reducer optimistic effects require a string id or id references");
    }
    if ((effect.operation === "patch" || effect.operation === "upsert") && !isJsonObject(
      effect.operation === "patch" ? effect.fields : effect.value,
    )) {
      throw new Error(`reducer optimistic ${effect.operation} effects require an object value`);
    }
  }
}

function isJsonObject(value: unknown): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function callSchemas(options: ObjectEntries | undefined, path: string): { args: ModuleSchema; result: ModuleSchema } {
  if (!options) throw new Error(`TypeScript function ${JSON.stringify(path)} must declare literal args and result schemas`);
  return {
    args: parseRequiredSchema(options, "args", path),
    result: parseRequiredSchema(options, "result", path),
  };
}

function parseRequiredSchema(options: ObjectEntries, field: "args" | "result", path: string): ModuleSchema {
  const entry = options.get(field);
  if (!entry) throw new Error(`TypeScript function ${JSON.stringify(path)} must declare ${field}: schema.*(...)`);
  const schema = parsePortableSchema(entry.text);
  if (!schema) throw new Error(`TypeScript function ${JSON.stringify(path)} ${field} must use a static schema.*(...) declaration`);
  if (!portableSchemaMatchesRuntime(schema)) {
    throw new Error(`TypeScript function ${JSON.stringify(path)} ${field} uses schema.optional outside an object field, which the runtime does not support`);
  }
  return schema;
}

/** Keep static artifacts aligned with the Rust ABI's optional-field rule. */
function portableSchemaMatchesRuntime(schema: ModuleSchema, optionalField = false): boolean {
  switch (schema.kind) {
    case "optional":
      return optionalField && portableSchemaMatchesRuntime(schema.value);
    case "array":
      return portableSchemaMatchesRuntime(schema.items);
    case "record":
      return portableSchemaMatchesRuntime(schema.values);
    case "object":
      return Object.values(schema.fields).every((field) => portableSchemaMatchesRuntime(field, field.kind === "optional"));
    default:
      return true;
  }
}

/** Parse only the module SDK's literal schema constructors; never evaluate source. */
function parsePortableSchema(text: string): ModuleSchema | undefined {
  const source = text.trim();
  const match = /^schema\.([A-Za-z_$][A-Za-z0-9_$]*)\s*\(/.exec(source);
  if (!match) return undefined;
  const openParen = source.indexOf("(", match.index + match[0].length - 1);
  const call = readCallArguments(source, openParen);
  if (call.end <= openParen || skipTrivia(source, call.end) !== source.length) return undefined;
  const name = match[1]!;
  const argument = call.args[0];
  switch (name) {
    case "string": {
      if (call.args.length > 1) return undefined;
      const options = schemaOptions(argument, ["format", "minLength", "maxLength"]);
      if (options === undefined) return argument === undefined ? { kind: "string" } : undefined;
      if (options.format !== undefined && !["email", "uri", "uuid", "datetime"].includes(String(options.format))) return undefined;
      return { kind: "string", ...options };
    }
    case "email":
    case "uri":
    case "uuid":
    case "datetime":
      return call.args.length === 0 ? { kind: "string", format: name } : undefined;
    case "number":
    case "integer": {
      if (call.args.length > 1) return undefined;
      const options = schemaOptions(argument, ["minimum", "maximum"]);
      if (options === undefined && argument !== undefined) return undefined;
      return { kind: "number", ...(name === "integer" ? { integer: true } : {}), ...(options ?? {}) };
    }
    case "boolean": return call.args.length === 0 ? { kind: "boolean" } : undefined;
    case "null": return call.args.length === 0 ? { kind: "null" } : undefined;
    case "any": return call.args.length === 0 ? { kind: "any" } : undefined;
    case "id": return call.args.length === 1 && typeof argument?.value === "string" && argument.value.trim() ? { kind: "id", entity: argument.value } : undefined;
    case "literal": return call.args.length === 1 && argument?.value !== undefined ? { kind: "literal", value: argument.value } : undefined;
    case "array":
      return call.args.length === 1 ? schemaChild(argument) : undefined;
    case "record":
      return call.args.length === 1 ? schemaChild(argument, "record") : undefined;
    case "optional":
      return call.args.length === 1 ? schemaChild(argument, "optional") : undefined;
    case "object": {
      if ((call.args.length !== 1 && call.args.length !== 2) || !argument?.entries || argument.text.includes("...")) return undefined;
      const fields: Record<string, ModuleSchema> = {};
      for (const [key, entry] of argument.entries) {
        const field = parsePortableSchema(entry.text);
        if (!field) return undefined;
        fields[key] = field;
      }
      const options = call.args.length === 2 ? schemaOptions(call.args[1], ["allowUnknown"]) : {};
      if (options === undefined) return undefined;
      return { kind: "object", fields, ...(options.allowUnknown === undefined ? {} : { allowUnknown: options.allowUnknown === true }) };
    }
    default: return undefined;
  }
}

function schemaChild(argument?: LiteralValue, wrapper?: "record" | "optional"): ModuleSchema | undefined {
  const child = argument && parsePortableSchema(argument.text);
  if (!child) return undefined;
  if (wrapper === "record") return { kind: "record", values: child };
  if (wrapper === "optional") return { kind: "optional", value: child };
  return { kind: "array", items: child };
}

function schemaOptions(argument: LiteralValue | undefined, allowed: readonly string[]): Record<string, JsonValue> | undefined {
  if (!argument) return {};
  if (!argument.entries || argument.text.includes("...")) return undefined;
  const result: Record<string, JsonValue> = {};
  for (const [key, entry] of argument.entries) {
    if (!allowed.includes(key) || entry.value === undefined) return undefined;
    result[key] = entry.value;
  }
  return result;
}

/** Validate a schema after JSON transport; used by manifest projections and tests. */
export function isModuleSchema(value: unknown): value is ModuleSchema {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  if (typeof record.kind !== "string") return false;
  switch (record.kind) {
    case "string": return schemaKeys(record, ["kind", "format", "minLength", "maxLength"])
      && (record.format === undefined || ["email", "uri", "uuid", "datetime"].includes(String(record.format)))
      && positiveIntegerOrUndefined(record.minLength) && positiveIntegerOrUndefined(record.maxLength);
    case "number": return schemaKeys(record, ["kind", "integer", "minimum", "maximum"])
      && (record.integer === undefined || typeof record.integer === "boolean")
      && numberOrUndefined(record.minimum) && numberOrUndefined(record.maximum);
    case "boolean":
    case "null":
    case "any": return schemaKeys(record, ["kind"]);
    case "id": return schemaKeys(record, ["kind", "entity"]) && typeof record.entity === "string" && record.entity.trim().length > 0;
    case "literal": return schemaKeys(record, ["kind", "value"]) && isJsonValue(record.value);
    case "array": return schemaKeys(record, ["kind", "items"]) && isModuleSchema(record.items);
    case "record": return schemaKeys(record, ["kind", "values"]) && isModuleSchema(record.values);
    case "optional": return schemaKeys(record, ["kind", "value"]) && isModuleSchema(record.value);
    case "object": {
      if (!schemaKeys(record, ["kind", "fields", "allowUnknown"]) || !record.fields || typeof record.fields !== "object" || Array.isArray(record.fields)) return false;
      if (record.allowUnknown !== undefined && typeof record.allowUnknown !== "boolean") return false;
      return Object.values(record.fields as Record<string, unknown>).every(isModuleSchema);
    }
    default: return false;
  }
}

function schemaKeys(record: Record<string, unknown>, allowed: readonly string[]): boolean {
  return Object.keys(record).every((key) => allowed.includes(key));
}
function numberOrUndefined(value: unknown): boolean { return value === undefined || (typeof value === "number" && Number.isFinite(value)); }
function positiveIntegerOrUndefined(value: unknown): boolean { return value === undefined || (typeof value === "number" && Number.isSafeInteger(value) && value >= 0); }
function isJsonValue(value: unknown): value is JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean") return true;
  if (typeof value === "number") return Number.isFinite(value);
  if (Array.isArray(value)) return value.every(isJsonValue);
  return typeof value === "object" && value !== null && Object.values(value).every(isJsonValue);
}

function signatureTypes(text: string): { args?: string; result?: string } {
  const openParen = text.indexOf("(");
  if (openParen < 0) return {};
  const closeParen = findMatching(text, openParen);
  if (closeParen < 0) return {};
  // Handlers take the context first and the arguments second.
  const parameter = splitTopLevel(text.slice(openParen + 1, closeParen)).at(1) ?? "";
  const colon = parameter.indexOf(":");
  const args = colon < 0 ? undefined : parameter.slice(colon + 1).trim();
  let result: string | undefined;
  const afterParams = skipTrivia(text, closeParen + 1);
  if (text[afterParams] === ":") {
    const arrow = indexOfTopLevel(text, "=>", afterParams + 1);
    result = text.slice(afterParams + 1, arrow < 0 ? text.length : arrow);
  }
  return { args: args || undefined, result: unwrapPromise(result) };
}

function unwrapPromise(type?: string) {
  const declared = type?.trim();
  if (!declared) return undefined;
  const match = /^Promise\s*<([\s\S]*)>$/.exec(declared);
  return (match ? match[1]!.trim() : declared) || undefined;
}

function dependenciesFromOptions(
  options: ObjectEntries | undefined,
): FunctionDependencies | undefined {
  if (!options) return undefined;
  const dependencies: FunctionDependencies = {};

  const liveQueryPlan = liveQueryPlanFromOptions(options);
  if (liveQueryPlan) {
    dependencies.liveQueryPlan = liveQueryPlan;
  }
  if (options.get("shareByPermissions")?.value === true) dependencies.shareByPermissions = true;
  const shareResultFrom = stringEntry(options, "shareResultFrom");
  if (shareResultFrom) dependencies.shareResultFrom = shareResultFrom;
  const shareResultField = stringEntry(options, "shareResultField");
  if (shareResultField) dependencies.shareResultField = shareResultField;
  const optimistic = options.get("optimistic")?.value;
  const nonOptimisticReason = stringEntry(options, "nonOptimisticReason");
  if (nonOptimisticReason) dependencies.nonOptimisticReason = nonOptimisticReason;
  return Object.keys(dependencies).length > 0 ? dependencies : undefined;
}

function normalizeDelivery(value: JsonValue | undefined): ModuleFunction["delivery"] | undefined {
  if (value === "oneShot" || value === "live" || value === "replica") return value;
  return undefined;
}

function liveQueryPlanFromOptions(options: ObjectEntries): LiveQueryPlan | undefined {
  return parseLiveQueryPlan(options.get("liveQueryPlan")?.value);
}

function parseLiveQueryPlan(value: JsonValue | undefined): LiveQueryPlan | undefined {
  const table = stringMember(value, "table");
  if (!table) return undefined;
  const plan: LiveQueryPlan = {
    table,
    key: stringMember(value, "key") ?? "id",
  };
  const columns = stringArray(readMember(value, "columns"));
  if (columns) plan.columns = columns;
  const resultPath = pathArray(readMember(value, "resultPath"));
  if (resultPath.length > 0) plan.resultPath = resultPath;
  const where = parseLiveExpression(readMember(value, "where"));
  if (where) plan.where = where;
  const searchValue = readMember(value, "search");
  const searchArgument = stringMember(searchValue, "argument");
  const searchColumns = stringArray(readMember(searchValue, "columns"));
  if (searchArgument && searchColumns) plan.search = { argument: searchArgument, columns: searchColumns };
  const filtersValue = readMember(value, "filters");
  const filtersArgument = stringMember(filtersValue, "argument");
  const filtersColumns = stringArray(readMember(filtersValue, "allowedColumns"));
  const filtersOperators = stringArray(readMember(filtersValue, "allowedOperators"));
  if (filtersArgument && filtersColumns && filtersOperators) {
    plan.filters = { argument: filtersArgument, allowedColumns: filtersColumns, allowedOperators: filtersOperators as FilterOperator[] };
  }
  const sortValue = readMember(value, "sort");
  const sortDefaultColumn = stringMember(sortValue, "defaultColumn");
  const sortDefaultDirection = stringMember(sortValue, "defaultDirection");
  const sortAllowedColumns = stringArray(readMember(sortValue, "allowedColumns"));
  if (sortDefaultColumn && (sortDefaultDirection === "asc" || sortDefaultDirection === "desc") && sortAllowedColumns) {
    plan.sort = {
      columnArgument: stringMember(sortValue, "columnArgument"),
      directionArgument: stringMember(sortValue, "directionArgument"),
      defaultColumn: sortDefaultColumn,
      defaultDirection: sortDefaultDirection,
      allowedColumns: sortAllowedColumns,
    };
  }
  const windowValue = readMember(value, "window");
  const offsetArgument = stringMember(windowValue, "offsetArgument");
  const limitArgument = stringMember(windowValue, "limitArgument");
  const defaultLimit = numberMember(windowValue, "defaultLimit");
  const maxLimit = numberMember(windowValue, "maxLimit");
  if (offsetArgument && limitArgument && defaultLimit !== undefined && maxLimit !== undefined) {
    const count = stringMember(windowValue, "count");
    if (count !== undefined && count !== "exact") throw new Error("live query window count must be exact");
    plan.window = { offsetArgument, limitArgument, defaultLimit, maxLimit, ...(count ? { count: "exact" as const } : {}) };
  }
  if (readMember(value, "serverOnly") === true) plan.serverOnly = true;
  return plan;
}

function parseLiveExpression(value: JsonValue | undefined): LiveExpression | undefined {
  const operator = stringMember(value, "operator");
  if (!operator || ![
    "eq", "neq", "gt", "gte", "lt", "lte", "in", "contains",
    "containsInsensitive", "range", "and", "or", "not", "server",
  ].includes(operator)) return undefined;
  const expression: LiveExpression = { operator: operator as LiveExpression["operator"] };
  const column = stringMember(value, "column");
  if (column) expression.column = column;
  const parsedValue = parseLiveValue(readMember(value, "value"));
  if (parsedValue) expression.value = parsedValue;
  const valueTo = parseLiveValue(readMember(value, "valueTo"));
  if (valueTo) expression.valueTo = valueTo;
  const childrenValue = readMember(value, "children");
  if (Array.isArray(childrenValue)) {
    const children = childrenValue.map(parseLiveExpression).filter((child): child is LiveExpression => child !== undefined);
    if (children.length > 0) expression.children = children;
  }
  return expression;
}

function parseLiveValue(value: JsonValue | undefined): LiveValue | undefined {
  const argument = stringMember(value, "argument");
  if (argument) return { argument };
  const literal = readMember(value, "literal");
  return literal === undefined ? undefined : { literal };
}

function replicaFromOptions(options?: ObjectEntries): ReplicaCollectionDefinition | undefined {
  const value = options?.get("replica")?.value;
  const table = stringMember(value, "table");
  if (!table) return undefined;
  const definition: ReplicaCollectionDefinition = {
    table,
    key: stringMember(value, "key") ?? "id",
    columns: stringArray(readMember(value, "columns")) ?? [],
  };
  const equalFilters = readMember(value, "equalFilters");
  if (equalFilters && typeof equalFilters === "object" && !Array.isArray(equalFilters)) {
    const filters: Record<string, string> = {};
    for (const [argument, column] of Object.entries(equalFilters)) {
      if (typeof column === "string") filters[argument] = column;
    }
    if (Object.keys(filters).length > 0) definition.equalFilters = filters;
  }
  const excludeWhenSet = stringArray(readMember(value, "excludeWhenSet"));
  if (excludeWhenSet) definition.excludeWhenSet = excludeWhenSet;
  const visibilityTables = stringArray(readMember(value, "visibilityTables"));
  if (visibilityTables) definition.visibilityTables = visibilityTables;
  const orderBy = stringMember(value, "orderBy");
  if (orderBy) {
    definition.orderBy = orderBy;
    definition.orderDirection = stringMember(value, "orderDirection")?.toLowerCase() === "asc" ? "asc" : "desc";
  }
  definition.mode = stringMember(value, "mode") === "progressive" ? "progressive" : "eager";
  const maxRows = numberMember(value, "maxRows");
  if (maxRows !== undefined && maxRows > 0) definition.maxRows = maxRows;
  const maxBytes = numberMember(value, "maxBytes");
  if (maxBytes !== undefined && maxBytes > 0) definition.maxBytes = maxBytes;
  if (!definition.columns.includes(definition.key)) definition.columns.push(definition.key);
  return definition;
}

type ObjectEntry = {
  /** Present only when the member is a pure literal. */
  value?: JsonValue;
  /** Raw source of the member, or the signature for method shorthands. */
  text: string;
};

type ObjectEntries = Map<string, ObjectEntry>;

type LiteralValue = {
  value?: JsonValue;
  entries?: ObjectEntries;
  text: string;
  end: number;
};

function readCallArguments(source: string, openParen: number): { args: LiteralValue[]; end: number } {
  const close = findMatching(source, openParen);
  if (close < 0) return { args: [], end: openParen + 1 };
  const args: LiteralValue[] = [];
  let cursor = openParen + 1;
  while (cursor < close) {
    cursor = skipTrivia(source, cursor);
    if (cursor >= close) break;
    const argument = readValue(source, cursor);
    if (argument.end <= cursor) break;
    args.push(argument);
    cursor = skipTrivia(source, argument.end);
    if (source[cursor] === ",") cursor += 1;
  }
  return { args, end: close + 1 };
}

function readValue(source: string, start: number): LiteralValue {
  const begin = skipTrivia(source, start);
  const char = source[begin];
  if (char === undefined) return { text: "", end: source.length };

  if (char === "{") {
    const close = findMatching(source, begin);
    if (close < 0) return { text: source.slice(begin), end: source.length };
    const entries = parseObjectEntries(source, begin, close);
    const object: Record<string, JsonValue> = {};
    let literal = true;
    for (const [key, entry] of entries) {
      if (entry.value === undefined) {
        literal = false;
        break;
      }
      object[key] = entry.value;
    }
    return { ...(literal ? { value: object } : {}), entries, text: source.slice(begin, close + 1), end: close + 1 };
  }

  if (char === "[") {
    const close = findMatching(source, begin);
    if (close < 0) return { text: source.slice(begin), end: source.length };
    const items: JsonValue[] = [];
    let literal = true;
    let cursor = begin + 1;
    while (cursor < close) {
      cursor = skipTrivia(source, cursor);
      if (cursor >= close) break;
      const item = readValue(source, cursor);
      if (item.end <= cursor) break;
      if (item.value === undefined) literal = false;
      else items.push(item.value);
      cursor = skipTrivia(source, item.end);
      if (source[cursor] === ",") cursor += 1;
    }
    return { ...(literal ? { value: items } : {}), text: source.slice(begin, close + 1), end: close + 1 };
  }

  if (char === '"' || char === "'" || char === "`") {
    const string = readStringLiteral(source, begin);
    const end = string?.end ?? source.length;
    return { ...(string?.value === undefined ? {} : { value: string.value }), text: source.slice(begin, end), end };
  }

  keywordPattern.lastIndex = begin;
  const keyword = keywordPattern.exec(source);
  if (keyword) {
    const end = begin + keyword[0].length;
    const value = keyword[0] === "true" ? true : keyword[0] === "false" ? false : keyword[0] === "null" ? null : undefined;
    return { ...(keyword[0] === "undefined" ? {} : { value }), text: keyword[0], end };
  }

  numberPattern.lastIndex = begin;
  const number = numberPattern.exec(source);
  if (number) {
    const parsed = Number(number[0].replace(/_/g, ""));
    const end = begin + number[0].length;
    return { ...(Number.isFinite(parsed) ? { value: parsed } : {}), text: number[0], end };
  }

  // Identifiers, calls, and arrow functions are kept as source text: the CLI
  // records what the module declared without pretending to evaluate it.
  let cursor = begin;
  while (cursor < source.length) {
    const current = source[cursor]!;
    const next = source[cursor + 1] ?? "";
    if (current === "/" && next === "/") {
      const newline = source.indexOf("\n", cursor + 2);
      cursor = newline < 0 ? source.length : newline + 1;
      continue;
    }
    if (current === "/" && next === "*") {
      const closeComment = source.indexOf("*/", cursor + 2);
      cursor = closeComment < 0 ? source.length : closeComment + 2;
      continue;
    }
    if (current === '"' || current === "'" || current === "`") {
      const string = readStringLiteral(source, cursor);
      cursor = string ? string.end : cursor + 1;
      continue;
    }
    if (current === "{" || current === "[" || current === "(") {
      const closeBracket = findMatching(source, cursor);
      cursor = closeBracket < 0 ? source.length : closeBracket + 1;
      continue;
    }
    if (current === "," || current === "}" || current === "]" || current === ")") break;
    cursor += 1;
  }
  return { text: source.slice(begin, cursor).trim(), end: cursor };
}

function parseObjectEntries(source: string, open: number, close: number): ObjectEntries {
  const entries: ObjectEntries = new Map();
  let cursor = open + 1;
  while (cursor < close) {
    cursor = skipTrivia(source, cursor);
    if (cursor >= close) break;
    const char = source[cursor]!;
    if (char === "," || char === ";") {
      cursor += 1;
      continue;
    }
    if (char === ".") {
      // Spread members contribute nothing the CLI can resolve statically.
      const spread = readValue(source, cursor);
      cursor = spread.end > cursor ? spread.end : cursor + 1;
      continue;
    }
    const key = readMemberKey(source, cursor);
    if (!key) break;
    const afterKey = skipTrivia(source, key.end);

    if (source[afterKey] === ":") {
      const value = readValue(source, afterKey + 1);
      entries.set(key.key, { ...(value.value === undefined ? {} : { value: value.value }), text: value.text });
      cursor = value.end > afterKey ? value.end : afterKey + 1;
      continue;
    }

    if (source[afterKey] === "(") {
      // Method shorthand: keep the signature only, so the declared parameter
      // and return types stay readable without the body.
      const closeParen = findMatching(source, afterKey);
      if (closeParen < 0) break;
      const body = indexOfTopLevel(source, "{", closeParen + 1, close);
      const signatureEnd = body < 0 ? closeParen + 1 : body;
      entries.set(key.key, { text: source.slice(key.end, signatureEnd).trim() });
      const bodyEnd = body < 0 ? -1 : findMatching(source, body);
      cursor = bodyEnd < 0 ? signatureEnd : bodyEnd + 1;
      continue;
    }

    entries.set(key.key, { text: key.key });
    cursor = afterKey;
  }
  return entries;
}

function readMemberKey(source: string, start: number): { key: string; end: number } | undefined {
  let cursor = skipTrivia(source, start);
  // `async`, `get`, and `set` prefix method shorthands; the key follows them.
  for (let guard = 0; guard < 3; guard += 1) {
    const char = source[cursor];
    if (char === undefined) return undefined;
    if (char === '"' || char === "'" || char === "`") {
      const string = readStringLiteral(source, cursor);
      if (!string || string.value === undefined) return undefined;
      return { key: string.value, end: string.end };
    }
    identifierPattern.lastIndex = cursor;
    const identifier = identifierPattern.exec(source);
    if (!identifier) return undefined;
    const name = identifier[0];
    const after = skipTrivia(source, cursor + name.length);
    const modifier = (name === "async" || name === "get" || name === "set") && /[A-Za-z_$"'`]/.test(source[after] ?? "");
    if (!modifier) return { key: name, end: cursor + name.length };
    cursor = after;
  }
  return undefined;
}

function readStringLiteral(source: string, start: number): { value?: string; end: number } | undefined {
  const quote = source[start];
  if (quote !== '"' && quote !== "'" && quote !== "`") return undefined;
  let interpolated = false;
  let raw = "";
  let cursor = start + 1;
  while (cursor < source.length) {
    const char = source[cursor]!;
    if (char === "\\") {
      raw += source.slice(cursor, cursor + 2);
      cursor += 2;
      continue;
    }
    if (quote === "`" && char === "$" && source[cursor + 1] === "{") {
      const close = findMatching(source, cursor + 1);
      if (close < 0) return { end: source.length };
      interpolated = true;
      cursor = close + 1;
      continue;
    }
    if (char === quote) {
      return interpolated ? { end: cursor + 1 } : { value: decodeStringLiteral(raw), end: cursor + 1 };
    }
    raw += char;
    cursor += 1;
  }
  return { end: source.length };
}

function decodeStringLiteral(raw: string) {
  let decoded = "";
  for (let index = 0; index < raw.length; index += 1) {
    const char = raw[index]!;
    if (char !== "\\") {
      decoded += char;
      continue;
    }
    const escape = raw[index + 1] ?? "";
    index += 1;
    if (escape === "n") decoded += "\n";
    else if (escape === "r") decoded += "\r";
    else if (escape === "t") decoded += "\t";
    else if (escape === "b") decoded += "\b";
    else if (escape === "f") decoded += "\f";
    else if (escape === "v") decoded += "\v";
    else if (escape === "0") decoded += "\0";
    else if (escape === "x") {
      const code = Number.parseInt(raw.slice(index + 1, index + 3), 16);
      if (Number.isFinite(code)) {
        decoded += String.fromCharCode(code);
        index += 2;
      }
    } else if (escape === "u") {
      if (raw[index + 1] === "{") {
        const close = raw.indexOf("}", index + 2);
        const code = close < 0 ? Number.NaN : Number.parseInt(raw.slice(index + 2, close), 16);
        if (Number.isFinite(code) && code <= 0x10ffff) {
          decoded += String.fromCodePoint(code);
          index = close;
        }
      } else {
        const code = Number.parseInt(raw.slice(index + 1, index + 5), 16);
        if (Number.isFinite(code)) {
          decoded += String.fromCharCode(code);
          index += 4;
        }
      }
    } else {
      decoded += escape;
    }
  }
  return decoded;
}

/**
 * Balanced scanner over braces, brackets, and parentheses that skips strings
 * and comments. Regular-expression literals are not tracked; declarative module
 * metadata does not use them.
 */
function findMatching(source: string, open: number): number {
  const opener = source[open];
  if (opener !== "{" && opener !== "[" && opener !== "(") return -1;
  let depth = 0;
  let cursor = open;
  while (cursor < source.length) {
    const char = source[cursor]!;
    const next = source[cursor + 1] ?? "";
    if (char === "/" && next === "/") {
      const newline = source.indexOf("\n", cursor + 2);
      cursor = newline < 0 ? source.length : newline + 1;
      continue;
    }
    if (char === "/" && next === "*") {
      const closeComment = source.indexOf("*/", cursor + 2);
      cursor = closeComment < 0 ? source.length : closeComment + 2;
      continue;
    }
    if (char === '"' || char === "'" || char === "`") {
      const string = readStringLiteral(source, cursor);
      cursor = string ? string.end : cursor + 1;
      continue;
    }
    if (char === "{" || char === "[" || char === "(") {
      depth += 1;
      cursor += 1;
      continue;
    }
    if (char === "}" || char === "]" || char === ")") {
      depth -= 1;
      if (depth === 0) return cursor;
      cursor += 1;
      continue;
    }
    cursor += 1;
  }
  return -1;
}

function indexOfTopLevel(source: string, token: string, from: number, limit = source.length): number {
  let cursor = from;
  while (cursor < limit) {
    if (source.startsWith(token, cursor)) return cursor;
    const char = source[cursor]!;
    const next = source[cursor + 1] ?? "";
    if (char === "/" && next === "/") {
      const newline = source.indexOf("\n", cursor + 2);
      cursor = newline < 0 ? limit : newline + 1;
      continue;
    }
    if (char === "/" && next === "*") {
      const closeComment = source.indexOf("*/", cursor + 2);
      cursor = closeComment < 0 ? limit : closeComment + 2;
      continue;
    }
    if (char === '"' || char === "'" || char === "`") {
      const string = readStringLiteral(source, cursor);
      cursor = string ? string.end : cursor + 1;
      continue;
    }
    if (char === "{" || char === "[" || char === "(") {
      const closeBracket = findMatching(source, cursor);
      cursor = closeBracket < 0 ? limit : closeBracket + 1;
      continue;
    }
    if (char === "}" || char === "]" || char === ")") return -1;
    cursor += 1;
  }
  return -1;
}

function splitTopLevel(text: string): string[] {
  const parts: string[] = [];
  let start = 0;
  let cursor = 0;
  while (cursor < text.length) {
    const char = text[cursor]!;
    const next = text[cursor + 1] ?? "";
    if (char === "/" && next === "/") {
      const newline = text.indexOf("\n", cursor + 2);
      cursor = newline < 0 ? text.length : newline + 1;
      continue;
    }
    if (char === "/" && next === "*") {
      const closeComment = text.indexOf("*/", cursor + 2);
      cursor = closeComment < 0 ? text.length : closeComment + 2;
      continue;
    }
    if (char === '"' || char === "'" || char === "`") {
      const string = readStringLiteral(text, cursor);
      cursor = string ? string.end : cursor + 1;
      continue;
    }
    if (char === "{" || char === "[" || char === "(") {
      const closeBracket = findMatching(text, cursor);
      cursor = closeBracket < 0 ? text.length : closeBracket + 1;
      continue;
    }
    if (char === "<") {
      const closeAngle = findMatchingAngle(text, cursor);
      cursor = closeAngle < 0 ? cursor + 1 : closeAngle + 1;
      continue;
    }
    if (char === ",") {
      parts.push(text.slice(start, cursor).trim());
      start = cursor + 1;
    }
    cursor += 1;
  }
  parts.push(text.slice(start).trim());
  return parts.filter((part) => part.length > 0);
}

function findMatchingAngle(text: string, open: number): number {
  let depth = 0;
  let cursor = open;
  while (cursor < text.length) {
    const char = text[cursor]!;
    if (char === "=" && text[cursor + 1] === ">") {
      cursor += 2;
      continue;
    }
    if (char === "<") {
      depth += 1;
      cursor += 1;
      continue;
    }
    if (char === ">") {
      depth -= 1;
      if (depth === 0) return cursor;
      cursor += 1;
      continue;
    }
    if (char === "{" || char === "[" || char === "(") {
      const closeBracket = findMatching(text, cursor);
      if (closeBracket < 0) return -1;
      cursor = closeBracket + 1;
      continue;
    }
    cursor += 1;
  }
  return -1;
}

function skipTrivia(source: string, start: number): number {
  let cursor = start;
  while (cursor < source.length) {
    const char = source[cursor]!;
    const next = source[cursor + 1] ?? "";
    if (/\s/.test(char)) {
      cursor += 1;
      continue;
    }
    if (char === "/" && next === "/") {
      const newline = source.indexOf("\n", cursor + 2);
      if (newline < 0) return source.length;
      cursor = newline + 1;
      continue;
    }
    if (char === "/" && next === "*") {
      const closeComment = source.indexOf("*/", cursor + 2);
      if (closeComment < 0) return source.length;
      cursor = closeComment + 2;
      continue;
    }
    break;
  }
  return cursor;
}

function identifierText(text?: string) {
  const trimmed = text?.trim();
  return trimmed && /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(trimmed) ? trimmed : undefined;
}

function stringEntry(options: ObjectEntries | undefined, key: string) {
  const value = options?.get(key)?.value;
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function readMember(value: JsonValue | undefined, key: string): JsonValue | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  return value[key];
}

function stringMember(value: JsonValue | undefined, key: string) {
  const member = readMember(value, key);
  return typeof member === "string" && member.trim() ? member.trim() : undefined;
}

function numberMember(value: JsonValue | undefined, key: string) {
  const member = readMember(value, key);
  return typeof member === "number" && Number.isFinite(member) ? member : undefined;
}

function stringArray(value: JsonValue | undefined): string[] | undefined {
  if (typeof value === "string") return value.trim() ? [value.trim()] : undefined;
  if (!Array.isArray(value)) return undefined;
  const values = value.filter((item): item is string => typeof item === "string" && item.trim().length > 0).map((item) => item.trim());
  return values.length > 0 ? values : undefined;
}

function pathArray(value: JsonValue | undefined): string[] {
  if (typeof value === "string") return value.split(".").map((segment) => segment.trim()).filter(Boolean);
  return stringArray(value) ?? [];
}

function functionPathPrefix(backendDir: string, file: string) {
  const withoutExtension = relative(backendDir, file).replace(/\\/g, "/").replace(/\.(?:tsx|ts|mts|cts)$/, "");
  const segments = withoutExtension.split("/").filter((segment) => segment && segment !== ".");
  if (segments[segments.length - 1] === "index") segments.pop();
  return segments.join(".");
}

function projectPath(root: string, file: string) {
  return relative(root, file).replace(/\\/g, "/");
}

function sortedRecord<T>(record: Record<string, T>): Record<string, T> {
  const sorted: Record<string, T> = {};
  for (const key of Object.keys(record).sort()) sorted[key] = record[key]!;
  return sorted;
}

function isModuleSourceFile(name: string) {
  if (skippedSourceFiles.has(name)) return false;
  if (/\.d\.(?:ts|mts|cts)$/.test(name)) return false;
  if (/\.(?:test|spec)\.(?:ts|tsx|mts|cts)$/.test(name)) return false;
  return moduleSourceExtensions.some((extension) => name.endsWith(extension));
}

async function walkFiles(dir: string, accept: (name: string) => boolean): Promise<string[]> {
  if (!existsSync(dir)) return [];
  const entries = await readdir(dir, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (skippedDirectories.has(entry.name) || entry.name.startsWith(".")) continue;
      files.push(...await walkFiles(path, accept));
    } else if (entry.isFile() && accept(entry.name)) {
      files.push(path);
    }
  }
  return files.sort();
}
