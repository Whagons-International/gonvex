import{renderLocalDataModel}from'./local-data-model.js';
import { nativeLocalBindings } from "./native-local-bindings.js";
import { rolldown } from "rolldown";
import { createHash } from "node:crypto";
import { readFile, readdir, writeFile, mkdir } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import type { LocalSchema } from "@gonvex/local-runtime/schema";
import { compileLocalSchema } from "./local-schema.js";
import type { Manifest } from "./manifest-types.js";

const runtimeGlobals = {
  Date: ["@gonvex/module-sdk/runtime-contract", "runtimeDate"],
  Math: ["@gonvex/module-sdk/runtime-contract", "runtimeMath"],
  crypto: ["@gonvex/module-sdk/runtime-contract", "runtimeCrypto"],
  "globalThis.Date": ["@gonvex/module-sdk/runtime-contract", "runtimeDate"],
  "globalThis.Math": ["@gonvex/module-sdk/runtime-contract", "runtimeMath"],
  "globalThis.crypto": ["@gonvex/module-sdk/runtime-contract", "runtimeCrypto"],
} satisfies Record<string, [string, string]>;
const runtimeWrapper = `import { withReducerRuntimeGlobals } from "@gonvex/module-sdk/runtime-contract";
const bindRuntimeGlobals = definition => Object.freeze({ ...definition, handler: (context, args) => withReducerRuntimeGlobals(context, () => definition.handler(context, args)) });`;

export async function projectLocalSchema(root: string): Promise<LocalSchema> {
  const migrationDirectory = join(root, "migrations");
  const names = (await readdir(migrationDirectory).catch(() => [] as string[])).filter((name) => name.endsWith(".sql")).sort();
  const migrations = await Promise.all(names.map(async (name) => ({ name, sql: await readFile(join(migrationDirectory, name), "utf8") })));
  const tenantMigrations = migrations.filter(({ sql }) => !/gonvex:scope\s+(control|landlord)/.test(sql));
  const digest = createHash("sha256").update("local-schema-v4-record-views").update(JSON.stringify(tenantMigrations)).digest("hex");
  const cacheFile = join(root, "gonvex", "_build", `local-schema-${digest}.json`);
  let schema: LocalSchema;
  try { schema = JSON.parse(await readFile(cacheFile, "utf8")) as LocalSchema; }
  catch {
    schema = await compileLocalSchema(tenantMigrations);
    await mkdir(dirname(cacheFile), { recursive: true });
    await writeFile(cacheFile, JSON.stringify(schema));
  }
  return schema;
}

/**
 * The condition under which a visibility predicate evaluates to `value` for a
 * row, built only from identity terms and, for `true`, rows whose column equals
 * an identity value (eqContext). Other row and set terms are unknown, so they
 * never prove it.
 */
function provenForEveryRow(where:any, value:boolean):any {
    switch (where?.operator) {
        case "public":
            return value;
        case "eqContext":
            return value && typeof where.column === "string" && typeof where.context === "string"
                ? { operator: "eqContext", column: where.column, context: where.context } : false;
        case "role":
        case "permission": {
            const term = { operator: where.operator, value: where.value };
            return value ? term : { operator: "not", children: [term] };
        }
        case "not":
            return where.children?.length === 1 ? provenForEveryRow(where.children[0], !value) : false;
        case "and":
        case "or": {
            if (!where.children?.length)
                return false;
            const parts = where.children.map((child:any) => provenForEveryRow(child, value));
            const every = (where.operator === "and") === value;
            const decided = every ? false : true;
            if (parts.includes(decided))
                return decided;
            const rest = parts.filter((part:any) => part !== !decided);
            if (!rest.length)
                return !decided;
            return rest.length === 1 ? rest[0] : { operator: every ? "and" : "or", children: rest };
        }
        default:
            return false;
    }
}
/**
 * Per local table, which rows its visibility plan provably admits for the
 * caller: all of them, given identity terms, or those whose column equals an
 * identity value. Only there can a complete replica window prove that a row
 * does not exist; elsewhere it proves only what this member may see.
 */
export function unrestrictedVisibility(manifest:Manifest, tables:Iterable<string|undefined>):Record<string,unknown> {
    const conditions:Record<string,unknown> = {};
    for (const table of [...tables].sort()) {
        if(!table)continue;
        const condition = provenForEveryRow(manifest.visibility?.[table]?.where, true);
        if (condition !== false)
            conditions[table] = condition === true ? { operator: "public" } : condition;
    }
    return conditions;
}

/** Share immutable column descriptors instead of allocating one per field. */
export function renderLocalSchema(schema: LocalSchema): string {
  const shapes = new Map<string, { count: number; name: string }>();
  for (const table of Object.values(schema)) for (const column of Object.values(table.columns)) {
    const shape = JSON.stringify(column);
    const entry = shapes.get(shape);
    if (entry) entry.count++;
    else shapes.set(shape, { count: 1, name: `column${shapes.size}` });
  }
  const declarations = [...shapes].filter(([, entry]) => entry.count > 1)
    .map(([shape, entry]) => `const ${entry.name} = Object.freeze(${shape} as const);`).join("\n");
  const tables = Object.entries(schema).map(([name, table]) => {
    const properties = Object.entries(table).map(([key, value]) => {
      if (key !== "columns") return `${JSON.stringify(key)}:${JSON.stringify(value)}`;
      const columns = Object.entries(table.columns).map(([field, column]) => {
        const shape = JSON.stringify(column);
        const entry = shapes.get(shape)!;
        return `${JSON.stringify(field)}:${entry.count > 1 ? entry.name : shape}`;
      });
      return `"columns":{${columns.join(",")}}`;
    });
    return `${JSON.stringify(name)}:{${properties.join(",")}}`;
  });
  return `${declarations}\nexport const localSchema = {${tables.join(",")}} as const;\n`;
}

export async function localBindings(root: string, manifest: Manifest): Promise<Record<string, string>> {
  const reducers = Object.entries(manifest.functions).filter(([, entry]) => entry.localExecution === 1);
  const schema = await projectLocalSchema(root);
  const dataModel=renderLocalDataModel(schema,manifest.functions);
  if(!reducers.length)return{'dataModel.ts':dataModel};
  const generatedDirectory = join(root, "gonvex", "_generated");
  const imports = reducers.map(([, entry], index) => {
    let file = relative(generatedDirectory, resolve(root, entry.file)).replaceAll("\\", "/").replace(/\.(?:tsx?|mts|cts)$/, ".js");
    if (!file.startsWith(".")) file = `./${file}`;
    return `import { ${entry.handler} as reducer${index} } from ${JSON.stringify(file)};`;
  });
  const entryFile = join(root, "gonvex", "_build", "local-entry.ts");
  await mkdir(dirname(entryFile), { recursive: true });
  await writeFile(entryFile, `${runtimeWrapper}\n${imports.join("\n")}\nexport const localReducers = {${reducers.map(([path], index) => `${JSON.stringify(path)}: bindRuntimeGlobals(reducer${index})`).join(",")}};`);
  const bundle = await rolldown({
    input: entryFile, cwd: root, platform: "browser", tsconfig: false, transform: { inject: runtimeGlobals },
    // These declarations describe server entry points. Their definitions and
    // schema construction are unnecessary when only Reducer exports are used.
    treeshake: { manualPureFunctions: ["action", "internalReducer", "query", "internalQuery", "liveQuery", "replicaCollection", "visibility", "cron", "tenantCron", "schema"] },
    resolve: { conditionNames: ["browser", "import", "default"], mainFields: ["browser", "module", "main"] },
  });
  let localCode: string;
  const webChunks: Record<string, string> = {};
  try {
    const output = await bundle.generate({ format: "esm", codeSplitting: false,
      // Suppress dependency-path region headers while preserving names and dead-code elimination.
      minify: { compress: { dropDebugger: false, keepNames: { function: true, class: true }, joinVars: false, sequences: false }, mangle: false, codegen: { removeWhitespace: true, legalComments: "inline" } }, comments: { legal: true },
    });
    if (output.output.length !== 1 || output.output[0]?.type !== "chunk") throw new Error("Local Reducers must compile into one self-contained module");
    localCode = output.output[0].code;
    // Browser controls preload their own Reducers. Unopened features keep
    // their code on disk instead of importing every application module at boot.
    const lazyEntry = join(root, 'gonvex', '_build', 'local-lazy-entry.ts');
    const loaders = reducers.map(([path, entry]) => {
      let file = relative(dirname(lazyEntry), resolve(root, entry.file)).replaceAll('\\', '/').replace(/\.(?:tsx?|mts|cts)$/, '.js');
      if (!file.startsWith('.')) file = `./${file}`;
      return `${JSON.stringify(path)}: () => import(${JSON.stringify(file)}).then(module => bindRuntimeGlobals(module.${entry.handler}))`;
    });
    await writeFile(lazyEntry, `${runtimeWrapper}\nexport const localReducers = {${loaders.join(',')}};`);
    const lazyBundle = await rolldown({ input: lazyEntry, cwd: root, platform: 'browser', tsconfig: false, transform: { inject: runtimeGlobals },
      treeshake: { manualPureFunctions: ['action', 'internalReducer', 'query', 'internalQuery', 'liveQuery', 'replicaCollection', 'visibility', 'cron', 'tenantCron', 'schema'] },
      resolve: { conditionNames: ['browser', 'import', 'default'], mainFields: ['browser', 'module', 'main'] },
    });
    let web;
    try { web = await lazyBundle.generate({ format: 'esm', entryFileNames: 'local-reducers.js', chunkFileNames: 'local-reducer-[name]-[hash].js',
      minify: { compress: { dropDebugger: false, keepNames: { function: true, class: true }, joinVars: false, sequences: false }, mangle: false, codegen: { removeWhitespace: true, legalComments: 'inline' } }, comments: { legal: true },
    }); }
    finally { await lazyBundle.close(); }
    for (const chunk of web.output) {
      if (chunk.type !== "chunk") throw new Error("Unexpected local Reducer asset");
      webChunks[chunk.fileName] = chunk.code;
    }
  } finally { await bundle.close(); }
  const contractBytes = manifest.module.files["client-contract.json"];
  const clientContract = contractBytes ? JSON.parse(Buffer.from(contractBytes, "base64").toString()) : undefined;
  const header = "// Generated by Gonvex. Do not edit.\n";
  // Tables each locally executing Reducer needs resident, limited to tables the
  // client can actually hydrate through a generated local collection.
  const localTables = new Set(Object.entries(manifest.functions).filter(([path]) => path.startsWith("__local.")).map(([, entry]) => entry.replica?.table));
  const localDependencies = Object.fromEntries(reducers
    .map(([path, entry]) => [path, (entry.localDependencies ?? []).filter((table) => localTables.has(table))] as const)
    .filter(([, tables]) => tables.length > 0));
  // Reuse the generated API's canonical collection references instead of
  // retaining a second copy of every table's projection metadata at boot.
  const collectionReferences = Object.keys(manifest.functions).filter(path => path.startsWith("__local."))
    .map(path => "api" + path.split(".").map(part => `[${JSON.stringify(part)}]`).join(""));
  const native = await nativeLocalBindings(root, localCode, schema, {
    clientContract, artifactHash: manifest.module.hash, tables: Object.keys(schema),
    collections: Object.entries(manifest.functions).filter(([path]) => path.startsWith("__local.")).map(([path,entry]) => ({kind:"query",path,delivery:"replica",replica:entry.replica})),
  });
  return {
    ...native,
    "dataModel.ts":dataModel,
    "local-schema.ts": header + renderLocalSchema(schema),
    ...Object.fromEntries(Object.entries(webChunks).map(([name, code]) => [name, header + code])),
    "local-reducers.d.ts": `${header}import type { ReducerDefinition } from "@gonvex/module-sdk";\nexport declare const localReducers: Readonly<Record<string, () => Promise<ReducerDefinition<any, any>>>>;\n`,
    "local-runtime.ts": `${header}export {localRuntime} from "./local-executor.js";\n`,
    "local-visibility.ts": `${header}export const unrestrictedVisibility = ${JSON.stringify(unrestrictedVisibility(manifest, localTables))} as const;\n`,
    "local-executor.ts": `${header}import { createPortableReducer } from "@gonvex/local-runtime/portable-client";\nimport { localSchema } from "./local-schema.js";\nimport { localReducers } from "./local-reducers.js";\nimport { api } from "./api.js";\nexport const localRuntime = { mode: "portable" as const, clientContract: ${JSON.stringify(clientContract)}, artifactHash: ${JSON.stringify(manifest.module.hash)}, tables: ${JSON.stringify(Object.keys(schema))}, collections: [${collectionReferences.join(",")}] as const, localDependencies: ${JSON.stringify(localDependencies)} as const, unrestrictedVisibility: () => import("./local-visibility.js").then(module => module.unrestrictedVisibility), create: () => createPortableReducer({schema: localSchema, reducers: localReducers, artifactHash: ${JSON.stringify(manifest.module.hash)}}) };\n`,
  };
}
