import { nativeLocalBindings } from "./native-local-bindings.js";
import { rolldown } from "rolldown";
import { createHash } from "node:crypto";
import { readFile, readdir, writeFile, mkdir } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import type { LocalSchema } from "@gonvex/local-runtime/schema";
import { compileLocalSchema } from "./local-schema.js";
import type { Manifest } from "./manifest-types.js";

export async function projectLocalSchema(root: string): Promise<LocalSchema> {
  const migrationDirectory = join(root, "migrations");
  const names = (await readdir(migrationDirectory).catch(() => [] as string[])).filter((name) => name.endsWith(".sql")).sort();
  const migrations = await Promise.all(names.map(async (name) => ({ name, sql: await readFile(join(migrationDirectory, name), "utf8") })));
  const tenantMigrations = migrations.filter(({ sql }) => !/gonvex:scope\s+(control|landlord)/.test(sql));
  const digest = createHash("sha256").update("local-schema-v1").update(JSON.stringify(tenantMigrations)).digest("hex");
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

export async function localBindings(root: string, manifest: Manifest): Promise<Record<string, string>> {
  const reducers = Object.entries(manifest.functions).filter(([, entry]) => entry.localExecution === 1);
  if (!reducers.length) return {};
  const schema = await projectLocalSchema(root);
  const generatedDirectory = join(root, "gonvex", "_generated");
  const imports = reducers.map(([, entry], index) => {
    let file = relative(generatedDirectory, resolve(root, entry.file)).replaceAll("\\", "/").replace(/\.(?:tsx?|mts|cts)$/, ".js");
    if (!file.startsWith(".")) file = `./${file}`;
    return `import { ${entry.handler} as reducer${index} } from ${JSON.stringify(file)};`;
  });
  const entryFile = join(root, "gonvex", "_build", "local-entry.ts");
  await mkdir(dirname(entryFile), { recursive: true });
  await writeFile(entryFile, `${imports.join("\n")}\nexport const localReducers = {${reducers.map(([path], index) => `${JSON.stringify(path)}: reducer${index}`).join(",")}};`);
  const bundle = await rolldown({
    input: entryFile, cwd: root, platform: "browser", tsconfig: false,
    // These declarations describe server entry points. Their definitions and
    // schema construction are unnecessary when only Reducer exports are used.
    treeshake: { manualPureFunctions: ["action", "internalReducer", "query", "internalQuery", "liveQuery", "replicaCollection", "visibility", "cron", "tenantCron", "schema"] },
    resolve: { conditionNames: ["browser", "import", "default"], mainFields: ["browser", "module", "main"] },
  });
  let localCode: string;
  const webChunks: Record<string, string> = {};
  try {
    const output = await bundle.generate({ format: "esm", codeSplitting: false });
    if (output.output.length !== 1 || output.output[0]?.type !== "chunk") throw new Error("Local Reducers must compile into one self-contained module");
    localCode = output.output[0].code;
    // Browser controls preload their own Reducers. Unopened features keep
    // their code on disk instead of importing every application module at boot.
    const lazyEntry = join(root, 'gonvex', '_build', 'local-lazy-entry.ts');
    const loaders = reducers.map(([path, entry]) => {
      let file = relative(dirname(lazyEntry), resolve(root, entry.file)).replaceAll('\\', '/').replace(/\.(?:tsx?|mts|cts)$/, '.js');
      if (!file.startsWith('.')) file = `./${file}`;
      return `${JSON.stringify(path)}: () => import(${JSON.stringify(file)}).then(module => module.${entry.handler})`;
    });
    await writeFile(lazyEntry, `export const localReducers = {${loaders.join(',')}};`);
    const lazyBundle = await rolldown({ input: lazyEntry, cwd: root, platform: 'browser', tsconfig: false,
      treeshake: { manualPureFunctions: ['action', 'internalReducer', 'query', 'internalQuery', 'liveQuery', 'replicaCollection', 'visibility', 'cron', 'tenantCron', 'schema'] },
      resolve: { conditionNames: ['browser', 'import', 'default'], mainFields: ['browser', 'module', 'main'] },
    });
    let web;
    try { web = await lazyBundle.generate({ format: 'esm', entryFileNames: 'local-reducers.js', chunkFileNames: 'local-reducer-[name]-[hash].js' }); }
    finally { await lazyBundle.close(); }
    for (const chunk of web.output) {
      if (chunk.type !== "chunk") throw new Error("Unexpected local Reducer asset");
      webChunks[chunk.fileName] = chunk.code;
    }
  } finally { await bundle.close(); }
  const contractBytes = manifest.module.files["client-contract.json"];
  const clientContract = contractBytes ? JSON.parse(Buffer.from(contractBytes, "base64").toString()) : undefined;
  const header = "// Generated by Gonvex. Do not edit.\n";
  const native = await nativeLocalBindings(root, localCode, schema, {
    clientContract, artifactHash: manifest.module.hash, tables: Object.keys(schema),
    collections: Object.entries(manifest.functions).filter(([path]) => path.startsWith("__local.")).map(([path,entry]) => ({kind:"query",path,delivery:"replica",replica:entry.replica})),
  });
  return {
    ...native,
    "local-schema.ts": `${header}export const localSchema = ${JSON.stringify(schema)} as const;\n`,
    ...Object.fromEntries(Object.entries(webChunks).map(([name, code]) => [name, header + code])),
    "local-reducers.d.ts": `${header}import type { ReducerDefinition } from "@gonvex/module-sdk";\nexport declare const localReducers: Readonly<Record<string, () => Promise<ReducerDefinition<any, any>>>>;\n`,
    "local-runtime.ts": `${header}export {localRuntime} from "./local-executor.js";\n`,
    "local-executor.ts": `${header}import { createPortableReducer } from "@gonvex/local-runtime/portable-client";\nimport { localSchema } from "./local-schema.js";\nimport { localReducers } from "./local-reducers.js";\nexport const localRuntime = { mode: "portable" as const, clientContract: ${JSON.stringify(clientContract)}, artifactHash: ${JSON.stringify(manifest.module.hash)}, tables: ${JSON.stringify(Object.keys(schema))}, collections: ${JSON.stringify(Object.entries(manifest.functions).filter(([path]) => path.startsWith("__local.")).map(([path, entry]) => ({kind:"query", path, delivery:"replica", replica:entry.replica})))} as const, create: () => createPortableReducer({schema: localSchema, reducers: localReducers, artifactHash: ${JSON.stringify(manifest.module.hash)}}) };\n`,
  };
}
