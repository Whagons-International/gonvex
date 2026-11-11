// Writes the fixture bundles of a conformance run into .generated/, so the
// K07 conformance Worker carries them (0016 G0-2: the bundle is part of the
// Worker version). Input: manifest.json files written by the CLI
// (`gonvex codegen`). Each bundle gets the kernel prelude, as K19's build
// will give the app bundle (kernel/host/src/bundle.ts).
//
//   node generate-bundles.mjs <manifest.json>...
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { moduleGlobalBindings } from "../../host/src/bundle.ts";

const HERE = dirname(fileURLToPath(import.meta.url));
const OUT = join(HERE, ".generated");
const MODULE_GLOBALS = "../../../host/src/module-globals.ts";

// The prelude of kernel/host/src/bundle.ts `wrapBundle` (Node loads the .ts file directly: it has no imports).
const BINDINGS = moduleGlobalBindings;

export async function generateBundles(manifestPaths) {
  await mkdir(OUT, { recursive: true });
  const entries = [];
  for (const path of manifestPaths) {
    const manifest = JSON.parse(await readFile(path, "utf8"));
    const hash = manifest.module?.hash;
    const code = manifest.module?.javascript?.code;
    if (!hash || !code) throw new Error(`${path}: manifest has no module hash or javascript`);
    const source = Buffer.from(code, "base64").toString("utf8");
    const file = `bundle-${hash}.js`;
    await atomicWrite(join(OUT, file), `import { ${BINDINGS.join(", ")} } from ${JSON.stringify(MODULE_GLOBALS)};\n${source}`);
    entries.push([hash, file]);
  }
  const index = entries.map(([, file], i) => `import * as b${i} from "./${file}";`).join("\n")
    + `\nexport const BUNDLES = {\n${entries.map(([hash], i) => `  ${JSON.stringify(hash)}: b${i},`).join("\n")}\n};\n`;
  await atomicWrite(join(OUT, "bundles.js"), index);
  return entries.map(([hash]) => hash);
}

async function atomicWrite(path, text) {
  const temp = `${path}.${process.pid}.tmp`;
  await writeFile(temp, text);
  await rename(temp, path);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const hashes = await generateBundles(process.argv.slice(2));
  console.log(`generated ${hashes.length} bundle(s): ${hashes.join(", ")}`);
}
