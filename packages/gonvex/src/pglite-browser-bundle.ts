import { resolve } from 'node:path';
import type { InputOptions } from 'rolldown';

/** The embedded local Reducer host uses disposable memory, never Node files. */
export function pgliteBrowserBundleOptions(distributionDirectory: string): Pick<InputOptions, 'plugins' | 'onLog'> {
  const nodeFs = resolve(distributionDirectory, 'fs/nodefs.js');
  const wasmLoaders = new Set([
    resolve(distributionDirectory, 'index.js'),
    // Pinned PGlite 0.5.8's initdb loader. Keep this allowlist narrow on upgrades.
    resolve(distributionDirectory, 'chunk-DDJLRBDX.js'),
  ]);
  const unavailable = '\0gonvex:pglite-nodefs-unavailable';
  return {
    plugins: [{
      name: 'gonvex-pglite-memory-host',
      resolveId(source, importer) {
        if (importer && source.startsWith('.') && resolve(importer, '..', source) === nodeFs) return unavailable;
        return null;
      },
      load(id) {
        if (id !== unavailable) return null;
        return `export class NodeFS { constructor() { throw new Error("Node filesystem persistence is unavailable in the Gonvex browser execution host"); } }`;
      },
    }],
    onLog(level, log, defaultHandler) {
      // Emscripten evaluates generated bindings in the loader's lexical scope.
      // Replacing direct eval with indirect eval would break WASM initialization.
      // Preserve it and only omit this diagnostic for the pinned loader files.
      if (log.code === 'EVAL' && log.id && wasmLoaders.has(resolve(log.id))) return;
      defaultHandler(level, log);
    },
  };
}
