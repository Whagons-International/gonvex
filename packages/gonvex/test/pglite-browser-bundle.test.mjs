import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { dirname, resolve } from 'node:path';
import test from 'node:test';
import { rolldown } from 'rolldown';
import { pgliteBrowserBundleOptions } from '../dist/pglite-browser-bundle.js';

const require = createRequire(import.meta.url);
const distribution = dirname(createRequire(require.resolve('@gonvex/local-runtime/worker')).resolve('@electric-sql/pglite'));

test('browser host excludes NodeFS and only filters known WASM loader eval diagnostics', async () => {
  const options = pgliteBrowserBundleOptions(distribution);
  const warnings = [];
  const bundle = await rolldown({
    ...options,
    input: resolve(distribution, 'index.js'), platform: 'browser', tsconfig: false,
    onLog: (level, log) => options.onLog(level, log, (_level, warning) => warnings.push(warning)),
  });
  try {
    const output = await bundle.generate({ format: 'esm', codeSplitting: false });
    assert.deepEqual(warnings, []);
    const modules = Object.keys(output.output[0].modules);
    assert.ok(!modules.includes(resolve(distribution, 'fs/nodefs.js')));
    assert.ok(modules.includes('\0gonvex:pglite-nodefs-unavailable'));
    assert.match(output.output[0].code, /Node filesystem persistence is unavailable/);
  } finally { await bundle.close(); }
});

test('application eval and other dependency diagnostics remain visible', () => {
  const options = pgliteBrowserBundleOptions(distribution);
  const forwarded = [];
  const logs = [
    {code:'EVAL',id:'/app/reducer.js',message:'eval'},
    {code:'IMPORT_IS_UNDEFINED',id:resolve(distribution,'index.js'),message:'bad import'},
    {code:'EVAL',id:resolve(distribution,'new-loader.js'),message:'new loader'},
  ];
  for (const log of logs) options.onLog('warn',log,(_level,warning)=>forwarded.push(warning));
  assert.deepEqual(forwarded,logs);
});
