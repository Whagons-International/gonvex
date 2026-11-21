import { pathToFileURL } from 'node:url';
import { preflight, Refusal } from './binding.mjs';
import { runCell, cleanup } from './driver.mjs';

export async function main(args) {
  const [mode, bindingPath, out] = args;
  if (!['preflight', 'runCell', 'cleanup'].includes(mode) || !bindingPath || args.length > 3 || (mode !== 'preflight' && !out)) throw new Refusal('NOT_ELIGIBLE', 'usage: preflight BINDING | runCell BINDING OUT | cleanup BINDING OUT');
  const p = await preflight(bindingPath);
  if (mode === 'preflight') return { state: p.state, missing: p.missing }; // Never print private bindings.
  const manifest = { driverBinding: bindingPath, driverOutput: out };
  if (mode === 'cleanup') return cleanup({ manifest });
  const controller = new AbortController();
  const stop = () => controller.abort(); process.once('SIGINT', stop); process.once('SIGTERM', stop);
  try { return await runCell({ cell: { id: p.binding.cell }, manifest, signal: controller.signal, emit() {} }); }
  finally { process.off('SIGINT', stop); process.off('SIGTERM', stop); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { console.log(JSON.stringify(await main(process.argv.slice(2)))); }
  catch (e) { console.log(JSON.stringify({ state: 'REFUSED', code: e instanceof Refusal ? e.code : 'UNKNOWN', field: e instanceof Refusal ? e.field : 'local-input/runtime-error' })); process.exitCode = 1; }
}
