import { constants } from 'node:fs';
import { open, readFile } from 'node:fs/promises';
import { config, estimate, regionalDefinitions } from './providers/config.mjs';
import { operations, activation } from './providers/operations.mjs';
import { replay } from './reports/replay.mjs';
import { auditTaskBinding } from './bindings/whagons-task.mjs';
const [command,file] = process.argv.slice(2);
async function readBoundedInput(file, limit) {
  // Nonblocking open lets the descriptor check reject an idle FIFO as well as
  // other non-regular inputs. Check and read the same opened file, not its path.
  if (!Number.isInteger(constants.O_NONBLOCK)) throw new Error('input-kind-unsupported');
  const handle = await open(file, constants.O_RDONLY | constants.O_NONBLOCK);
  try {
    const info = await handle.stat();
    if (!info.isFile() || info.size > limit) throw new Error('input-cap');
    const bytes = Buffer.alloc(limit + 1);
    let used = 0;
    while (used < bytes.length) {
      const { bytesRead } = await handle.read(bytes, used, bytes.length - used, null);
      if (!bytesRead) break;
      used += bytesRead;
    }
    if (used > limit) throw new Error('input-cap');
    return bytes.toString('utf8', 0, used);
  } finally { await handle.close(); }
}
try {
if (command==='binding-audit') {
  const report=auditTaskBinding(file?JSON.parse(await readBoundedInput(file,64*1024)):undefined);
  console.log(JSON.stringify(report,null,2));
  // A valid offline plan is still BLOCKED. Never turn a provider invocation green.
  process.exitCode=2;
}
else if (command==='audit') console.log(JSON.stringify({ config,definitions:regionalDefinitions(),operations,activation },null,2));
else if(command==='estimate') console.log(JSON.stringify(estimate(30,file?JSON.parse(await readFile(file,'utf8')):null),null,2));
else if(command==='replay' && file) {
  const input = await readBoundedInput(file,8*1024*1024);
  console.log(JSON.stringify(replay(JSON.parse(input)),null,2));
} else throw new Error('Usage: node cloudflare/observability/cli.mjs audit | binding-audit [declarations.json] | estimate [verified-quote.json] | replay input.json');

} catch {
  // Never serialize input, parser messages, arbitrary errors, paths or provider payloads.
  process.stderr.write(['replay','binding-audit'].includes(command)?'monitor-input-integrity\n':'monitor-command-failed\n');
  process.exitCode=1;
}
