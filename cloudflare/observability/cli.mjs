import { readFile, stat } from 'node:fs/promises';
import { config, estimate, regionalDefinitions } from './providers/config.mjs';
import { operations, activation } from './providers/operations.mjs';
import { replay } from './reports/replay.mjs';
const [command,file] = process.argv.slice(2);
if (command==='audit') console.log(JSON.stringify({ config,definitions:regionalDefinitions(),operations,activation },null,2));
else if(command==='estimate') console.log(JSON.stringify(estimate(30,file?JSON.parse(await readFile(file,'utf8')):null),null,2));
else if(command==='replay' && file) {
  if((await stat(file)).size>8*1024*1024) throw new Error('replay-input-cap');
  const input = await readFile(file,'utf8');
  console.log(JSON.stringify(replay(JSON.parse(input)),null,2));
} else throw new Error('Usage: node cloudflare/observability/cli.mjs audit | estimate [verified-quote.json] | replay input.json');
