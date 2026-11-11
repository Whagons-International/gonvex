#!/usr/bin/env node
// Deploy or delete the P03 Worker (gonvex-cf-dev-p03) on the development
// account. Run under the credential wrapper; never prints a credential:
//
//   whagons-dev credentials exec full-cloudflare-access -- node scripts/deploy.mjs deploy TOKEN_FILE
//   whagons-dev credentials exec full-cloudflare-access -- node scripts/deploy.mjs delete
//
// deploy: creates TOKEN_FILE (mode 600, TRANSFER_TOKEN=<random>) if missing,
// deploys the Worker, stores the token as the Worker secret, prints the URL.
// delete: deletes the Worker and its Durable Object namespace.

import { spawnSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const NAME = 'gonvex-cf-dev-p03';

function credentials() {
  const file = process.env.WHAGONS_CREDENTIAL_FILE;
  if (file) {
    const parsed = JSON.parse(readFileSync(file, 'utf8'));
    if (typeof parsed?.accountId === 'string' && typeof parsed?.apiToken === 'string') return parsed;
    throw new Error('credential is missing accountId or apiToken');
  }
  if (process.env.CLOUDFLARE_ACCOUNT_ID && process.env.CLOUDFLARE_API_TOKEN) {
    return { accountId: process.env.CLOUDFLARE_ACCOUNT_ID, apiToken: process.env.CLOUDFLARE_API_TOKEN };
  }
  throw new Error('run under `whagons-dev credentials exec full-cloudflare-access --`');
}

function wrangler(args, { input } = {}) {
  const { accountId, apiToken } = credentials();
  const env = { ...process.env, CLOUDFLARE_ACCOUNT_ID: accountId, CLOUDFLARE_API_TOKEN: apiToken, WRANGLER_SEND_METRICS: 'false' };
  delete env.WHAGONS_CREDENTIAL_FILE;
  const result = spawnSync(join(root, 'node_modules', '.bin', 'wrangler'), args, { cwd: root, env, input, encoding: 'utf8' });
  // wrangler output never contains the API token; strip the account id anyway.
  const scrub = (text) => (text ?? '').replaceAll(accountId, '<account>');
  if (result.status !== 0) throw new Error(`wrangler ${args[0]} failed:\n${scrub(result.stderr)}\n${scrub(result.stdout)}`);
  return scrub(result.stdout);
}

const [command, tokenFile] = process.argv.slice(2);
if (command === 'deploy') {
  if (!tokenFile) throw new Error('deploy needs TOKEN_FILE');
  if (!existsSync(tokenFile)) writeFileSync(tokenFile, `TRANSFER_TOKEN=${randomBytes(24).toString('hex')}\n`, { mode: 0o600 });
  const token = readFileSync(tokenFile, 'utf8').match(/^TRANSFER_TOKEN=(.+)$/m)[1].trim();
  const out = wrangler(['deploy']);
  wrangler(['secret', 'put', 'TRANSFER_TOKEN', '--name', NAME], { input: token });
  const url = out.match(/https:\/\/[\w.-]+\.workers\.dev/)?.[0];
  console.log(JSON.stringify({ worker: NAME, url }));
} else if (command === 'delete') {
  console.log(wrangler(['delete', '--name', NAME, '--force']).trim().split('\n').at(-1));
} else {
  throw new Error('usage: deploy.mjs deploy TOKEN_FILE | delete');
}
