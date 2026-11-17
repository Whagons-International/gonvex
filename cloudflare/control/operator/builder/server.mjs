#!/usr/bin/env node
// The K19 release builder as an HTTP server: the same process runs in a
// Container on the platform (Dockerfile) and as a local Node process for the
// local kernel (router/dev/local/start.mjs). Node 22.18+ (TypeScript type
// stripping loads the shared builder core directly).
//
//   POST /inspect  {incoming, headerProject}   -> Inspection
//   POST /prepare  {incoming, project}         -> {ok, summary} | {ok:false, status, error}
//   POST /activate {releases}                  -> {ok, version} | {ok:false, error}
//   POST /deploy   {version}                   -> {ok} | {ok:false, error}
//   GET  /healthz
//
// Every request needs `authorization: Bearer $GONVEX_RELEASE_BUILDER_TOKEN`.
//
// Environment:
//   GONVEX_RELEASE_BUILDER_TOKEN   shared secret with the kernel (required)
//   GONVEX_KERNEL_URL              the kernel's router, for the release bucket (its /_gonvex/k19/blobs route)
//   GONVEX_BUILDER_PORT            listen port (default 8790); GONVEX_BUILDER_HOST (default 127.0.0.1, 0.0.0.0 in a Container)
//   GONVEX_BUILDER_TARGET          "local" (default) or "wrangler"
//   GONVEX_KERNEL_GENERATED_DIR    local: the directory the kernel's `gonvex:releases` alias points into
//   GONVEX_KERNEL_STAGING_DIR      local: where release modules are staged (default: the generated dir). Keep it
//                                  outside the tree `wrangler dev` watches, or staging reloads workerd mid-sync.
//   GONVEX_DEPLOY_DEFER_MS         local: delay before the swap that reloads workerd (default 1500)
//   GONVEX_KERNEL_SCRIPT           stable kernel name; only its Actions/runtime companions are app targets
import { createServer } from 'node:http';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { timingSafeEqual } from 'node:crypto';
import { PRELUDE_SPECIFIER, ReleaseBuilderCore, RELEASES_ENTRY } from './core.ts';
import { PublicationExecutor } from './publication.ts';
import {LocalTarget,WranglerTarget} from './targets.mjs';
import {cloudflareCredentials,accountApi,deployConfig} from '../../../kernel/assembly/deploy.mjs';

const here = dirname(fileURLToPath(import.meta.url));

const token = process.env.GONVEX_RELEASE_BUILDER_TOKEN ?? '';
if (!token) {
  console.error('GONVEX_RELEASE_BUILDER_TOKEN is required');
  process.exit(2);
}
const kernelUrl = (process.env.GONVEX_KERNEL_URL ?? '').replace(/\/+$/, '');
if (!kernelUrl) {
  console.error('GONVEX_KERNEL_URL is required');
  process.exit(2);
}
const port = Number(process.env.GONVEX_BUILDER_PORT ?? '8790');
const targetKind = process.env.GONVEX_BUILDER_TARGET ?? 'local';
const log = (message) => console.log(`[release-builder] ${new Date().toISOString()} ${message}`);

/** The release bucket through the kernel's blob route (a Container cannot bind R2). */
class HttpBlobStore {
  constructor(base, secret) {
    this.base = base;
    this.secret = secret;
  }
  url(key) {
    return `${this.base}/_gonvex/k19/blobs/${encodeURIComponent(key)}`;
  }
  async get(key) {
    const response = await fetch(this.url(key), { headers: { authorization: `Bearer ${this.secret}` } });
    if (response.status === 404) return null;
    if (!response.ok) throw new Error(`blob GET ${key}: HTTP ${response.status}`);
    const text = await response.text();
    return { text: async () => text };
  }
  async put(key, value) {
    const response = await fetch(this.url(key), { method: 'PUT', headers: { authorization: `Bearer ${this.secret}` }, body: value });
    if (!response.ok) throw new Error(`blob PUT ${key}: HTTP ${response.status}`);
  }
  async delete(key) {
    await fetch(this.url(key), { method: 'DELETE', headers: { authorization: `Bearer ${this.secret}` } });
  }
}

/**
 * Local target: files go under the kernel's generated directory. Staging
 * writes the release modules (nothing imports them yet, so workerd does not
 * reload); deploy rewrites `releases.js`, the module the kernel imports, which
 * makes `wrangler dev` rebuild and restart workerd with the new version.
 */

const generatedDir = resolve(process.env.GONVEX_KERNEL_GENERATED_DIR ?? resolve(here, '../../../router/dev/local/.generated'));
const stagingDir = resolve(process.env.GONVEX_KERNEL_STAGING_DIR ?? generatedDir);
await mkdir(generatedDir, { recursive: true });
await mkdir(stagingDir, { recursive: true });
// Before the first sync the kernel must still build: an empty releases module.
const entryFile = join(generatedDir, RELEASES_ENTRY);
try {
  await readFile(entryFile);
} catch {
  await writeFile(entryFile, 'export const releases = {};\n');
}
const target =
  targetKind === 'wrangler'
    ? new WranglerTarget(generatedDir, process.env.GONVEX_KERNEL_SCRIPT ?? '', async(path,name)=>{
        if(!['runtime','actions'].some(k=>name===`${process.env.GONVEX_KERNEL_SCRIPT}-${k}`))throw new Error('stable Worker deployment refused');
        const credentials=await cloudflareCredentials(),api=accountApi(credentials);
        if((await api('')).id!==credentials.accountId)throw new Error('deployment account identity differs');
        for(const kind of ['actions','runtime'])if(!await api(`/workers/scripts/${process.env.GONVEX_KERNEL_SCRIPT}-${kind}/settings`,{missing:true}))throw new Error('application companion is missing; framework setup required');
        await deployConfig(['deploy','--config',path,'--keep-vars'],credentials,generatedDir);
      })
    : new LocalTarget(generatedDir, Number(process.env.GONVEX_DEPLOY_DEFER_MS ?? '1500'), stagingDir);

// app.js lives at <generated>/releases/<project>/<hash>/app.js: three levels below the generated dir.
const builder = new ReleaseBuilderCore({ blobs: new HttpBlobStore(kernelUrl, token), target, preludeSpecifier: PRELUDE_SPECIFIER, log });

// Fresh ingress also works when deployment resets Control during this call.
// These authority operations are idempotent; no provider write is retried here.
const authority = async (action,ref,result={}) => {
  const deadline=Date.now()+120_000;
  for(;;){
    try{
      const response=await fetch(`${kernelUrl}/_gonvex/k19/deployments/${ref.id}/${ref.fence}/${action}`,{
        method:'POST',headers:{authorization:`Bearer ${token}`,'content-type':'application/json'},body:JSON.stringify({version:ref.version,...result})});
      let value;
      try{value=await response.json();}catch{value={error:`deployment authority HTTP ${response.status}`};}
      if(response.ok)return value;
      if(response.status<500||Date.now()>=deadline)throw Object.assign(new Error(value.error??`deployment authority HTTP ${response.status}`),{terminal:true});
    }catch(cause){if(cause.terminal||Date.now()>=deadline)throw cause;}
    await new Promise(resolveRetry=>setTimeout(resolveRetry,100));
  }
};
const publications=new PublicationExecutor(builder,{
  authorize:async(ref)=>(await authority('authorize',ref)).grant,
  settle:async(ref,result)=>{await authority('settle',ref,result);},
  abort:async(ref,error)=>{await authority('abort',ref,{error});},
},async(ref,catalog)=>{
  // Container restart loses the in-memory stage map, not the pinned catalog.
  if(target.staged.has(ref.version))return;
  const staged=await builder.activate(catalog);
  if(!staged.ok||staged.version!==ref.version)throw new Error(staged.ok?'recovered catalog version differs from its publication':staged.error);
});

const authorized = (request) => {
  const given = Buffer.from(request.headers.authorization ?? '');
  const expected = Buffer.from(`Bearer ${token}`);
  return given.length === expected.length && timingSafeEqual(given, expected);
};

async function readJson(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  return JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}');
}

// One build at a time: a release build holds the parsed manifest and the transform in memory.
let queue = Promise.resolve();
const serial = (fn) => {
  const run = queue.then(fn, fn);
  queue = run.catch(() => undefined);
  return run;
};

createServer(async (request, response) => {
  const send = (status, body) => {
    response.writeHead(status, { 'content-type': 'application/json' });
    response.end(JSON.stringify(body));
  };
  if (request.method === 'GET' && request.url === '/healthz') return send(200, { ok: true, target: targetKind });
  if (!authorized(request)) return send(401, { error: 'release builder token required' });
  if (request.method !== 'POST') return send(405, { error: 'method not allowed' });
  try {
    const body = await readJson(request);
    const started = Date.now();
    let result;
    switch (request.url) {
      case '/inspect':
        result = await serial(() => builder.inspect(String(body.incoming), String(body.headerProject ?? '')));
        break;
      case '/prepare':
        result = await serial(() => builder.prepare(String(body.incoming), String(body.project)));
        break;
      case '/activate':
        result = await serial(() => builder.activate(body.releases ?? []));
        break;
      case '/deploy':
        if(!body.publication||body.version!==body.publication.version)return send(400,{error:'fenced publication required'});
        result = await serial(() => publications.deploy(body.publication));
        break;
      case '/abort':
        if(!body.publication)return send(400,{error:'fenced publication required'});
        result = await serial(() => publications.abort(body.publication,String(body.error??'publication aborted')));
        break;
      default:
        return send(404, { error: 'not found' });
    }
    log(`${request.url} ${result.ok ? 'ok' : `failed: ${result.error}`} in ${Date.now() - started} ms (heap ${Math.round(process.memoryUsage().heapUsed / 1e6)} MB)`);
    send(200, result);
  } catch (cause) {
    log(`${request.url} crashed: ${cause?.stack ?? cause}`);
    send(500, { error: String(cause?.message ?? cause) });
  }
}).listen(port, process.env.GONVEX_BUILDER_HOST ?? '127.0.0.1', () => log(`listening on ${process.env.GONVEX_BUILDER_HOST ?? '127.0.0.1'}:${port} (target ${targetKind}, generated ${generatedDir})`));

process.on('SIGTERM', () => process.exit(0));
