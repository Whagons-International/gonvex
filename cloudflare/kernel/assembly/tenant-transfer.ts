// Development-only P03 transport into the serving tenant's SQLite, never a second object.
import { rpc } from '../session/rpc';
import { timingSafeEqual } from 'node:crypto';
import { TenantTransfer } from '../../tools/tenant-transfer/src/transport.mjs';

export const IMPORT_KEY = 'assembly/tenant-import';
export interface TransferEnv {
  GONVEX_DEV_TRANSFER_TARGET?: string;
  GONVEX_DEV_TRANSFER_TOKEN?: string;
  /** Explicit fresh object names only; no existing customer tenant can match by default. */
  GONVEX_DEV_TRANSFER_TENANTS?: string;
}
export function transferAllowed(env: TransferEnv, tenant: string): boolean {
  return !!env.GONVEX_DEV_TRANSFER_TARGET?.startsWith('gonvex-cf-dev-') &&
    /^devcopy-[A-Za-z0-9_-]{1,100}$/.test(tenant) &&
    (env.GONVEX_DEV_TRANSFER_TENANTS ?? '').split(',').includes(tenant);
}
const operations = new Set(['status', 'schema', 'insert', 'sequence', 'catalog', 'read', 'checksum', 'check', 'pragma', 'wipe', 'verify', 'complete']);
export async function handleTenantTransfer(request: Request, env: TransferEnv & { TENANT: DurableObjectNamespace<any> }): Promise<Response | null> {
  const path = new URL(request.url).pathname;
  if (!path.startsWith('/__tenant-transfer/')) return null;
  const match = /^\/__tenant-transfer\/t\/([A-Za-z0-9_-]+)\/([a-z]+)$/.exec(path);
  const expected = env.GONVEX_DEV_TRANSFER_TOKEN;
  const supplied = request.headers.get('authorization') ?? '';
  const wanted = Buffer.from(`Bearer ${expected}`);
  const given = Buffer.from(supplied);
  if (!expected || !match || !transferAllowed(env, match[1]!) || given.length !== wanted.length ||
      !timingSafeEqual(given, wanted)) return new Response(null, { status: 404 });
  if (request.method !== 'POST' || !operations.has(match[2]!)) return new Response(null, { status: 405 });
  const bytes = await request.arrayBuffer();
  if (bytes.byteLength > 4 * 1024 * 1024) return new Response(null, { status: 413 });
  return rpc<{ assemblyTransfer(request: Request): Promise<Response> }>(env.TENANT, match[1]!).assemblyTransfer(new Request(request, { body: bytes }));
}

type ImportState = { phase: 'copying' | 'ready'; schemaFailures: number; checked: boolean; verified: Record<string, { rows: number; sha256: string }> };
/** Caller serializes with the kernel's ordinary reducer/migration queue. */
export async function transferIntoTenant(ctx: DurableObjectState, request: Request, active: boolean): Promise<Response> {
  const op = new URL(request.url).pathname.split('/').at(-1)!;
  let state = ctx.storage.kv.get<ImportState>(IMPORT_KEY);
  if (active) return Response.json({ error: 'tenant already belongs to an application' }, { status: 409 });
  if (op === 'wipe') {
    // First use is permitted only on a fresh serving object. A partial import may be restarted.
    if (!state) {
      const tables = ctx.storage.sql.exec<{ name: string }>("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\'").toArray();
      if (tables.some(({ name }) => !['_gonvex_files', '_gonvex_storage_tickets', '__miniflare_do_name'].includes(name))) {
        return Response.json({ error: 'tenant storage is not fresh' }, { status: 409 });
      }
      for (const { name } of tables.filter(({ name }) => name !== '__miniflare_do_name')) {
        if (ctx.storage.sql.exec<{ n: number }>(`SELECT count(*) AS n FROM "${name}"`).one().n !== 0) return Response.json({ error: 'tenant storage is not empty' }, { status: 409 });
      }
    }
    if (state?.phase === 'ready') return Response.json({ error: 'import already completed' }, { status: 409 });
    await ctx.storage.deleteAlarm();
    await ctx.storage.deleteAll();
    ctx.storage.kv.put(IMPORT_KEY, { phase: 'copying', schemaFailures: 0, checked: false, verified: {} } satisfies ImportState);
    return Response.json({ wiped: true });
  }
  if (!state) return Response.json({ error: 'begin with wipe on a fresh tenant' }, { status: 409 });
  if (op === 'verify') {
    if (state.phase !== 'copying') return Response.json({ error: 'import already completed' }, { status: 409 });

    const body = await request.json<{ checksums: Record<string, { rows: number; sha256: string }> }>();
    const expected = body.checksums;
    if (!expected || typeof expected !== 'object' || Array.isArray(expected) || Object.keys(expected).length > 60 ||
        Object.values(expected).some(v => !v || !Number.isSafeInteger(v.rows) || v.rows < 0 || !/^[a-f0-9]{64}$/.test(v.sha256))) return Response.json({ error: 'invalid checksum page' }, { status: 400 });
    const response = await new TenantTransfer(ctx).fetch(new Request('http://transfer/checksum', {method:'POST', body:JSON.stringify({tables:Object.keys(expected)})}));
    const result = await response.json() as { checksums: typeof expected };
    if (!response.ok || Object.entries(expected).some(([table, value]) => result.checksums?.[table]?.rows !== value.rows || result.checksums?.[table]?.sha256 !== value.sha256)) return Response.json({ error: 'P03 table checksum mismatch' }, { status: 409 });
    state.verified = { ...state.verified, ...expected }; ctx.storage.kv.put(IMPORT_KEY, state);
    return Response.json({ verified: Object.keys(expected).length });
  }
  if (op === 'complete') {
    if (state.phase !== 'copying' || !state.checked || state.schemaFailures) return Response.json({ error: 'P03 integrity checks and zero schema refusals required' }, { status: 409 });
    const response = await new TenantTransfer(ctx).fetch(new Request('http://transfer/catalog', {method:'POST',body:'{}'}));
    if (!response.ok) return response;
    const catalog = await response.json() as { plans: { name: string }[] };
    if (!Array.isArray(catalog.plans)) return Response.json({ error: 'P03 catalog unavailable' }, { status: 409 });
    if (!catalog.plans.length || catalog.plans.some(p => !Object.hasOwn(state!.verified, p.name)) || Object.keys(state.verified).length !== catalog.plans.length) return Response.json({ error: 'verify every staged table checksum first' }, { status: 409 });
    state.phase = 'ready'; ctx.storage.kv.put(IMPORT_KEY, state);
    return Response.json({ ready: true });
  }
  const writes = ['schema', 'insert', 'sequence', 'pragma'].includes(op);
  if (state.phase === 'ready' && writes) return Response.json({ error: 'import already completed' }, { status: 409 });
  const response = await new TenantTransfer(ctx).fetch(request);
  if (writes || op === 'check') {
    const result = await response.clone().json() as { failed?: unknown[]; quickCheck?: string; foreignKeyViolations?: number; fts?: Record<string, string> };
    if (writes) { state.checked = false; state.verified = {}; }
    if (op === 'schema') state.schemaFailures += result.failed?.length ?? 0;
    if (op === 'check') state.checked = response.ok && result.quickCheck === 'ok' && result.foreignKeyViolations === 0 && Object.values(result.fts ?? {}).every(value => value === 'ok');
    ctx.storage.kv.put(IMPORT_KEY, state);
  }
  return response;
}
