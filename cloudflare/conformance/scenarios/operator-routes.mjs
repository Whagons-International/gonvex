/**
 * Deploy and operator routes (CONTRACT.md D.1, D.2, D.3; K19): /dev/sync
 * refusals and the response shape, operator authentication (admin key,
 * dashboard accounts and sessions, personal access tokens with bounded
 * grants), projects, keys, members, invitations, env with dotenv, tenants,
 * the manifest, the data browser (reads, filters, writes, admin-only delete)
 * and errors, compared status and body on every backend.
 *
 * Seeds no tenant or Member (`tenants: {}`): the operator routes create their
 * own tenant, so the K19 local kernel runs it too
 * (`--backend rust=rust --backend kernel=cloudflare-kernel --scenario operator-routes`).
 *
 * Values each backend mints (tokens, ids, times, keys, session revisions)
 * are scrubbed with named rules before observing; everything else is exact.
 */

const SCRUB = [
  [/"(?:createdAt|expiresAt|lastUsedAt|revokedAt|updatedAt|lastSignedInAt|time)":"[0-9T:.\-+Z]+"/g, (m) => `${m.slice(0, m.indexOf(':'))}:"<time>"`],
  [/"expiresAt":\d{13}/g, '"expiresAt":"<ms>"'],
  [/gvx_pat_[0-9a-f-]{36}(?:\.[A-Za-z0-9_-]{43})?/g, (m) => (m.includes('.') ? '<pat-token>' : '<pat-prefix>')],
  [/pat_[0-9a-f-]{36}/g, '<pat-id>'],
  [/pinv_[0-9a-f-]{36}/g, '<project-invitation-id>'],
  [/invite_[A-Za-z0-9_-]{43}/g, '<invitation-token>'],
  [/gvx_[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}/g, '<project-key>'],
  [/"accessToken":"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"/g, '"accessToken":"<dashboard-session>"'],
  [/"sessionRevision":"[^"]*"/g, '"sessionRevision":"<revision>"'],
  [/tenant_[0-9a-f]{12}/g, '<database-alias>'],
];

const scrub = (value) => {
  let text = JSON.stringify(value);
  for (const [pattern, replacement] of SCRUB) text = text.replace(pattern, replacement);
  return JSON.parse(text);
};

async function http(ctx, method, path, { key = ctx.backend.adminKey, body, headers = {}, raw } = {}) {
  const init = { method, headers: { ...headers } };
  if (key !== null) init.headers.authorization = `Bearer ${key}`;
  if (raw !== undefined) init.body = raw;
  else if (body !== undefined) {
    init.body = JSON.stringify(body);
    init.headers['content-type'] ??= 'application/json';
  }
  const response = await fetch(ctx.backend.httpUrl + path, init);
  const text = await response.text();
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = text === '' ? '<empty>' : '<text>';
  }
  return scrub({ status: response.status, body: parsed });
}

export default {
  name: 'operator-routes',
  description: 'Deploy and operator routes: /dev/sync refusals, operator auth, PATs, projects, env, tenants, manifest, data browser, errors',
  tenants: {},
  tables: [],
  async run(ctx) {
    const projectKey = ctx.backend.keys.get(ctx.project);
    const q = (params) => new URLSearchParams({ project: ctx.project, ...params }).toString();
    const tenantId = 'operator-routes-shard';
    let pat;

    await ctx.step('sync refusals and the response shape', async () => {
      const manifest = { ...ctx.fixtures[1].manifest, project: ctx.project };
      const headers = { 'x-gonvex-project-id': ctx.project, 'x-gonvex-key': projectKey };
      ctx.observe('wrongKey', await http(ctx, 'POST', '/dev/sync', { key: 'not-the-key', body: manifest, headers: { 'x-gonvex-project-id': ctx.project } }));
      ctx.observe('mismatch', await http(ctx, 'POST', '/dev/sync', { key: projectKey, body: { ...manifest, project: 'another-project' }, headers }));
      ctx.observe('noModule', await http(ctx, 'POST', '/dev/sync', { key: projectKey, body: { project: ctx.project }, headers }));
      const badHash = { ...manifest, module: { ...manifest.module, hash: 'f'.repeat(64) } };
      ctx.observe('badHash', await http(ctx, 'POST', '/dev/sync', { key: projectKey, body: badHash, headers }));
      const before = await ctx.backend.workerVersion?.();
      ctx.observe('resync', await http(ctx, 'POST', '/dev/sync', { key: projectKey, body: manifest, headers }));
      // The local kernel's new Worker version goes live just after the answer (0036 §4); Rust has none to wait for.
      await ctx.backend.waitForWorkerVersion?.(before);
    });

    await ctx.step('operator authentication', async () => {
      ctx.observe('anonymous', await http(ctx, 'GET', '/dev/projects', { key: null }));
      ctx.observe('adminMe', await http(ctx, 'GET', '/dev/auth/me'));
      ctx.observe('shortPassword', await http(ctx, 'POST', '/dev/auth/accounts', { body: { email: 'Op@Conformance.invalid', name: '', password: 'short', role: 'standard' } }));
      ctx.observe('badRole', await http(ctx, 'POST', '/dev/auth/accounts', { body: { email: 'op@conformance.invalid', name: '', password: 'a-long-password-1', role: 'owner' } }));
      ctx.observe('account', await http(ctx, 'POST', '/dev/auth/accounts', { body: { email: 'Op@Conformance.invalid', name: '', password: 'a-long-password-1', role: 'standard' } }));
      ctx.observe('badLogin', await http(ctx, 'POST', '/dev/auth/login', { key: null, body: { email: 'op@conformance.invalid', password: 'nope' } }));
      const login = await http(ctx, 'POST', '/dev/auth/login', { key: null, body: { email: 'op@conformance.invalid', password: 'a-long-password-1' } });
      ctx.observe('login', login);
      // The scrubbed token cannot be reused: sign in again unscrubbed for the session's own calls.
      const session = await (await fetch(ctx.backend.httpUrl + '/dev/auth/login', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ email: 'op@conformance.invalid', password: 'a-long-password-1' }) })).json();
      ctx.observe('sessionMe', await http(ctx, 'GET', '/dev/auth/me', { key: session.session.accessToken }));
      ctx.observe('sessionNotAdmin', await http(ctx, 'GET', '/dev/auth/accounts', { key: session.session.accessToken }));
      ctx.observe('sessionProjects', await http(ctx, 'GET', '/dev/projects', { key: session.session.accessToken }));
    });

    await ctx.step('personal access tokens', async () => {
      const created = await fetch(ctx.backend.httpUrl + '/dev/auth/tokens', { method: 'POST', headers: { authorization: `Bearer ${ctx.backend.adminKey}`, 'content-type': 'application/json' }, body: JSON.stringify({ name: 'ci', permissions: ['projects:read', 'PROJECTS:READ', 'tokens:*'] }) });
      const body = await created.json();
      pat = body.accessToken;
      ctx.observe('created', scrub({ status: created.status, body }));
      ctx.observe('patMe', await http(ctx, 'GET', '/dev/auth/me', { key: pat }));
      ctx.observe('patForbidden', await http(ctx, 'POST', '/dev/projects', { key: pat, body: { name: 'x' } }));
      ctx.observe('patEscalate', await http(ctx, 'POST', '/dev/auth/tokens', { key: pat, body: { name: 'x', permissions: ['projects:delete'] } }));
      ctx.observe('patGlobal', await http(ctx, 'POST', '/dev/auth/tokens', { key: pat, body: { name: 'x', permissions: ['*'] } }));
      ctx.observe('invalidPermission', await http(ctx, 'POST', '/dev/auth/tokens', { body: { name: 'x', permissions: ['nope'] } }));
      ctx.observe('pastExpiry', await http(ctx, 'POST', '/dev/auth/tokens', { body: { name: 'x', expiresAt: '2001-01-01T00:00:00Z' } }));
      ctx.observe('list', await http(ctx, 'GET', '/dev/auth/tokens', { key: pat }));
    });

    await ctx.step('projects, members, invitations, keys', async () => {
      const projects = await http(ctx, 'GET', '/dev/projects');
      ctx.observe('listed', projects.body.projects?.filter((p) => p.id === '<project>'));
      ctx.observe('noFields', await http(ctx, 'PATCH', `/dev/projects/${ctx.project}`, { body: {} }));
      ctx.observe('badStatus', await http(ctx, 'PATCH', `/dev/projects/${ctx.project}`, { body: { status: 'disabled' } }));
      ctx.observe('badMode', await http(ctx, 'PATCH', `/dev/projects/${ctx.project}`, { body: { databaseMode: 'x' } }));
      ctx.observe('tracking', await http(ctx, 'PATCH', `/dev/projects/${ctx.project}`, { body: { errorTrackingEnabled: true } }));
      ctx.observe('member', await http(ctx, 'POST', `/dev/projects/${ctx.project}/members`, { body: { email: ' Dev@Conformance.invalid ', name: 'Dev', role: '' } }));
      ctx.observe('badMemberRole', await http(ctx, 'POST', `/dev/projects/${ctx.project}/members`, { body: { email: 'x@conformance.invalid', role: 'boss' } }));
      ctx.observe('memberByKey', await http(ctx, 'POST', `/dev/projects/${ctx.project}/members`, { key: projectKey, body: { email: 'x@conformance.invalid' } }));
      ctx.observe('invitation', await http(ctx, 'POST', `/dev/projects/${ctx.project}/invitations`, { body: { email: 'Invitee@Conformance.invalid', role: 'admin' } }));
      ctx.observe('members', await http(ctx, 'GET', `/dev/projects/${ctx.project}/members`, { key: projectKey }));
      ctx.observe('key', await http(ctx, 'POST', `/dev/projects/${ctx.project}/key`));
      ctx.observe('rotateByKey', await http(ctx, 'POST', `/dev/projects/${ctx.project}/key/rotate`, { key: projectKey }));
    });

    await ctx.step('env', async () => {
      ctx.observe('badName', await http(ctx, 'POST', `/dev/projects/${ctx.project}/env`, { key: projectKey, body: { name: '1BAD', value: 'x' } }));
      ctx.observe('set', await http(ctx, 'POST', `/dev/projects/${ctx.project}/env`, { key: projectKey, body: { name: 'API_URL', value: 'https://a' } }));
      ctx.observe('replace', await http(ctx, 'PUT', `/dev/projects/${ctx.project}/env`, { key: projectKey, body: { content: "# c\nexport TOKEN='abc'\nQUOTED=\"x y\"\nbad-name=1\nEMPTY=\n", variables: [{ name: 'EXTRA', value: '1' }, { name: '9no', value: 'x' }] } }));
      ctx.observe('delete', await http(ctx, 'DELETE', `/dev/projects/${ctx.project}/env`, { key: projectKey, body: { name: 'TOKEN' } }));
      ctx.observe('get', await http(ctx, 'GET', `/dev/projects/${ctx.project}/env`, { key: projectKey }));
    });

    await ctx.step('tenants and the manifest', async () => {
      ctx.alias(tenantId, 'tenant:shard');
      ctx.observe('created', await http(ctx, 'POST', '/dev/tenants', { key: projectKey, body: { projectId: ctx.project, id: tenantId, name: 'Shard' } }));
      ctx.observe('invalidId', await http(ctx, 'POST', '/dev/tenants', { key: projectKey, body: { projectId: ctx.project, id: 'bad id', name: 'x' } }));
      ctx.observe('list', await http(ctx, 'GET', `/dev/tenants?project=${ctx.project}`));
      ctx.observe('noProject', await http(ctx, 'GET', '/dev/tenants', { key: pat }));
      ctx.observe('realm', await http(ctx, 'GET', `/dev/projects/${ctx.project}/auth/tenants`, { key: projectKey }));
      const manifest = await http(ctx, 'GET', `/dev/manifest?project=${ctx.project}`, { key: projectKey });
      ctx.observe('manifestKeys', Object.keys(manifest.body));
      ctx.observe('manifestFunctions', Object.keys(manifest.body.functions ?? {}).sort());
      ctx.observe('manifestModuleHash', manifest.body.module?.hash);
      ctx.observe('noManifestProject', await http(ctx, 'GET', '/dev/manifest'));
    });

    await ctx.step('data browser', async () => {
      const t = tenantId;
      const tables = await http(ctx, 'GET', `/dev/data/tables?${q({ tenant: t })}`, { key: projectKey });
      ctx.observe('tables', tables.body.tables?.filter((table) => ['board', 'notes', 'note_shares', 'storage_values'].includes(table.name)));
      ctx.observe('insert', await http(ctx, 'POST', `/dev/data/tables/board/rows?${q({ tenant: t })}`, { key: projectKey, body: { id: 'b1', title: 'First', position: 2 } }));
      ctx.observe('insert2', await http(ctx, 'POST', `/dev/data/tables/board/rows?${q({ tenant: t })}`, { key: projectKey, body: { id: 'b2', title: 'Second', position: 1 } }));
      ctx.observe('duplicate', await http(ctx, 'POST', `/dev/data/tables/board/rows?${q({ tenant: t })}`, { key: projectKey, body: { id: 'b1', title: 'Again' } }));
      ctx.observe('badColumn', await http(ctx, 'POST', `/dev/data/tables/board/rows?${q({ tenant: t })}`, { key: projectKey, body: { nope: 1 } }));
      ctx.observe('typed', await http(ctx, 'POST', `/dev/data/tables/storage_values/rows?${q({ tenant: t })}`, { key: projectKey, body: { id: 's1', label: 'x', meta: { a: [1, 2] }, flag: 't', n: 3 } }));
      ctx.observe('rows', await http(ctx, 'GET', `/dev/data/tables/board/rows?${q({ tenant: t })}`, { key: projectKey }));
      ctx.observe('sorted', await http(ctx, 'GET', `/dev/data/tables/board/rows?${q({ tenant: t, sort: 'position', columns: 'id,position' })}`, { key: projectKey }));
      ctx.observe('filtered', await http(ctx, 'GET', `/dev/data/tables/board/rows?${q({ tenant: t, filters: JSON.stringify([{ column: 'title', operator: 'startsWith', value: 'Sec' }]) })}`, { key: projectKey }));
      ctx.observe('searched', await http(ctx, 'GET', `/dev/data/tables/board/rows?${q({ tenant: t, search: 'first', count: 'estimate', limit: '1' })}`, { key: projectKey }));
      ctx.observe('typedRead', await http(ctx, 'GET', `/dev/data/tables/storage_values/rows?${q({ tenant: t })}`, { key: projectKey }));
      ctx.observe('badSort', await http(ctx, 'GET', `/dev/data/tables/board/rows?${q({ tenant: t, sort: 'nope' })}`, { key: projectKey }));
      ctx.observe('internal', await http(ctx, 'GET', `/dev/data/tables/members/rows?${q({ tenant: t })}`, { key: projectKey }));
      ctx.observe('update', await http(ctx, 'PATCH', `/dev/data/tables/board/rows/b1?${q({ tenant: t })}`, { key: projectKey, body: { title: 'First (edited)' } }));
      ctx.observe('updateIdentity', await http(ctx, 'PATCH', `/dev/data/tables/board/rows/b1?${q({ tenant: t })}`, { key: projectKey, body: { id: 'x' } }));
      ctx.observe('updateMissing', await http(ctx, 'PATCH', `/dev/data/tables/board/rows/zz?${q({ tenant: t })}`, { key: projectKey, body: { title: 'x' } }));
      ctx.observe('deleteByKey', await http(ctx, 'DELETE', `/dev/data/tables/board/rows/b2?${q({ tenant: t })}`, { key: projectKey }));
      ctx.observe('deleteByAdmin', await http(ctx, 'DELETE', `/dev/data/tables/board/rows/b2?${q({ tenant: t })}`));
      ctx.observe('unknownTenant', await http(ctx, 'GET', `/dev/data/tables?${q({ tenant: 'nope' })}`, { key: projectKey }));
    });

    await ctx.step('errors, logs, cache', async () => {
      ctx.observe('status', await http(ctx, 'GET', `/dev/errors/status?${q({})}`));
      ctx.observe('groups', await http(ctx, 'GET', `/dev/errors/groups?${q({})}`));
      ctx.observe('missingGroup', await http(ctx, 'GET', `/dev/errors/groups/nope?${q({})}`));
      ctx.observe('badStatus', await http(ctx, 'PATCH', `/dev/errors/groups/nope?${q({})}`, { body: { status: 'maybe' } }));
      ctx.observe('clearLogs', await http(ctx, 'DELETE', `/dev/logs?${q({})}`, { key: projectKey }));
      ctx.observe('clearCache', await http(ctx, 'DELETE', `/dev/cache?${q({})}`, { key: projectKey }));
      ctx.observe('metricsNoProject', await http(ctx, 'GET', '/dev/metrics'));
      ctx.observe('notFound', await http(ctx, 'GET', '/dev/nope'));
      ctx.observe('preflight', (await fetch(ctx.backend.httpUrl + '/dev/anything', { method: 'OPTIONS' })).status);
    });
  },
};
