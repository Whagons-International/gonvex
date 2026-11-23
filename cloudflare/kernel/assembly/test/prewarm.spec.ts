// Tenant prewarm (kernel/session/prewarm.ts → SessionControlObject.prewarmTenant). The selector resolves with the
// same function as every tenant auth (`resolveTenant`), so the tenant it wakes is the tenant an auth with that
// selector would enter; anything `resolveTenant` refuses wakes nothing; each tenant is woken at most once per
// interval per Control instance.
import { env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';

type Control = {
  devCreateProject(input: { id: string; name: string }): Promise<unknown>;
  fixtureTenant(project: string, tenant: string): Promise<void>;
  fixtureTenantRow(tenant: string, domain: string, status?: string, databaseUrl?: string): Promise<void>;
  prewarmTenant(project: string, selector: string): Promise<'warming' | 'recent' | 'unknown'>;
  fixturePrewarmedSize(): Promise<number>;
  fixturePrewarmMax(max: number): Promise<void>;
  fixtureNoteCoTenants(result: unknown): Promise<void>;
  fixtureRestart(): Promise<void>;
  fixtureAdvanceClock(ms: number): Promise<void>;
  fixtureCoTenantWrites(): Promise<number>;
  fixtureFailCoTenantWrites(on: boolean): Promise<void>;
  fixturePutRawCoTenants(tenant: string, value: unknown): Promise<void>;
};
type Tenant = { fixturePings(): Promise<number> };
const tenantObject = (id: string) => (env as unknown as { PUB_TENANT: DurableObjectNamespace }).PUB_TENANT.getByName(id) as unknown as Tenant;
/** The tenant's ping count once the background wake has had time to land. */
async function pingsOf(id: string, expected: number): Promise<number> {
  for (let i = 0; i < 100; i++) { const n = await tenantObject(id).fixturePings(); if (n >= expected) return n; await new Promise((r) => setTimeout(r, 10)); }
  return tenantObject(id).fixturePings();
}
const freshControl = () => (env as unknown as { PUB_CONTROL: DurableObjectNamespace }).PUB_CONTROL.getByName('prewarm-' + crypto.randomUUID()) as unknown as Control;
const newTenant = () => 'pw-' + crypto.randomUUID().slice(0, 8);

async function world() {
  const control = freshControl();
  const tenant = newTenant();
  await control.devCreateProject({ id: 'pw-project', name: 'prewarm fixture' });
  await control.fixtureTenant('pw-project', tenant);
  return { control, tenant };
}

describe('SessionControlObject.prewarmTenant', () => {
  it('wakes a tenant named by id, then not again within the interval (Control-side throttle)', async () => {
    const { control, tenant } = await world();
    expect(await control.prewarmTenant('pw-project', tenant)).toBe('warming');
    expect(await control.prewarmTenant('pw-project', tenant)).toBe('recent');
  });

  it('resolves a domain selector, case-insensitively, to the same tenant (one wake for both names)', async () => {
    const { control, tenant } = await world();
    await control.fixtureTenantRow(tenant, 'El-Rey2');
    expect(await control.prewarmTenant('pw-project', 'el-rey2')).toBe('warming');
    expect(await control.prewarmTenant('pw-project', tenant)).toBe('recent');
  });

  it('prefers an exact tenant id over another tenant whose domain is that selector, as resolveTenant does', async () => {
    const { control, tenant } = await world();
    const other = newTenant();
    await control.fixtureTenant('pw-project', other);
    await control.fixtureTenantRow(other, tenant); // other's domain equals tenant's id
    expect(await control.prewarmTenant('pw-project', tenant)).toBe('warming');
    expect(await control.prewarmTenant('pw-project', tenant)).toBe('recent');
    expect(await control.prewarmTenant('pw-project', other)).toBe('warming'); // `other` was not the one woken
  });

  it('wakes nothing that resolveTenant refuses: unknown, another project, no database, disabled, deleted', async () => {
    const { control, tenant } = await world();
    expect(await control.prewarmTenant('pw-project', 'nobody')).toBe('unknown');
    expect(await control.prewarmTenant('other-project', tenant)).toBe('unknown');
    const unprovisioned = newTenant();
    await control.fixtureTenant('pw-project', unprovisioned);
    await control.fixtureTenantRow(unprovisioned, '', 'active', '');
    expect(await control.prewarmTenant('pw-project', unprovisioned)).toBe('unknown');
    await control.fixtureTenantRow(tenant, '', 'disabled');
    expect(await control.prewarmTenant('pw-project', tenant)).toBe('unknown');
    await control.fixtureTenantRow(tenant, '', 'deleted');
    expect(await control.prewarmTenant('pw-project', tenant)).toBe('unknown');
  });

  it('rejects empty or oversized input before any lookup', async () => {
    const { control, tenant } = await world();
    expect(await control.prewarmTenant('', tenant)).toBe('unknown');
    expect(await control.prewarmTenant('pw-project', '')).toBe('unknown');
    expect(await control.prewarmTenant('pw-project', 'x'.repeat(64))).toBe('unknown');
    expect(await control.prewarmTenant('p'.repeat(129), tenant)).toBe('unknown');
  });

  it('the resolved tenant object itself receives the ping, once per interval', async () => {
    const { control, tenant } = await world();
    await control.fixtureTenantRow(tenant, 'pinged-domain');
    expect(await control.prewarmTenant('pw-project', 'pinged-domain')).toBe('warming');
    expect(await pingsOf(tenant, 1)).toBe(1); // TENANT.getByName(<canonical id>), not the selector's name
    expect(await control.prewarmTenant('pw-project', tenant)).toBe('recent');
    await new Promise((r) => setTimeout(r, 50));
    expect(await tenantObject(tenant).fixturePings()).toBe(1);
  });

  it('pings nothing when the selector does not resolve', async () => {
    const { control, tenant } = await world();
    await control.fixtureTenantRow(tenant, '', 'disabled');
    expect(await control.prewarmTenant('pw-project', tenant)).toBe('unknown');
    expect(await control.prewarmTenant('pw-project', 'pinged-nowhere')).toBe('unknown');
    await new Promise((r) => setTimeout(r, 50));
    expect(await tenantObject(tenant).fixturePings()).toBe(0);
    expect(await tenantObject('pinged-nowhere').fixturePings()).toBe(0);
  });

  it('refuses a selector longer than a DNS label even when resolveTenant would accept it', async () => {
    const control = freshControl();
    const long = 'l'.repeat(64);
    await control.devCreateProject({ id: 'pw-project', name: 'prewarm fixture' });
    await control.fixtureTenant('pw-project', long);
    expect(await control.prewarmTenant('pw-project', long)).toBe('unknown');
    const ok = 'l'.repeat(63);
    await control.fixtureTenant('pw-project', ok);
    expect(await control.prewarmTenant('pw-project', ok)).toBe('warming');
  });

  it('the per-tenant throttle map is bounded', async () => {
    const control = freshControl();
    await control.devCreateProject({ id: 'pw-project', name: 'prewarm fixture' });
    await control.fixturePrewarmMax(3);
    const ids = Array.from({ length: 5 }, newTenant);
    for (const id of ids) { await control.fixtureTenant('pw-project', id); expect(await control.prewarmTenant('pw-project', id)).toBe('warming'); }
    expect(await control.fixturePrewarmedSize()).toBe(3); // the lowered bound
    expect(await control.prewarmTenant('pw-project', ids[0]!)).toBe('warming'); // evicted, so woken again
    expect(await control.prewarmTenant('pw-project', ids[4]!)).toBe('recent');
  });
});

describe('co-tenant prewarm (multi-tenant accounts)', () => {
  async function tenants(n: number) {
    const { control, tenant } = await world();
    const others: string[] = [];
    for (let i = 0; i < n; i++) { const t = newTenant(); others.push(t); await control.fixtureTenant('pw-project', t); }
    return { control, active: tenant, others };
  }
  const reply = (active: string, ids: string[]) => ({ accessToken: 'x', activeTenantId: active, tenants: [active, ...ids].map((id) => ({ id, name: id })) });

  it('a page of the active tenant also wakes the tenants a recent sign-in admitted with it', async () => {
    const { control, active, others } = await tenants(2);
    await control.fixtureNoteCoTenants(reply(active, others));
    expect(await control.prewarmTenant('pw-project', active)).toBe('warming');
    for (const other of others) expect(await control.prewarmTenant('pw-project', other)).toBe('recent'); // already woken
  });

  it('records nothing for a single-tenant sign-in or a malformed reply', async () => {
    const { control, active, others } = await tenants(1);
    await control.fixtureNoteCoTenants(reply(active, []));
    await control.fixtureNoteCoTenants({ activeTenantId: active, tenants: 'not a list' });
    await control.fixtureNoteCoTenants(null);
    expect(await control.prewarmTenant('pw-project', active)).toBe('warming');
    expect(await control.prewarmTenant('pw-project', others[0]!)).toBe('warming'); // not woken by the page of `active`
  });

  it('records at most 9 tenants of one reply (each keeps at most 8 others)', async () => {
    const { control, active, others } = await tenants(10);
    await control.fixtureNoteCoTenants(reply(active, others)); // 11 tenants: the first 9 are recorded
    expect(await control.prewarmTenant('pw-project', active)).toBe('warming');
    for (const other of others.slice(0, 8)) expect(await control.prewarmTenant('pw-project', other)).toBe('recent');
    for (const other of others.slice(8)) expect(await control.prewarmTenant('pw-project', other)).toBe('warming'); // not recorded
  });

  it("a page of the reply's non-active tenant also wakes the active one (records are symmetric)", async () => {
    const { control, active, others } = await tenants(2);
    await control.fixtureNoteCoTenants(reply(active, others)); // e.g. a sign-in without a requested tenant: active = first by name
    expect(await control.prewarmTenant('pw-project', others[1]!)).toBe('warming');
    expect(await pingsOf(active, 1)).toBe(1);
    expect(await pingsOf(others[0]!, 1)).toBe(1);
  });

  it('co-tenant wakes share the per-tenant throttle: a recently woken tenant is not pinged again', async () => {
    const { control, active, others } = await tenants(1);
    expect(await control.prewarmTenant('pw-project', others[0]!)).toBe('warming');
    expect(await pingsOf(others[0]!, 1)).toBe(1);
    await control.fixtureNoteCoTenants(reply(active, others));
    expect(await control.prewarmTenant('pw-project', active)).toBe('warming');
    expect(await pingsOf(active, 1)).toBe(1);
    await new Promise((r) => setTimeout(r, 50));
    expect(await tenantObject(others[0]!).fixturePings()).toBe(1); // the co-tenant wake was throttled, not sent
    expect(await control.prewarmTenant('pw-project', others[0]!)).toBe('recent');
  });

  it('a page wake pings each co-tenant exactly once', async () => {
    const { control, active, others } = await tenants(2);
    await control.fixtureNoteCoTenants(reply(active, others));
    expect(await control.prewarmTenant('pw-project', active)).toBe('warming');
    for (const other of others) expect(await pingsOf(other, 1)).toBe(1);
  });

  it('a co-tenant not seen for longer than the TTL is not woken', async () => {
    const { control, active, others } = await tenants(2);
    await control.fixtureNoteCoTenants(reply(active, [others[0]!]));
    await control.fixtureAdvanceClock(23 * 3_600_000);
    await control.fixtureNoteCoTenants(reply(active, [others[1]!])); // seen again later
    await control.fixtureAdvanceClock(2 * 3_600_000); // others[0]: 25 h old, others[1]: 2 h old
    expect(await control.prewarmTenant('pw-project', active)).toBe('warming');
    expect(await pingsOf(others[1]!, 1)).toBe(1);
    await new Promise((r) => setTimeout(r, 50));
    expect(await tenantObject(others[0]!).fixturePings()).toBe(0);
  });

  it('recency spans replies: an older co-tenant seen again survives the bound', async () => {
    const { control, active, others } = await tenants(9);
    await control.fixtureNoteCoTenants(reply(active, others.slice(0, 8)));
    await control.fixtureAdvanceClock(1_000);
    await control.fixtureNoteCoTenants(reply(active, [others[0]!])); // others[0] is now the most recent
    await control.fixtureAdvanceClock(1_000);
    await control.fixtureNoteCoTenants(reply(active, [others[8]!])); // a 9th: the least recently seen (others[1]) goes
    expect(await control.prewarmTenant('pw-project', active)).toBe('warming');
    expect(await control.prewarmTenant('pw-project', others[1]!)).toBe('warming'); // evicted, so not woken
    for (const other of [others[0]!, ...others.slice(2)]) expect(await control.prewarmTenant('pw-project', other)).toBe('recent');
  });

  it('writes KV on a change, not again for an unchanged set within the hour, and once after an hour', async () => {
    const { control, active, others } = await tenants(2);
    // Each reply records both directions: active -> {others}, and each other -> {active}.
    await control.fixtureNoteCoTenants(reply(active, [others[0]!]));
    expect(await control.fixtureCoTenantWrites()).toBe(2); // two new sets
    await control.fixtureNoteCoTenants(reply(active, [others[0]!]));
    await control.fixtureAdvanceClock(30 * 60_000);
    await control.fixtureNoteCoTenants(reply(active, [others[0]!]));
    expect(await control.fixtureCoTenantWrites()).toBe(2); // unchanged within the hour
    await control.fixtureAdvanceClock(31 * 60_000);
    await control.fixtureNoteCoTenants(reply(active, [others[0]!]));
    expect(await control.fixtureCoTenantWrites()).toBe(4); // an hour after the last writes
    await control.fixtureNoteCoTenants(reply(active, [others[0]!]));
    expect(await control.fixtureCoTenantWrites()).toBe(4);
    await control.fixtureNoteCoTenants(reply(active, [others[1]!]));
    expect(await control.fixtureCoTenantWrites()).toBe(6); // active's set changed; others[1]'s is new; others[0] untouched
  });

  it('an entry expiring by the TTL changes the set (written), while a stale-but-live entry alone does not force writes', async () => {
    const { control, active, others } = await tenants(2);
    await control.fixtureNoteCoTenants(reply(active, [others[0]!])); // writes 1-2 (active, others[0])
    await control.fixtureAdvanceClock(2 * 3_600_000);
    await control.fixtureNoteCoTenants(reply(active, [others[1]!])); // active's set changed ({0, 1}) and others[1] is new: 3-4
    await control.fixtureNoteCoTenants(reply(active, [others[1]!]));
    await control.fixtureNoteCoTenants(reply(active, [others[1]!]));
    expect(await control.fixtureCoTenantWrites()).toBe(4); // others[0] is 2 h old in active's set, but nothing changed since the write
    await control.fixtureAdvanceClock(23 * 3_600_000); // others[0] expires (25 h) from active's set
    await control.fixtureNoteCoTenants(reply(active, [others[1]!]));
    expect(await control.fixtureCoTenantWrites()).toBe(6); // active's set changed; others[1]'s last write is 23 h old
  });

  it('tolerates malformed stored rows and keeps only valid ones', async () => {
    const name = 'prewarm-' + crypto.randomUUID();
    const stub = () => (env as unknown as { PUB_CONTROL: DurableObjectNamespace }).PUB_CONTROL.getByName(name) as unknown as Control;
    const active = newTenant(), good = newTenant(), malformed = [null, 7, 'x', [], [1, 2], ['', 5], ['only-id'], [good, Date.now()], [newTenant(), 'nan'], { a: 1 }];
    await stub().devCreateProject({ id: 'pw-project', name: 'prewarm fixture' });
    for (const t of [active, good]) await stub().fixtureTenant('pw-project', t);
    await stub().fixturePutRawCoTenants(active, malformed);
    await stub().fixtureRestart().catch(() => undefined); // a new instance reads the row from KV
    expect(await stub().prewarmTenant('pw-project', active)).toBe('warming');
    expect(await pingsOf(good, 1)).toBe(1);
    await stub().fixturePutRawCoTenants(newTenant(), 'not even a list');
    await stub().fixtureNoteCoTenants(reply(active, [good])); // a reply after reading malformed rows still records
    await stub().fixturePutRawCoTenants(active, { broken: true });
    await stub().fixtureRestart().catch(() => undefined);
    expect(await stub().prewarmTenant('pw-project', active)).toBe('warming'); // a non-list row: no co-tenants, no error
  });

  it('survives a Control restart (KV), without the old in-memory throttle', async () => {
    const name = 'prewarm-' + crypto.randomUUID();
    const stub = () => (env as unknown as { PUB_CONTROL: DurableObjectNamespace }).PUB_CONTROL.getByName(name) as unknown as Control;
    const active = newTenant(), other = newTenant();
    await stub().devCreateProject({ id: 'pw-project', name: 'prewarm fixture' });
    for (const t of [active, other]) await stub().fixtureTenant('pw-project', t);
    await stub().fixtureNoteCoTenants(reply(active, [other]));
    await stub().fixtureRestart().catch(() => undefined);
    expect(await stub().prewarmTenant('pw-project', active)).toBe('warming'); // a new instance: no throttle state
    expect(await stub().prewarmTenant('pw-project', other)).toBe('recent');   // co-tenant read back from KV and woken
  });
});
