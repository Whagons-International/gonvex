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
    const ids = Array.from({ length: 5 }, newTenant);
    for (const id of ids) { await control.fixtureTenant('pw-project', id); expect(await control.prewarmTenant('pw-project', id)).toBe('warming'); }
    expect(await control.fixturePrewarmedSize()).toBe(3); // TestControl's bound
    expect(await control.prewarmTenant('pw-project', ids[0]!)).toBe('warming'); // evicted, so woken again
    expect(await control.prewarmTenant('pw-project', ids[4]!)).toBe('recent');
  });
});
