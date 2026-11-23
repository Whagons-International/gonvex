// Tenant prewarm (kernel/session/prewarm.ts → SessionControlObject.prewarmTenant): the page's selector resolves as
// resolveTenant does, only for an active provisioned tenant of that project, and each tenant is woken at most once per
// interval per Control instance.
import { env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';

type Control = {
  devCreateProject(input: { id: string; name: string }): Promise<unknown>;
  fixtureTenant(project: string, tenant: string): Promise<void>;
  fixtureTenantRow(tenant: string, domain: string, status?: string): Promise<void>;
  prewarmTenant(project: string, selector: string): Promise<'warming' | 'recent' | 'unknown'>;
};
const freshControl = () => (env as unknown as { PUB_CONTROL: DurableObjectNamespace }).PUB_CONTROL.getByName('prewarm-' + crypto.randomUUID()) as unknown as Control;

async function world() {
  const control = freshControl();
  const tenant = 'pw-' + crypto.randomUUID().slice(0, 8);
  await control.devCreateProject({ id: 'pw-project', name: 'prewarm fixture' });
  await control.fixtureTenant('pw-project', tenant);
  return { control, tenant };
}

describe('SessionControlObject.prewarmTenant', () => {
  it('wakes a tenant named by id, then not again within the interval', async () => {
    const { control, tenant } = await world();
    expect(await control.prewarmTenant('pw-project', tenant)).toBe('warming');
    expect(await control.prewarmTenant('pw-project', tenant)).toBe('recent');
  });

  it('resolves a domain selector, case-insensitively, to the same tenant', async () => {
    const { control, tenant } = await world();
    await control.fixtureTenantRow(tenant, 'El-Rey2');
    expect(await control.prewarmTenant('pw-project', 'el-rey2')).toBe('warming');
    expect(await control.prewarmTenant('pw-project', tenant)).toBe('recent'); // same object: one wake
  });

  it('does nothing for unknown selectors, another project, disabled or deleted tenants, or oversized input', async () => {
    const { control, tenant } = await world();
    expect(await control.prewarmTenant('pw-project', 'nobody')).toBe('unknown');
    expect(await control.prewarmTenant('other-project', tenant)).toBe('unknown');
    expect(await control.prewarmTenant('pw-project', 'x'.repeat(64))).toBe('unknown');
    expect(await control.prewarmTenant('', tenant)).toBe('unknown');
    await control.fixtureTenantRow(tenant, '', 'disabled');
    expect(await control.prewarmTenant('pw-project', tenant)).toBe('unknown');
    await control.fixtureTenantRow(tenant, '', 'deleted');
    expect(await control.prewarmTenant('pw-project', tenant)).toBe('unknown');
  });
});
