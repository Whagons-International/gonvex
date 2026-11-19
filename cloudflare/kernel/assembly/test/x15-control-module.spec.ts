// X15: a gateway's auth registers its holder and reads the module in ONE Control call (`watchModule`), and the
// Control object serves module info and replica epochs from one parsed manifest per row version.
import { env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { manifest } from './catalog';
import { missingMethod } from '../gateway';

type Control = {
  devCreateProject(input: { id: string; name: string }): Promise<unknown>;
  devSaveManifest(project: string, manifest: Record<string, unknown>): Promise<unknown>;
  watchModule(project: string, binding: string, id: string): Promise<{ artifactHash: string } | null>;
  moduleInfo(project: string): Promise<{ artifactHash: string } | null>;
  moduleEpoch(project: string, database: string): Promise<string>;
  fixtureWatchers(project: string): Promise<Array<{ binding: string; name: string }>>;
};
// A fresh SessionControlObject (the stable test Worker's TestControl) per test.
const realControl = () => (env as unknown as { PUB_CONTROL: DurableObjectNamespace }).PUB_CONTROL.getByName('x15-' + crypto.randomUUID());

describe('X15 Control module reads', () => {
  it('watchModule registers the holder before answering the same module info as moduleInfo', async () => {
    const control = realControl() as unknown as Control;
    await control.devCreateProject({ id: 'p', name: 'x15 fixture' });
    await control.devSaveManifest('p', manifest);
    const info = await control.watchModule('p', 'GATEWAY', 'holder-1');
    expect(info).toEqual(await control.moduleInfo('p'));
    expect(info?.artifactHash).toBe((manifest.module as { hash: string }).hash);
    expect(await control.fixtureWatchers('p')).toContainEqual({ binding: 'GATEWAY', name: 'holder-1' });
  });

  it('a new manifest is read again: module info and the replica epoch follow it', async () => {
    const control = realControl() as unknown as Control;
    await control.devCreateProject({ id: 'p', name: 'x15 fixture' });
    await control.devSaveManifest('p', manifest);
    const before = { info: await control.moduleInfo('p'), epoch: await control.moduleEpoch('p', 'do://tenant/a') };
    expect(await control.moduleEpoch('p', 'do://tenant/a')).toBe(before.epoch);
    expect(await control.moduleEpoch('p', 'do://tenant/b')).not.toBe(before.epoch);
    const module = { ...(manifest.module as Record<string, unknown>), hash: 'x15-next-hash' };
    await control.devSaveManifest('p', { ...manifest, module });
    expect((await control.moduleInfo('p'))?.artifactHash).toBe('x15-next-hash');
    expect(await control.moduleEpoch('p', 'do://tenant/a')).not.toBe(before.epoch);
  });

  it("recognizes workerd's missing-method error only for the named method", () => {
    expect(missingMethod(new TypeError('The RPC receiver does not implement the method "watchModule".'), 'watchModule')).toBe(true);
    expect(missingMethod(new TypeError('The RPC receiver does not implement "watchModule".'), 'watchModule')).toBe(true);
    expect(missingMethod(new TypeError('The RPC receiver does not implement the method "moduleInfo".'), 'watchModule')).toBe(false);
    expect(missingMethod(new Error('control unavailable'), 'watchModule')).toBe(false);
  });
});
