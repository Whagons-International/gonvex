import { afterEach, expect, it, vi } from 'vitest';
import { createScratchFilesystem } from './scratch-filesystem.js';

vi.mock('@electric-sql/pglite/opfs-ahp', () => ({ OpfsAhpFS: class { constructor(readonly dataDir: string) {} } }));
afterEach(() => vi.unstubAllGlobals());

it('removes only abandoned scratch directories and leaves a live tab locked', async () => {
  const abandoned = '00000000-0000-4000-8000-000000000001';
  const live = '00000000-0000-4000-8000-000000000002';
  const removed: string[] = [];
  const root = {
    async *keys() { yield abandoned; yield live; yield 'unrecognized'; },
    removeEntry: async (name: string) => { removed.push(name); },
  };
  const held = new Set([`gonvex-reducer-scratch-v1/${live}`]);
  vi.stubGlobal('navigator', {
    storage: { getDirectory: async () => ({ getDirectoryHandle: async () => root }) },
    locks: { request: (name: string, options: any, callback?: any) => {
      const run = callback ?? options;
      if (held.has(name)) return run(null);
      held.add(name);
      return Promise.resolve(run({ name })).finally(() => held.delete(name));
    } },
  });
  const first = await createScratchFilesystem();
  const second = await createScratchFilesystem();
  expect(first).toBeDefined();
  expect(second?.dataDir).not.toBe(first?.dataDir);
  expect(removed).toEqual([abandoned, abandoned]);
  expect(held.has(`gonvex-reducer-scratch-v1/${live}`)).toBe(true);
  expect(held.has(first!.dataDir)).toBe(true);
  expect(held.has(second!.dataDir)).toBe(true);
});

it('falls back when browser storage is unavailable', async () => {
  vi.stubGlobal('navigator', { storage: { getDirectory: async () => { throw new Error('not allowed'); } }, locks: {} });
  expect(await createScratchFilesystem()).toBeUndefined();
  vi.stubGlobal('navigator', {});
  expect(await createScratchFilesystem()).toBeUndefined();
});
