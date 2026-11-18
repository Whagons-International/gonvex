// X13 fixture app: an agent Action that stands in for `assistant.rewriteText`
// (the provider round trip is a timer, `args.ms`), the create Reducer the task
// dialog sends right after it on the same socket. Synthetic; never a
// deployed-application proof.
import { ModuleRegistry } from '../../host/src/index';
import { projectModuleFromManifest, moduleArtifact } from '../../reducer/module';
import { manifest as base } from './catalog';
export const X13_PROJECT = 'x13-fixture';
const any = { kind: 'any' };
export const x13Manifest = { ...base, functions: {
 'x13.rewrite': { kind: 'action', handler: 'rewrite', args: any, result: any, actionProfile: 'agent' },
 'x13.create': { kind: 'reducer', handler: 'create', args: any, result: any },
}, module: { ...base.module, hash: 'x13-fixture-1' } };
export const x13Exports: Record<string, unknown> = {
 rewrite: async (_ctx: unknown, args: { ms: number; text: string }) => { await new Promise((resolve) => setTimeout(resolve, args.ms)); return args.text.toUpperCase(); },
 create: async (ctx: { db: { query(sql: string, params: unknown[]): Promise<unknown> } }, args: { id: string }) => {
  await ctx.db.query('INSERT INTO board (id,title,position,bumps) VALUES ($1,$1,0,0)', [args.id]); return { id: args.id };
 },
};
let registry: ModuleRegistry | undefined;
export function x13Registry(): ModuleRegistry {
 if (!registry) { registry = new ModuleRegistry(X13_PROJECT); const module = projectModuleFromManifest(X13_PROJECT, x13Manifest); registry.install(moduleArtifact(module, 1, x13Exports)); }
 return registry;
}
