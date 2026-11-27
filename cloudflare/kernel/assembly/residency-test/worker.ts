import { DurableObject } from "cloudflare:workers";
import { idJurisdiction, resident, residentEnv, residentHandler } from "../residency";
import { finishRestore, restoreStep, snapshotObject } from "../snapshot";

export interface ProbeEnv { PROBE: DurableObjectNamespace<Probe>; PLAIN: DurableObjectNamespace<Probe>; BACKUPS: R2Bucket; GONVEX_JURISDICTION?: string }

/** A SQLite object with just enough surface to test residency, snapshots and restores. */
export class Probe extends DurableObject<ProbeEnv> {
  async info() { return { id: this.ctx.id.toString(), jurisdiction: idJurisdiction(this.ctx.id) ?? null, name: this.ctx.id.name ?? null }; }
  /** Reaches another probe through this object's own env (resident when the class is). */
  async peer(name: string) { return (this.env.PROBE.getByName(name) as unknown as Probe).info(); }
  async exec(sql: string, ...params: SqlStorageValue[]) { return this.ctx.storage.sql.exec(sql, ...params).toArray(); }
  /** Several statements in one exec (and one transaction). */
  async script(statements: string[]) { this.ctx.storage.transactionSync(() => { this.ctx.storage.sql.exec(statements.join(";\n")); }); }
  async kvPut(key: string, value: unknown) { this.ctx.storage.kv.put(key, value); }
  async kvGet(key: string) { return this.ctx.storage.kv.get(key); }
  async kvKeys() { return [...this.ctx.storage.kv.list()].map(([k]) => k); }
  async snapshot(prefix: string) { return snapshotObject(this.ctx, this.env.BACKUPS, prefix, { kind: "probe", name: this.ctx.id.name ?? "" }); }
  /** The error text instead of a thrown RPC error (workerd reports those as uncaught in tests). */
  async trySnapshot(prefix: string) { try { await this.snapshot(prefix); return null; } catch (error) { return String((error as Error).message); } }
  async restore(prefix: string) { return restoreStep(this.ctx, this.env.BACKUPS, prefix); }
  async finish() { await finishRestore(this.ctx); }
}
export const ResidentProbe = resident(Probe);
export const PlainProbe = Probe;
export default residentHandler({
  async fetch(_request: Request, env: ProbeEnv) {
    const id = env.PROBE.idFromName("handler");
    return Response.json({ jurisdiction: idJurisdiction(id) ?? null, same: residentEnv(env) === env });
  },
});
