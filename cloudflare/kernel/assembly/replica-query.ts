// K10's collection scanner, also used by one-shot and delegated Action reads.
import { parseDefinition, type ReplicaModule } from "../delivery/definition";
import { scanPlan, scanWindow } from "../delivery/scan";
import { resolveCollection } from "../delivery/subscription";
import { readClock } from "../query/execute";
import { snapshotRunner } from "../query/live";
import type { QueryFunction, CallOutcome } from "../query/index";
import type { TenantSession } from "../session/ports";
import type { TenantStorage } from "../storage";
import type { VisibilityRegistry } from "../visibility/tenant";
import type { JsonValue } from "../visibility/json";

export async function collectionQuery(storage: TenantStorage, registry: VisibilityRegistry, module: ReplicaModule, session: TenantSession, fn: QueryFunction, args: unknown): Promise<CallOutcome> {
  try {
    const definition = parseDefinition(fn.replica);
    if (!definition) throw new Error("invalid replica collection definition");
    const visibility = module.visibility.get(definition.table);
    if (!visibility) throw new Error(`visibility plan required for replica table ${JSON.stringify(definition.table)}`);
    const read = snapshotRunner(storage);
    const plan = await read((db) => {
      const revision = readClock(db).revision;
      const resolved = resolveCollection(db, { session, module, registry }, definition, visibility, revision);
      return scanPlan(definition, visibility, resolved, args as JsonValue, db.catalogOf(definition.table).columns);
    });
    const rows: string[] = [];
    await scanWindow(read, definition, plan, (row) => { rows.push(row.text); }, { chunkBytes: 2 << 20, between: () => new Promise((resolve) => setTimeout(resolve, 0)) });
    return { ok: true, text: `[${rows.join(",")}]` };
  } catch (error) { return { ok: false, error: `module database capability failed: ${(error as Error).message}` }; }
}
