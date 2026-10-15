import { GonvexClient, GonvexClientError, type ReplicaRow } from "../index.js";

// Engine-only lifecycle and cold reads stay outside the default entry. These
// helpers use the same client lanes and replica fences as the legacy host.
export function engineScope(client: GonvexClient): string { return client["outboxScope"]; }
export async function readReplicaEntities<T extends ReplicaRow = ReplicaRow>(client: GonvexClient, entity: string, ids: readonly string[]): Promise<Array<T | undefined>> {
  const generation = client["outboxScopeGeneration"];
  const ready = await client["localSessionReady"];
  if (!ready) throw new GonvexClientError("No authenticated local session is available for a cold row read", { code: "disconnected" });
  if (generation !== client["outboxScopeGeneration"]) throw new GonvexClientError("Session changed during cold row read", { code: "superseded" });
  const scope = client["replicaScope"];
  const hydration = client["replicaReady"];
  await hydration;
  if (generation !== client["outboxScopeGeneration"] || scope !== client["replicaScope"] || hydration !== client["replicaReady"]) throw new GonvexClientError("Session changed during cold row read", { code: "superseded" });
  const release = client["replica"].retainRows(entity, ids);
  try {
    await client["replica"]["application"];
    if (generation !== client["outboxScopeGeneration"] || scope !== client["replicaScope"]) throw new GonvexClientError("Session changed during cold row read", { code: "superseded" });
    return client["replica"].entityBatch<T>(entity, ids);
  } finally { release(); }
}
export async function closeEngine(client: GonvexClient): Promise<void> {
  await client["localLane"];
  client["manuallyClosed"] = true;
  client["socket"]?.close();
  client["rejectPendingCalls"]((call: { kind: "query" | "action" | "reducer"; path: string }) => new GonvexClientError(`Engine stopped while waiting for ${call.kind} ${call.path}`, { code: "closed", path: call.path, operation: call.kind }));
  client["outboxProgressWake"]?.();
  while (client["drainingOutbox"]) await new Promise(resolve => setTimeout(resolve, 10));
  await client["localLane"];
  await client["preparationLane"];
  await client["localSessionRestore"].catch(() => undefined);
  await client["replicaFrames"];
  await client["replica"]["application"];
  await client["replica"]["persistence"];
  client["close"]();
}
