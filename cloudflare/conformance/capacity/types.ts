// Compile against the real public SDK; no proposed admission/transport method or fabricated receipt type.
import type { GonvexClient, FunctionReference } from '../../../packages/client/src/index.js';
export function publicCalls(client: GonvexClient, ref: FunctionReference, args: Record<string, string>) {
  const replica = client.watchReplica(ref, args);
  return {
    close: () => client.close(), reconnect: () => client.connect(),
    observe: () => replica.localReplicaResult(), state: () => replica.localReplicaState(),
    live: () => client.watchLiveQuery(ref, args).localLiveQueryResult(),
    query: () => client.query(ref, args, { timeoutMs: 1000 }), action: () => client.action(ref, args, { timeoutMs: 1000 }),
    evidence: () => client.reducerWithTransactionEvidence(ref, args, { offline: 'reject', optimistic: [], timeoutMs: 1000, execution: 'server' }),
    admission: () => client.tenantAdmission(), artifact: () => client.activeArtifactHash(),
  };
}
