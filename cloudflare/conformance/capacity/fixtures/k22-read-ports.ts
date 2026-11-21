// Exact pure port helpers from471, isolated from the DO class module for LOCAL DATA projection tests.
const DEFAULT_GATEWAY_SHARDS = 8;
interface GatewayEnv { GONVEX_GATEWAY_SHARDS?: string }
export function gatewayShards(env: GatewayEnv): number {
  const n = Number(env.GONVEX_GATEWAY_SHARDS ?? DEFAULT_GATEWAY_SHARDS);
  return Number.isInteger(n) && n >= 1 && n <= 4096 ? n : DEFAULT_GATEWAY_SHARDS;
}
export function rpc<T>(namespace: DurableObjectNamespace<any>, name: string): T {
  return (namespace as unknown as { getByName(name: string): unknown }).getByName(name) as T;
}
