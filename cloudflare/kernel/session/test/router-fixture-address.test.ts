/**
 * X03: Cloudflare's edge refuses a client-supplied CF-Connecting-IP (403 `error code: 1000`), so a
 * conformance run against a deployed Worker names its per-step client address with the fixture headers.
 * The router honours them only with the deployment's exact fixture key and never forwards them.
 */
import { describe, expect, it } from "vitest";
import { handleWebSocketUpgrade, type WebSocketRouteEnv } from "../router";
import { UPGRADE_HEADERS } from "../placement/gateway";
import { FIXTURE_ADDRESS_HEADERS } from "../../public/address";

function gatewayCapturing(seen: Headers[]): DurableObjectNamespace {
  const stub = { fetch: async (request: Request) => { seen.push(request.headers); return new Response(null, { status: 426 }); } };
  return { getByName: () => stub, idFromName: () => ({}), get: () => stub } as unknown as DurableObjectNamespace;
}
const upgrade = (key: string) => new Request("https://gonvex.test/ws?public=1", { headers: {
  upgrade: "websocket", "cf-connecting-ip": "198.51.100.23", [FIXTURE_ADDRESS_HEADERS.key]: key, [FIXTURE_ADDRESS_HEADERS.address]: "203.0.113.7" } });

describe("X03 fixture client address at the router", () => {
  it("uses the fixture address only with the exact key, and strips both fixture headers before the gateway", async () => {
    for (const [configured, sent, expected] of [["dev-fixture", "dev-fixture", "203.0.113.7"], ["dev-fixture", "guess", "198.51.100.23"], [undefined, "dev-fixture", "198.51.100.23"]] as const) {
      const seen: Headers[] = [];
      const env = { GATEWAY: gatewayCapturing(seen), ...(configured ? { GONVEX_ASSEMBLY_FIXTURE_KEY: configured } : {}) } as unknown as WebSocketRouteEnv;
      await handleWebSocketUpgrade(upgrade(sent), env);
      expect(seen).toHaveLength(1);
      expect(seen[0]!.get(UPGRADE_HEADERS.address)).toBe(expected);
      expect(seen[0]!.get(FIXTURE_ADDRESS_HEADERS.key)).toBeNull();
      expect(seen[0]!.get(FIXTURE_ADDRESS_HEADERS.address)).toBeNull();
    }
  });
});
