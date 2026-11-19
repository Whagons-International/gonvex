/**
 * X02: a refused `GET /ws` names its router stage and a content-free cause class in
 * `x-gonvex-admission-reason`, and logs the same, instead of a bare 503. The dev
 * 503s of 2026-11-17 18:55:29 had no log line and no header to tell which step
 * missed its deadline.
 */
import { beforeEach, afterEach, describe, expect, it, vi } from "vitest";
import { handleWebSocketUpgrade, resetWarmCell, refusalCause, type WebSocketRouteEnv } from "../router";
// X17g: each test starts without a warm holder, so an earlier test's admission does not route this one directly.
beforeEach(() => resetWarmCell());
import { ADMISSION_REASON_HEADER } from "../placement/gateway";

const upgrade = () => new Request("https://gonvex.test/ws", { headers: { upgrade: "websocket" } });

function namespace(stub: object): DurableObjectNamespace {
  return { getByName: () => stub, idFromName: () => ({}), get: () => stub } as unknown as DurableObjectNamespace;
}

afterEach(() => { vi.restoreAllMocks(); });

describe("X02 router refusal diagnostics", () => {
  it("names the placement stage and cause when the controller refuses", async () => {
    const log = vi.spyOn(console, "error").mockImplementation(() => {});
    const env = {
      GATEWAY: namespace({}),
      GATEWAY_CONTROLLER: namespace({ acquire: () => Promise.reject(new Error("gateway allocation busy")), release: () => Promise.resolve() }),
    } as unknown as WebSocketRouteEnv;
    const response = await handleWebSocketUpgrade(upgrade(), env);
    expect(response.status).toBe(503);
    expect(await response.text()).toBe("gateway capacity unavailable");
    expect(response.headers.get(ADMISSION_REASON_HEADER)).toBe("router-placement-capacity");
    expect(log).toHaveBeenCalledWith("router: upgrade refused", expect.objectContaining({ stage: "placement", cause: "capacity" }));
  });

  it("names the gateway stage when the holder's upgrade fails, and logs no identifiers", async () => {
    const log = vi.spyOn(console, "error").mockImplementation(() => {});
    const env = { GATEWAY: namespace({ fetch: () => Promise.reject(new Error("Subrequest depth limit exceeded. secret-id-123")) }) } as unknown as WebSocketRouteEnv;
    const response = await handleWebSocketUpgrade(upgrade(), env);
    expect(response.status).toBe(503);
    expect(await response.text()).toBe("gateway admission unavailable");
    expect(response.headers.get(ADMISSION_REASON_HEADER)).toBe("router-gateway-depth");
    const logged = JSON.stringify(log.mock.calls);
    expect(logged).toContain('"stage":"gateway"');
    expect(logged).not.toContain("secret-id-123");
  });

  it("classifies the router's own deadline", () => {
    expect(refusalCause(new Error("gateway RPC deadline"))).toBe("deadline");
    expect(refusalCause(new Error("Durable Object is overloaded"))).toBe("overloaded");
    expect(refusalCause("anything else")).toBe("error");
  });
});
