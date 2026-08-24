import { afterEach, describe, expect, it, vi } from "vitest";
import { GonvexErrorReporter } from "./error-reporter";

describe("GonvexErrorReporter", () => {
  afterEach(() => vi.restoreAllMocks());

  it("registers error tracking for the configured project", () => {
    const transport = vi.fn().mockResolvedValue(undefined);

    const reporter = new GonvexErrorReporter({ transport, project: "shop", captureGlobalErrors: false });

    expect(transport).toHaveBeenCalledWith("register", expect.any(Object));

    reporter.setProject("admin");
    expect(transport).toHaveBeenCalledTimes(2);
  });

  it("accepts a public GonvexClient adapter without exposing a private sender", () => {
    let connectionListener: ((state: { isWebSocketConnected: boolean }) => void) | undefined;
    const client = {
      reportError: vi.fn().mockResolvedValue(undefined),
      connectionState: () => ({ isWebSocketConnected: false }),
      subscribeToConnectionState: (listener: (state: { isWebSocketConnected: boolean }) => void) => { connectionListener = listener; return vi.fn(); },
    };
    const reporter = new GonvexErrorReporter({ client, project:"shop", captureGlobalErrors:false });
    expect(client.reportError).toHaveBeenCalledWith("register",expect.any(Object));
    connectionListener?.({ isWebSocketConnected: true });
    expect(client.reportError.mock.calls.filter(([type]) => type === "register")).toHaveLength(2);
    expect(client.reportError).toHaveBeenCalledWith("heartbeat", {});
    reporter.close();
  });

  it("batches errors with tenant, release and device context while filtering secrets", async () => {
    const transport = vi.fn().mockResolvedValue(undefined);
    const reporter = new GonvexErrorReporter({
      transport, project: "shop", tenant: "acme", release: "1.4.2",
      account: { id: "acct-1", email: "owner@example.test" }, captureGlobalErrors: false,
    });
    reporter.captureException(new Error("checkout failed"), { password: "nope", cartId: "cart-1" });
    await reporter.flush();
    const envelopeCall = transport.mock.calls.find(([type]) => type === "envelope");
    expect(envelopeCall).toBeTruthy();
    const body = envelopeCall![1];
    expect(body.events[0]).toMatchObject({
      project: "shop", tenant: "acme", release: "1.4.2", message: "checkout failed",
      account: { id: "acct-1", email: "owner@example.test" },
      context: { password: "[Filtered]", cartId: "cart-1" },
    });
    expect(body.events[0]).not.toHaveProperty("user");
    expect(body.events[0].deviceId).toBeTruthy();
  });

  it("requeues a failed batch", async () => {
    const transport = vi.fn().mockResolvedValueOnce(undefined).mockRejectedValueOnce(new Error("offline")).mockResolvedValue(undefined);
    const reporter = new GonvexErrorReporter({ transport, project: "shop", captureGlobalErrors: false });
    reporter.captureException("boom");
    await reporter.flush();
    await reporter.flush();
    expect(transport.mock.calls.filter(([type]) => type === "envelope")).toHaveLength(2);
  });

  it("drains bounded batches without allowing an unbounded client queue", async () => {
    const transport = vi.fn().mockResolvedValue(undefined);
    const reporter = new GonvexErrorReporter({ transport, project: "shop", captureGlobalErrors: false, maxQueueSize: 25 });
    for (let index = 0; index < 40; index += 1) reporter.captureException(`boom-${index}`);

    await reporter.flush();
    await reporter.flush();

    const envelopes = transport.mock.calls.filter(([type]) => type === "envelope");
    expect(envelopes.map(([, payload]) => payload.events.length)).toEqual([20, 5]);
    reporter.close();
  });

  it("registers again and sends a heartbeat after reconnect", () => {
    const transport = vi.fn().mockResolvedValue(undefined);
    const reporter = new GonvexErrorReporter({ transport, project: "shop", captureGlobalErrors: false });

    reporter.connectionRestored();

    expect(transport.mock.calls.filter(([type]) => type === "register")).toHaveLength(2);
    expect(transport).toHaveBeenCalledWith("heartbeat", {});
    reporter.close();
  });
});
