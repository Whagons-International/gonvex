// Dual-mode DOD capability negotiation tests (Phase 2b):
// Ensures legacy clients/servers are byte-for-byte unaffected while opt-in clients negotiate
// `replicaPackedPages: 1` (WLP1 binary wire) and `edgeSessionToken: 1` (Ed25519 edge auth).
import { describe, expect, it } from "vitest";
import { NOTHING_NEGOTIATED, negotiate, sessionReadyFrame } from "../capabilities";
import { decodeClientFrame } from "../decode";

describe("dual-mode serverless DOD capability negotiation", () => {
  it("keeps legacy session.ready and Negotiated byte-identical when DOD flags are off", () => {
    const ready = JSON.parse(sessionReadyFrame({ runtimeVersion: "test", presence: true }));
    expect(ready.capabilities.replicaPackedPages).toBeUndefined();
    expect(ready.capabilities.edgeSessionToken).toBeUndefined();

    const decoded = decodeClientFrame(JSON.stringify({
      type: "auth",
      id: "a1",
      capabilities: { replicaSnapshotPages: 1, replicaPackedPages: 1, edgeSessionToken: 1 },
    }));
    expect(decoded.ok).toBe(true);
    if (!decoded.ok || decoded.frame.type !== "auth") throw new Error("expected auth");
    const frame = decoded.frame;
    expect(frame.capabilities?.replicaPackedPages).toBe(1);
    expect(frame.capabilities?.edgeSessionToken).toBe(1);

    // Server flags off -> negotiated omits both keys (exact legacy shape).
    const negotiated = negotiate(frame.capabilities, false);
    expect(negotiated).toEqual({ ...NOTHING_NEGOTIATED, snapshotPages: true });
    expect("packedPages" in negotiated).toBe(false);
    expect("edgeSessionToken" in negotiated).toBe(false);
  });

  it("advertises and negotiates replicaPackedPages and edgeSessionToken only when both sides opt in with value 1", () => {
    const ready = JSON.parse(sessionReadyFrame({
      runtimeVersion: "test",
      presence: true,
      packedPages: true,
      edgeSessionToken: true,
    }));
    expect(ready.capabilities.replicaPackedPages).toBe(1);
    expect(ready.capabilities.edgeSessionToken).toBe(1);

    // Both opt in with 1 -> enabled
    const enabled = negotiate(
      { replicaSnapshotPages: 1, replicaPackedPages: 1, edgeSessionToken: 1 },
      true,
      { packedPages: true, edgeSessionToken: true },
    );
    expect(enabled.packedPages).toBe(true);
    expect(enabled.edgeSessionToken).toBe(true);

    // Only value 1 counts (A.1.4 rule); 0 or 2 stay off
    const nonOne = negotiate(
      { replicaPackedPages: 2, edgeSessionToken: 0 },
      true,
      { packedPages: true, edgeSessionToken: true },
    );
    expect(nonOne.packedPages).toBeUndefined();
    expect(nonOne.edgeSessionToken).toBeUndefined();
  });
});
