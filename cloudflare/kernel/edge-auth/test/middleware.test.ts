// Workerd tests for stateless EdgeAuthVerifier request middleware (0 DO hops + legacy fallthrough).
import { describe, expect, it } from "vitest";
import {
  EdgeAuthVerifier,
  exportPublicKeyRaw,
  generateEdgeSigningKey,
  mintSessionToken,
} from "../index";

describe("EdgeAuthVerifier request middleware", () => {
  it("verifies wlt1 bearer and query tokens statelessly, falls through on legacy tokens, and enforces revocation", async () => {
    const key = await generateEdgeSigningKey("edge-k1");
    const verifier = new EdgeAuthVerifier();
    await verifier.addRawPublicKey("edge-k1", await exportPublicKeyRaw(key.publicKey));
    expect(verifier.keyCount).toBe(1);

    const nowSec = 1_795_100_000;
    const token = await mintSessionToken(key, {
      jti: "jti-42",
      projectId: "proj-1",
      tenantId: "tenant-12",
      accountId: "acc-1",
      memberId: "mem-1",
      role: "admin",
      membershipRev: 3,
      permsLo: 7,
      permsHi: 0,
      anonymous: false,
      iatSec: nowSec,
      expSec: nowSec + 300,
    });

    // 1. Authorization: Bearer wlt1.<tok>
    const bearerRes = await verifier.verifyRequest(
      new Request("https://api.whagons.test/sync/checkpoint", { headers: { authorization: `Bearer ${token}` } }),
      nowSec + 5,
    );
    expect(bearerRes.kind).toBe("edge");
    if (bearerRes.kind === "edge") {
      expect(bearerRes.claims.tenantId).toBe("tenant-12");
      expect(bearerRes.claims.memberId).toBe("mem-1");
    }

    // 2. ?edgeToken=wlt1.<tok> (for WebSocket upgrades)
    const wsRes = await verifier.verifyRequest(
      new Request(`https://api.whagons.test/ws?edgeToken=${encodeURIComponent(token)}`),
      nowSec + 5,
    );
    expect(wsRes.kind).toBe("edge");

    // 3. Legacy opaque token falls through cleanly to Control DO path
    const legacyRes = await verifier.verifyRequest(
      new Request("https://api.whagons.test/ws", { headers: { authorization: "Bearer opaque-legacy-session-id" } }),
      nowSec + 5,
    );
    expect(legacyRes).toEqual({ kind: "legacy", token: "opaque-legacy-session-id" });

    // 4. Missing token falls through cleanly
    const anonRes = await verifier.verifyRequest(new Request("https://api.whagons.test/ws"), nowSec + 5);
    expect(anonRes).toEqual({ kind: "legacy", token: null });

    // 5. Revoked membership revision rejects 401 at edge
    verifier.revocations.fenceMember("mem-1", 4);
    const revokedRes = await verifier.verifyRequest(
      new Request("https://api.whagons.test/sync/checkpoint", { headers: { authorization: `Bearer ${token}` } }),
      nowSec + 5,
    );
    expect(revokedRes).toEqual({ kind: "rejected", status: 401, reason: "revoked" });

    // 6. Removing key rejects with unknown_kid
    expect(verifier.removeKey("edge-k1")).toBe(true);
    const rotatedRes = await verifier.verifyRequest(
      new Request("https://api.whagons.test/sync/checkpoint", { headers: { authorization: `Bearer ${token}` } }),
      nowSec + 5,
    );
    expect(rotatedRes).toEqual({ kind: "rejected", status: 401, reason: "unknown_kid" });
  });
});
