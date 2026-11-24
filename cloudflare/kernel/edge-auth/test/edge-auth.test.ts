// Workerd tests for stateless Ed25519 edge session tokens (0 DO hops, 0 JSON parsing).
import { describe, expect, it } from "vitest";
import {
  RevocationSet,
  exportPublicKeyRaw,
  generateEdgeSigningKey,
  importPublicKeyRaw,
  mintSessionToken,
  verifySessionToken,
  type EdgeVerifyKey,
} from "../index";

describe("edge-auth stateless Ed25519 session tokens", () => {
  it("mints, exports/imports public key, and verifies claims with zero DO hops", async () => {
    const signing = await generateEdgeSigningKey("k-2026-11");
    const pubRaw = await exportPublicKeyRaw(signing.publicKey);
    expect(pubRaw.byteLength).toBe(32);

    const verifyKey = await importPublicKeyRaw("k-2026-11", pubRaw);
    const keyring = new Map<string, EdgeVerifyKey>([[verifyKey.kid, verifyKey]]);

    const nowSec = 1_795_000_000;
    const token = await mintSessionToken(signing, {
      jti: "tok-001",
      projectId: "proj-whagons",
      tenantId: "tenant-hotel-100k",
      accountId: "acc-alice",
      memberId: "mem-alice",
      role: "admin",
      membershipRev: 42,
      permsLo: 0x00ff00aa,
      permsHi: 0x12345678,
      anonymous: false,
      iatSec: nowSec,
      expSec: nowSec + 900,
    });

    expect(token.startsWith("wlt1.")).toBe(true);
    expect(token.length).toBeLessThan(260);

    const res = await verifySessionToken(token, keyring, { nowSec: nowSec + 10 });
    expect(res.ok).toBe(true);
    if (!res.ok) throw new Error("expected ok");
    expect(res.claims).toEqual({
      kid: "k-2026-11",
      jti: "tok-001",
      projectId: "proj-whagons",
      tenantId: "tenant-hotel-100k",
      accountId: "acc-alice",
      memberId: "mem-alice",
      role: "admin",
      membershipRev: 42,
      permsLo: 0x00ff00aa,
      permsHi: 0x12345678,
      anonymous: false,
      iatSec: nowSec,
      expSec: nowSec + 900,
    });
  });

  it("rejects tampered bytes, expired tokens, unknown kids, and revoked JTI/membership revisions", async () => {
    const k1 = await generateEdgeSigningKey("k1");
    const k2 = await generateEdgeSigningKey("k2");
    const keyring = new Map<string, EdgeVerifyKey>([
      ["k1", { kid: "k1", publicKey: k1.publicKey }],
      ["k2", { kid: "k2", publicKey: k2.publicKey }],
    ]);

    const nowSec = 1_795_000_000;
    const token = await mintSessionToken(k1, {
      jti: "tok-rev",
      projectId: "p",
      tenantId: "t",
      accountId: "a",
      memberId: "m-bob",
      role: "member",
      membershipRev: 5,
      permsLo: 1,
      permsHi: 0,
      anonymous: false,
      iatSec: nowSec,
      expSec: nowSec + 60,
    });

    // Tamper one character in the middle of the payload
    const mid = Math.floor(token.length / 2);
    const tampered = token.slice(0, mid) + (token[mid] === "A" ? "B" : "A") + token.slice(mid + 1);
    expect(await verifySessionToken(tampered, keyring, { nowSec })).toEqual({ ok: false, reason: "bad_signature" });

    // Expired
    expect(await verifySessionToken(token, keyring, { nowSec: nowSec + 60 })).toEqual({ ok: false, reason: "expired" });

    // Unknown kid after key rotation removes k1
    const rotated = new Map<string, EdgeVerifyKey>([["k2", { kid: "k2", publicKey: k2.publicKey }]]);
    expect(await verifySessionToken(token, rotated, { nowSec })).toEqual({ ok: false, reason: "unknown_kid" });

    // Revocation by membership revision fence
    const revocations = new RevocationSet();
    revocations.fenceMember("m-bob", 6);
    expect(await verifySessionToken(token, keyring, { nowSec, revocations })).toEqual({ ok: false, reason: "revoked" });

    // Revocation by explicit JTI + pruning
    const rev2 = new RevocationSet();
    rev2.revokeToken("tok-rev", nowSec + 60);
    expect(await verifySessionToken(token, keyring, { nowSec, revocations: rev2 })).toEqual({ ok: false, reason: "revoked" });
    expect(rev2.prune(nowSec + 61)).toBe(1);
    expect(await verifySessionToken(token, keyring, { nowSec, revocations: rev2 })).toMatchObject({ ok: true });
  });
});
