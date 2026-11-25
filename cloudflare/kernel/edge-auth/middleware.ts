// Stateless Edge Worker authentication verifier (Phase 2c).
// Verifies `wlt1.` Ed25519 tokens directly at the Edge Router / Gateway / Checkpoint Worker
// with 0 Control DO hops and graceful fallthrough for legacy opaque session tokens.

import {
  RevocationSet,
  importPublicKeyRaw,
  verifySessionToken,
  type EdgeSessionClaims,
  type EdgeVerifyKey,
} from "./token";

export type EdgeRequestAuth =
  | { readonly kind: "edge"; readonly claims: EdgeSessionClaims }
  | { readonly kind: "legacy"; readonly token: string | null }
  | { readonly kind: "rejected"; readonly status: 401; readonly reason: string };

/** Extracts a bearer token from `Authorization: Bearer <tok>` or `?edgeToken=<tok>`. */
export function extractRequestToken(request: Request): string | null {
  const auth = request.headers.get("authorization");
  if (auth !== null && auth.length > 7) {
    const prefix = auth.slice(0, 7).toLowerCase();
    if (prefix === "bearer ") {
      const tok = auth.slice(7).trim();
      if (tok.length > 0) return tok;
    }
  }
  const url = request.url;
  const qIdx = url.indexOf("?");
  if (qIdx >= 0) {
    const params = new URLSearchParams(url.slice(qIdx + 1));
    const qTok = params.get("edgeToken");
    if (qTok !== null && qTok.length > 0) return qTok;
  }
  return null;
}

/**
 * In-isolate Ed25519 keyring + revocation filter for Edge Workers.
 * Supports zero-downtime key rotation and O(1) stateless request verification.
 */
export class EdgeAuthVerifier {
  private readonly keys = new Map<string, EdgeVerifyKey>();
  readonly revocations = new RevocationSet();

  addKey(key: EdgeVerifyKey): void {
    this.keys.set(key.kid, key);
  }

  async addRawPublicKey(kid: string, raw32: Uint8Array): Promise<void> {
    const imported = await importPublicKeyRaw(kid, raw32);
    this.keys.set(kid, imported);
  }

  removeKey(kid: string): boolean {
    return this.keys.delete(kid);
  }

  get keyCount(): number {
    return this.keys.size;
  }

  /**
   * Verifies a request at the edge:
   * - If the token starts with `wlt1.`, verifies Ed25519 signature + expiry + revocations (0 DO hops).
   * - If absent or an opaque legacy token, returns `{ kind: "legacy" }` so callers fall back to Control DO.
   */
  async verifyRequest(request: Request, nowSec?: number): Promise<EdgeRequestAuth> {
    const token = extractRequestToken(request);
    if (token === null || !token.startsWith("wlt1.")) {
      return { kind: "legacy", token };
    }
    const opts = nowSec === undefined
      ? { revocations: this.revocations }
      : { nowSec, revocations: this.revocations };
    const result = await verifySessionToken(token, this.keys, opts);
    if (!result.ok) {
      return { kind: "rejected", status: 401, reason: result.reason };
    }
    return { kind: "edge", claims: result.claims };
  }
}
