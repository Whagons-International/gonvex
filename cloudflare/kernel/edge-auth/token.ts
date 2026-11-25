// Stateless Ed25519 Edge Session Tokens (WLT1 binary format, Web Crypto API).
// Verifies tenant/member identity at the edge Worker with 0 Control DO hops and 0 JSON parsing.
//
// Binary layout (little-endian):
//   [0..3]   magic 'WLT1' (0x31544c57)
//   [4]      version = 1 (u8)
//   [5]      flags (u8, bit 0 = anonymous)
//   [6]      kidLen (u8)
//   [7]      reserved = 0 (u8)
//   [8..15]  iatSec (f64, unix seconds)
//   [16..23] expSec (f64, unix seconds)
//   [24..27] membershipRev (u32)
//   [28..31] permsLo (u32)
//   [32..35] permsHi (u32)
//   [36..]   kid UTF-8 bytes (kidLen)
//            6 length-prefixed UTF-8 strings (u16 byteLen + bytes):
//            jti, projectId, tenantId, accountId, memberId, role
//   [last 64B] Ed25519 signature over all preceding bytes.

const TOKEN_MAGIC = 0x31544c57; // 'WLT1'
const TOKEN_VERSION = 1;
const FIXED_HEADER = 36;
const SIG_BYTES = 64;
const TOKEN_PREFIX = "wlt1.";

const te = new TextEncoder();
const td = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });

export interface EdgeSessionClaims {
  readonly kid: string;
  readonly jti: string;
  readonly projectId: string;
  readonly tenantId: string;
  readonly accountId: string;
  readonly memberId: string;
  readonly role: string;
  readonly membershipRev: number;
  readonly permsLo: number;
  readonly permsHi: number;
  readonly anonymous: boolean;
  readonly iatSec: number;
  readonly expSec: number;
}

export interface EdgeSigningKey {
  readonly kid: string;
  readonly privateKey: CryptoKey;
  readonly publicKey: CryptoKey;
}

export interface EdgeVerifyKey {
  readonly kid: string;
  readonly publicKey: CryptoKey;
}

export type VerifyResult =
  | { readonly ok: true; readonly claims: EdgeSessionClaims }
  | { readonly ok: false; readonly reason: "malformed" | "unknown_kid" | "bad_signature" | "expired" | "revoked" };

/** Bounded in-isolate revocation filter for instant logout and membership-revision fencing. */
export class RevocationSet {
  private readonly revokedJti = new Map<string, number>(); // jti -> expSec
  private readonly minMemberRev = new Map<string, number>(); // memberId -> min allowed membershipRev

  revokeToken(jti: string, expSec: number): void {
    this.revokedJti.set(jti, expSec);
  }

  fenceMember(memberId: string, minRev: number): void {
    const cur = this.minMemberRev.get(memberId);
    if (cur === undefined || minRev > cur) this.minMemberRev.set(memberId, minRev);
  }

  isRevoked(claims: EdgeSessionClaims): boolean {
    if (this.revokedJti.has(claims.jti)) return true;
    const minRev = this.minMemberRev.get(claims.memberId);
    return minRev !== undefined && claims.membershipRev < minRev;
  }

  /** Drops expired JTI entries so memory stays bounded. */
  prune(nowSec: number): number {
    let removed = 0;
    const iter = this.revokedJti.entries();
    for (;;) {
      const next = iter.next();
      if (next.done) break;
      if (next.value[1] <= nowSec) {
        this.revokedJti.delete(next.value[0]);
        removed++;
      }
    }
    return removed;
  }
}

/** Generates a fresh Ed25519 keypair in Web Crypto for token minting and verification. */
export async function generateEdgeSigningKey(kid: string): Promise<EdgeSigningKey> {
  const pair = (await crypto.subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"])) as CryptoKeyPair;
  return { kid, privateKey: pair.privateKey, publicKey: pair.publicKey };
}

/** Exports a public key as raw 32-byte Ed25519 bytes (for distributing to edge Workers / KV / env). */
export async function exportPublicKeyRaw(publicKey: CryptoKey): Promise<Uint8Array> {
  return new Uint8Array((await crypto.subtle.exportKey("raw", publicKey)) as ArrayBuffer);
}

/** Imports a raw 32-byte Ed25519 public key for stateless edge verification. */
export async function importPublicKeyRaw(kid: string, raw: Uint8Array): Promise<EdgeVerifyKey> {
  const publicKey = await crypto.subtle.importKey("raw", raw, { name: "Ed25519" }, false, ["verify"]);
  return { kid, publicKey };
}

function toBase64Url(bytes: Uint8Array): string {
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]!);
  return btoa(bin).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

function fromBase64Url(str: string): Uint8Array | null {
  if (str.length === 0) return null;
  const padLen = (4 - (str.length & 3)) & 3;
  const padded = str.replaceAll("-", "+").replaceAll("_", "/") + "=".repeat(padLen);
  try {
    const bin = atob(padded);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  } catch {
    return null;
  }
}

/** Signs `claims` with `key` into a compact `wlt1.<base64url>` binary token. */
export async function mintSessionToken(
  key: EdgeSigningKey,
  claims: Omit<EdgeSessionClaims, "kid">,
): Promise<string> {
  const kidBytes = te.encode(key.kid);
  if (kidBytes.length === 0 || kidBytes.length > 255) throw new RangeError("kid must be 1..255 UTF-8 bytes");
  const fields = [
    te.encode(claims.jti),
    te.encode(claims.projectId),
    te.encode(claims.tenantId),
    te.encode(claims.accountId),
    te.encode(claims.memberId),
    te.encode(claims.role),
  ];

  let payloadLen = FIXED_HEADER + kidBytes.length;
  for (let i = 0; i < fields.length; i++) {
    const f = fields[i]!;
    if (f.length > 65535) throw new RangeError("claim field exceeds 65535 bytes");
    payloadLen += 2 + f.length;
  }

  const total = new Uint8Array(payloadLen + SIG_BYTES);
  const dv = new DataView(total.buffer, total.byteOffset, total.byteLength);
  dv.setUint32(0, TOKEN_MAGIC, true);
  dv.setUint8(4, TOKEN_VERSION);
  dv.setUint8(5, claims.anonymous ? 1 : 0);
  dv.setUint8(6, kidBytes.length);
  dv.setUint8(7, 0);
  dv.setFloat64(8, claims.iatSec, true);
  dv.setFloat64(16, claims.expSec, true);
  dv.setUint32(24, claims.membershipRev >>> 0, true);
  dv.setUint32(28, claims.permsLo >>> 0, true);
  dv.setUint32(32, claims.permsHi >>> 0, true);

  let off = FIXED_HEADER;
  total.set(kidBytes, off);
  off += kidBytes.length;
  for (let i = 0; i < fields.length; i++) {
    const f = fields[i]!;
    dv.setUint16(off, f.length, true);
    off += 2;
    total.set(f, off);
    off += f.length;
  }

  const payloadView = total.subarray(0, payloadLen);
  const sig = new Uint8Array(await crypto.subtle.sign({ name: "Ed25519" }, key.privateKey, payloadView));
  if (sig.length !== SIG_BYTES) throw new Error("unexpected Ed25519 signature length");
  total.set(sig, payloadLen);
  return TOKEN_PREFIX + toBase64Url(total);
}

/**
 * Verifies a `wlt1.` token completely statelessly at the edge (0 DO hops, 0 JSON parsing).
 * Accepts a `Map<kid, EdgeVerifyKey>` to support zero-downtime key rotation.
 */
export async function verifySessionToken(
  token: string,
  keys: ReadonlyMap<string, EdgeVerifyKey>,
  options: { readonly nowSec?: number; readonly revocations?: RevocationSet } = {},
): Promise<VerifyResult> {
  if (!token.startsWith(TOKEN_PREFIX)) return { ok: false, reason: "malformed" };
  const raw = fromBase64Url(token.slice(TOKEN_PREFIX.length));
  if (raw === null || raw.length < FIXED_HEADER + SIG_BYTES + 12) return { ok: false, reason: "malformed" };

  const payloadLen = raw.length - SIG_BYTES;
  const dv = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
  if (dv.getUint32(0, true) !== TOKEN_MAGIC || dv.getUint8(4) !== TOKEN_VERSION) {
    return { ok: false, reason: "malformed" };
  }
  const flags = dv.getUint8(5);
  const kidLen = dv.getUint8(6);
  if (kidLen === 0 || FIXED_HEADER + kidLen > payloadLen) return { ok: false, reason: "malformed" };

  let kid: string;
  try {
    kid = td.decode(raw.subarray(FIXED_HEADER, FIXED_HEADER + kidLen));
  } catch {
    return { ok: false, reason: "malformed" };
  }

  const key = keys.get(kid);
  if (key === undefined) return { ok: false, reason: "unknown_kid" };

  const sig = raw.subarray(payloadLen);
  const payload = raw.subarray(0, payloadLen);
  const validSig = await crypto.subtle.verify({ name: "Ed25519" }, key.publicKey, sig, payload);
  if (!validSig) return { ok: false, reason: "bad_signature" };

  const iatSec = dv.getFloat64(8, true);
  const expSec = dv.getFloat64(16, true);
  const nowSec = options.nowSec ?? Math.floor(Date.now() / 1000);
  if (!(expSec > nowSec)) return { ok: false, reason: "expired" };

  const membershipRev = dv.getUint32(24, true);
  const permsLo = dv.getUint32(28, true);
  const permsHi = dv.getUint32(32, true);

  let off = FIXED_HEADER + kidLen;
  const strs = new Array<string>(6);
  try {
    for (let i = 0; i < 6; i++) {
      if (off + 2 > payloadLen) return { ok: false, reason: "malformed" };
      const slen = dv.getUint16(off, true);
      off += 2;
      if (off + slen > payloadLen) return { ok: false, reason: "malformed" };
      strs[i] = td.decode(raw.subarray(off, off + slen));
      off += slen;
    }
  } catch {
    return { ok: false, reason: "malformed" };
  }
  if (off !== payloadLen) return { ok: false, reason: "malformed" };

  const claims: EdgeSessionClaims = {
    kid,
    jti: strs[0]!,
    projectId: strs[1]!,
    tenantId: strs[2]!,
    accountId: strs[3]!,
    memberId: strs[4]!,
    role: strs[5]!,
    membershipRev,
    permsLo,
    permsHi,
    anonymous: (flags & 1) !== 0,
    iatSec,
    expSec,
  };
  if (options.revocations !== undefined && options.revocations.isRevoked(claims)) {
    return { ok: false, reason: "revoked" };
  }
  return { ok: true, claims };
}
