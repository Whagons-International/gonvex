// Proxy URL signatures (RT/storage.rs proxy_url, proxy_signature, verify_proxy).
//
// Download and upload signatures are byte-identical to Rust:
//   hex(HMAC-SHA256("gonvex-storage:"        + secret, "{key}\n{exp}"))   download
//   hex(HMAC-SHA256("gonvex-storage-upload:" + secret, "{key}\n{exp}"))   upload (&upload=1)
//
// IB1 / 0012 R13 adds one scope: a download URL for a private file is minted
// only after the owner check, and is signed with the owner bound in:
//   hex(HMAC-SHA256("gonvex-storage-private:" + secret, "{key}\n{exp}\n{ownerId}"))
// The URL shape is unchanged. The proxy looks the file row up and verifies
// the scope the row calls for, so a plain download signature (minted by a
// path that did not check the owner) never opens a private file.
import { encodeKey } from "./keys";

export type SignatureScope =
  | { kind: "download" }
  | { kind: "upload" }
  | { kind: "private"; ownerId: string };

const encoder = new TextEncoder();
const keyCache = new Map<string, Promise<CryptoKey>>();

function hmacKey(material: string): Promise<CryptoKey> {
  let key = keyCache.get(material);
  if (!key) {
    key = crypto.subtle.importKey("raw", encoder.encode(material), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
    if (keyCache.size > 16) keyCache.clear();
    keyCache.set(material, key);
  }
  return key;
}

export function hex(bytes: ArrayBuffer | Uint8Array): string {
  return Array.from(bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes), (b) => b.toString(16).padStart(2, "0")).join("");
}

export async function hmacSha256(key: Uint8Array | string, message: string): Promise<Uint8Array> {
  const material = typeof key === "string" ? encoder.encode(key) : key;
  const imported = await crypto.subtle.importKey("raw", material, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return new Uint8Array(await crypto.subtle.sign("HMAC", imported, encoder.encode(message)));
}

export async function proxySignature(secret: string, key: string, expires: number, scope: SignatureScope): Promise<string> {
  const domain = scope.kind === "upload" ? "gonvex-storage-upload:" : scope.kind === "private" ? "gonvex-storage-private:" : "gonvex-storage:";
  const message = scope.kind === "private" ? `${key}\n${expires}\n${scope.ownerId}` : `${key}\n${expires}`;
  const signature = await crypto.subtle.sign("HMAC", await hmacKey(domain + secret), encoder.encode(message));
  return hex(signature);
}

/** `{publicUrl}/storage/{encoded key}?exp={unix s}&sig={hex}[&upload=1]` (storage.rs:883-892). */
export async function proxyUrl(publicUrl: string, secret: string, key: string, expires: number, scope: SignatureScope): Promise<string> {
  const signature = await proxySignature(secret, key, expires, scope);
  return `${publicUrl.replace(/\/+$/, "")}/storage/${encodeKey(key)}?exp=${expires}&sig=${signature}${scope.kind === "upload" ? "&upload=1" : ""}`;
}

/** Constant-time comparison of two ASCII strings (storage.rs constant_time: lengths differ → false). */
export function constantTimeEqual(left: string, right: string): boolean {
  if (left.length !== right.length) return false;
  let difference = 0;
  for (let index = 0; index < left.length; index++) difference |= left.charCodeAt(index) ^ right.charCodeAt(index);
  return difference === 0;
}

/** `verify_proxy`: unexpired, non-empty, constant-time equal. */
export async function verifyProxy(secret: string, key: string, expires: number, signature: string, scope: SignatureScope, nowSeconds: number): Promise<boolean> {
  if (expires <= nowSeconds || signature.length === 0) return false;
  return constantTimeEqual(await proxySignature(secret, key, expires, scope), signature);
}
