/**
 * Provider secrets at rest (control.rs:4661-4738; contract C.3.3).
 *
 * AES-256-GCM, key = SHA-256("gonvex-control-secret-v1\0" || secret), AAD
 * "gonvex-auth-provider", stored as nonce(12) || ciphertext || tag(16), the
 * byte layout of the `aes-gcm` crate, so rows imported from Rust decrypt.
 * The secret comes from the `GONVEX_DASHBOARD_SESSION_SECRET` Secrets binding.
 */
import { createCipheriv, createDecipheriv, createHash, randomBytes } from "node:crypto";
import { invalid } from "./errors";

const AAD = Buffer.from("gonvex-auth-provider");

function key(secret: string): Buffer {
  return createHash("sha256").update(Buffer.concat([Buffer.from("gonvex-control-secret-v1\0"), Buffer.from(secret)])).digest();
}

export function encryptControlSecret(secret: string | undefined, plaintext: string): Uint8Array {
  if (!secret) throw invalid("GONVEX_DASHBOARD_SESSION_SECRET is required to store provider credentials");
  const nonce = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key(secret), nonce);
  cipher.setAAD(AAD);
  const body = Buffer.concat([cipher.update(Buffer.from(plaintext, "utf8")), cipher.final()]);
  return new Uint8Array(Buffer.concat([nonce, body, cipher.getAuthTag()]));
}

export function decryptControlSecret(secret: string | undefined, stored: Uint8Array): string {
  if (!secret) throw invalid("GONVEX_DASHBOARD_SESSION_SECRET is required to read provider credentials");
  if (stored.length < 13) throw invalid("stored provider credential is invalid");
  if (stored.length < 12 + 16) throw invalid("provider credential decryption failed");
  const bytes = Buffer.from(stored);
  let plain: Buffer;
  try {
    const decipher = createDecipheriv("aes-256-gcm", key(secret), bytes.subarray(0, 12));
    decipher.setAAD(AAD);
    decipher.setAuthTag(bytes.subarray(bytes.length - 16));
    plain = Buffer.concat([decipher.update(bytes.subarray(12, bytes.length - 16)), decipher.final()]);
  } catch {
    throw invalid("provider credential decryption failed");
  }
  try {
    return new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(plain);
  } catch {
    throw invalid("stored provider credential is invalid");
  }
}
