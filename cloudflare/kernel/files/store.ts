// `ctx.storage.store` bytes: strict base64 (the `base64` crate's STANDARD
// engine: padded, no whitespace, canonical trailing bits) and the 32 MiB cap
// of 0012 R26.
import { MAX_STORE_BYTES } from "./config";
import { hex } from "./signing";

// A flat character class: a grouped repetition recurses per group and overflows the stack on 43 MiB.
const BASE64 = /^[A-Za-z0-9+/]*={0,2}$/;

export const INVALID_BASE64 = "storage content is not valid base64";
export const storeLimitError = (limit = MAX_STORE_BYTES) => `storage content exceeds the ${limit} byte limit`;

interface Base64Decoder { fromBase64?: (text: string, options?: { lastChunkHandling?: string }) => Uint8Array }

/**
 * Decodes `contentBase64`, refusing more than `limit` decoded bytes before it
 * allocates them (a 32 MiB file is 43 MiB of base64 in a 128 MB isolate).
 */
export function decodeStoreContent(content: string, limit = MAX_STORE_BYTES): Uint8Array {
  if (content.length % 4 !== 0 || !BASE64.test(content)) throw new Error(INVALID_BASE64);
  const padding = content.endsWith("==") ? 2 : content.endsWith("=") ? 1 : 0;
  if ((content.length / 4) * 3 - padding > limit) throw new Error(storeLimitError(limit));
  const native = (Uint8Array as unknown as Base64Decoder).fromBase64;
  if (native) {
    try {
      return native(content, { lastChunkHandling: "strict" });
    } catch {
      throw new Error(INVALID_BASE64);
    }
  }
  const binary = atob(content);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index++) bytes[index] = binary.charCodeAt(index);
  // Canonical trailing bits, as the strict decoder checks.
  if (padding && btoa(binary.slice(-3 + padding)).slice(-4) !== content.slice(-4)) throw new Error(INVALID_BASE64);
  return bytes;
}

export async function sha256Hex(bytes: Uint8Array): Promise<string> {
  return hex(await crypto.subtle.digest("SHA-256", bytes));
}
