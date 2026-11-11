/**
 * Raw request bodies for the webhook routes (K18).
 *
 * A provider signs the exact bytes it sends, so the body must reach the Action
 * byte for byte. The Action arguments are JSON strings (`rawBody:
 * schema.string()`), and the module re-encodes them with `TextEncoder` before
 * it verifies. That round trip is the identity for every valid UTF-8 input as
 * long as decoding is strict and keeps a leading byte-order mark, which the
 * default `TextDecoder` silently drops.
 */

export class BodyTooLarge extends Error {
  constructor(readonly limit: number) {
    super(`request body exceeds ${limit} bytes`);
  }
}

/**
 * Reads the whole body, refusing it as soon as it passes `limit` bytes. A
 * `Content-Length` above the limit is refused before any byte is read.
 */
export async function readRawBody(request: Request, limit: number): Promise<Uint8Array> {
  const declared = request.headers.get("content-length");
  if (declared !== null && /^\d+$/.test(declared.trim()) && Number(declared.trim()) > limit) throw new BodyTooLarge(limit);
  if (request.body === null) return new Uint8Array(0);
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const part = await reader.read();
      if (part.done) break;
      size += part.value.byteLength;
      if (size > limit) throw new BodyTooLarge(limit);
      chunks.push(part.value);
    }
  } catch (error) {
    await reader.cancel().catch(() => undefined);
    throw error;
  }
  if (chunks.length === 1) return chunks[0]!;
  const out = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

const strict = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

/** The body as a string that `TextEncoder` turns back into the same bytes, or `null` if it is not UTF-8. */
export function exactUtf8(bytes: Uint8Array): string | null {
  try {
    return strict.decode(bytes);
  } catch {
    return null;
  }
}
