// Download response hardening (RT/lib.rs protect_storage_response, 724-778).
// Only these media types render inline on the runtime origin; everything else
// downloads as an opaque attachment, including SVG, XML and HTML.

export const INLINE_MEDIA_TYPES: ReadonlySet<string> = new Set([
  "image/png", "image/jpeg", "image/gif", "image/webp", "image/avif",
  "audio/mpeg", "audio/ogg", "audio/wav", "audio/webm", "audio/mp4",
  "video/mp4", "video/webm", "video/ogg",
  "application/pdf",
]);

/** Rewrites `content-type` and adds disposition, nosniff and CSP, exactly as Rust does. */
export function protectStorageResponse(headers: Headers): void {
  const mime = (headers.get("content-type") ?? "").split(";")[0]!.trim().toLowerCase();
  const inline = INLINE_MEDIA_TYPES.has(mime);
  headers.set("content-type", inline ? mime : "application/octet-stream");
  headers.set("content-disposition", inline ? "inline" : "attachment");
  headers.set("x-content-type-options", "nosniff");
  headers.set("content-security-policy", inline ? "default-src 'none'; base-uri 'none'" : "sandbox; default-src 'none'; base-uri 'none'");
}

/** `storage_cors` (lib.rs:863-884): on every /storage response. */
export function storageCors(headers: Headers): void {
  headers.set("access-control-allow-origin", "*");
  headers.set("access-control-allow-methods", "GET, POST, PUT, OPTIONS");
  headers.set("access-control-allow-headers", "Content-Type");
  headers.set("access-control-max-age", "600");
}
