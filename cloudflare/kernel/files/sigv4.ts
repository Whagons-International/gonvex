// SigV4 query presigning for the R2 S3 API (RT/storage.rs presign), used for
// direct uploads that cannot go through the Worker proxy (bodies over the
// platform's request-body limit; S4, 0016 G0-8).
//
// Same algorithm as Rust: path-style URL, query parameters sorted and
// aws-encoded, `UNSIGNED-PAYLOAD`, `X-Amz-Expires` clamped to 1..604,800 s.
// Rust signs only `host`; callers here may sign more headers (for example
// `if-none-match` and `content-length`), which the client must then send.
import { awsEncode, encodeKey } from "./keys";
import { hex, hmacSha256 } from "./signing";

export interface S3Credentials {
  /** `https://<account>.r2.cloudflarestorage.com` for R2. */
  endpoint: string;
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
}

const encoder = new TextEncoder();

async function sha256Hex(value: string): Promise<string> {
  return hex(await crypto.subtle.digest("SHA-256", encoder.encode(value)));
}

function amzDate(now: Date): { stamp: string; date: string } {
  const stamp = now.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}/, "");
  return { stamp, date: stamp.slice(0, 8) };
}

/** Path-style object URL: `{endpoint}/{bucket}/{encoded key}` (storage.rs object_url, force_path_style). */
export function objectUrl(credentials: S3Credentials, key: string): URL {
  const url = new URL(credentials.endpoint.replace(/\/+$/, ""));
  url.pathname = `/${awsEncode(credentials.bucket)}/${encodeKey(key)}`;
  return url;
}

async function signingKey(secret: string, date: string, region: string): Promise<Uint8Array> {
  const dateKey = await hmacSha256(`AWS4${secret}`, date);
  const regionKey = await hmacSha256(dateKey, region);
  const serviceKey = await hmacSha256(regionKey, "s3");
  return hmacSha256(serviceKey, "aws4_request");
}

/**
 * A presigned URL for `method` on `url`. `headers` are extra signed headers
 * (lower-case names); `host` is always signed.
 */
export async function presignUrl(
  credentials: Pick<S3Credentials, "region" | "accessKeyId" | "secretAccessKey">,
  method: string,
  url: URL,
  seconds: number,
  now: Date,
  headers: Record<string, string> = {},
): Promise<string> {
  const { stamp, date } = amzDate(now);
  const host = url.host;
  const scope = `${date}/${credentials.region}/s3/aws4_request`;
  const signed: [string, string][] = [["host", host], ...Object.entries(headers).map(([name, value]) => [name.toLowerCase(), value.trim()] as [string, string])];
  signed.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  const signedHeaders = signed.map(([name]) => name).join(";");
  const query: [string, string][] = [
    ["X-Amz-Algorithm", "AWS4-HMAC-SHA256"],
    ["X-Amz-Credential", `${credentials.accessKeyId}/${scope}`],
    ["X-Amz-Date", stamp],
    ["X-Amz-Expires", String(Math.min(Math.max(Math.trunc(seconds), 1), 604_800))],
    ["X-Amz-SignedHeaders", signedHeaders],
  ];
  query.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  const canonicalQuery = query.map(([k, v]) => `${awsEncode(k)}=${awsEncode(v)}`).join("&");
  const canonicalHeaders = signed.map(([name, value]) => `${name}:${value}\n`).join("");
  const canonicalRequest = `${method}\n${url.pathname}\n${canonicalQuery}\n${canonicalHeaders}\n${signedHeaders}\nUNSIGNED-PAYLOAD`;
  const stringToSign = `AWS4-HMAC-SHA256\n${stamp}\n${scope}\n${await sha256Hex(canonicalRequest)}`;
  const signature = hex(await hmacSha256(await signingKey(credentials.secretAccessKey, date, credentials.region), stringToSign));
  // Rust re-sorts with the signature in (BTreeMap): it lands before X-Amz-SignedHeaders.
  query.push(["X-Amz-Signature", signature]);
  query.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  const out = new URL(url.toString());
  out.search = query.map(([k, v]) => `${awsEncode(k)}=${awsEncode(v)}`).join("&");
  return out.toString();
}
