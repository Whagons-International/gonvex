import { env, SELF } from "cloudflare:test";
import type { IngressObject } from "../object.ts";
import type { Recorder, RecordedCall, TestControlObject } from "./worker.ts";

export interface TestEnv {
  CONTROL: DurableObjectNamespace<TestControlObject>;
  INGRESS: DurableObjectNamespace<IngressObject>;
  RECORDER: DurableObjectNamespace<Recorder>;
  WHAGONS_BUILT: string;
}
export const testEnv = env as unknown as TestEnv;

export const control = () => testEnv.CONTROL.getByName("control");
export const recorder = (project = "proj-fixture") => testEnv.RECORDER.getByName(project);

export async function calls(project = "proj-fixture"): Promise<RecordedCall[]> {
  return (await recorder(project).list()) as RecordedCall[];
}

const seeded = new Set<string>();

/** A project with active tenants (`domain` set), created once per test file run. */
export async function seed(projectId: string, tenants: { id: string; domain?: string; status?: string }[]): Promise<void> {
  if (seeded.has(projectId)) return;
  seeded.add(projectId);
  await control().devCreateProject({ name: projectId, id: projectId });
  for (const tenant of tenants) {
    await control().devCreateTenant(projectId, tenant.id, tenant.id);
    await control().testTenant(projectId, tenant.id, { ...(tenant.domain ? { domain: tenant.domain } : {}), ...(tenant.status ? { status: tenant.status } : {}) });
  }
}

export interface Reply {
  status: number;
  text: string;
  body: unknown;
  headers: Headers;
}

export async function send(path: string, init: RequestInit = {}): Promise<Reply> {
  const response = await SELF.fetch(`https://ingress.test${path}`, init);
  const text = await response.text();
  let body: unknown;
  try {
    body = JSON.parse(text);
  } catch {
    body = undefined;
  }
  return { status: response.status, text, body, headers: response.headers };
}

const encoder = new TextEncoder();
export const bytesOf = (value: string | Uint8Array): Uint8Array => (typeof value === "string" ? encoder.encode(value) : value);

export const base64 = (bytes: Uint8Array | ArrayBuffer): string => btoa(String.fromCharCode(...new Uint8Array(bytes)));

export async function sha256Hex(value: string | Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", bytesOf(value));
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

/** An Ed25519 key pair from a fixed 32-byte seed (PKCS#8 prefix of RFC 8410). */
export async function ed25519(seedByte: number): Promise<{ privateKey: CryptoKey; publicRaw: string }> {
  const pkcs8 = new Uint8Array([0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20, ...new Array(32).fill(seedByte)]);
  const privateKey = await crypto.subtle.importKey("pkcs8", pkcs8, { name: "Ed25519" }, true, ["sign"]);
  const jwk = (await crypto.subtle.exportKey("jwk", privateKey)) as JsonWebKey;
  const x = (jwk.x ?? "").replace(/-/g, "+").replace(/_/g, "/");
  return { privateKey, publicRaw: x + "=".repeat((4 - (x.length % 4)) % 4) };
}

/** Telnyx's signature: Ed25519 over `${timestamp}|` followed by the body bytes. */
export async function telnyxSignature(privateKey: CryptoKey, timestamp: string, body: string | Uint8Array): Promise<string> {
  const head = encoder.encode(`${timestamp}|`);
  const raw = bytesOf(body);
  const message = new Uint8Array(head.length + raw.length);
  message.set(head);
  message.set(raw, head.length);
  return base64(await crypto.subtle.sign("Ed25519", privateKey, message));
}

/** GitHub's `X-Hub-Signature-256`. */
export async function githubSignature(secret: string, body: string | Uint8Array): Promise<string> {
  const key = await crypto.subtle.importKey("raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const mac = new Uint8Array(await crypto.subtle.sign("HMAC", key, bytesOf(body)));
  return `sha256=${Array.from(mac, (b) => b.toString(16).padStart(2, "0")).join("")}`;
}

export const nowSeconds = () => String(Math.floor(Date.now() / 1000));

export function telnyxRequest(body: string | Uint8Array, headers: Record<string, string>): RequestInit {
  return { method: "POST", headers: { "content-type": "application/json", ...headers }, body: bytesOf(body) };
}
