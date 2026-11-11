// K13 files in workerd: a real SQLite-backed tenant object, the /storage
// router and a local R2 bucket.
import { env, runInDurableObject, SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { type FilesSession, OWNER_REFUSED } from "../access";
import { filesConfigFromEnv, MAX_STORE_BYTES } from "../config";
import { encodeKey, objectKey, safeSegment } from "../keys";
import { ActionFiles, type FilesTenantRpc } from "../rpc";
import { rfc3339 } from "../schema";
import { proxySignature, proxyUrl } from "../signing";
import { presignUrl } from "../sigv4";
import { decodeStoreContent } from "../store";
import type { TestFilesTenant } from "./worker";

const config = filesConfigFromEnv(env as unknown as Record<string, unknown>)!;
let tenants = 0;

interface Upload { fileId: string; url: string; method: string; headers: Record<string, string> }

function person(name: string, tenantId: string, extra: Partial<FilesSession> = {}): FilesSession {
  return { projectId: "proj", tenantId, accountId: `acct-${name}`, memberId: `mem-${name}`, ...extra };
}

/** A fresh tenant object and its people. */
function tenant() {
  const id = `t${++tenants}`;
  const stub = env.TENANT.getByName(id);
  const rpc = stub as unknown as FilesTenantRpc;
  const actions = new ActionFiles({ tenant: rpc, bucket: env.FILES, config });
  const as = (session: FilesSession) => ({
    call: (operation: string, payload?: unknown) => actions.call(session, operation, payload),
    fails: async (operation: string, payload?: unknown) => {
      try {
        await actions.call(session, operation, payload);
      } catch (error) {
        return String((error as Error).message);
      }
      throw new Error(`${operation} did not fail`);
    },
    readFile: (fileId: string, max = 1 << 20) => actions.readFile(session, fileId, max),
  });
  const within = <T>(run: (instance: TestFilesTenant, state: DurableObjectState) => T | Promise<T>) => runInDurableObject(stub, run);
  const rows = (sql: string, ...binds: SqlStorageValue[]) => within((_i, state) => state.storage.sql.exec(sql, ...binds).toArray());
  /** Reducer `ctx.storage.getMetadata` through K04's host, as `who`. */
  const reducerMetadata = (who: FilesSession, fileId: string) => within((instance) => instance.host.run({
    function: "files.metadata", kind: "reducer", args: { id: fileId }, now: 1792324800123,
    identity: {
      account: { id: who.accountId, email: "", name: "" },
      tenant: { id: who.tenantId, projectId: who.projectId, name: "" },
      member: { id: who.memberId, accountId: who.accountId, status: "active", role: "member", displayName: "", permissions: {} },
    },
    invocation: { channel: "ui", rootChannel: "ui", commandId: `cmd-${Math.random()}`, rootCommandId: "c", actorAccountId: who.accountId, actorMemberId: who.memberId },
  }));
  return { id, stub, rpc, as, rows, within, reducerMetadata, alice: person("alice", id), bob: person("bob", id) };
}

async function post(url: string, body: BodyInit, contentType = "application/octet-stream", method = "POST") {
  return SELF.fetch(url, { method, body, headers: { "content-type": contentType } });
}

const bytes = (text: string) => new TextEncoder().encode(text);
const PNG = bytes("\x89PNG-0123456789");

async function uploaded(t: ReturnType<typeof tenant>, who: FilesSession, options: Record<string, unknown> = {}, body: Uint8Array = PNG, contentType = "image/png") {
  const target = await t.as(who).call("generateUploadUrl", { contentType, ...options }) as Upload;
  const response = await post(target.url, body, contentType);
  expect(response.status).toBe(200);
  expect(await response.json()).toEqual({ storageId: target.fileId });
  return target.fileId;
}

describe("signed proxy uploads", () => {
  it("mints a ticketed POST URL, accepts it once and answers Rust's statuses and bodies", async () => {
    const t = tenant();
    const target = await t.as(t.alice).call("generateUploadUrl", { contentType: "image/png", visibility: "tenant" }) as Upload;
    expect(target.method).toBe("POST");
    expect(target.headers).toEqual({ "Content-Type": "image/png" });
    expect(target.url).toMatch(new RegExp(`^https://gonvex\\.test/storage/proj/${t.id}/${target.fileId}\\?exp=\\d+&sig=[0-9a-f]{64}&upload=1$`));
    expect(target.fileId).toMatch(/^[0-9a-f]{32}$/);

    const first = await post(target.url, PNG, "image/png");
    expect(first.status).toBe(200);
    expect(first.headers.get("access-control-allow-origin")).toBe("*");
    expect(first.headers.get("access-control-allow-methods")).toBe("GET, POST, PUT, OPTIONS");
    expect(await first.json()).toEqual({ storageId: target.fileId });

    const second = await post(target.url, bytes("overwrite"), "image/png", "PUT");
    expect(second.status).toBe(409);
    expect(await second.json()).toEqual({ error: "upload already consumed" });
    const object = await env.FILES.get(objectKey("proj", t.id, target.fileId));
    expect(new Uint8Array(await object!.arrayBuffer())).toEqual(PNG);
  });

  it("refuses bad, expired and download signatures, and a ticket whose file was deleted", async () => {
    const t = tenant();
    const target = await t.as(t.alice).call("generateUploadUrl", {}) as Upload;
    const url = new URL(target.url);
    const tampered = target.url.replace(/sig=([0-9a-f])/, (_m, c: string) => `sig=${c === "0" ? "1" : "0"}`);
    for (const bad of [tampered, target.url.replace("&upload=1", ""), target.url.replace(/exp=\d+/, "exp=1")]) {
      const response = await post(bad, PNG);
      expect(response.status).toBe(403);
      expect(await response.text()).toBe("invalid or expired storage signature");
    }
    // A download-scope signature is not an upload signature.
    const key = decodeURIComponent(url.pathname.slice("/storage/".length));
    const exp = Number(url.searchParams.get("exp"));
    const downloadSigned = `https://gonvex.test/storage/${encodeKey(key)}?exp=${exp}&sig=${await proxySignature(config.secret, key, exp, { kind: "download" })}&upload=1`;
    expect((await post(downloadSigned, PNG)).status).toBe(403);

    await t.as(t.alice).call("delete", { fileId: target.fileId });
    const gone = await post(target.url, PNG);
    expect(gone.status).toBe(409);
    expect(await gone.json()).toEqual({ error: "upload already consumed" });
  });

  it("releases the claim when the conditional PUT fails", async () => {
    const t = tenant();
    const target = await t.as(t.alice).call("generateUploadUrl", {}) as Upload;
    const key = objectKey("proj", t.id, target.fileId);
    await env.FILES.put(key, bytes("already there"));
    const response = await post(target.url, PNG);
    expect(response.status).toBe(409);
    expect(await response.json()).toEqual({ error: "upload already consumed" });
    expect(await t.rows("SELECT consumed_at FROM _gonvex_storage_uploads WHERE object_key = ?", key)).toEqual([{ consumed_at: null }]);
    expect(await (await env.FILES.get(key))!.text()).toBe("already there");
  });

  it("answers OPTIONS preflights only for valid upload signatures", async () => {
    const t = tenant();
    const target = await t.as(t.alice).call("generateUploadUrl", {}) as Upload;
    const ok = await SELF.fetch(target.url, { method: "OPTIONS" });
    expect(ok.status).toBe(204);
    expect(ok.headers.get("access-control-allow-headers")).toBe("Content-Type");
    expect(ok.headers.get("access-control-max-age")).toBe("600");
    const bad = await SELF.fetch(target.url.replace("&upload=1", ""), { method: "OPTIONS" });
    expect(bad.status).toBe(403);
  });

  it("rejects a malformed query before the handler, as axum does", async () => {
    const cases: [string, string][] = [
      ["https://gonvex.test/storage/p/t/f?sig=x", "Failed to deserialize query string: missing field `exp`"],
      ["https://gonvex.test/storage/p/t/f?exp=1", "Failed to deserialize query string: missing field `sig`"],
      ["https://gonvex.test/storage/p/t/f?exp=soon&sig=x", "Failed to deserialize query string: exp: invalid digit found in string"],
      ["https://gonvex.test/storage/p/t/f?exp=1&sig=x&upload=300", "Failed to deserialize query string: upload: number too large to fit in target type"],
    ];
    for (const [url, message] of cases) {
      const response = await SELF.fetch(url);
      expect(response.status).toBe(400);
      expect(await response.text()).toBe(message);
      expect(response.headers.get("access-control-allow-origin")).toBeNull();
    }
  });

  it("refuses a declared upload over 128 MiB and a body over 128 MiB", async () => {
    const t = tenant();
    expect(await t.as(t.alice).fails("generateUploadUrl", { size: (128 << 20) + 1 })).toBe("upload too large");
    const target = await t.as(t.alice).call("generateUploadUrl", {}) as Upload;
    const response = await SELF.fetch(target.url, { method: "POST", body: "x", headers: { "content-length": String((128 << 20) + 1) } }).catch(() => undefined);
    // workerd may refuse a lying Content-Length itself; when it reaches the router the answer is Rust's 413.
    if (response) {
      expect(response.status).toBe(413);
      expect(await response.text()).toBe("upload too large");
    }
  });
});

describe("signed downloads", () => {
  it("streams the object with Rust's headers, the inline allowlist and range requests", async () => {
    const t = tenant();
    const fileId = await uploaded(t, t.alice, { visibility: "tenant" });
    const url = await t.as(t.alice).call("getUrl", { fileId }) as string;
    expect(url).toMatch(new RegExp(`^https://gonvex\\.test/storage/proj/${t.id}/${fileId}\\?exp=\\d+&sig=[0-9a-f]{64}$`));

    const full = await SELF.fetch(url);
    expect(full.status).toBe(200);
    expect(new Uint8Array(await full.arrayBuffer())).toEqual(PNG);
    const headers = Object.fromEntries(full.headers);
    expect(headers).toMatchObject({
      "accept-ranges": "bytes",
      "content-length": String(PNG.byteLength),
      "content-type": "image/png",
      "content-disposition": "inline",
      "x-content-type-options": "nosniff",
      "content-security-policy": "default-src 'none'; base-uri 'none'",
      "cache-control": "private, max-age=300",
      "access-control-allow-origin": "*",
    });
    expect(headers["etag"]).toMatch(/^"[0-9a-f]{32}"$/);
    expect(new Date(headers["last-modified"]!).getTime()).toBeGreaterThan(0);

    const part = await SELF.fetch(url, { headers: { range: "bytes=2-5" } });
    expect(part.status).toBe(206);
    expect(part.headers.get("content-range")).toBe(`bytes 2-5/${PNG.byteLength}`);
    expect(part.headers.get("content-length")).toBe("4");
    expect(new Uint8Array(await part.arrayBuffer())).toEqual(PNG.slice(2, 6));

    const tail = await SELF.fetch(url, { headers: { range: "bytes=-3" } });
    expect(tail.status).toBe(206);
    expect(tail.headers.get("content-range")).toBe(`bytes ${PNG.byteLength - 3}-${PNG.byteLength - 1}/${PNG.byteLength}`);
    expect(new Uint8Array(await tail.arrayBuffer())).toEqual(PNG.slice(-3));

    const open = await SELF.fetch(url, { headers: { range: "bytes=10-" } });
    expect(open.status).toBe(206);
    expect(new Uint8Array(await open.arrayBuffer())).toEqual(PNG.slice(10));

    const head = await SELF.fetch(url, { method: "HEAD" });
    expect(head.status).toBe(200);
    expect(head.headers.get("content-length")).toBe(String(PNG.byteLength));
    expect(await head.text()).toBe("");
  });

  it("downloads everything outside the allowlist as an opaque, sandboxed attachment", async () => {
    const t = tenant();
    for (const type of ["text/html", "image/svg+xml", "application/xml", "text/plain; charset=utf-8"]) {
      const fileId = await uploaded(t, t.alice, { visibility: "tenant" }, bytes("<script>alert(1)</script>"), type);
      const response = await SELF.fetch(await t.as(t.alice).call("generateDownloadUrl", { fileId, ttlMs: 60_000 }) as string);
      expect(response.headers.get("content-type")).toBe("application/octet-stream");
      expect(response.headers.get("content-disposition")).toBe("attachment");
      expect(response.headers.get("content-security-policy")).toBe("sandbox; default-src 'none'; base-uri 'none'");
      await response.arrayBuffer();
    }
    const pdf = await uploaded(t, t.alice, { visibility: "tenant" }, bytes("%PDF-1.7"), "Application/PDF; x=y");
    const response = await SELF.fetch(await t.as(t.alice).call("getUrl", { fileId: pdf }) as string);
    expect(response.headers.get("content-type")).toBe("application/pdf");
    expect(response.headers.get("content-disposition")).toBe("inline");
    await response.arrayBuffer();
  });

  it("refuses an upload signature, a bad signature and an expired URL; a deleted file is 404", async () => {
    const t = tenant();
    const fileId = await uploaded(t, t.alice, { visibility: "tenant" });
    const url = await t.as(t.alice).call("getUrl", { fileId }) as string;
    for (const bad of [`${url}&upload=1`, url.replace(/sig=[0-9a-f]{4}/, "sig=abcd"), url.replace(/exp=\d+/, "exp=100"), url.replace(/sig=[0-9a-f]+/, "sig=")]) {
      const response = await SELF.fetch(bad);
      expect(response.status).toBe(403);
      expect(await response.text()).toBe("invalid or expired storage signature");
    }
    // TTLs are clamped like a presign: at most 7 days.
    const long = new URL(await t.as(t.alice).call("generateDownloadUrl", { fileId, ttlMs: 10 * 365 * 86_400_000 }) as string);
    expect(Number(long.searchParams.get("exp")) - Math.floor(Date.now() / 1000)).toBeLessThanOrEqual(604_800);

    expect(await t.as(t.alice).call("delete", { fileId })).toBeNull();
    expect(await env.FILES.head(objectKey("proj", t.id, fileId))).toBeNull();
    const gone = await SELF.fetch(url);
    expect(gone.status).toBe(404);
    expect(await gone.text()).toBe("object not found");
    expect(await t.as(t.alice).fails("getMetadata", { fileId })).toBe("file not found");
  });

  it("answers 404 for a signed key that no tenant row backs, and a missing object", async () => {
    const t = tenant();
    const exp = Math.floor(Date.now() / 1000) + 60;
    const orphan = await proxyUrl(config.publicUrl, config.secret, `proj/${t.id}/${"0".repeat(32)}`, exp, { kind: "download" });
    expect((await SELF.fetch(orphan)).status).toBe(404);
    const unroutable = await proxyUrl(config.publicUrl, config.secret, "no-tenant-segment", exp, { kind: "download" });
    expect((await SELF.fetch(unroutable)).status).toBe(404);
    const fileId = await uploaded(t, t.alice, { visibility: "tenant" });
    await env.FILES.delete(objectKey("proj", t.id, fileId));
    const missing = await SELF.fetch(await t.as(t.alice).call("getUrl", { fileId }) as string);
    expect(missing.status).toBe(404);
    expect(await missing.text()).toBe("object not found");
  });
});

describe("IB1: a private file is refused to a non-owner on every route", () => {
  it("getUrl, generateDownloadUrl, getMetadata, delete, the Reducer, the sandbox read and the proxy", async () => {
    const t = tenant();
    const fileId = await uploaded(t, t.alice); // no visibility: private, owner = alice's account
    const owner = await t.as(t.alice).call("getMetadata", { fileId }) as Record<string, unknown>;
    expect(owner).toMatchObject({ visibility: "private", ownerId: "acct-alice", status: "uploaded" });

    for (const [operation, payload] of [["getUrl", { fileId }], ["generateDownloadUrl", { fileId, ttlMs: 1000 }], ["getMetadata", { fileId }], ["delete", { fileId }]] as const) {
      expect(await t.as(t.bob).fails(operation, payload)).toBe("file not found");
    }
    const reducer = await t.reducerMetadata(t.bob, fileId);
    expect(reducer.ok).toBe(false);
    expect(reducer.error?.message).toContain("host call failed: storage host call failed [host_call_failed]: file not found");
    await expect(t.as(t.bob).readFile(fileId)).rejects.toThrow("file not found");
    // A system principal (no account) is not exempt on reads either.
    expect(await t.as({ ...t.bob, accountId: "", memberId: "", system: true }).fails("getMetadata", { fileId })).toBe("file not found");

    // The proxy: a URL signed without the owner check (the plain download scope Rust uses for every file,
    // and the operator listing mints) does not open a private file. The owner's own URL does.
    const exp = Math.floor(Date.now() / 1000) + 600;
    const key = objectKey("proj", t.id, fileId);
    const unchecked = await proxyUrl(config.publicUrl, config.secret, key, exp, { kind: "download" });
    expect((await SELF.fetch(unchecked)).status).toBe(403);
    const forOther = await proxyUrl(config.publicUrl, config.secret, key, exp, { kind: "private", ownerId: "acct-bob" });
    expect((await SELF.fetch(forOther)).status).toBe(403);
    const mine = await SELF.fetch(await t.as(t.alice).call("generateDownloadUrl", { fileId, ttlMs: 60_000 }) as string);
    expect(mine.status).toBe(200);
    expect(new Uint8Array(await mine.arrayBuffer())).toEqual(PNG);

    // The owner still reaches it everywhere, and the file survived bob's delete.
    expect((await t.reducerMetadata(t.alice, fileId)).ok).toBe(true);
    expect((await t.as(t.alice).readFile(fileId)).bytes).toEqual(PNG);
    expect(await env.FILES.head(key)).not.toBeNull();
    expect(await t.as(t.alice).call("delete", { fileId })).toBeNull();
  });

  it("treats the caller's Member id as the caller (Whagons writes ownerId: ctx.member.id)", async () => {
    const t = tenant();
    const fileId = await uploaded(t, t.alice, { ownerId: "mem-alice", visibility: "private" });
    expect(await t.as(t.alice).call("getMetadata", { fileId })).toMatchObject({ ownerId: "mem-alice" });
    expect(await t.as(t.bob).fails("getUrl", { fileId })).toBe("file not found");
    expect((await t.reducerMetadata(t.bob, fileId)).ok).toBe(false);
    expect((await t.reducerMetadata(t.alice, fileId)).ok).toBe(true);
  });

  it("refuses an ownerId that names someone else, except for a system principal", async () => {
    const t = tenant();
    for (const ownerId of ["acct-bob", "mem-bob", "calling-recording:call-1"]) {
      expect(await t.as(t.alice).fails("generateUploadUrl", { ownerId })).toBe(OWNER_REFUSED);
      expect(await t.as(t.alice).fails("store", { contentBase64: "AA==", ownerId })).toBe(OWNER_REFUSED);
    }
    expect(await t.rows("SELECT count(*) AS n FROM _gonvex_files")).toEqual([{ n: 0 }]);
    const system: FilesSession = { projectId: "proj", tenantId: t.id, accountId: "", memberId: "", system: true };
    const stored = await t.as(system).call("store", { contentBase64: "AA==", ownerId: "acct-bob" }) as Record<string, unknown>;
    // As in Rust, a caller with no account stores `tenant` files.
    expect(stored).toMatchObject({ ownerId: "acct-bob", visibility: "tenant" });
  });

  it("lets every tenant caller read tenant and public files, and stores anonymous files as tenant with no owner", async () => {
    const t = tenant();
    const shared = await uploaded(t, t.alice, { visibility: "tenant" });
    expect(await t.as(t.bob).call("getMetadata", { fileId: shared })).toMatchObject({ visibility: "tenant", ownerId: "acct-alice" });
    const open = await uploaded(t, t.alice, { visibility: "public" });
    expect((await SELF.fetch(await t.as(t.bob).call("getUrl", { fileId: open }) as string)).status).toBe(200);
    const anonymous: FilesSession = { projectId: "proj", tenantId: t.id, accountId: "", memberId: "" };
    const target = await t.as(anonymous).call("generateUploadUrl", { visibility: "private", ownerId: "acct-alice" }) as Upload;
    const metadata = await t.as(t.bob).call("getMetadata", { fileId: target.fileId }) as Record<string, unknown>;
    expect(metadata["visibility"]).toBe("tenant");
    expect("ownerId" in metadata).toBe(false);
  });
});

describe("metadata and finalize", () => {
  it("finalizes a pending file on getUrl/getMetadata, not on generateDownloadUrl, with Rust's JSON", async () => {
    const t = tenant();
    const target = await t.as(t.alice).call("generateUploadUrl", { visibility: "tenant", size: 3 }) as Upload;
    const pending = await t.as(t.alice).call("getMetadata", { fileId: target.fileId }) as Record<string, unknown>;
    expect(pending).toEqual({
      id: target.fileId, tenantId: t.id, ownerId: "acct-alice", bucket: "gonvex-cf-dev-files-test",
      objectKey: `proj/${t.id}/${target.fileId}`, size: 3, visibility: "tenant", status: "pending", createdAt: expect.stringMatching(/Z$/),
    });
    expect((await post(target.url, bytes("hello"), "audio/mpeg")).status).toBe(200);
    await t.as(t.alice).call("generateDownloadUrl", { fileId: target.fileId });
    expect(await t.rows("SELECT status FROM _gonvex_files")).toEqual([{ status: "pending" }]);
    const done = await t.as(t.alice).call("getMetadata", { fileId: target.fileId }) as Record<string, unknown>;
    expect(done).toMatchObject({ status: "uploaded", size: 5, contentType: "audio/mpeg", checksum: "5d41402abc4b2a76b9719d911017c592" });
    expect(Object.keys(done)).toEqual(["id", "tenantId", "ownerId", "bucket", "objectKey", "contentType", "size", "checksum", "visibility", "status", "createdAt", "uploadedAt"]);
  });

  it("finalizes a pending file inside a Reducer's transaction (R2 HEAD awaited in it)", async () => {
    const t = tenant();
    const target = await t.as(t.alice).call("generateUploadUrl", { visibility: "tenant" }) as Upload;
    expect((await post(target.url, bytes("abc"), "image/webp")).status).toBe(200);
    const report = await t.reducerMetadata(t.bob, target.fileId);
    expect(report.ok).toBe(true);
    expect(JSON.parse(report.ok ? report.value as string : "{}")).toMatchObject({ status: "uploaded", size: 3, contentType: "image/webp" });
    expect(report.committed).toBe(true);
    expect(await t.rows("SELECT status, size_bytes FROM _gonvex_files")).toEqual([{ status: "uploaded", size_bytes: 3 }]);
    const missing = await t.reducerMetadata(t.alice, "nope");
    expect(missing.error?.message).toContain("file not found");
  });

  it("formats times as chrono does", () => {
    expect(rfc3339(1792324800000)).toBe("2026-10-18T12:00:00Z");
    expect(rfc3339(1792324800120)).toBe("2026-10-18T12:00:00.120Z");
  });
});

describe("store (R26: 32 MiB)", () => {
  it("stores bytes as an uploaded file with a SHA-256 checksum", async () => {
    const t = tenant();
    const content = btoa("hello world");
    const stored = await t.as(t.alice).call("store", { contentBase64: content, contentType: "text/plain", visibility: "tenant" }) as Record<string, unknown>;
    expect(stored).toMatchObject({
      status: "uploaded", size: 11, contentType: "text/plain", visibility: "tenant", ownerId: "acct-alice",
      checksum: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
    });
    expect("uploadedAt" in stored).toBe(true);
    const object = await env.FILES.get(stored["objectKey"] as string);
    expect(await object!.text()).toBe("hello world");
  });

  it("refuses invalid base64 and content over 32 MiB with a clear error", async () => {
    const t = tenant();
    for (const content of ["not base64", "AAA", "AB==", "AA==\n", " AAAA"]) {
      expect(await t.as(t.alice).fails("store", { contentBase64: content })).toBe("storage content is not valid base64");
    }
    const over = "A".repeat((Math.floor(MAX_STORE_BYTES / 3) + 2) * 4);
    expect(await t.as(t.alice).fails("store", { contentBase64: over })).toBe(`storage content exceeds the ${MAX_STORE_BYTES} byte limit`);
    expect(decodeStoreContent("A".repeat(8), 6)).toHaveLength(6);
    expect(() => decodeStoreContent("A".repeat(12), 6)).toThrow("storage content exceeds the 6 byte limit");
    expect(await t.rows("SELECT count(*) AS n FROM _gonvex_files")).toEqual([{ n: 0 }]);
  });
});

describe("direct presigned upload over the Worker body limit (S4)", () => {
  it("returns a presigned R2 PUT bound to If-None-Match and the declared size", async () => {
    const t = tenant();
    const target = await t.as(t.alice).call("generateUploadUrl", { size: 120_000_000, contentType: "video/mp4", visibility: "tenant", expiresMs: 3_600_000 }) as Upload;
    expect(target.method).toBe("PUT");
    expect(target.headers).toEqual({ "Content-Type": "video/mp4", "If-None-Match": "*" });
    const url = new URL(target.url);
    expect(url.host).toBe("k13-test-account.r2.cloudflarestorage.com");
    expect(url.pathname).toBe(`/gonvex-cf-dev-files-test/proj/${t.id}/${target.fileId}`);
    expect(url.searchParams.get("X-Amz-SignedHeaders")).toBe("content-length;content-type;host;if-none-match");
    expect(url.searchParams.get("X-Amz-Expires")).toBe("3600");
    expect(url.searchParams.get("X-Amz-Credential")).toMatch(/^AKIAIOSFODNN7EXAMPLE\/\d{8}\/auto\/s3\/aws4_request$/);
    expect([...url.searchParams.keys()]).toEqual(["X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Expires", "X-Amz-Signature", "X-Amz-SignedHeaders"]);
    // No proxy ticket for it; the row is pending until a read finalizes it.
    expect(await t.rows("SELECT count(*) AS n FROM _gonvex_storage_uploads")).toEqual([{ n: 0 }]);
    expect(await t.rows("SELECT status, size_bytes FROM _gonvex_files")).toEqual([{ status: "pending", size_bytes: 120_000_000 }]);
  });

  it("signs exactly as SigV4 (the AWS documentation's presigned GET example)", async () => {
    const url = await presignUrl(
      { region: "us-east-1", accessKeyId: "AKIAIOSFODNN7EXAMPLE", secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" },
      "GET", new URL("https://examplebucket.s3.amazonaws.com/test.txt"), 86400, new Date("2013-05-24T00:00:00Z"),
    );
    expect(new URL(url).searchParams.get("X-Amz-Signature")).toBe("aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404");
  });
});

describe("payloads, keys and signatures", () => {
  it("parses the payload as serde does, before matching the operation", async () => {
    const t = tenant();
    const fails = t.as(t.alice).fails;
    expect(await fails("getMetadata", { fileId: "x", extra: 1 })).toBe("invalid storage payload: unknown field `extra`, expected one of `fileId`, `ttlMs`, `contentBase64`, `contentType`, `size`, `visibility`, `ownerId`, `expiresMs`");
    expect(await fails("generateDownloadUrl", { fileId: "x", ttlMs: "soon" })).toBe('invalid storage payload: invalid type: string "soon", expected i64');
    expect(await fails("generateDownloadUrl", { fileId: "x", ttlMs: 1.5 })).toBe("invalid storage payload: invalid type: floating point `1.5`, expected i64");
    expect(await fails("getMetadata", { fileId: 7 })).toBe("invalid storage payload: invalid type: integer `7`, expected a string");
    expect(await fails("getMetadata", { fileId: null })).toBe("invalid storage payload: invalid type: null, expected a string");
    expect(await fails("getMetadata", "x")).toBe('invalid storage payload: invalid type: string "x", expected struct StorageRequest');
    expect(await fails("rename", { nope: 1 })).toBe("invalid storage payload: unknown field `nope`, expected one of `fileId`, `ttlMs`, `contentBase64`, `contentType`, `size`, `visibility`, `ownerId`, `expiresMs`");
    expect(await fails("rename", {})).toBe('unknown storage operation "rename"');
    expect(await fails("getMetadata", {})).toBe("fileId is required");
    expect(await fails("getUrl", { fileId: "missing" })).toBe("file not found");
  });

  it("encodes keys as Rust does", () => {
    expect(encodeKey("project/a b/file")).toBe("project/a%20b/file");
    expect(safeSegment("../tenant one")).toBe("tenant_one");
    expect(safeSegment(" é-x.y_ ")).toBe("-x.y");
    expect(objectKey("p!", "t", "f")).toBe("p/t/f");
  });

  it("scopes signatures by method and, for private files, by owner", async () => {
    const download = await proxySignature("secret", "p/t/f", 100, { kind: "download" });
    const upload = await proxySignature("secret", "p/t/f", 100, { kind: "upload" });
    const mine = await proxySignature("secret", "p/t/f", 100, { kind: "private", ownerId: "a" });
    expect(new Set([download, upload, mine, await proxySignature("secret", "p/t/f", 100, { kind: "private", ownerId: "b" })]).size).toBe(4);
    // Rust's message and key domain: hex(HMAC-SHA256("gonvex-storage:secret", "p/t/f\n100")).
    const key = await crypto.subtle.importKey("raw", new TextEncoder().encode("gonvex-storage:secret"), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
    const expected = Array.from(new Uint8Array(await crypto.subtle.sign("HMAC", key, new TextEncoder().encode("p/t/f\n100"))), (b) => b.toString(16).padStart(2, "0")).join("");
    expect(download).toBe(expected);
  });

  it("fails every call with Rust's text when storage is not configured", async () => {
    const actions = new ActionFiles({ tenant: env.TENANT.getByName("unconfigured") as unknown as FilesTenantRpc, bucket: env.FILES, config: undefined });
    await expect(actions.call(person("a", "unconfigured"), "getMetadata", { fileId: "x" })).rejects.toThrow("storage is not configured");
  });
});

describe("rows read and written (PR numbers)", () => {
  it("measures each operation", async () => {
    const t = tenant();
    const measure = async (label: string, run: () => Promise<unknown>) => {
      const before = await t.within((instance) => ({ ...instance.files.stats }));
      await run();
      const after = await t.within((instance) => ({ ...instance.files.stats }));
      return [label, after.statements - before.statements, after.rowsRead - before.rowsRead, after.rowsWritten - before.rowsWritten] as const;
    };
    const target = await t.as(t.alice).call("generateUploadUrl", { visibility: "tenant" }) as Upload;
    const table = [
      await measure("generateUploadUrl", () => t.as(t.alice).call("generateUploadUrl", { visibility: "tenant" })),
      await measure("proxy upload (claim)", () => post(target.url, PNG)),
      await measure("getMetadata (finalize)", () => t.as(t.alice).call("getMetadata", { fileId: target.fileId })),
      await measure("getUrl", () => t.as(t.alice).call("getUrl", { fileId: target.fileId })),
      await measure("proxy download (lookup)", async () => (await SELF.fetch(await t.as(t.alice).call("getUrl", { fileId: target.fileId }) as string)).arrayBuffer()),
      await measure("store", () => t.as(t.alice).call("store", { contentBase64: "AAAA" })),
      await measure("delete", () => t.as(t.alice).call("delete", { fileId: target.fileId })),
    ];
    // [operation, statements, rows read, rows written] on the tenant object. Writes count index entries.
    expect(table).toEqual([
      ["generateUploadUrl", 2, 0, 6],
      ["proxy upload (claim)", 3, 4, 1],
      ["getMetadata (finalize)", 2, 3, 1],
      ["getUrl", 1, 1, 0],
      ["proxy download (lookup)", 2, 2, 0], // getUrl's row, then the proxy's lookup
      ["store", 1, 1, 4],
      ["delete", 2, 2, 1],
    ]);
  });
});
