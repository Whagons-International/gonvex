// Fixture Actions for the K18 tests: the provider signature checks as the
// providers document them, run through K04's engine like any module Action.
// Plain module code: the vitest prelude gives it the module globals.
//
// - Telnyx (calling and relay routes): Ed25519 over `${timestamp}|${rawBody}`,
//   with the account's public key (base64, 32 bytes raw) from a secret.
// - GitHub: `sha256=` + hex HMAC-SHA256 of the body with the hook secret.
//
// The real Whagons Actions run in test/whagons.test.ts when a build exists.

const bytes = (value) => new TextEncoder().encode(value);
const fromBase64 = (value) => Uint8Array.from(atob(value), (c) => c.charCodeAt(0));
const hex = (buffer) => Array.from(new Uint8Array(buffer), (b) => b.toString(16).padStart(2, "0")).join("");

async function telnyxVerified(ctx, args) {
  const key = await crypto.subtle.importKey("raw", fromBase64(ctx.secrets.TELNYX_PUBLIC_KEY ?? ""), { name: "Ed25519" }, false, ["verify"]);
  let signature;
  try { signature = fromBase64(args.signature); } catch { return false; }
  return crypto.subtle.verify("Ed25519", key, signature, bytes(`${args.timestamp}|${args.rawBody}`));
}

async function received(ctx, args) {
  return {
    ok: true,
    bodySha256: hex(await crypto.subtle.digest("SHA-256", bytes(args.rawBody))),
    bodyBytes: bytes(args.rawBody).byteLength,
    channel: ctx.invocation.channel,
    rootChannel: ctx.invocation.rootChannel,
    commandId: ctx.invocation.commandId,
    member: ctx.member?.id ?? null,
    role: ctx.member?.role ?? null,
    tenant: ctx.tenant?.id ?? null,
  };
}

export async function calling_ingestWebhook(ctx, args) {
  if (!(await telnyxVerified(ctx, args))) throw new Error("Invalid Telnyx webhook signature");
  return received(ctx, args);
}

export async function relay_ingestTelnyxWebhook(ctx, args) {
  if (!(await telnyxVerified(ctx, args))) throw new Error("Telnyx webhook signature is invalid");
  return received(ctx, args);
}

export async function github_ingestWebhook(ctx, args) {
  const key = await crypto.subtle.importKey("raw", bytes(ctx.secrets.GITHUB_WEBHOOK_SECRET ?? ""), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const expected = `sha256=${hex(await crypto.subtle.sign("HMAC", key, bytes(args.rawBody)))}`;
  if (args.signature !== expected) throw new Error("GitHub webhook signature is invalid");
  if (args.event === "fail") throw new Error("database is busy");
  return { ...(await received(ctx, args)), event: args.event };
}

export async function taskPublicShares_getPublicPreviewByToken(ctx, args) {
  return ctx.tools.resolvePublicPreview({ token: args.token });
}
