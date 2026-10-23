import type { GonvexClientAuth } from "./index.js";

function authIdentityKey(auth: GonvexClientAuth) {
  if (auth.token) {
    const tokenIdentity = authIdentityKeyFromToken(auth);
    if (tokenIdentity) {
      // Conflicting account credentials cannot reuse either account's cache.
      if (auth.identity?.sub && !tokenIdentity.endsWith(`\u0000${auth.identity.sub}`)) return "";
      return tokenIdentity;
    }
  }
  // Token-free fallback: an explicit identity hint carries the same claims a
  // token would supply, so both paths derive the same key for the same Account.
  const hint = auth.identity;
  if (hint && typeof hint.sub === "string" && hint.sub.trim()) {
    return [auth.project ?? "", auth.tenant ?? "", hint.iss ?? "", hint.sub].join("\u0000");
  }
  return "";
}

function authIdentityKeyFromToken(auth: GonvexClientAuth) {
  if (!auth.token) return "";
  const parts = auth.token.split(".");
  if (parts.length < 2) return "";
  try {
    const encoded = parts[1]!.replace(/-/g, "+").replace(/_/g, "/");
    const padded = encoded.padEnd(Math.ceil(encoded.length / 4) * 4, "=");
    const payload = JSON.parse(globalThis.atob(padded)) as { sub?: unknown; iss?: unknown };
    if (typeof payload.sub !== "string" || !payload.sub.trim()) return "";
    return [
      auth.project ?? "",
      auth.tenant ?? "",
      typeof payload.iss === "string" ? payload.iss : "",
      payload.sub,
    ].join("\u0000");
  } catch {
    return "";
  }
}

export function sameAuthTokenIdentity(left: GonvexClientAuth, right: GonvexClientAuth) {
  const leftIdentity = authIdentityKey(left);
  const rightIdentity = authIdentityKey(right);
  return leftIdentity !== "" && leftIdentity === rightIdentity;
}

export function reducerOutboxScope(url: string, auth: GonvexClientAuth, ephemeralScope: string) {
  const identity = authIdentityKey(auth);
  if (identity) return ["identity", url, identity].join("\u0000");
  if (auth.token || auth.identity || auth.fetchToken) {
    // Opaque tokens (or credentials installed before tenant selection) do not
    // expose a stable Account key. A per-client scope preserves current-session
    // queue semantics without ever restoring those rows under another Account.
    return ["ephemeral-auth", url, ephemeralScope].join("\u0000");
  }
  // Anonymous/dev-auth clients still need a stable namespace, but it must be
  // isolated by deployment and tenant. Once an authenticated identity is
  // installed, applyAuth switches away from this scope before restoring or
  // sending its durable reducers.
  return ["anonymous", url, auth.project ?? "", auth.tenant ?? ""].join("\u0000");
}

