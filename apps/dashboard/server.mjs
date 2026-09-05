import { createHmac, timingSafeEqual } from "node:crypto";
import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import { createServer, request as requestHTTP } from "node:http";
import { request as requestHTTPS } from "node:https";
import { basename, extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";

export const sessionCookieName = "gonvex_dashboard_session";

const rootDir = process.env.DASHBOARD_STATIC_ROOT ?? "/usr/share/nginx/html";
const port = Number(process.env.PORT ?? "80");
const authEnabled = envFlag(process.env.DASHBOARD_AUTH_ENABLED, true);
const authAccount = normalizeEmail(process.env.DASHBOARD_AUTH_ACCOUNT ?? "");
const authPassword = process.env.DASHBOARD_AUTH_PASSWORD ?? "";
const sessionSecret = process.env.DASHBOARD_SESSION_SECRET ?? authPassword;
const cookieSecure = envFlag(process.env.DASHBOARD_COOKIE_SECURE, true);
const runtimeURL = (process.env.GONVEX_RUNTIME_URL ?? process.env.VITE_GONVEX_RUNTIME_URL ?? process.env.VITE_GONVEX_URL ?? "http://127.0.0.1:8080").replace(/\/+$/, "");

const mimeTypes = new Map([
  [".css", "text/css; charset=utf-8"],
  [".html", "text/html; charset=utf-8"],
  [".js", "text/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".map", "application/json; charset=utf-8"],
  [".png", "image/png"],
  [".svg", "image/svg+xml"],
  [".webp", "image/webp"],
  [".woff2", "font/woff2"],
]);

export function normalizeEmail(value) {
  return String(value ?? "").trim().toLowerCase();
}

export function envFlag(value, fallback) {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (!normalized) return fallback;
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

export function verifyCredentials(email, password, expectedEmail, expectedPassword) {
  return constantTimeEqual(normalizeEmail(email), expectedEmail) && constantTimeEqual(String(password ?? ""), expectedPassword);
}

export function signSession(session, secret) {
  const payload = Buffer.from(JSON.stringify(session)).toString("base64url");
  const signature = createHmac("sha256", secret).update(payload).digest("base64url");
  return `${payload}.${signature}`;
}

export function verifySessionCookie(value, secret) {
  const [payload, signature] = String(value ?? "").split(".");
  if (!payload || !signature) return null;
  const expected = createHmac("sha256", secret).update(payload).digest("base64url");
  if (!constantTimeEqual(signature, expected)) return null;
  try {
    const parsed = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    if (!parsed || typeof parsed.email !== "string" || typeof parsed.name !== "string") return null;
    if (Number(parsed.expiresAt) < Date.now()) return null;
    return parsed;
  } catch {
    return null;
  }
}

function constantTimeEqual(left, right) {
  const leftBuffer = Buffer.from(String(left ?? ""));
  const rightBuffer = Buffer.from(String(right ?? ""));
  if (leftBuffer.length !== rightBuffer.length) return false;
  return timingSafeEqual(leftBuffer, rightBuffer);
}

function displayNameFromEmail(email) {
  const local = normalizeEmail(email).split("@")[0] || "Gonvex user";
  return local
    .split(/[._-]+/)
    .filter(Boolean)
    .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
    .join(" ");
}

function parseCookies(header) {
  const cookies = new Map();
  for (const part of String(header ?? "").split(";")) {
    const index = part.indexOf("=");
    if (index === -1) continue;
    cookies.set(part.slice(0, index).trim(), decodeURIComponent(part.slice(index + 1).trim()));
  }
  return cookies;
}

function cookieOptions(maxAgeSeconds) {
  const parts = [
    "Path=/",
    "HttpOnly",
    "SameSite=Lax",
    `Max-Age=${maxAgeSeconds}`,
  ];
  if (cookieSecure) parts.push("Secure");
  return parts.join("; ");
}

function writeJSON(response, status, payload, headers = {}) {
  response.writeHead(status, { "Content-Type": "application/json; charset=utf-8", ...headers });
  response.end(JSON.stringify(payload));
}

function redirect(response, location) {
  response.writeHead(302, { Location: location, "Cache-Control": "no-store" });
  response.end();
}

const hopByHopHeaders = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);

function proxyHeaders(headers, targetHost) {
  const next = { ...headers, host: targetHost };
  for (const header of hopByHopHeaders) delete next[header];
  delete next.cookie;
  return next;
}

function proxyResponseHeaders(headers) {
  const next = { ...headers };
  for (const header of hopByHopHeaders) delete next[header];
  return next;
}

async function proxyRuntimeRequest(request, response, url) {
  const target = new URL(`${url.pathname}${url.search}`, `${runtimeURL}/`);
  const requestRuntime = target.protocol === "https:" ? requestHTTPS : requestHTTP;
  await new Promise((resolve, reject) => {
    const upstreamRequest = requestRuntime(target, {
      headers: proxyHeaders(request.headers, target.host),
      method: request.method,
    }, (upstreamResponse) => {
      response.writeHead(
        upstreamResponse.statusCode ?? 502,
        upstreamResponse.statusMessage,
        proxyResponseHeaders(upstreamResponse.headers),
      );
      upstreamResponse.on("error", (error) => response.destroy(error));
      response.once("close", resolve);
      response.once("finish", resolve);
      upstreamResponse.pipe(response);
    });
    upstreamRequest.once("error", reject);
    request.once("aborted", () => upstreamRequest.destroy());
    request.pipe(upstreamRequest);
  });
}

async function readBody(request) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > 16_384) throw new Error("request too large");
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}

function sessionFromRequest(request) {
  if (!authEnabled) return { email: "local@gonvex.dev", name: "Local Developer", provider: "dev" };
  return verifySessionCookie(parseCookies(request.headers.cookie).get(sessionCookieName), sessionSecret);
}

function signedSession(session) {
  const { accessToken: _ignored, ...payload } = session;
  const accessToken = signSession(payload, sessionSecret);
  return { accessToken, session: { ...payload, accessToken } };
}

async function handleAPI(request, response, url) {
  if (url.pathname === "/api/dashboard/session" && request.method === "GET") {
    const session = sessionFromRequest(request);
    if (!session) return writeJSON(response, 401, { error: "unauthorized" });
    const token = parseCookies(request.headers.cookie).get(sessionCookieName);
    return writeJSON(response, 200, { session: { ...session, accessToken: token } });
  }

  if (url.pathname === "/api/dashboard/logout" && request.method === "POST") {
    return writeJSON(response, 200, { ok: true }, {
      "Set-Cookie": `${sessionCookieName}=; ${cookieOptions(0)}`,
    });
  }

  if (url.pathname === "/api/dashboard/login" && request.method === "POST") {
    if (!authEnabled) {
      return writeJSON(response, 200, {
        session: { email: "local@gonvex.dev", name: "Local Developer", provider: "dev" },
      });
    }
    let parsed;
    try {
      parsed = JSON.parse(await readBody(request));
    } catch {
      return writeJSON(response, 400, { error: "invalid login request" });
    }
    let session;
    try {
      const runtimeResponse = await fetch(`${runtimeURL}/dev/auth/login`, {
        body: JSON.stringify({ email: parsed.email, password: parsed.password }),
        headers: { "content-type": "application/json" },
        method: "POST",
      });
      const payload = await runtimeResponse.json().catch(() => ({}));
      if (!runtimeResponse.ok || !payload.session) {
        throw new Error(payload.error ?? "runtime dashboard login failed");
      }
      session = payload.session;
    } catch {
      if (!authAccount || !authPassword) return writeJSON(response, 503, { error: "dashboard auth is not configured" });
      if (!verifyCredentials(parsed.email, parsed.password, authAccount, authPassword)) {
        return writeJSON(response, 401, { error: "invalid email or password" });
      }
      session = {
        email: authAccount,
        expiresAt: Date.now() + 7 * 24 * 60 * 60 * 1000,
        name: displayNameFromEmail(authAccount),
        provider: "gonvex",
      };
    }
    const signed = signedSession(session);
    return writeJSON(response, 200, { session: signed.session }, {
      "Set-Cookie": `${sessionCookieName}=${encodeURIComponent(signed.accessToken)}; ${cookieOptions(7 * 24 * 60 * 60)}`,
    });
  }

  writeJSON(response, 404, { error: "not found" });
}

async function serveFile(response, filePath, immutable = false) {
  try {
    const fileStat = await stat(filePath);
    if (!fileStat.isFile()) throw new Error("not a file");
    response.writeHead(200, {
      "Cache-Control": immutable ? "public, max-age=31536000, immutable" : "no-cache",
      "Content-Length": fileStat.size,
      "Content-Type": mimeTypes.get(extname(filePath)) ?? "application/octet-stream",
    });
    createReadStream(filePath).pipe(response);
  } catch {
    response.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    response.end("Not found\n");
  }
}

function staticPath(pathname) {
  const decoded = decodeURIComponent(pathname);
  const safe = normalize(decoded).replace(/^(\.\.(\/|\\|$))+/, "");
  return join(rootDir, safe);
}

async function handleRequest(request, response) {
  const url = new URL(request.url ?? "/", "http://dashboard.local");
  if (url.pathname === "/healthz") {
    response.writeHead(200, { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" });
    response.end("ok\n");
    return;
  }
  if (url.pathname.startsWith("/api/dashboard/")) return handleAPI(request, response, url);
  if (url.pathname === "/dev" || url.pathname.startsWith("/dev/")) return proxyRuntimeRequest(request, response, url);
  if (url.pathname.startsWith("/assets/")) return serveFile(response, staticPath(url.pathname), true);
  if (basename(url.pathname).includes(".")) return serveFile(response, staticPath(url.pathname));

  // Authentication for native Google sessions lives in the SPA's rotating
  // Gonvex token store, so the static server cannot decide access from an
  // HttpOnly password-session cookie alone. Serve the shell for every route;
  // the runtime APIs remain authenticated and App.tsx owns route gating.
  if (url.pathname === "/") return redirect(response, "/projects");
  return serveFile(response, join(rootDir, "index.html"));
}

function proxyRuntimeUpgrade(request, socket, head) {
  const url = new URL(request.url ?? "/", "http://dashboard.local");
  if (!["/dev/dashboard/ws", "/dev/metrics/stream", "/ws"].includes(url.pathname)) {
    socket.end("HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n");
    return;
  }
  const target = new URL(`${url.pathname}${url.search}`, `${runtimeURL}/`);
  const requestRuntime = target.protocol === "https:" ? requestHTTPS : requestHTTP;
  const upstream = requestRuntime(target, {
    headers: { ...proxyHeaders(request.headers, target.host), connection: "Upgrade", upgrade: "websocket" },
  });
  upstream.setTimeout(10_000, () => upstream.destroy());
  upstream.on("upgrade", (response, peer, upstreamHead) => {
    upstream.setTimeout(0);
    const headers = Object.entries(response.headers).flatMap(([name, value]) =>
      Array.isArray(value) ? value.map((item) => `${name}: ${item}`) : [`${name}: ${value}`]);
    socket.write(`HTTP/1.1 101 Switching Protocols\r\n${headers.join("\r\n")}\r\n\r\n`);
    if (upstreamHead.length) socket.write(upstreamHead);
    if (head.length) peer.write(head);
    socket.on("error", () => peer.destroy());
    peer.on("error", () => socket.destroy());
    socket.on("close", () => peer.destroy());
    peer.on("close", () => socket.destroy());
    socket.pipe(peer).pipe(socket);
  });
  upstream.on("response", (response) => {
    response.resume();
    socket.end(`HTTP/1.1 ${response.statusCode ?? 502} Upstream rejected upgrade\r\nConnection: close\r\n\r\n`);
  });
  upstream.on("error", () => socket.destroy());
  socket.on("error", () => upstream.destroy());
  socket.on("close", () => upstream.destroy());
  upstream.end();
}

export function createDashboardServer() {
  if (authEnabled && !sessionSecret) {
    throw new Error("DASHBOARD_SESSION_SECRET is required.");
  }
  const server = createServer((request, response) => {
    handleRequest(request, response).catch((error) => {
      console.error(error);
      if (response.headersSent) response.destroy(error);
      else writeJSON(response, 500, { error: "internal server error" });
    });
  });
  server.on("upgrade", proxyRuntimeUpgrade);
  return server;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  createDashboardServer().listen(port, () => {
    console.log(`Gonvex dashboard listening on ${port}`);
  });
}
