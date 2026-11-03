import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  envFlag,
  normalizeEmail,
  signSession,
  verifyCredentials,
  verifySessionCookie,
} from "./server.mjs";

test("normalizes dashboard login emails", () => {
  assert.equal(normalizeEmail(" Malek.Gabriel33@GMAIL.COM "), "malek.gabriel33@gmail.com");
});

test("parses dashboard auth flags", () => {
  assert.equal(envFlag(undefined, true), true);
  assert.equal(envFlag("false", true), false);
  assert.equal(envFlag("true", false), true);
  assert.equal(envFlag("wat", false), false);
});

test("verifies credentials without accepting casing or password drift", () => {
  assert.equal(verifyCredentials("MALEK.GABRIEL33@gmail.com", "secret", "malek.gabriel33@gmail.com", "secret"), true);
  assert.equal(verifyCredentials("other@gmail.com", "secret", "malek.gabriel33@gmail.com", "secret"), false);
  assert.equal(verifyCredentials("malek.gabriel33@gmail.com", "nope", "malek.gabriel33@gmail.com", "secret"), false);
});

test("signs and rejects dashboard sessions", () => {
  const session = {
    email: "malek.gabriel33@gmail.com",
    expiresAt: Date.now() + 60_000,
    name: "Malek Gabriel33",
    provider: "gonvex",
  };
  const cookie = signSession(session, "session-secret");

  assert.equal(verifySessionCookie(cookie, "session-secret").email, session.email);
  assert.equal(verifySessionCookie(cookie, "wrong-secret"), null);
  assert.equal(verifySessionCookie(cookie.replace(/\.[^.]+$/, ".tampered"), "session-secret"), null);
});

test("proxies runtime project discovery instead of serving the SPA shell", async (t) => {
  const staticRoot = await mkdtemp(join(tmpdir(), "gonvex-dashboard-"));
  await writeFile(join(staticRoot, "index.html"), "<!doctype html><title>Gonvex</title>");
  t.after(() => rm(staticRoot, { recursive: true, force: true }));

  const runtime = createServer((request, response) => {
    assert.equal(request.url, "/dev/projects?source=dashboard");
    assert.equal(request.headers.authorization, "Bearer dashboard-session");
    response.writeHead(200, { "content-type": "application/json; charset=utf-8" });
    response.end(JSON.stringify({ projects: [{ id: "maker", name: "Maker" }] }));
  });
  await new Promise((resolve) => runtime.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve, reject) => runtime.close((error) => error ? reject(error) : resolve())));
  const runtimeAddress = runtime.address();
  assert.ok(runtimeAddress && typeof runtimeAddress === "object");

  const previousEnvironment = {
    DASHBOARD_AUTH_ENABLED: process.env.DASHBOARD_AUTH_ENABLED,
    DASHBOARD_STATIC_ROOT: process.env.DASHBOARD_STATIC_ROOT,
    GONVEX_RUNTIME_URL: process.env.GONVEX_RUNTIME_URL,
  };
  process.env.DASHBOARD_AUTH_ENABLED = "false";
  process.env.DASHBOARD_STATIC_ROOT = staticRoot;
  process.env.GONVEX_RUNTIME_URL = `http://127.0.0.1:${runtimeAddress.port}`;
  t.after(() => {
    for (const [name, value] of Object.entries(previousEnvironment)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  });

  const { createDashboardServer } = await import(`./server.mjs?proxy-test=${Date.now()}`);
  const dashboard = createDashboardServer();
  await new Promise((resolve) => dashboard.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve, reject) => dashboard.close((error) => error ? reject(error) : resolve())));
  const dashboardAddress = dashboard.address();
  assert.ok(dashboardAddress && typeof dashboardAddress === "object");

  const response = await fetch(`http://127.0.0.1:${dashboardAddress.port}/dev/projects?source=dashboard`, {
    headers: { authorization: "Bearer dashboard-session" },
  });

  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type") ?? "", /^application\/json\b/);
  assert.deepEqual(await response.json(), { projects: [{ id: "maker", name: "Maker" }] });
});

test("proxies the private dashboard WebSocket without forwarding cookies", async (t) => {
  const { WebSocket, WebSocketServer } = await import("ws");
  const upstream = createServer();
  const sockets = new WebSocketServer({ server: upstream });
  sockets.on("connection", (socket, request) => {
    assert.equal(request.url, "/dev/dashboard/ws");
    assert.equal(request.headers.cookie, undefined);
    socket.on("message", (message) => socket.send(message));
  });
  await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  const previous = { auth: process.env.DASHBOARD_AUTH_ENABLED, runtime: process.env.GONVEX_RUNTIME_URL };
  process.env.DASHBOARD_AUTH_ENABLED = "false";
  process.env.GONVEX_RUNTIME_URL = `http://127.0.0.1:${upstream.address().port}`;
  const { createDashboardServer } = await import(`./server.mjs?websocket=${Date.now()}`);
  const server = createDashboardServer();
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    for (const socket of sockets.clients) socket.terminate();
    await new Promise((resolve) => sockets.close(resolve));
    await Promise.all([new Promise((resolve) => server.close(resolve)), new Promise((resolve) => upstream.close(resolve))]);
    for (const [key, value] of [["DASHBOARD_AUTH_ENABLED", previous.auth], ["GONVEX_RUNTIME_URL", previous.runtime]]) {
      if (value === undefined) delete process.env[key]; else process.env[key] = value;
    }
  });
  const client = new WebSocket(`ws://127.0.0.1:${server.address().port}/dev/dashboard/ws`, { headers: { cookie: "private-session=secret" }, handshakeTimeout: 1000 });
  t.after(() => client.terminate());
  await new Promise((resolve, reject) => { client.once("open", resolve); client.once("error", reject); });
  const response = new Promise((resolve) => client.once("message", (data) => resolve(String(data))));
  client.send(JSON.stringify({ type: "auth", token: "dashboard-test" }));
  assert.equal(await response, JSON.stringify({ type: "auth", token: "dashboard-test" }));
  client.terminate();
});


test("dashboard retains runtime tokens and revalidates cookies without password fallback", async (t) => {
  let revoked = false;
  const runtime = createServer((request, response) => {
    response.setHeader('content-type', 'application/json');
    if (request.url === '/dev/auth/login' && !revoked) {
      response.end(JSON.stringify({ session: { email: 'operator@example.test', name: 'Operator', role: 'admin', expiresAt: Date.now() + 60000, accessToken: 'runtime-owned-token' } }));
    } else if (request.url === '/dev/auth/me' && !revoked) {
      assert.equal(request.headers.authorization, 'Bearer runtime-owned-token');
      response.end(JSON.stringify({ account: { email: 'operator@example.test', name: 'Operator', role: 'admin' } }));
    } else { response.writeHead(401); response.end('{}'); }
  });
  await new Promise(resolve => runtime.listen(0, '127.0.0.1', resolve));
  const values = { DASHBOARD_AUTH_ENABLED: 'true', DASHBOARD_AUTH_ACCOUNT: 'operator@example.test', DASHBOARD_AUTH_PASSWORD: 'configured-password', DASHBOARD_SESSION_SECRET: 'cookie-secret', GONVEX_RUNTIME_URL: `http://127.0.0.1:${runtime.address().port}` };
  const before = Object.fromEntries(Object.keys(values).map(key => [key, process.env[key]]));
  Object.assign(process.env, values);
  const { createDashboardServer } = await import(`./server.mjs?revocation=${Date.now()}`);
  for (const [key, value] of Object.entries(before)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
  const server = createDashboardServer();
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { await Promise.all([new Promise(resolve => server.close(resolve)), new Promise(resolve => runtime.close(resolve))]); });
  const base = `http://127.0.0.1:${server.address().port}`;
  const login = () => fetch(`${base}/api/dashboard/login`, { method: 'POST', body: JSON.stringify({ email: 'operator@example.test', password: 'configured-password' }) });
  const response = await login();
  assert.equal(response.status, 200);
  assert.equal((await response.json()).session.accessToken, 'runtime-owned-token');
  const cookie = response.headers.get('set-cookie').split(';')[0];
  const session = () => fetch(`${base}/api/dashboard/session`, { headers: { cookie } });
  assert.equal((await session()).status, 200);
  revoked = true;
  assert.equal((await session()).status, 401);
  assert.equal((await login()).status, 401);
});

test("serves frame and MIME protection on health, redirects, errors and SPA responses", async (t) => {
  const previous = process.env.DASHBOARD_AUTH_ENABLED;
  process.env.DASHBOARD_AUTH_ENABLED = "false";
  const { createDashboardServer } = await import(`./server.mjs?headers=${Date.now()}`);
  if (previous === undefined) delete process.env.DASHBOARD_AUTH_ENABLED;
  else process.env.DASHBOARD_AUTH_ENABLED = previous;
  const server = createDashboardServer();
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  for (const path of ["/healthz", "/", "/projects", "/assets/missing.js", "/api/dashboard/missing"]) {
    const response = await fetch(`http://127.0.0.1:${server.address().port}${path}`, { redirect: "manual" });
    assert.equal(response.headers.get("content-security-policy"), "frame-ancestors 'none'", path);
    assert.equal(response.headers.get("x-frame-options"), "DENY", path);
    assert.equal(response.headers.get("x-content-type-options"), "nosniff", path);
    if (path === "/healthz") assert.equal(await response.text(), "ok\n");
  }
});
