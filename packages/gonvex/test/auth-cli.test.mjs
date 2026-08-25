import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { main } from "../dist/index.js";

const settingsKeys = ["GONVEX_PROJECT_ID", "GONVEX_PROJECT", "GONVEX_RUNTIME_URL", "GONVEX_PROJECT_KEY", "GONVEX_DEPLOY_KEY", "GONVEX_KEY", "GONVEX_APP_ORIGIN", "GONVEX_CONFIG_PATH", "VITE_APP_URL"];

test("project-scoped commands reject a project id that disagrees with the project key", async (t) => {
  const previous = new Map(settingsKeys.map((key) => [key, process.env[key]]));
  for (const key of settingsKeys) delete process.env[key];
  t.after(() => {
    for (const [key, value] of previous) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  });

  const root = await mkdtemp(join(tmpdir(), "gonvex-project-key-mismatch-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const encodedProject = Buffer.from("registered-project", "utf8").toString("base64url");
  const projectKey = `gvx_${encodedProject}.test-secret`;

  await assert.rejects(
    main(["auth", "status", "--project", root, "--project-id", "wrong-project", "--key", projectKey]),
    /does not match the project key for "registered-project"/,
  );
});

test("auth add google registers the callback and writes the native auth module", async (t) => {
  const previous = new Map(settingsKeys.map((key) => [key, process.env[key]]));
  for (const key of settingsKeys) delete process.env[key];
  t.after(() => {
    for (const [key, value] of previous) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  });

  const received = [];
  const redirectUris = [];
  const server = createServer(async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk;
    received.push({ body, headers: request.headers, method: request.method, url: request.url });
    if (request.method === "DELETE") {
      const redirectUri = new URL(request.url, "http://runtime.test").searchParams.get("redirect_uri");
      const index = redirectUris.indexOf(redirectUri);
      if (index >= 0) redirectUris.splice(index, 1);
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ provider: "google", enabled: true, redirectUris }));
      return;
    }
    const parsed = JSON.parse(body);
    redirectUris.push(parsed.redirectUri);
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      provider: "google",
      enabled: true,
      redirectUris,
      runtimeConfigured: true,
      ready: true,
      signupMode: parsed.signupMode,
      brokerCallbackUrl: "https://auth.gonvex.dev/auth/google/callback",
    }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve())));
  const address = server.address();
  assert.equal(typeof address, "object");
  const runtimeURL = `http://127.0.0.1:${address.port}`;

  const root = await mkdtemp(join(tmpdir(), "gonvex-auth-cli-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(join(root, "gonvex.json"), JSON.stringify({ project: "project-auth", runtime: runtimeURL }, null, 2));
  await writeFile(join(root, ".env.local"), [
    "GONVEX_PROJECT_ID=project-auth",
    `GONVEX_RUNTIME_URL=${runtimeURL}`,
    "GONVEX_PROJECT_KEY=gvx_project-auth-key",
    "",
  ].join("\n"));

  await main(["auth", "add", "google", "--project", root, "--origin", "http://localhost:4173", "--origin", "https://app.example.com", "--signup-mode", "inviteOnly"]);

  assert.equal(received.length, 2);
  assert.equal(received[0].method, "PUT");
  assert.equal(received[0].url, "/dev/projects/project-auth/auth/google");
  assert.equal(received[0].headers.authorization, "Bearer gvx_project-auth-key");
  assert.equal(received[0].headers["x-gonvex-key"], "gvx_project-auth-key");
  assert.deepEqual(received.map(({ body }) => JSON.parse(body)), [
    { redirectUri: "http://localhost:4173/", signupMode: "inviteOnly" },
    { redirectUri: "https://app.example.com/", signupMode: "inviteOnly" },
  ]);

  const config = JSON.parse(await readFile(join(root, "gonvex.json"), "utf8"));
  assert.deepEqual(config.auth.providers.google, {
    enabled: true,
    callbackPath: "/",
    redirectUris: ["http://localhost:4173/", "https://app.example.com/"],
    signupMode: "inviteOnly",
  });
  const module = await readFile(join(root, "gonvex", "auth.tsx"), "utf8");
  assert.match(module, /createGonvexAuth/);
  assert.match(module, /project-auth/);
  assert.match(module, /GoogleSignInButton/);

  await main(["auth", "remove", "google", "--project", root, "--origin", "http://localhost:4173"]);
  assert.equal(received[2].method, "DELETE");
  assert.equal(received[2].url, "/dev/projects/project-auth/auth/google?redirect_uri=http%3A%2F%2Flocalhost%3A4173%2F");
  const prunedConfig = JSON.parse(await readFile(join(root, "gonvex.json"), "utf8"));
  assert.deepEqual(prunedConfig.auth.providers.google.redirectUris, ["https://app.example.com/"]);
});

test("auth configure firebase sends project-scoped configuration without printing credentials", async (t) => {
  const previous = new Map(settingsKeys.map((key) => [key, process.env[key]]));
  for (const key of settingsKeys) delete process.env[key];
  t.after(() => {
    for (const [key, value] of previous) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  });

  const requests = [];
  const server = createServer(async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk;
    requests.push({ body, headers: request.headers, method: request.method, url: request.url });
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      provider: "firebase",
      authMode: "firebase",
      enabled: true,
      firebaseProjectId: "whagons-prod",
      hasAdminCredentials: true,
    }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve())));
  const address = server.address();
  assert.equal(typeof address, "object");
  const runtimeURL = `http://127.0.0.1:${address.port}`;

  const root = await mkdtemp(join(tmpdir(), "gonvex-firebase-auth-cli-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(join(root, "gonvex.json"), JSON.stringify({ project: "project-auth", runtime: runtimeURL }, null, 2));
  await writeFile(join(root, ".env.local"), [
    "GONVEX_PROJECT_ID=project-auth",
    `GONVEX_RUNTIME_URL=${runtimeURL}`,
    "GONVEX_PROJECT_KEY=gvx_project-auth-key",
    "",
  ].join("\n"));
  await writeFile(join(root, "firebase-admin.json"), JSON.stringify({ private_key: "never-print-this", client_email: "admin@example.test" }));

  const messages = [];
  const originalLog = console.log;
  console.log = (...args) => messages.push(args.join(" "));
  t.after(() => { console.log = originalLog; });
  await main([
    "auth", "configure", "firebase", "--project", root,
    "--firebase-project-id", "whagons-prod",
    "--firebase-tenant-id", "tenant-one",
    "--admin-credentials-file", "firebase-admin.json",
  ]);

  assert.equal(requests.length, 1);
  assert.equal(requests[0].method, "PUT");
  assert.equal(requests[0].url, "/dev/projects/project-auth/auth/providers/firebase");
  assert.equal(requests[0].headers.authorization, "Bearer gvx_project-auth-key");
  assert.deepEqual(JSON.parse(requests[0].body), {
    authMode: "firebase",
    enabled: true,
    signupMode: "inviteOnly",
    firebaseProjectId: "whagons-prod",
    firebaseTenantId: "tenant-one",
    adminCredentials: JSON.stringify({ private_key: "never-print-this", client_email: "admin@example.test" }),
  });
  assert.doesNotMatch(messages.join("\n"), /never-print-this|admin@example\.test/);
});

test("create provisions and wires a multi-tenant Google app in one command", async (t) => {
  const previous = new Map(settingsKeys.map((key) => [key, process.env[key]]));
  for (const key of settingsKeys) delete process.env[key];
  t.after(() => {
    for (const [key, value] of previous) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  });

  const requests = [];
  const server = createServer(async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk;
    requests.push({ body, method: request.method, url: request.url });
    response.writeHead(request.method === "POST" ? 201 : 200, { "content-type": "application/json" });
    if (request.url === "/dev/projects") {
      response.end(JSON.stringify({
        project: { id: "created-auth-app", name: "auth-workspaces", databaseMode: "multiTenant" },
        projectKey: "gvx_created-auth-app-key",
      }));
      return;
    }
    if (request.url === "/dev/projects/created-auth-app/auth/tenants") {
      response.end(JSON.stringify({ tenant: { id: "created-workspace", name: "auth-workspaces workspace" } }));
      return;
    }
    response.end(JSON.stringify({
      provider: "google",
      enabled: true,
      redirectUris: ["https://app.example.com/"],
      runtimeConfigured: true,
      ready: true,
      signupMode: "inviteOnly",
      brokerCallbackUrl: "https://auth.gonvex.dev/auth/google/callback",
      tenantCount: 1,
      invitationCount: 1,
    }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve())));
  const address = server.address();
  assert.equal(typeof address, "object");
  const runtimeURL = `http://127.0.0.1:${address.port}`;

  const sandbox = await mkdtemp(join(tmpdir(), "gonvex-create-auth-cli-"));
  t.after(() => rm(sandbox, { recursive: true, force: true }));
  process.env.GONVEX_CONFIG_PATH = join(sandbox, "cli-config.json");
  const root = join(sandbox, "auth-workspaces");
  await main(["create", root, "--runtime-url", runtimeURL, "--database-mode", "multiTenant", "--google-auth", "--origin", "https://app.example.com", "--signup-mode", "inviteOnly", "--owner", "owner@example.com"]);

  assert.deepEqual(JSON.parse(requests[0].body), { name: "auth-workspaces", databaseMode: "multiTenant" });
  assert.equal(requests[1].url, "/dev/projects/created-auth-app/auth/google");
  assert.deepEqual(JSON.parse(requests[1].body), { redirectUri: "https://app.example.com/", signupMode: "inviteOnly" });
  assert.equal(requests[2].url, "/dev/projects/created-auth-app/auth/tenants");
  assert.deepEqual(JSON.parse(requests[2].body), { name: "auth-workspaces workspace", ownerEmail: "owner@example.com" });
  assert.equal(requests[3].method, "GET");
  assert.equal(requests[3].url, "/dev/projects/created-auth-app/auth/google");
  assert.match(await readFile(join(root, "src", "main.tsx"), "utf8"), /GonvexAuthProvider/);
  assert.match(await readFile(join(root, "src", "App.tsx"), "utf8"), /function AuthenticatedApp/);
  assert.match(await readFile(join(root, ".env.local"), "utf8"), /VITE_GONVEX_PROJECT_ID=created-auth-app/);
  assert.match(await readFile(join(root, ".gitignore"), "utf8"), /^\.env\.local$/m);
  assert.equal((await stat(join(root, ".env.local"))).mode & 0o777, 0o600);
  const config = JSON.parse(await readFile(join(root, "gonvex.json"), "utf8"));
  assert.equal(config.project, "created-auth-app");
  assert.equal(config.auth.providers.google.signupMode, "inviteOnly");
});

test("create rolls back its new app and runtime project when Google setup fails", async (t) => {
  const previous = new Map(settingsKeys.map((key) => [key, process.env[key]]));
  for (const key of settingsKeys) delete process.env[key];
  t.after(() => {
    for (const [key, value] of previous) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  });

  const requests = [];
  const server = createServer(async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk;
    requests.push({ body, method: request.method, url: request.url });
    response.setHeader("content-type", "application/json");
    if (request.url === "/dev/projects" && request.method === "POST") {
      response.writeHead(201);
      response.end(JSON.stringify({
        project: { id: "rollback-auth-app", name: "rollback-app", databaseMode: "single" },
        projectKey: "gvx_rollback-auth-app-key",
      }));
      return;
    }
    if (request.url === "/dev/projects/rollback-auth-app" && request.method === "DELETE") {
      response.writeHead(200);
      response.end(JSON.stringify({ ok: true }));
      return;
    }
    response.writeHead(503);
    response.end(JSON.stringify({ error: "broker unavailable" }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve())));
  const address = server.address();
  assert.equal(typeof address, "object");
  const runtimeURL = `http://127.0.0.1:${address.port}`;

  const sandbox = await mkdtemp(join(tmpdir(), "gonvex-create-auth-rollback-"));
  t.after(() => rm(sandbox, { recursive: true, force: true }));
  process.env.GONVEX_CONFIG_PATH = join(sandbox, "cli-config.json");
  const root = join(sandbox, "rollback-app");
  await assert.rejects(
    main(["create", root, "--runtime-url", runtimeURL, "--google-auth", "--origin", "http://localhost:4173"]),
    /broker unavailable/,
  );
  assert.equal(requests.at(-1)?.method, "DELETE");
  assert.equal(requests.at(-1)?.url, "/dev/projects/rollback-auth-app");
  await assert.rejects(stat(root), { code: "ENOENT" });
});
