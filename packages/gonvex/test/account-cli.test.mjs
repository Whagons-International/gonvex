import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { main } from "../dist/index.js";

test("password login can mint an account token that provisions and configures a project", async (t) => {
  const received = [];
  const personalAccessToken = "gvx_pat_test.secret";
  const server = createServer(async (request, response) => {
    let body = "";
    request.setEncoding("utf8");
    for await (const chunk of request) body += chunk;
    received.push({ body, headers: request.headers, method: request.method, url: request.url });
    response.setHeader("content-type", "application/json");

    if (request.method === "POST" && request.url === "/dev/auth/login") {
      assert.deepEqual(JSON.parse(body), { email: "owner@example.test", password: "test-password" });
      response.end(JSON.stringify({ session: { accessToken: "session-token", expiresAt: Date.now() + 60_000 } }));
      return;
    }
    if (request.method === "GET" && request.url === "/dev/auth/me") {
      const bearer = request.headers.authorization;
      if (bearer !== "Bearer session-token" && bearer !== `Bearer ${personalAccessToken}`) {
        response.writeHead(401);
        response.end(JSON.stringify({ error: "invalid token" }));
        return;
      }
      response.end(JSON.stringify({
        account: { email: "owner@example.test", name: "Owner", role: "user" },
        authentication: bearer === "Bearer session-token" ? "session" : "personalAccessToken",
        permissions: bearer === "Bearer session-token" ? ["*"] : ["projects:read", "projects:create", "projects:keys:read"],
      }));
      return;
    }
    if (request.method === "POST" && request.url === "/dev/auth/tokens") {
      assert.equal(request.headers.authorization, "Bearer session-token");
      assert.deepEqual(JSON.parse(body), { name: "automation", permissions: ["projects:read", "projects:create", "projects:keys:read"] });
      response.writeHead(201);
      response.end(JSON.stringify({
        token: { id: "pat_test", name: "automation", prefix: "gvx_pat_test", permissions: ["projects:read", "projects:create", "projects:keys:read"], createdAt: new Date().toISOString() },
        accessToken: personalAccessToken,
      }));
      return;
    }
    if (request.method === "GET" && request.url === "/dev/projects") {
      assert.equal(request.headers.authorization, `Bearer ${personalAccessToken}`);
      response.end(JSON.stringify({ projects: [] }));
      return;
    }
    if (request.method === "POST" && request.url === "/dev/projects") {
      assert.equal(request.headers.authorization, `Bearer ${personalAccessToken}`);
      assert.deepEqual(JSON.parse(body), { name: "cli-app", databaseMode: "single" });
      response.writeHead(201);
      response.end(JSON.stringify({
        project: { id: "project-v6-id", name: "cli-app", environment: "production", database: "gonvex_project_v6" },
        projectKey: "gvx_project-v6-key",
      }));
      return;
    }
    response.writeHead(404);
    response.end(JSON.stringify({ error: "not found" }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve())));
  const address = server.address();
  assert.notEqual(address, null);
  assert.equal(typeof address, "object");
  const runtimeURL = `http://127.0.0.1:${address.port}`;

  const root = await mkdtemp(join(tmpdir(), "gonvex-account-cli-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const configPath = join(root, "global", "config.json");
  const envNames = ["GONVEX_CONFIG_PATH", "GONVEX_ACCOUNT_TOKEN", "GONVEX_RUNTIME_URL", "GONVEX_PASSWORD"];
  const previousEnv = new Map(envNames.map((name) => [name, process.env[name]]));
  process.env.GONVEX_CONFIG_PATH = configPath;
  delete process.env.GONVEX_ACCOUNT_TOKEN;
  delete process.env.GONVEX_RUNTIME_URL;
  delete process.env.GONVEX_PASSWORD;
  t.after(() => {
    for (const [name, value] of previousEnv) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  });

  const messages = [];
  const originalLog = console.log;
  const originalError = console.error;
  console.log = (message) => messages.push(String(message));
  console.error = (message) => messages.push(String(message));
  t.after(() => {
    console.log = originalLog;
    console.error = originalError;
  });

  await main(["login", "--runtime-url", runtimeURL, "--email", "owner@example.test", "--password", "test-password"]);
  let config = JSON.parse(await readFile(configPath, "utf8"));
  assert.equal(config.currentRuntime, runtimeURL);
  assert.equal(config.runtimes[runtimeURL].accessToken, "session-token");
  if (process.platform !== "win32") assert.equal((await stat(configPath)).mode & 0o777, 0o600);

  await main([
    "token", "create", "automation", "--runtime-url", runtimeURL,
    "--permission", "projects:read",
    "--permission", "projects:create",
    "--permission", "projects:keys:read",
  ]);
  assert.ok(messages.includes(personalAccessToken));

  await main(["logout", "--runtime-url", runtimeURL]);
  await main(["login", "--runtime-url", runtimeURL, "--token", personalAccessToken]);
  config = JSON.parse(await readFile(configPath, "utf8"));
  assert.equal(config.runtimes[runtimeURL].accessToken, personalAccessToken);
  assert.equal(config.runtimes[runtimeURL].authentication, "personalAccessToken");

  await writeFile(join(root, ".env.local"), "UNRELATED=value\n");
  await main(["project", "list", "--runtime-url", runtimeURL]);
  await main(["project", "create", "cli-app", "--runtime-url", runtimeURL, "--project-root", root]);
  const env = await readFile(join(root, ".env.local"), "utf8");
  assert.doesNotMatch(env, /^(VALKEY_URL|REDIS_URL)=/m);
  assert.match(env, /^UNRELATED=value$/m);
  assert.match(env, /^GONVEX_PROJECT_ID=project-v6-id$/m);
  assert.match(env, new RegExp(`^GONVEX_RUNTIME_URL=${runtimeURL.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "m"));
  assert.match(env, /^GONVEX_PROJECT_KEY=gvx_project-v6-key$/m);

  assert.ok(received.some((request) => request.method === "POST" && request.url === "/dev/auth/login"));
  assert.ok(received.some((request) => request.method === "POST" && request.url === "/dev/auth/tokens"));
  assert.ok(received.some((request) => request.method === "POST" && request.url === "/dev/projects"));
});
