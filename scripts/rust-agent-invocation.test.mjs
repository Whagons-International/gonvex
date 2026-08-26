import assert from "node:assert/strict";
import test from "node:test";

import { GonvexClient, control } from "../packages/client/dist/index.js";

const requiredEnvironment = [
  "GONVEX_TEST_RUNTIME_URL",
  "GONVEX_TEST_PROJECT_ID",
  "GONVEX_TEST_TENANT_ID",
  "GONVEX_TEST_EMAIL",
  "GONVEX_TEST_PASSWORD",
];

const refs = {
  agentInvoke: { kind: "action", path: "messages.agentInvoke" },
  echo: { kind: "action", path: "messages.echo" },
  get: { kind: "query", path: "messages.get" },
  list: {
    kind: "query",
    path: "messages.list",
    delivery: "live",
    live: {
      entity: "messages",
      key: "id",
      resultPath: [],
      plan: {
        table: "messages",
        key: "id",
        columns: ["id", "body", "author", "created_at"],
        sort: {
          defaultColumn: "created_at",
          defaultDirection: "asc",
          allowedColumns: ["created_at"],
        },
      },
    },
  },
  send: { kind: "reducer", path: "messages.send" },
};

test("Rust runtime preserves direct/delegated invocation, Replica delivery, and HMR", {
  skip: !requiredEnvironment.every((name) => process.env[name]?.trim()),
}, async () => {
  const runtime = required("GONVEX_TEST_RUNTIME_URL");
  const project = required("GONVEX_TEST_PROJECT_ID");
  const tenant = required("GONVEX_TEST_TENANT_ID");
  const email = required("GONVEX_TEST_EMAIL");
  const password = required("GONVEX_TEST_PASSWORD");
  const url = runtime.replace(/^http/, "ws").replace(/\/$/, "") + "/ws";
  const client = new GonvexClient(url, { project });

  try {
    const session = await client.action(control.auth.passwordLogin, { email, password });
    await client.authenticate({ project, tenant, token: session.accessToken });
    const artifactHash = client.activeArtifactHash();
    assert.match(artifactHash, /^[a-f0-9]{64}$/);

    let latestRows = [];
    const unsubscribe = client.subscribeLiveQuery(refs.list, {}, (message) => {
      if (message.type === "query.result" && Array.isArray(message.result)) {
        latestRows = message.result;
      }
    });

    const direct = await client.reducer(refs.send, { body: "permission-equivalent reducer" });
    const delegated = await client.action(refs.agentInvoke, {
      path: refs.send.path,
      args: { body: "permission-equivalent reducer" },
      artifactHash,
    });
    assert.deepEqual(Object.keys(delegated).sort(), Object.keys(direct).sort());
    assert.equal(delegated.body, direct.body);
    assert.equal(delegated.author, direct.author);

    const queried = await client.action(refs.agentInvoke, {
      path: refs.get.path,
      args: { id: delegated.id },
      artifactHash,
    });
    assert.equal(queried.length, 1);
    assert.equal(queried[0].id, delegated.id);

    const echoed = await client.action(refs.agentInvoke, {
      path: refs.echo.path,
      args: { message: "interactive action" },
      artifactHash,
    });
    assert.deepEqual(echoed, { message: "interactive action" });

    await assert.rejects(
      client.action(refs.agentInvoke, {
        path: refs.agentInvoke.path,
        args: { path: refs.echo.path, args: { message: "forbidden" }, artifactHash },
        artifactHash,
      }),
      /not classified as interactive/,
    );
    await assert.rejects(
      client.action(refs.agentInvoke, {
        path: refs.send.path,
        args: { body: 42 },
        artifactHash,
      }),
      /arguments do not match its schema/,
    );
    await assert.rejects(
      client.action(refs.agentInvoke, {
        path: refs.echo.path,
        args: { message: "stale" },
        artifactHash: "0".repeat(64),
      }),
      /STALE_AGENT_CATALOG/,
    );

    const rows = await waitFor(
      () => latestRows.some((row) => row.id === direct.id)
        && latestRows.some((row) => row.id === delegated.id)
        && latestRows,
      10_000,
      "Live Query did not observe both Reducers",
    );
    unsubscribe();
    assert(rows.some((row) => row.id === direct.id));
    assert(rows.some((row) => row.id === delegated.id));
    assert(client.localReplica.entity("messages", delegated.id));

    let reloadedArtifactHash;
    if (process.env.GONVEX_TEST_WAIT_FOR_HMR === "1") {
      const connectionCount = client.connectionState().connectionCount;
      reloadedArtifactHash = await waitFor(
        () => client.activeArtifactHash() !== artifactHash && client.activeArtifactHash(),
        15_000,
        "module artifact hash did not change",
      );
      assert.equal(client.connectionState().connectionCount, connectionCount);
      assert.equal(client.connectionState().isWebSocketConnected, true);
    }

    process.stdout.write(
      JSON.stringify({ artifactHash, reloadedArtifactHash, direct, delegated }) + "\n",
    );
  } finally {
    client.close();
  }
});

function required(name) {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

async function waitFor(read, timeoutMs, failure) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = read();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(failure);
}
