#!/usr/bin/env python3
"""Module-host micro-driver for the V8 memory study (docs/performance/v8-memory.md).

Starts one `gonvex-module-host`, loads the real Whagons bundle over the module
host socket protocol, and invokes a heartbeat-sized reducer and a task-update
reducer. Host calls are answered by a small fake host backed by a *copy* of a
lab tenant SQLite database, so reducers read real rows and their writes land in
the copy. It is a component micro-benchmark, not a user simulator.

Usage:
  driver.py --host BIN --manifest manifest.json --tenant-db COPY.db \
      [--host-arg ...] [--env K=V ...] [--calls N] [--second-generation] [--json OUT]

Reports: load (cold start) wall time, RSS/anon/file after load, after warm-up,
after N calls, and with a second generation staged and after activation/reap,
plus per-invoke round-trip p50/p95/p99 and host-call counts per function.
"""

import argparse
import base64
import hashlib
import json
import os
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import time


def proc_mem(pid):
    out = {}
    with open(f"/proc/{pid}/status") as status:
        for line in status:
            key, _, value = line.partition(":")
            if key in ("VmRSS", "RssAnon", "RssFile", "VmHWM", "Threads"):
                out[key] = int(value.split()[0])
    return out


class Host:
    def __init__(self, binary, args, env, socket_path):
        full_env = dict(os.environ)
        full_env.update(env)
        self.proc = subprocess.Popen(
            [binary, "--listen", f"unix:{socket_path}", *args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=open(socket_path + ".log", "w"),
            env=full_env,
        )
        ready = self.proc.stdout.readline()
        if not ready:
            raise SystemExit("module host did not start; see " + socket_path + ".log")
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(socket_path)
        self.next_id = 1
        frame = self.read()
        assert frame["type"] == "ready", frame

    def write(self, value):
        data = json.dumps(value, separators=(",", ":")).encode()
        self.sock.sendall(struct.pack(">I", len(data)) + data)

    def read(self):
        header = self._exact(4)
        (size,) = struct.unpack(">I", header)
        return json.loads(self._exact(size))

    def _exact(self, size):
        chunks = bytearray()
        while len(chunks) < size:
            chunk = self.sock.recv(size - len(chunks))
            if not chunk:
                raise EOFError("module host closed the connection")
            chunks += chunk
        return bytes(chunks)

    def request(self, payload, on_host_call=None):
        request_id = self.next_id
        self.next_id += 1
        self.write({"type": "request", "id": request_id, "payload": payload})
        host_calls = 0
        while True:
            frame = self.read()
            if frame["type"] == "hostCall":
                host_calls += 1
                try:
                    value = on_host_call(frame["payload"])
                    self.write({"type": "hostResponse", "id": frame["id"], "value": value})
                except Exception as error:  # noqa: BLE001 - reported to the module
                    self.write({
                        "type": "hostError",
                        "id": frame["id"],
                        "error": {"code": "host_call_failed", "message": str(error)},
                    })
                continue
            if frame.get("id") != request_id:
                continue
            if frame["type"] == "error":
                raise RuntimeError(json.dumps(frame["error"]))
            return frame["payload"], host_calls

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=20)
        except Exception:  # noqa: BLE001
            self.proc.kill()


class FakeTenant:
    """Answers module host calls from a copied tenant SQLite file."""

    def __init__(self, path):
        # Like the runtime, decode declared BOOLEAN and JSON columns.
        sqlite3.register_converter("BOOLEAN", lambda raw: raw not in (b"0", b""))
        sqlite3.register_converter("JSON", json.loads)
        self.db = sqlite3.connect(
            path, isolation_level=None, detect_types=sqlite3.PARSE_DECLTYPES)
        self.db.row_factory = sqlite3.Row
        # Runtime-registered SQL functions used by the tenant triggers. The
        # benchmark measures the module host, so they are inert here.
        self.db.create_function("gonvex_capture", -1, lambda *args: None)
        self.db.create_function("gonvex_reference_guard", -1, lambda *args: None)
        self.json_columns = {}
        self.counter = 0

    def _json_cols(self, table):
        if table not in self.json_columns:
            cols = set()
            for row in self.db.execute(f'PRAGMA table_info("{table}")'):
                if (row["type"] or "").upper() == "JSON":
                    cols.add(row["name"])
            self.json_columns[table] = cols
        return self.json_columns[table]

    def _row(self, table, row):
        out = dict(row)
        for column in self._json_cols(table) if table else ():
            if isinstance(out.get(column), str):
                try:
                    out[column] = json.loads(out[column])
                except ValueError:
                    pass
        return out

    def handle(self, call):
        kind = call["kind"]
        if kind == "dbQuery":
            params = [
                json.dumps(p) if isinstance(p, (list, dict)) else (int(p) if isinstance(p, bool) else p)
                for p in (call.get("parameters") or [])
            ]
            statement = call["statement"]
            # The runtime binds $1..$n; sqlite3 understands ?NNN.
            import re
            statement = re.sub(r"\$(\d+)", r"?\1", statement)
            rows = self.db.execute(statement, params).fetchall()
            return [dict(row) for row in rows]
        if kind == "dbInsert":
            table, row = call["table"], dict(call["row"])
            if "_id" not in row:
                self.counter += 1
                row["_id"] = call.get("generatedId") or f"bench_{os.getpid()}_{self.counter}"
            row.setdefault("_creationTime", int(time.time() * 1000))
            cols = list(row)
            values = [json.dumps(v) if isinstance(v, (list, dict)) else v for v in row.values()]
            quoted = ",".join(f'"{c}"' for c in cols)
            marks = ",".join("?" for _ in cols)
            inserted = self.db.execute(
                f'INSERT INTO "{table}" ({quoted}) VALUES ({marks}) RETURNING *', values
            ).fetchone()
            return self._row(table, inserted)
        if kind == "dbUpdate":
            table, patch = call["table"], call["patch"]
            key = call.get("key") or "_id"
            sets = ",".join(f'"{c}"=?' for c in patch)
            values = [json.dumps(v) if isinstance(v, (list, dict)) else v for v in patch.values()]
            updated = self.db.execute(
                f'UPDATE "{table}" SET {sets} WHERE "{key}"=? RETURNING *', [*values, call["id"]]
            ).fetchone()
            return self._row(table, updated) if updated else None
        if kind in ("dbDelete", "dbDeleteMany"):
            return None
        if kind == "toolInvoke":
            return {"connected": False, "configured": False}
        if kind in ("actionEnqueue", "scheduleAfter", "scheduleAt"):
            self.counter += 1
            return f"job_{self.counter}"
        raise RuntimeError(f"fake host does not implement {kind}")


def artifact_from_manifest(manifest):
    module = manifest["module"]
    functions = []
    for path, function in sorted(module["functions"].items()):
        metadata = {
            name: function[name]
            for name in (
                "public", "actionProfile", "offline", "optimistic", "interactive",
                "classification", "description", "agent", "actionCapabilities",
            )
            if name in function
        }
        functions.append({
            "path": path,
            "kind": function["kind"],
            "internal": bool(function.get("internal", False)),
            "delivery": function.get("delivery") or None,
            "handler": function.get("handler"),
            "export": function.get("export"),
            "file": function.get("file"),
            "args": function.get("args"),
            "result": function.get("result"),
            "metadata": metadata,
        })
    artifact = {
        "language": module.get("language", "typescript"),
        "entrypoint": module["entrypoint"],
        "hash": module["hash"],
        "javascript": module["javascript"],
        "functions": functions,
    }
    if module.get("reducerBundle"):
        artifact["reducerBundle"] = module["reducerBundle"]
    return artifact


def percentile(values, q):
    ordered = sorted(values)
    if not ordered:
        return None
    index = max(0, min(len(ordered) - 1, int(round(q * len(ordered) + 0.5)) - 1))
    return ordered[index]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--tenant-db", required=True)
    parser.add_argument("--tenant-id", default="8ec9f43e-7adb-4105-af75-1e5dc27e185a")
    parser.add_argument("--host-arg", action="append", default=[])
    parser.add_argument("--env", action="append", default=[])
    parser.add_argument("--calls", type=int, default=500)
    parser.add_argument("--second-generation", action="store_true")
    parser.add_argument("--bundle", help="replace the manifest bundle with this JS file")
    parser.add_argument("--no-split", action="store_true", help="drop reducerBundle from the artifact")
    parser.add_argument("--action", help="also invoke this Action (on-demand isolate in split mode)")
    parser.add_argument("--idle-wait", type=float, default=0.0,
                        help="seconds to wait after the Action before sampling RSS again")
    parser.add_argument("--json")
    options = parser.parse_args()

    manifest = json.load(open(options.manifest))
    artifact = artifact_from_manifest(manifest)
    if options.no_split:
        artifact.pop("reducerBundle", None)
    if options.bundle:
        code = open(options.bundle, "rb").read()
        artifact["javascript"] = {
            "path": artifact["javascript"]["path"],
            "hash": hashlib.sha256(code).hexdigest(),
            "code": base64.b64encode(code).decode(),
        }
    env = dict(item.split("=", 1) for item in options.env)
    tenant = FakeTenant(options.tenant_db)
    member_row = tenant.db.execute(
        "select * from members where status='active' and role='admin' order by id limit 1"
    ).fetchone()
    member = {
        "id": member_row["id"],
        "accountId": member_row["account_id"],
        "status": "active",
        "role": member_row["role"],
        "displayName": member_row["display_name"],
        "permissions": member_row["permissions"] or {},
    }
    tasks = [row[0] for row in tenant.db.execute(
        "select _id from tasks where deletedAt is null order by _id limit 200")]

    result = {"options": vars(options)}
    with tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR")) as scratch:
        socket_path = os.path.join(scratch, "host.sock")
        host = Host(options.host, options.host_arg, env, socket_path)
        pid = host.proc.pid
        result["mem_idle"] = proc_mem(pid)

        load_payload = {"op": "load", "moduleId": "whagons", "generation": 1, "artifact": artifact}
        started = time.perf_counter()
        host.request(load_payload)
        result["load_ms"] = (time.perf_counter() - started) * 1000
        host.request({"op": "activate", "moduleId": "whagons", "generation": 1})
        result["mem_after_load"] = proc_mem(pid)

        def context(command):
            return {
                "projectId": "bench",
                "tenantId": options.tenant_id,
                "tenant": {"id": options.tenant_id, "projectId": "bench"},
                "account": {"id": member["accountId"]},
                "member": member,
                "permissions": member["permissions"],
                "invocation": {
                    "channel": "ui", "rootChannel": "ui", "rootCommandId": command,
                    "commandId": command, "artifactHash": manifest["module"]["hash"],
                    "actorAccountId": member["accountId"], "actorMemberId": member["id"],
                },
                "capabilities": {"dbRead": True, "dbWrite": True, "actionOutbox": True,
                                 "scheduler": True, "functions": True},
                "intentEntropy": hashlib.sha256(command.encode()).hexdigest(),
            }

        def invoke(function, args, index):
            command = f"cmd-{function}-{index}"
            started = time.perf_counter()
            tenant.db.execute("BEGIN")
            try:
                payload, host_calls = host.request(
                    {"op": "invoke", "moduleId": "whagons", "function": function,
                     "kind": "reducer", "args": json.dumps(args), "context": context(command)},
                    tenant.handle,
                )
            finally:
                tenant.db.execute("ROLLBACK")
            return (time.perf_counter() - started) * 1000, host_calls, payload

        workloads = {
            "presence.beat": lambda i: {},
            "tasks.updateDescription": lambda i: {
                "taskId": tasks[i % len(tasks)], "description": f"bench description {i}"},
        }
        # Warm-up: first calls compile lazily-parsed functions.
        result["first_call_ms"] = {}
        for name, args in workloads.items():
            elapsed, calls, payload = invoke(name, args(0), 0)
            result["first_call_ms"][name] = elapsed
            result.setdefault("host_calls", {})[name] = calls
            result.setdefault("sample_result", {})[name] = str(payload)[:200]
        for i in range(1, 20):
            for name, args in workloads.items():
                invoke(name, args(i), i)
        result["mem_after_warmup"] = proc_mem(pid)

        result["latency_ms"] = {}
        for name, args in workloads.items():
            samples = [invoke(name, args(i), i)[0] for i in range(20, 20 + options.calls)]
            result["latency_ms"][name] = {
                "p50": percentile(samples, 0.50), "p95": percentile(samples, 0.95),
                "p99": percentile(samples, 0.99), "mean": sum(samples) / len(samples)}
        result["mem_after_calls"] = proc_mem(pid)

        if options.action:
            spec = manifest["module"]["functions"][options.action]
            tools = sorted((spec.get("actionCapabilities") or {}).get("tools", {}))

            def invoke_action(index):
                ctx = context(f"action-{index}")
                ctx["actionTools"] = tools
                ctx["capabilities"] = {"actionTools": True, "actionOutbox": True, "scheduler": True,
                                       "functions": True, "secrets": True, "network": True, "storage": True}
                started = time.perf_counter()
                try:
                    payload, _ = host.request(
                        {"op": "invoke", "moduleId": "whagons", "function": options.action,
                         "kind": "action", "args": "{}", "context": ctx}, tenant.handle)
                    outcome = str(payload)[:160]
                except RuntimeError as error:
                    outcome = "error: " + str(error)[:160]
                return (time.perf_counter() - started) * 1000, outcome

            result["action_first_ms"], result["action_outcome"] = invoke_action(0)
            result["action_second_ms"], _ = invoke_action(1)
            result["mem_action_warm"] = proc_mem(pid)
            for i in range(50):
                invoke("presence.beat", {}, 20_000 + i)
            if options.idle_wait:
                time.sleep(options.idle_wait)
                result["mem_after_action_idle"] = proc_mem(pid)

        if options.second_generation:
            load_payload["generation"] = 2
            started = time.perf_counter()
            host.request(load_payload)
            result["load2_ms"] = (time.perf_counter() - started) * 1000
            result["mem_two_generations"] = proc_mem(pid)
            host.request({"op": "activate", "moduleId": "whagons", "generation": 2, "drainMs": 5000})
            for i in range(20):
                invoke("presence.beat", {}, 10_000 + i)
            time.sleep(1.0)
            result["mem_after_activate_gen2"] = proc_mem(pid)
        host.close()
        result["host_log_tail"] = open(socket_path + ".log").read()[-3000:]

    text = json.dumps(result, indent=1)
    if options.json:
        open(options.json, "w").write(text)
    print(text)


if __name__ == "__main__":
    sys.exit(main())
