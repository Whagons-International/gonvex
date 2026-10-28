import importlib.util
import io
import json
import os
from pathlib import Path
import selectors
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "recorder", Path(__file__).with_name("postgres-stall-recorder.py"))
recorder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recorder)
CONTAINER = os.environ.get("GONVEX_STALL_TEST_CONTAINER")


class RecorderTests(unittest.TestCase):
    def test_authentication_errors_do_not_expose_psql_output(self):
        failure = subprocess.CompletedProcess([], 1, "private stdout", "password=private")
        with patch.object(recorder.subprocess, "run", return_value=failure):
            sample = recorder.database_sample("unused", 2)
        self.assertEqual(sample["error"], "psql_failed")
        self.assertNotIn("private", json.dumps(sample))

    def test_runtime_inspection_failure_is_recorded(self):
        with patch.object(recorder, "runtime_origin", side_effect=ValueError("private")):
            sample = recorder.health_sample("unused", 2)
        self.assertEqual(sample["error"], "ValueError")
        self.assertNotIn("private", json.dumps(sample))

    def test_unhealthy_http_response_releases_its_body(self):
        body = io.BytesIO(b"private response")
        error = recorder.urllib.error.HTTPError("http://local", 503, "unhealthy", {}, body)
        with patch.object(recorder, "runtime_origin", return_value="http://local"), \
                patch.object(recorder.urllib.request, "build_opener") as opener:
            opener.return_value.open.side_effect = error
            sample = recorder.health_sample("unused", 2)
        self.assertEqual(sample["status"], 503)
        self.assertTrue(body.closed)
        self.assertNotIn("private", json.dumps(sample))

    def test_rotation_keeps_three_private_files(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "samples.jsonl"
            output.write_text("")
            output.chmod(0o644)
            with patch.object(recorder, "database_sample", return_value={"sessions": []}), \
                    patch.object(recorder, "health_sample", return_value={"status": 200}):
                recorder.record("unused", "unused", output, 10, 0.001, 2, 250)
            files = list(Path(directory).iterdir())
            self.assertEqual(len(files), 3)
            for path in files:
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                for line in path.read_text().splitlines():
                    self.assertEqual(json.loads(line)["runtime"]["status"], 200)

    @unittest.skipUnless(CONTAINER, "requires an isolated local PostgreSQL container")
    def test_real_lock_chain_identifies_blocker_without_query_text(self):
        def command(interactive=False):
            return ["docker", "exec", *(["-i"] if interactive else []), CONTAINER,
                    "psql", "-X", "-At", "-v", "ON_ERROR_STOP=1",
                    "-U", "postgres", "-d", "postgres"]

        def execute(sql):
            return subprocess.run(command() + ["-c", sql], check=True,
                                  capture_output=True, text=True, timeout=5)

        def read_line(process):
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                self.assertTrue(selector.select(5), "psql did not acknowledge the lock")
                return process.stdout.readline().strip()

        table = "gonvex_stall_recorder_test"
        execute(f"CREATE TABLE {table}(value text)")
        holder = waiter = None
        try:
            holder = subprocess.Popen(command(True) + ["-q"], stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            holder.stdin.write(f"BEGIN; LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n")
            holder.stdin.flush()
            self.assertEqual(read_line(holder), "locked")
            waiter = subprocess.Popen(command() + ["-c", f"SELECT 'private-value' FROM {table}"],
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            deadline = time.monotonic() + 5
            blocked = []
            while time.monotonic() < deadline:
                sample = recorder.database_sample(CONTAINER, 2)
                blocked = [s for s in sample.get("sessions", []) if s["blockers"]]
                if blocked:
                    break
                time.sleep(0.05)
            self.assertTrue(blocked, sample)
            victim = blocked[0]
            self.assertEqual(victim["wait_event_type"], "Lock")
            self.assertTrue(any(s["pid"] in victim["blockers"]
                                and s["state"] == "idle in transaction"
                                for s in sample["sessions"]))
            self.assertNotIn("private-value", json.dumps(sample))
            self.assertNotIn(table, json.dumps(sample))
        finally:
            if holder:
                holder.stdin.close()
                holder.wait(timeout=5)
                holder.stdout.close()
                holder.stderr.close()
            if waiter:
                waiter.communicate(timeout=5)
            execute(f"DROP TABLE {table}")


if __name__ == "__main__":
    unittest.main()
