import json
from pathlib import Path
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import unittest
from urllib.error import HTTPError

from run import Collector, launch_phase


class NativeLaunchTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.bundle_requested, self.release_bundle = threading.Event(), threading.Event()
        self.allow_manifest, self.bundle_status = True, 200
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/":
                    self.send_response(200)
                    self.end_headers()
                    value = {"launchAsset": {"url": owner.url + "/index.bundle?platform=android"}} if owner.allow_manifest else {}
                    self.wfile.write(json.dumps(value).encode())
                else:
                    owner.bundle_requested.set()
                    owner.release_bundle.wait(2)
                    self.send_response(owner.bundle_status)
                    self.end_headers()
                    self.wfile.write(b"actual complete Android launch asset")

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.worker.start()

    def tearDown(self):
        self.release_bundle.set()
        self.server.shutdown()
        self.server.server_close()
        self.worker.join()
        self.directory.cleanup()

    def run_launch(self, opened):
        class Device:
            def open(_self, url):
                opened.append(url)

            def pids(_self):
                return {"42"}

        class CollectorStub:
            output, run_id, events = Path(self.directory.name), "owned-run", []

            def wait(_self, phase, start, process, **kwargs):
                self.assertEqual(phase, "historical-replica-held")
                self.assertGreater(kwargs["timeout"], 0)
                self.assertLessEqual(kwargs["timeout"], 1)
                self.assertIsInstance(kwargs["device"], Device)
                self.assertEqual(kwargs["process_pids"], {"42"})
                return ["native evidence is still required"]

        return launch_phase(Device(), CollectorStub(), "historical-replica-held",
                            {"bundleSha256": "source"}, object(), self.url, timeout=1)

    def test_cold_start_waits_for_complete_bundle_and_keeps_experience_identity(self):
        opened, result, failures = [], [], []

        def launch():
            try:
                result.extend(self.run_launch(opened))
            except Exception as error:
                failures.append(error)

        thread = threading.Thread(target=launch)
        thread.start()
        self.assertTrue(self.bundle_requested.wait(1))
        self.assertEqual(opened, [], "the native process must not race bundle generation")
        self.release_bundle.set()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(opened, [self.url.replace("http://", "exp://", 1)])
        self.assertEqual(result, ["native evidence is still required"])
        receipt, started = [json.loads(line) for line in Path(self.directory.name, "launch.ndjson").read_text().splitlines()]
        self.assertEqual(started["startedPids"], ["42"])
        self.assertEqual(receipt["phase"], "historical-replica-held")
        self.assertGreater(receipt["launchAssetBytes"], 0)

    def test_missing_manifest_asset_does_not_launch_or_count_as_native_success(self):
        self.allow_manifest = False
        opened = []
        with self.assertRaisesRegex(AssertionError, "Android launch asset"):
            self.run_launch(opened)
        self.assertEqual(opened, [])

    def test_bundle_failure_never_retries_or_launches_a_native_phase(self):
        self.bundle_status = 503
        self.release_bundle.set()
        opened = []
        with self.assertRaises(HTTPError) as failure:
            self.run_launch(opened)
        self.assertEqual(failure.exception.code, 503)
        failure.exception.close()
        self.assertEqual(opened, [])
        self.assertFalse(Path(self.directory.name, "launch.ndjson").exists())

    def test_process_exit_is_a_failure_before_any_storage_observation(self):
        collector = Collector(0, Path(self.directory.name), "owned-run")

        class Metro:
            def poll(self):
                return None

        class DeadDevice:
            def pids(self):
                return {"telemetry-restart"}

        try:
            with self.assertRaisesRegex(AssertionError, "native process exited"):
                collector.wait("historical-replica-held", 0, Metro(), timeout=1,
                               device=DeadDevice(), process_pids={"launched-process"})
        finally:
            collector.server.shutdown()
            collector.server.server_close()
