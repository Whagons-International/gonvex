#!/usr/bin/env python3
"""Run storage contracts in an already-booted, dedicated Android test device."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.request
import uuid

from contract import TERMINAL, require, validate_phase

PROJECT = Path(__file__).resolve().parent
ROOT = PROJECT.parent.parent
PACKAGE = "host.exp.exponent"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Device:
    def __init__(self, serial, output, driver_file):
        self.serial, self.output = serial, output
        self.adb = [os.environ.get("ADB", "adb"), "-s", serial]
        self.driver = None
        if driver_file:
            driver = json.loads(driver_file.read_text())
            driver = driver.get("agentDevice", driver)
            args = driver.get("targetArgs", driver.get("args", []))
            require("--serial" in args and args[args.index("--serial") + 1] == serial, "agent-device serial must match")
            self.driver = [driver["command"], *args]
        else:
            require(os.environ.get("CI") == "true", "local runs require --agent-device-json from device_open")

    def command(self, args, timeout=60, check=True):
        result = subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        with (self.output / "device.log").open("a") as log:
            log.write(json.dumps({"command": args, "exit": result.returncode, "output": result.stdout}) + "\n")
        if check and result.returncode:
            raise RuntimeError(result.stdout)
        return result

    def installed(self):
        return "package:" in self.command([*self.adb, "shell", "pm", "path", PACKAGE], check=False).stdout

    def install(self, apk):
        require(not self.installed(), "test Expo Go is already installed; refuse to replace an existing host")
        if self.driver:
            self.command([*self.driver, "install", PACKAGE, str(apk)], timeout=180)
        else:
            self.command([*self.adb, "install", str(apk)], timeout=180)
        require(self.installed(), "test APK installation was not confirmed")

    def open(self, url):
        if self.driver:
            self.command([*self.driver, "open", PACKAGE, url], timeout=90)
        else:
            self.command([*self.adb, "shell", "am", "start", "-W", "-a", "android.intent.action.VIEW", "-d", url, "-p", PACKAGE], timeout=90)

    def pids(self):
        result = self.command([*self.adb, "shell", "pidof", PACKAGE], check=False)
        require(result.returncode in (0, 1), "could not inspect native process")
        return set(result.stdout.strip().split())

    def stop(self):
        before = self.pids()
        require(before, "native process was not running before fault injection")
        if self.driver:
            result = self.command([*self.driver, "close", PACKAGE], check=False)
            require(result.returncode == 0 or "process remained running after close" in result.stdout, "agent-device close failed")
        else:
            self.command([*self.adb, "shell", "am", "force-stop", PACKAGE])
        end = time.monotonic() + 15
        while True:
            after = self.pids()
            if not before.intersection(after):
                break
            require(time.monotonic() < end, "old native process survived stop")
            time.sleep(0.1)
        # A telemetry service can restart the package immediately. What matters
        # is that the old JS/SQLite process actually died, not an empty pidof.
        logs = self.command([*self.adb, "logcat", "-d", "-b", "events", "-v", "epoch"]).stdout
        with (self.output / "process-events.log").open("a") as stream:
            stream.write("\n".join(line for line in logs.splitlines() if PACKAGE in line) + "\n")
        return {"beforePids": sorted(before), "afterStopPids": sorted(after), "oldProcessesGone": True}

    def uninstall(self):
        if self.driver:
            self.command([*self.driver, "uninstall", PACKAGE])
        else:
            self.command([*self.adb, "uninstall", PACKAGE])
        require(not self.installed(), "test APK cleanup failed")


class Collector:
    def __init__(self, port, output, run_id):
        self.output, self.run_id = output, run_id
        self.events, self.config, self.failures = [], {}, []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path != "/config":
                    self.send_error(404)
                    return
                payload = json.dumps(owner.config).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(payload)

            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                if self.path != "/event" or not 0 < length < 2_000_000:
                    self.send_error(400)
                    return
                try:
                    event = json.loads(self.rfile.read(length))
                    require(isinstance(event, dict), "native event must be an object")
                except (ValueError, AssertionError) as error:
                    owner.failures.append({"type": "invalid-event", "message": str(error)})
                    self.send_error(400)
                    return
                if event.get("type") == "fatal":
                    owner.failures.append(event)
                elif any(event.get(key) != owner.config.get(key) for key in ("runId", "phase", "bundleSha256")):
                    owner.failures.append({"type": "foreign-event", "event": event})
                with (owner.output / "events.ndjson").open("a") as log:
                    log.write(json.dumps(event) + "\n")
                owner.events.append(event)
                print(json.dumps({key: event[key] for key in ("type", "phase", "variant", "case") if key in event}), flush=True)
                self.send_response(200)
                self.end_headers()
                try:
                    self.wfile.write(b"{}")
                except BrokenPipeError:
                    # The host intentionally kills the process at a posted
                    # write witness, potentially before its HTTP reply arrives.
                    pass

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def wait(self, phase, start, process, timeout=120):
        end = time.monotonic() + timeout
        while True:
            require(not self.failures, "native fixture error: " + json.dumps(self.failures))
            require(process.poll() is None, "Metro exited before native observation")
            events = self.events[start:]
            if any(event.get("type") == TERMINAL[phase] for event in events):
                return events
            require(time.monotonic() < end, f"native phase {phase} timed out; last event: {events[-1:]}")
            time.sleep(0.1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--serial", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--agent-device-json", type=Path)
    parser.add_argument("--apk", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    output = args.output.resolve()
    tool = json.loads((PROJECT / "toolchain.json").read_text())
    metro_port, collector_port = free_port(), free_port()
    require(metro_port != collector_port, "port allocation collision")
    run_id = uuid.uuid4().hex
    device = Device(args.serial, output, args.agent_device_json)
    require(not device.installed(), "refuse to replace an existing Expo Go host")
    apk = args.apk or output / "expo-go.apk"
    if not args.apk:
        urllib.request.urlretrieve(tool["expoGo"]["url"], apk)
    require(hashlib.sha256(apk.read_bytes()).hexdigest() == tool["expoGo"]["sha256"], "Expo Go checksum mismatch")
    environment = dict(os.environ, GONVEX_NATIVE_COLLECTOR_PORT=str(collector_port))
    subprocess.run(["node", str(PROJECT / "prepare.mjs")], cwd=ROOT, env=environment, check=True)
    source = json.loads((PROJECT / ".work/source.json").read_text())
    (output / "source.json").write_text(json.dumps(source, indent=2) + "\n")
    cache = output / "cache"
    cache.mkdir()
    environment.update(CI="1", EXPO_OFFLINE="1", EXPO_NO_TELEMETRY="1", EXPO_NO_TYPESCRIPT_SETUP="1", EXPO_NO_DEPENDENCY_VALIDATION="1", TMPDIR=str(cache))
    collector = Collector(collector_port, output, run_id)
    metro, installation_attempted, reverse = None, False, []
    receipt = {"complete": False, "runId": run_id, "source": source, "toolchain": tool, "phases": {}, "processStops": []}
    try:
        installation_attempted = True
        device.install(apk)
        for port in (metro_port, collector_port):
            device.command([*device.adb, "reverse", f"tcp:{port}", f"tcp:{port}"])
            reverse.append(port)
        with (output / "metro.log").open("w") as log:
            metro = subprocess.Popen(["node", str(PROJECT / "node_modules/expo/bin/cli"), "start", "--localhost", "--port", str(metro_port), "--max-workers", "1"], cwd=PROJECT, env=environment, stdout=log, stderr=log)
        end = time.monotonic() + 60
        while True:
            require(metro.poll() is None, "Metro startup failed")
            try:
                if urllib.request.urlopen(f"http://127.0.0.1:{metro_port}/status", timeout=1).read() == b"packager-status:running":
                    break
            except OSError:
                pass
            require(time.monotonic() < end, "Metro did not become ready")
            time.sleep(0.1)
        old = set()
        for phase in TERMINAL:
            if receipt["phases"]:
                stop = device.stop()
                receipt["processStops"].append(stop)
                old = set(stop["beforePids"])
            collector.config = {"runId": run_id, "phase": phase, "bundleSha256": source["bundleSha256"]}
            start = len(collector.events)
            device.open(f"exp://127.0.0.1:{metro_port}")
            events = collector.wait(phase, start, metro)
            result = validate_phase(events, phase, run_id, source["bundleSha256"], tool)
            current = device.pids()
            require(current and not current.intersection(old), "native phase reused a stopped process")
            receipt["phases"][phase] = {"pids": sorted(current), "result": result}
        receipt["testsPassed"] = True
    finally:
        try:
            if installation_attempted and device.installed():
                device.uninstall()
                receipt["testAppRemoved"] = True
        finally:
            for port in reverse:
                device.command([*device.adb, "reverse", "--remove", f"tcp:{port}"], check=False)
            if metro and metro.poll() is None:
                metro.terminate()
                try:
                    metro.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    metro.kill()
                    metro.wait()
            collector.server.shutdown()
            receipt["complete"] = bool(receipt.get("testsPassed") and receipt.get("testAppRemoved"))
            (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    require(receipt["complete"] and receipt.get("testAppRemoved"), "native verification or cleanup incomplete")
    print("Native SQLite: 3 counterexamples, 6 fixed cases, witnessed process recovery and restart passed")


if __name__ == "__main__":
    main()
