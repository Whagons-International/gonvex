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
from urllib.parse import urlparse
import uuid

from contract import TERMINAL, historical_transition, require, validate_phase

PROJECT = Path(__file__).resolve().parent
ROOT = PROJECT.parent.parent
PACKAGE = "host.exp.exponent"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Device:
    def __init__(self, serial, output, driver_file, launch):
        self.serial, self.output, self.launch = serial, output, launch
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
            # The pinned APK exports a direct cold-start path. Supplying its
            # manifest URL avoids the launcher's optimistic callback handoff.
            self.command([*self.adb, "shell", "am", "start", "-W",
                          "-n", self.launch["activity"], "-a", "android.intent.action.VIEW",
                          "-d", url, "--es", self.launch["manifestExtra"], url,
                          "--ez", self.launch["optimisticExtra"], "false"], timeout=90)

    def startup_diagnostics(self, phase):
        # Read-only diagnostics must not replace the original phase failure.
        for label, args in [("pids", [*self.adb, "shell", "pidof", PACKAGE]),
                            ("events", [*self.adb, "logcat", "-d", "-b", "events", "-v", "epoch"]),
                            ("activities", [*self.adb, "shell", "dumpsys", "activity", "activities"]),
                            ("logcat", [*self.adb, "logcat", "-d", "-t", "500", "-v", "threadtime"])]:
            try:
                result = self.command(args, check=False)
                value = {"phase": phase, "kind": label, "exit": result.returncode, "output": result.stdout}
            except (OSError, subprocess.TimeoutExpired) as error:
                value = {"phase": phase, "kind": label, "diagnosticError": str(error)}
            with (self.output / "startup.log").open("a") as log:
                log.write(json.dumps(value) + "\n")

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

    def wait(self, phase, start, process, timeout=120, device=None, process_pids=None):
        end = time.monotonic() + timeout
        next_process_check = 0.0
        while True:
            require(not self.failures, "native fixture error: " + json.dumps(self.failures))
            require(process.poll() is None, "Metro exited before native observation")
            events = self.events[start:]
            if any(event.get("type") == TERMINAL[phase] for event in events):
                return events
            if device and time.monotonic() >= next_process_check:
                current = device.pids()
                require(current and (process_pids is None or current.intersection(process_pids)),
                        f"native process exited during phase {phase}; last event: {events[-1:]}")
                next_process_check = time.monotonic() + 1
            require(time.monotonic() < end, f"native phase {phase} timed out; last event: {events[-1:]}")
            time.sleep(0.1)


def prepare_launch(metro_url, deadline):
    """Require the actual Android launch asset, not only Metro's status socket.

    Expo cold-start callbacks must never race a still-building bundle. Keep the
    experience URL unchanged: Expo's SQLite namespace must survive every kill.
    This performs one fetch per resource, not a phase retry.
    """
    def fetch(url, headers):
        remaining = deadline - time.monotonic()
        require(remaining > 0, "native launch readiness exceeded the phase deadline")
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=remaining) as response:
            value = response.read()
        require(time.monotonic() < deadline, "native launch readiness exceeded the phase deadline")
        return value

    manifest_bytes = fetch(metro_url, {"Accept": "application/expo+json", "Expo-Platform": "android",
                                       "Expo-Protocol-Version": "1"})
    manifest = json.loads(manifest_bytes)
    asset_url = manifest.get("launchAsset", {}).get("url")
    require(isinstance(asset_url, str), "Expo manifest did not provide an Android launch asset")
    origin, asset = urlparse(metro_url), urlparse(asset_url)
    require(asset.scheme == "http" and asset.hostname in ("127.0.0.1", "localhost")
            and asset.port == origin.port, "Expo launch asset must belong to the owned Metro server")
    bundle = fetch(asset_url, {"Expo-Platform": "android"})
    require(bundle, "Expo Android launch asset was empty")
    return {"manifestSha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "launchAssetUrl": asset_url, "launchAssetBytes": len(bundle),
            "launchAssetSha256": hashlib.sha256(bundle).hexdigest()}


def launch_phase(device, collector, phase, source, metro, metro_url, timeout=120):
    # The unchanged deadline covers readiness, cold launch and native evidence.
    deadline = time.monotonic() + timeout
    ready = prepare_launch(metro_url, deadline)
    with (collector.output / "launch.ndjson").open("a") as log:
        log.write(json.dumps({"phase": phase, "runId": collector.run_id,
                              "bundleSha256": source["bundleSha256"], **ready}) + "\n")
    start = len(collector.events)
    device.open(metro_url.replace("http://", "exp://", 1))
    started = device.pids()
    require(started, f"native process did not start for phase {phase}")
    with (collector.output / "launch.ndjson").open("a") as log:
        log.write(json.dumps({"phase": phase, "runId": collector.run_id,
                              "startedPids": sorted(started)}) + "\n")
    remaining = deadline - time.monotonic()
    require(remaining > 0, "native cold launch exceeded the phase deadline")
    return collector.wait(phase, start, metro, timeout=remaining, device=device, process_pids=started)


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
    device = Device(args.serial, output, args.agent_device_json, tool["expoGo"]["androidLaunch"])
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
    metro, logcat, installation_attempted, reverse = None, None, False, []
    receipt = {"complete": False, "launchMode": "agent-device" if device.driver else "explicit-experience-cold-start", "runId": run_id, "source": source, "toolchain": tool, "phases": {}, "processStops": []}
    try:
        installation_attempted = True
        device.install(apk)
        # Retain the launch window even when later Wi-Fi/system logs displace
        # a native crash or low-memory kill from logcat's finite ring buffer.
        with (output / "native-logcat.log").open("w") as log:
            logcat = subprocess.Popen([*device.adb, "logcat", "-b", "all", "-v", "threadtime"],
                                      stdout=log, stderr=log)
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
            try:
                events = launch_phase(device, collector, phase, source, metro, f"http://127.0.0.1:{metro_port}")
            except Exception:
                try:
                    device.startup_diagnostics(phase)
                except Exception as error:
                    print(f"Startup diagnostics failed: {error}", flush=True)
                raise
            result = validate_phase(events, phase, run_id, source["bundleSha256"], tool)
            if phase.startswith("historical-"):
                historical_transition({name: value["result"] for name, value in receipt["phases"].items()}, phase, result)
            if phase == "journal-corrupt-recover":
                require(result["rows"] == receipt["phases"]["journal-corrupt-seed"]["result"]["rows"], "corrupt accepted journal bytes changed across process restart")
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
            if logcat and logcat.poll() is None:
                logcat.terminate()
                try:
                    logcat.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    logcat.kill()
                    logcat.wait()
            collector.server.shutdown()
            receipt["complete"] = bool(receipt.get("testsPassed") and receipt.get("testAppRemoved"))
            (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    require(receipt["complete"] and receipt.get("testAppRemoved"), "native verification or cleanup incomplete")
    print("Native SQLite: shared-handle, historical migration, corruption and process-recovery contracts passed")


if __name__ == "__main__":
    main()
