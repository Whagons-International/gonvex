"""Pinned Verus execution and negative-control checks for plain Rust policies."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time

VERSION = "0.2026.10.18.7d82994"


def item(path, declaration):
    source = path.read_text()
    if source.count(declaration) != 1:
        raise RuntimeError("expected one production declaration: " + declaration)
    start = source.index(declaration)
    end = source.index("\n}", start) + 2
    code = source[start:end]
    if re.search(r"\b(assume|admit|axiom|external_body|external_fn_specification|unsafe)\b", code):
        raise RuntimeError("unexpected trust escape in " + declaration)
    return code


def run(verus, output, root, sources, variants, scope, adapter):
    output.mkdir(parents=True, exist_ok=True)
    version = subprocess.check_output([verus, "--version"], text=True).strip()
    if VERSION not in version:
        raise RuntimeError(f"expected Verus {VERSION}, got {version}")
    hashes = {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in sources}
    adapter_hashes = {path: hashlib.sha256(path.read_bytes()).hexdigest()
                      for path in [adapter, Path(__file__)]}
    receipt = {"verus": version, "sources": hashes, "scope": scope,
               "adapterSha256": hashlib.sha256(adapter.read_bytes()).hexdigest(),
               "runnerSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
               "checks": [], "expectedChecks": len(variants), "success": False}
    for name, code, expected in variants:
        harness = "use vstd::prelude::*;\nverus! {\n" + code + "\n}\n"
        path = output / (name + ".rs")
        path.write_text(harness)
        started = time.monotonic()
        result = subprocess.run([verus, "--edition=2021", "--crate-type=lib", str(path)],
                                capture_output=True, text=True, timeout=300)
        log = result.stdout + result.stderr
        (output / (name + ".log")).write_text(log)
        if expected:
            passed = result.returncode == 0 and re.search(r"[1-9][0-9]* verified, 0 errors", log) is not None
        else:
            passed = result.returncode != 0 and re.search(r"verification results.*[1-9][0-9]* errors", log) is not None
        receipt["checks"].append({"name": name, "expectedToVerify": expected,
                                  "exit": result.returncode, "passed": passed,
                                  "seconds": time.monotonic() - started,
                                  "harnessSha256": hashlib.sha256(harness.encode()).hexdigest(),
                                  "summary": re.findall(r"verification results.*", log)})
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        if not passed:
            raise RuntimeError(name + " had an unexpected result; inspect its log")
    for name, expected in hashes.items():
        if hashlib.sha256((root / name).read_bytes()).hexdigest() != expected:
            raise RuntimeError("production source changed during verification: " + name)
    for path, expected in adapter_hashes.items():
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise RuntimeError("proof adapter changed during verification: " + str(path))
    receipt["success"] = True
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt
