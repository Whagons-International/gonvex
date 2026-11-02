#!/usr/bin/env python3
"""Hold only owned sandbox children at native exit; verify permit conservation."""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
TEST = 'sandbox::retirement_tests::native_child_exit_retains_capacity_until_retirement'
MODES = ['abort-response', 'abort-write', 'cancel-write', 'timeout-write']


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def atomic(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value) + '\n')
    temporary.replace(path)


def await_file(path, process, seconds=10):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if path.exists():
            return json.loads(path.read_text())
        require(process.poll() is None, 'native test ended before coordinator phase ' + path.name)
        time.sleep(0.01)
    raise TimeoutError('native coordinator phase not observed: ' + path.name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-metadata', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--expect-early-release', action='store_true')
    args = parser.parse_args()
    output = args.output.resolve(); output.mkdir(parents=True, exist_ok=False)
    artifacts = [json.loads(line) for line in args.build_metadata.read_text().splitlines() if line.startswith('{')]
    binaries = {Path(row['executable']) for row in artifacts if row.get('reason') == 'compiler-artifact' and row.get('target', {}).get('name') == 'gonvex_runtime' and row.get('executable') and row.get('profile', {}).get('test')}
    require(len(binaries) == 1, 'one native runtime test executable required')
    binary = binaries.pop()
    binary_hash = digest(binary)
    sources = ['rust/crates/runtime/src/sandbox.rs', 'rust/crates/runtime/src/sandbox_cancellation_tests.rs',
               'rust/crates/runtime/src/sandbox_retirement_tests.rs', 'rust/Cargo.lock',
               'verification/backend-receipts/check_sandbox_retirement.py', 'verification/verus/verify_sandbox_ownership.py',
               '.github/workflows/runtime-regression.yml', '.github/workflows/rust-policy-verification.yml']
    source_hashes = {name: digest(ROOT / name) for name in sources}
    environment = dict(os.environ)
    environment.pop('GONVEX_SANDBOX_RETIREMENT_DIR', None)
    missing = subprocess.run([str(binary), TEST, '--ignored', '--exact', '--nocapture', '--test-threads=1'], env=environment,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=20)
    (output / 'missing-coordinator.log').write_text(missing.stdout)
    require(missing.returncode != 0 and 'native sandbox retirement requires its coordinator' in missing.stdout,
            'missing native coordinator was not rejected')
    environment['GONVEX_SANDBOX_RETIREMENT_DIR'] = str(output)
    libc = ctypes.CDLL(None, use_errno=True)
    libc.ptrace.argtypes = [ctypes.c_uint, ctypes.c_uint, ctypes.c_void_p, ctypes.c_void_p]
    libc.ptrace.restype = ctypes.c_long

    def trace(request, pid, data=0):
        if libc.ptrace(request, pid, None, ctypes.c_void_p(data)) == -1:
            raise OSError(ctypes.get_errno(), 'ptrace of verified owned child failed')

    def event(pid, seconds=5):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            waited, status = os.waitpid(pid, os.WNOHANG | os.WUNTRACED)
            if waited:
                return status
            time.sleep(0.01)
        raise TimeoutError('owned native child did not report trace event')

    attached = set(); cases = []; process = None
    try:
        with (output / 'native.log').open('w') as log:
            process = subprocess.Popen([str(binary), TEST, '--ignored', '--exact', '--nocapture', '--test-threads=1'],
                                       env=environment, stdout=log, stderr=subprocess.STDOUT)
            for mode in MODES:
                directory = output / mode
                request = await_file(directory / 'request.json', process)
                pid = request['pid']
                require(request['parent'] == process.pid and request['mode'] == mode, 'native fixture owner mismatch')
                require(isinstance(pid, int) and pid > 1, 'valid owned native child PID required')
                status = Path(f'/proc/{pid}/status').read_text()
                ppid = int(next(line.split(':', 1)[1] for line in status.splitlines() if line.startswith('PPid:')))
                require(ppid == process.pid, 'refuse to trace a child outside the owned native test')
                trace(16, pid); attached.add(pid)
                require(os.WIFSTOPPED(event(pid)), 'owned child did not enter trace stop')
                trace(0x4200, pid, 0x40 | 0x100000)  # TRACEEXIT + EXITKILL
                trace(7, pid)
                atomic(directory / 'attached.json', {'pid': pid, 'verifiedParent': process.pid})
                end = time.monotonic() + 10
                while True:
                    status = event(pid)
                    if os.WIFSTOPPED(status) and status >> 16 == 6:
                        break
                    require(os.WIFSTOPPED(status) and time.monotonic() < end, 'unexpected native child exit protocol')
                    trace(7, pid)
                mappings = Path(f'/proc/{pid}/maps').read_bytes()
                require(bool(mappings.strip()), 'native exit hold did not retain process mappings')
                held = {'pid': pid, 'exitEventHeld': True, 'nativeMappingsRetained': True, 'mappingsSha256': hashlib.sha256(mappings).hexdigest()}
                atomic(directory / 'held.json', held)
                observed = await_file(directory / 'observed.json', process)
                require(observed['pid'] == pid and observed['mode'] == mode, 'wrong native retirement observation')
                if args.expect_early_release:
                    require(observed['availableWhileNativeExitHeld'] == 1 and observed['replacementStartedWhileNativeExitHeld'],
                            'old policy did not reproduce actual overlapping native children')
                else:
                    require(observed['availableWhileNativeExitHeld'] == 0 and not observed['replacementStartedWhileNativeExitHeld'],
                            'sandbox admission passed a held native child exit')
                await_file(directory / 'continue.json', process)
                trace(7, pid)
                status = event(pid)
                require(os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGKILL, 'owned child did not finish its requested kill')
                attached.remove(pid)
                atomic(directory / 'reaped.json', {'pid': pid, 'tracerReaped': True})
                complete = await_file(directory / 'complete.json', process)
                require(complete == {'complete': True, 'replacementProgressedAfterExit': True, 'exactCapacity': True}, 'native replacement or capacity cleanup failed')
                cases.append({'mode': mode, **held, **observed, **complete})
            code = process.wait(timeout=20)
        log = (output / 'native.log').read_text()
        if args.expect_early_release:
            require(code != 0 and 'sandbox admission was reused before native child exit' in log,
                    'baseline did not fail the native-retirement ownership assertion')
        else:
            require(code == 0 and '1 passed; 0 failed; 0 ignored' in log, 'native child retirement test did not pass')
        require(digest(binary) == binary_hash, 'runtime executable changed during native retirement check')
        require(all(digest(ROOT / name) == expected for name, expected in source_hashes.items()), 'native retirement source changed during execution')
        atomic(output / 'receipt.json', {'complete': True, 'expectedEarlyRelease': args.expect_early_release,
               'missingCoordinatorRejected': True, 'runtimeBinarySha256': binary_hash,
               'sources': source_hashes, 'cases': cases,
               'boundary': 'Actual Linux child processes through SandboxManager. An owning ancestor ptracer holds exit with memory mappings retained. Four selected cancellation paths and one physical capacity. Depends on Linux ptrace/kill/wait and live Tokio scheduling; not a whole-runtime process or arbitrary OS failure proof.'})
        print('Owned native child retirement schedules completed: ' + ('four early-release counterexamples' if args.expect_early_release else 'four retained-capacity schedules'))
    finally:
        for pid in list(attached):
            try:
                os.kill(pid, signal.SIGKILL)
                trace(7, pid)
                end = time.monotonic() + 5
                while time.monotonic() < end:
                    status = event(pid)
                    if os.WIFSTOPPED(status): trace(7, pid)
                    else: break
            except (OSError, ChildProcessError, TimeoutError):
                pass
        if process and process.poll() is None:
            process.kill(); process.wait(timeout=10)


if __name__ == '__main__':
    main()
