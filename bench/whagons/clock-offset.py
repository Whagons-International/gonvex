#!/usr/bin/env python3
"""Clock offset of the generator host vs this host, over one SSH session.

    clock-offset.py REMOTE [samples=15]  ->  JSON {offsetMs, rttMs, samples, at}

offsetMs = remote clock - local clock at the midpoint of the lowest-RTT round
trip (precision about rttMs / 2). Positive: the generator's clock is ahead."""
import json, subprocess, sys, time

remote = sys.argv[1]
n = int(sys.argv[2]) if len(sys.argv) > 2 else 15
p = subprocess.Popen(['ssh', '-T', remote, "python3 -u -c 'import sys,time\nfor _ in sys.stdin: print(time.time_ns(), flush=True)'"],
                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)
best = None
try:
    for _ in range(n):
        t0 = time.time_ns()
        p.stdin.write('x\n'); p.stdin.flush()
        line = p.stdout.readline()
        t1 = time.time_ns()
        if not line:
            break
        rtt = t1 - t0
        off = int(line) - (t0 + t1) // 2
        if best is None or rtt < best[0]:
            best = (rtt, off)
        time.sleep(0.05)
finally:
    p.stdin.close(); p.wait(timeout=10)
if best is None:
    print(json.dumps({'error': 'no samples', 'at': time.time()})); sys.exit(1)
print(json.dumps({'offsetMs': round(best[1] / 1e6, 2), 'rttMs': round(best[0] / 1e6, 2), 'samples': n, 'at': time.time()}))
