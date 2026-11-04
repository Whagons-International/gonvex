#!/usr/bin/env bash
# Off-CPU flamegraph weighted by blocked time, without root or tracepoints.
#   bench/profiler/offcpu.sh <pid> [seconds=30] [out=offcpu-<pid>.svg]
# Needs perf (perf_event_paranoid <= 1, same uid) and inferno. One session:
# the `cs` software event samples the stack of every thread of <pid> as it
# switches out, and --switch-events records each thread's switch-in. Per thread,
# blocked time = next switch-in - switch-out, added to the switch-out stack
# (rooted at the thread name). Tracepoint data (sched:sched_switch) does not
# decode on this host ("broken or missing trace data"), hence this form.
# A binary inside a container resolves after `perf buildid-cache -a <host copy>`.
set -euo pipefail
pid=${1:?usage: offcpu.sh <pid> [seconds] [out.svg]}
seconds=${2:-30}
out=${3:-offcpu-$pid.svg}
folded="${out%.svg}.folded"
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
perf record -q -e cs --call-graph "${PERF_CALLGRAPH:-dwarf,16384}" --switch-events -p "$pid" -o "$work/perf.data" -- sleep "$seconds"
perf script -i "$work/perf.data" ${PERF_SYMFS:+--symfs=$PERF_SYMFS --kallsyms=/proc/kallsyms} --show-switch-events -F comm,tid,time,event,ip,sym 2>/dev/null > "$work/out.txt"
# PERF_KEEP_SWITCHES=1 keeps the per-switch text (timeline of each block) next to the svg.
[ -z "${PERF_KEEP_SWITCHES:-}" ] || cp "$work/out.txt" "${out%.svg}.switches.txt"
python3 - "$work/out.txt" > "$folded" <<'PY'
import collections, re, sys
head = re.compile(r'^(\S.*?)\s+(\d+)\s+([\d.]+):\s+(.*)$')
out = {}      # tid -> (time, stack)
totals = collections.Counter()
current = None
def finish():
    global current
    if current:
        comm, tid, t, frames = current
        out[tid] = (t, [comm] + list(reversed(frames)))
    current = None
for line in open(sys.argv[1]):
    m = head.match(line)
    if m:
        finish()
        comm, tid, t, rest = re.sub(r'\s+', '_', m.group(1)), int(m.group(2)), float(m.group(3)), m.group(4)
        if rest.startswith('PERF_RECORD_SWITCH IN') or rest.startswith('PERF_RECORD_SWITCH_CPU_WIDE IN'):
            prev = out.pop(tid, None)
            if prev:
                us = int((t - prev[0]) * 1e6)
                if us > 0: totals[';'.join(prev[1])] += us
        elif rest.startswith('cs:'):
            current = (comm, tid, t, [])
    elif current and line.strip():
        parts = line.strip().split(None, 1)
        if len(parts) == 2: current[3].append(re.sub(r'\+0x[0-9a-f]+$', '', parts[1]).replace(';', ':'))
    elif not line.strip():
        finish()
finish()
for stack, us in totals.items(): print(stack, us)
PY
inferno-flamegraph --title "off-CPU (blocked time) pid $pid (${seconds}s)" --countname us < "$folded" > "$out"
echo "wrote $out and $folded"
