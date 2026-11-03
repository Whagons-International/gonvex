#!/usr/bin/env bash
# Off-CPU flamegraph (where threads block/wait) of a running process.
#   bench/profiler/offcpu.sh <pid> [seconds=30] [out=offcpu-<pid>.svg]
# Preferred: bpftrace (`pacman -S bpftrace`, root) — sums blocked time per
# stack in-kernel, low overhead. Fallback: perf sched:sched_switch tracepoint
# (counts switches per stack, not time; heavier).
# Plus inferno (cargo install inferno) or FlameGraph scripts for the SVG.
set -euo pipefail
pid=${1:?usage: offcpu.sh <pid> [seconds] [out.svg]}
seconds=${2:-30}
out=${3:-offcpu-$pid.svg}
if command -v inferno-flamegraph >/dev/null; then graph=inferno-flamegraph
elif command -v flamegraph.pl >/dev/null; then graph=flamegraph.pl
else echo "need inferno (cargo install inferno) or flamegraph.pl on PATH" >&2; exit 2; fi
folded="${out%.svg}.folded"
if command -v bpftrace >/dev/null; then
  # Off-CPU time (us) per user+kernel stack for every thread of $pid.
  # offcputime.bt pattern: when a task of $pid is switched out, remember the
  # time; when it runs again (finish_task_switch in its context), add the
  # blocked microseconds to its user+kernel stack.
  sudo bpftrace -e "
    #include <linux/sched.h>
    kprobe:finish_task_switch* {
      \$prev = (struct task_struct *)arg0;
      if (\$prev->tgid == $pid) { @start[\$prev->pid] = nsecs; }
      if (pid == $pid) {
        \$s = @start[tid];
        if (\$s) { @us[ustack(perf), kstack(perf), comm] = sum((nsecs - \$s) / 1000); delete(@start[tid]); }
      }
    }
    interval:s:$seconds { exit(); }
    END { clear(@start); }" > "${out%.svg}.bt" 2>/dev/null
  # bpftrace map lines -> folded "frame;frame;... value"
  python3 - "${out%.svg}.bt" > "$folded" <<'PY'
import re, sys
text = open(sys.argv[1]).read()
for block in re.finditer(r"@us\[(.*?)\]: (\d+)", text, re.S):
    frames = [l.strip().split(" ", 1)[-1].split("+")[0] for l in block.group(1).splitlines() if l.strip() and not l.strip().startswith(",")]
    frames = [f for f in frames if f]
    if frames:
        print(";".join(reversed(frames)), block.group(2))
PY
else
  command -v perf >/dev/null || { echo "need bpftrace or perf" >&2; exit 2; }
  command -v inferno-collapse-perf >/dev/null && collapse=inferno-collapse-perf || collapse=stackcollapse-perf.pl
  work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
  perf record -e sched:sched_switch -g -p "$pid" -o "$work/perf.data" -- sleep "$seconds"
  perf script -i "$work/perf.data" | $collapse > "$folded"
  echo "(perf fallback: weights are context-switch counts, not blocked time)"
fi
$graph --title "off-CPU pid $pid (${seconds}s)" --countname us < "$folded" > "$out"
echo "wrote $out and $folded"
