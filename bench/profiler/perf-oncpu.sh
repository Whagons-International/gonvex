#!/usr/bin/env bash
# On-CPU flamegraph of a running process (e.g. the gonvex runtime).
#   bench/profiler/perf-oncpu.sh <pid> [seconds=30] [out=oncpu-<pid>.svg]
# Requires: perf (linux-tools / `pacman -S perf`), and either inferno
# (`cargo install inferno`: inferno-collapse-perf, inferno-flamegraph) or
# Brendan Gregg's FlameGraph scripts (stackcollapse-perf.pl, flamegraph.pl) on PATH.
# Needs kernel.perf_event_paranoid <= 1 (or root) to sample another process; the
# process must run as your uid (the lab runtime runs as LOAD_UID).
# Defaults: 199 Hz (PERF_FREQ), DWARF unwinding (PERF_CALLGRAPH=dwarf,16384; the
# release binary has no frame pointers). A binary inside a container resolves
# after `perf buildid-cache -a <host copy of the binary>`. Build the runtime with debug symbols for readable stacks, e.g.
#   CARGO_PROFILE_RELEASE_DEBUG=line-tables-only cargo build --release -p gonvex-runtime
set -euo pipefail
pid=${1:?usage: perf-oncpu.sh <pid> [seconds] [out.svg]}
seconds=${2:-30}
out=${3:-oncpu-$pid.svg}
command -v perf >/dev/null || { echo "perf not found (install linux perf tools)" >&2; exit 2; }
if command -v inferno-collapse-perf >/dev/null && command -v inferno-flamegraph >/dev/null; then
  collapse=inferno-collapse-perf; graph=inferno-flamegraph
elif command -v stackcollapse-perf.pl >/dev/null && command -v flamegraph.pl >/dev/null; then
  collapse=stackcollapse-perf.pl; graph=flamegraph.pl
else
  echo "need inferno (cargo install inferno) or FlameGraph scripts on PATH" >&2; exit 2
fi
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
perf record -q -F "${PERF_FREQ:-199}" --call-graph "${PERF_CALLGRAPH:-dwarf,16384}" -p "$pid" -o "$work/perf.data" -- sleep "$seconds"
# PERF_SYMFS: a directory mirroring the target's mount namespace (container
# paths such as /binhost/gonvex-runtime), so user frames resolve and unwind.
symfs=${PERF_SYMFS:+--symfs=$PERF_SYMFS --kallsyms=/proc/kallsyms}
perf script -i "$work/perf.data" $symfs | $collapse > "${out%.svg}.folded"
[ -z "${PERF_KEEP_DATA:-}" ] || cp "$work/perf.data" "${out%.svg}.perf.data"
$graph --title "on-CPU pid $pid (${seconds}s)" < "${out%.svg}.folded" > "$out"
echo "wrote $out and ${out%.svg}.folded"
