#!/usr/bin/env python3
"""crontable.py PROFILE.json[.gz] [EARLIER.json[.gz]]: per-cron table (delta when two dumps)."""
import gzip, json, sys
def load(p):
    return json.load(gzip.open(p) if p.endswith(".gz") else open(p))
b = load(sys.argv[1]); a = load(sys.argv[2]) if len(sys.argv) > 2 else None
F = b["crons"]["fields"]
prev = {r["name"]: dict(zip(F, r["values"])) for r in (a["crons"]["rows"] if a else [])}
print(f"{'cron':<48} {'runs':>5} {'err':>4} {'skip':>4} {'mean ms':>8} {'max ms':>8} {'V8 ms':>7} {'hold ms':>8} {'maxhold':>8} {'calls':>6} {'read':>8} {'written':>8} {'revs':>5} {'fanout':>6} {'delay s':>8} {'maxdly s':>8}")
tot = {}
for r in sorted(b["crons"]["rows"], key=lambda r: r["name"]):
    v = dict(zip(F, r["values"])); p = prev.get(r["name"], {})
    d = {k: v[k] - (0 if k.startswith("max") else p.get(k, 0)) for k in F}
    n = d["invocations"]
    if n <= 0: continue
    for k in F: tot[k] = tot.get(k, 0) + (d[k] if not k.startswith("max") else 0)
    print(f"{r['name']:<48} {n:>5} {d['errors']:>4} {d['skipped']:>4} {d['totalNs']/n/1e6:>8.1f} {v['maxNs']/1e6:>8.1f} {d['invokeNs']/n/1e6:>7.1f} {d['writerHoldNs']/n/1e6:>8.1f} {v['maxWriterHoldNs']/1e6:>8.1f} {d['hostCalls']/n:>6.1f} {d['rowsRead']/n:>8.1f} {d['rowsWritten']/n:>8.1f} {d['revisions']:>5} {d['fanout']:>6} {d['startDelayNs']/n/1e9:>8.2f} {v['maxStartDelayNs']/1e9:>8.2f}")
n = tot.get("invocations", 0)
if n:
    print(f"{'TOTAL':<48} {n:>5} {tot['errors']:>4} {tot['skipped']:>4} {tot['totalNs']/n/1e6:>8.1f} {'':>8} {tot['invokeNs']/n/1e6:>7.1f} {tot['writerHoldNs']/n/1e6:>8.1f} {'':>8} {tot['hostCalls']/n:>6.1f} {tot['rowsRead']/n:>8.1f} {tot['rowsWritten']/n:>8.1f} {tot['revisions']:>5} {tot['fanout']:>6}")
    print(f"sum: total {tot['totalNs']/1e9:.1f} s, V8 {tot['invokeNs']/1e9:.1f} s, writer hold {tot['writerHoldNs']/1e9:.1f} s, rows read {tot['rowsRead']}, rows written {tot['rowsWritten']}")
