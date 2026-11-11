#!/usr/bin/env python3
"""Prints a scenario result's timeline (renewals collapsed) and summary line."""
import json, sys
for path in sys.argv[1:]:
    d = json.load(open(path))
    s = d["summary"]
    print(f"== {path.split('/')[-1]}: {s['status']} attempts={s['attempts']} effects={s['effects']} applied_total={s['appliedTotalOnTenant']} renew={s['renewGapMs']} total={s['totalS']}s")
    t0 = d["events"][0]["at"] if d["events"] else 0
    for e in d["events"]:
        if e["kind"] != "renewed":
            print(f"   {(e['at'] - t0) / 1000:8.1f}s {e['kind']:<18} a={e['attempt']} {(e['detail'] or '')[:110]}")
    if s.get("lastError"): print("   last_error:", s["lastError"][:200])
