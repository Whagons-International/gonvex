#!/usr/bin/env python3
"""Compare PostgreSQL execution of old/new visibility negation on equal row sets."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess

ROOT = Path(__file__).resolve().parents[2]
BASELINE = "3ad2ec077bd5c6b82d4f76a7a5174ede93fd42b9"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--psql", default=shutil.which("psql"))
    parser.add_argument("--baseline", default=BASELINE)
    parser.add_argument("--repetitions", type=int, default=1)
    args = parser.parse_args()
    url = os.environ.get("GONVEX_TEST_POSTGRES_URL")
    if not args.psql or not url:
        parser.error("set GONVEX_TEST_POSTGRES_URL and provide --psql")
    if args.repetitions < 1:
        parser.error("--repetitions must be positive")
    relative = "rust/crates/runtime/src/visibility.rs"
    before = subprocess.check_output(["git", "show", args.baseline + ":" + relative], cwd=ROOT)
    after = (ROOT / relative).read_bytes()
    script_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if b'"NOT ({})"' not in before or b'"({}) IS NOT TRUE"' not in after:
        raise RuntimeError("negation lowering changed; review benchmark queries")
    cases = [
        {"name": "not_equality", "baseline": "NOT (status = 'blocked')", "current": "status IS DISTINCT FROM 'blocked'"},
        {"name": "double_not", "baseline": "NOT (NOT (status = 'blocked'))", "current": "status = 'blocked'"},
        {"name": "not_conjunction", "baseline": "NOT ((status = 'blocked') AND enabled)", "current": "((status = 'blocked') AND enabled) IS NOT TRUE"},
    ]
    sql = """
SET jit = off;
SET max_parallel_workers_per_gather = 0;
CREATE TEMP TABLE visibility_negation_bench AS
 SELECT n AS id, CASE WHEN n % 2 = 0 THEN 'blocked' ELSE 'allowed' END AS status,
        n % 3 = 0 AS enabled FROM generate_series(1,31000) AS n;
CREATE INDEX ON visibility_negation_bench(status);
ANALYZE visibility_negation_bench;
CREATE TEMP TABLE benchmark_results(case_name text, variant text, elapsed_ms float8, plan jsonb);
DO $timing$
DECLARE cases jsonb := $cases$CASES$cases$::jsonb;
        item jsonb; variant text; variants text[]; statement text; plan jsonb;
        expected bigint; actual bigint; round integer; repetition integer; elapsed float8;
BEGIN
 FOR item IN SELECT * FROM jsonb_array_elements(cases) LOOP
  EXECUTE 'SELECT count(*) FROM visibility_negation_bench WHERE ' || (item->>'baseline') INTO expected;
  EXECUTE 'SELECT count(*) FROM visibility_negation_bench WHERE ' || (item->>'current') INTO actual;
  IF actual <> expected THEN RAISE EXCEPTION 'benchmark row sets differ'; END IF;
 END LOOP;
 FOR round IN 0..8 LOOP
  variants := CASE WHEN round % 2 = 0 THEN ARRAY['baseline','current'] ELSE ARRAY['current','baseline'] END;
  FOR item IN SELECT * FROM jsonb_array_elements(cases) LOOP
   FOREACH variant IN ARRAY variants LOOP
    statement := 'SELECT count(*) FROM visibility_negation_bench WHERE ' || (item->>variant);
    elapsed := 0;
    FOR repetition IN 1..REPETITIONS LOOP
     EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) ' || statement INTO plan;
     elapsed := elapsed + (plan->0->>'Execution Time')::float8;
    END LOOP;
    INSERT INTO benchmark_results VALUES(item->>'name', variant, elapsed / REPETITIONS, plan->0->'Plan');
   END LOOP;
  END LOOP;
 END LOOP;
END $timing$;
SELECT jsonb_build_object('postgres',version(),'samples',jsonb_agg(to_jsonb(r))) FROM benchmark_results AS r;
""".replace("CASES", json.dumps(cases)).replace("REPETITIONS", str(args.repetitions))
    result = subprocess.run([args.psql, "--dbname", url, "-XqAt", "-v", "ON_ERROR_STOP=1"], input=sql,
                            capture_output=True, text=True, timeout=120)
    if result.returncode:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        log = args.output.with_suffix(".log")
        log.write_text(result.stderr.replace(url, "<configured fixture>"))
        log.chmod(0o600)
        raise RuntimeError("PostgreSQL benchmark failed; inspect its private log")
    raw = json.loads(result.stdout)
    samples = {}
    plans = {}
    for row in raw["samples"]:
        samples.setdefault(row["case_name"], {"baseline": [], "current": []})[row["variant"]].append(row["elapsed_ms"])
        plans.setdefault(row["case_name"], {})[row["variant"]] = row["plan"]
    receipt = {
        "baseline": args.baseline, "postgres": raw["postgres"], "rows": 31000,
        "scope": "PostgreSQL execution of explicit templates matching the lowering rules, on a non-null-valued temporary fixture with equal admitted row sets. SQL is not emitted by the Rust compiler in this benchmark. Not runtime throughput, nullable-result cost, TypeScript, or network performance. Other workstation jobs are not paused.",
        "sampling": "One connection; warm-up counts; nine paired rounds of batch means with alternating variant order; JIT and parallel query disabled; EXPLAIN ANALYZE with per-node timing off.",
        "repetitionsPerBatch": args.repetitions,
        "cpuAffinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "source": relative, "baselineSourceSha256": hashlib.sha256(before).hexdigest(),
        "currentSourceSha256": hashlib.sha256(after).hexdigest(),
        "scriptSha256": script_hash,
        "predicates": cases, "samplesMilliseconds": samples,
        "medianMilliseconds": {name: {variant: statistics.median(values) for variant, values in data.items()} for name, data in samples.items()},
        "plans": plans, "success": True,
    }
    if after != (ROOT / relative).read_bytes() or script_hash != hashlib.sha256(Path(__file__).read_bytes()).hexdigest():
        raise RuntimeError("benchmark inputs changed during measurement")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt["medianMilliseconds"]))


if __name__ == "__main__":
    main()
