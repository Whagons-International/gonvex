#!/usr/bin/env python3
"""Sample PostgreSQL independently of Gonvex's admission and connection pools.

Run on the Docker host. Credentials stay inside the selected PostgreSQL
container. Records contain session metadata and query hashes, never query text,
bind values, environment variables or connection URLs.
"""

import argparse
import concurrent.futures
import datetime
import json
import logging
import logging.handlers
import math
import os
import subprocess
import time
import urllib.error
import urllib.request


ACTIVITY_SQL = """
SELECT json_build_object(
  'serverTime', clock_timestamp(),
  'connections', (SELECT count(*) FROM pg_stat_activity),
  'states', (SELECT json_agg(s) FROM (
    SELECT datname AS database, state, wait_event_type, wait_event, count(*)
    FROM pg_stat_activity WHERE pid <> pg_backend_pid()
    GROUP BY datname,state,wait_event_type,wait_event
  ) s),
  'sessions', COALESCE((SELECT json_agg(s) FROM (
    SELECT pid, datname AS database, application_name, state,
      wait_event_type, wait_event,
      round(extract(epoch FROM (clock_timestamp()-query_start))*1000) AS query_age_ms,
      round(extract(epoch FROM (clock_timestamp()-xact_start))*1000) AS transaction_age_ms,
      pg_blocking_pids(pid) AS blockers, md5(query) AS query_hash
    FROM pg_stat_activity
    WHERE pid <> pg_backend_pid()
      AND (state IS DISTINCT FROM 'idle' OR cardinality(pg_blocking_pids(pid)) > 0)
      AND backend_type = 'client backend'
    ORDER BY query_start
  ) s), '[]'::json)
)
"""


def database_sample(container, timeout):
    started = time.monotonic()
    # psql authenticates over its container's local socket. Never extract the
    # password, interpolate an operator argument into shell code, or use SQL
    # text from application sessions as an output field.
    command = [
        "docker", "exec", "-e",
        "PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=1000 "
        "-c application_name=gonvex-stall-recorder",
        container, "sh", "-c",
        'exec psql -X -At -v ON_ERROR_STOP=1 -U "${POSTGRES_USER:-postgres}" '
        '-d "${POSTGRES_DB:-postgres}" -c "$1"', "sh", ACTIVITY_SQL,
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            # Database/CLI stderr may contain authentication details.
            sample = {"error": "psql_failed", "exitCode": result.returncode}
        else:
            sample = json.loads(result.stdout)
    except (subprocess.TimeoutExpired, OSError, ValueError) as error:
        sample = {"error": type(error).__name__}
    sample["elapsedMs"] = round((time.monotonic() - started) * 1000, 1)
    return sample


def health_sample(container, timeout):
    started = time.monotonic()
    try:
        # A direct origin URL avoids attributing DNS/proxy delays to Gonvex.
        # Health doesn't issue SQL, so it also works when its pool is full.
        url = runtime_origin(container, timeout)
        remaining = timeout - (time.monotonic() - started)
        if remaining <= 0:
            raise TimeoutError()
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(url + "/healthz", timeout=remaining) as response:
            sample = {"status": response.status}
    except urllib.error.HTTPError as error:
        sample = {"status": error.code}
        error.close()
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        sample = {"error": type(error).__name__}
    sample["elapsedMs"] = round((time.monotonic() - started) * 1000, 1)
    return sample


def runtime_origin(container, timeout):
    result = subprocess.run(
        ["docker", "inspect", "--format",
         "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}", container],
        capture_output=True, text=True, timeout=timeout, check=True,
    )
    addresses = result.stdout.split()
    if not addresses:
        raise ValueError("runtime container has no network address")
    return "http://" + addresses[0] + ":8080"


def record(postgres, runtime, output, samples, interval, timeout, max_bytes):
    os.umask(0o077)
    # Match the deployed Docker log budget: three files, 10 MiB each. Rotation
    # bounds disk usage even if an operator requests a long recording.
    handler = logging.handlers.RotatingFileHandler(
        output, maxBytes=max_bytes, backupCount=2, encoding="utf-8",
    )
    os.chmod(output, 0o600)
    logger = logging.Logger("gonvex-stall-recorder")
    logger.addHandler(handler)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            for index in range(samples):
                started = time.monotonic()
                at = datetime.datetime.now(datetime.timezone.utc).isoformat()
                database = executor.submit(database_sample, postgres, timeout)
                health = executor.submit(health_sample, runtime, timeout)
                logger.info(json.dumps({
                    "time": at, "database": database.result(), "runtime": health.result(),
                }, separators=(",", ":")))
                if index + 1 < samples:
                    time.sleep(max(0, interval - (time.monotonic() - started)))
    finally:
        handler.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-container", required=True)
    parser.add_argument("--runtime-container", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--samples", required=True, type=int)
    # Existing SQLx thresholds are 1 s for statements and 2 s for acquisition.
    # Sample at statement resolution and bound each independent probe at the
    # acquisition warning threshold. These are diagnostics, not query limits.
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--probe-timeout", type=float, default=2)
    parser.add_argument("--max-bytes", type=int, default=10 * 1024 * 1024)
    args = parser.parse_args()
    if (not all(math.isfinite(v) for v in (args.interval, args.probe_timeout))
            or min(args.samples, args.interval, args.probe_timeout, args.max_bytes) <= 0):
        parser.error("samples, interval, probe-timeout and max-bytes must be positive")
    record(args.postgres_container, args.runtime_container, args.output,
           args.samples, args.interval, args.probe_timeout, args.max_bytes)


if __name__ == "__main__":
    main()
