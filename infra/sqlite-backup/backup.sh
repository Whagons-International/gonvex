#!/bin/sh
# Snapshot every SQLite database under GONVEX_DATA_DIR into
# BACKUP_DIR/gonvex-<UTC stamp>/, mirroring relative paths.
#
# Consistency: each file is a transactionally consistent snapshot taken with
# SQLite's online backup API (`.backup`), and each copy must pass
# `PRAGMA integrity_check` before the run is published. Files are copied one
# after another, so the set is NOT one cross-file point in time: a tenant file
# can be a few seconds newer than control.db. Restore by stopping the runtime
# and copying a whole run directory back into the data directory.
#
# Continuous replication (Litestream) is the documented follow-up; see
# apps/docs/content/docs/deployment.mdx.
set -eu
umask 077
DATA_DIR="${GONVEX_DATA_DIR:-/data}"
BACKUP_DIR="${BACKUP_DIR:-/backups}"
RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-14}"
INTERVAL_SECONDS="${BACKUP_INTERVAL_SECONDS:-86400}"

run_once() {
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  partial="$BACKUP_DIR/gonvex-$stamp.partial"
  final="$BACKUP_DIR/gonvex-$stamp"
  rm -rf "$partial"
  mkdir -p "$partial"
  count=0
  # Only main database files; -wal/-shm are folded in by the backup API.
  for db in $(cd "$DATA_DIR" && find . -type f -name '*.db' | sort); do
    target="$partial/${db#./}"
    mkdir -p "$(dirname "$target")"
    sqlite3 "$DATA_DIR/${db#./}" ".timeout 30000" ".backup '$target'"
    result="$(sqlite3 "$target" 'PRAGMA integrity_check;')"
    if [ "$result" != "ok" ]; then
      echo "integrity_check failed for $db: $result" >&2
      return 1
    fi
    count=$((count + 1))
  done
  if [ "$count" -eq 0 ]; then
    echo "no SQLite databases found under $DATA_DIR" >&2
    rm -rf "$partial"
    return 1
  fi
  mv "$partial" "$final"
  echo "backed up $count database(s) to $final"
  find "$BACKUP_DIR" -mindepth 1 -maxdepth 1 -type d -name 'gonvex-*' ! -name '*.partial' \
    -mtime +"$RETENTION_DAYS" -exec rm -rf {} +
}

# Healthy when the newest published run holds a control.db that opens and
# passes a quick check.
check() {
  latest="$(ls -1d "$BACKUP_DIR"/gonvex-* 2>/dev/null | grep -v '\.partial$' | sort | tail -n 1)"
  test -n "$latest"
  test -f "$latest/control.db"
  test "$(sqlite3 "$latest/control.db" 'PRAGMA quick_check;')" = "ok"
}

case "${1:-loop}" in
  once) run_once ;;
  check) check ;;
  loop)
    while true; do
      if run_once; then
        sleep "$INTERVAL_SECONDS"
      else
        echo "backup run failed; retrying in 5 minutes" >&2
        sleep 300
      fi
    done
    ;;
  *) echo "usage: gonvex-sqlite-backup [loop|once|check]" >&2; exit 2 ;;
esac
