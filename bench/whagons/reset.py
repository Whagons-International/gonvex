#!/usr/bin/env python3
"""Reset only exercised synthetic task state between comparison steps."""
import json, sys, uuid
from sqlitelab import require_stopped, tenant_db, finalize

require_stopped()
state = json.load(open(sys.argv[1]))
for f in state['fixtures']:
    db = tenant_db(str(uuid.UUID(f['tenant'])))
    db.execute('BEGIN IMMEDIATE')
    db.execute("""UPDATE tasks SET "statusId" = ?1, "completedAt" = NULL, "firstViewedAt" = NULL,
                  "updatedAt" = CAST(unixepoch('subsec') * 1000 AS INTEGER)
                  WHERE "workspaceId" = ?2 AND ("statusId" IS NOT ?1 OR "completedAt" IS NOT NULL OR "firstViewedAt" IS NOT NULL)""",
               (f['statusId'], f['workspaceId']))
    finalize(db, 'load-lab-reset')
    db.execute('COMMIT')
    db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
print('Synthetic task state reset; WAL checkpointed')
