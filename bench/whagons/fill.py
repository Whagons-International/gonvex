#!/usr/bin/env python3
"""Populate disposable SQLite tenant files to the target task counts."""
import json, sys, uuid
from sqlitelab import require_stopped, tenant_db, finalize, columns, ident

require_stopped()
state = sys.argv[1]
data = json.load(open(state))
for f in data['fixtures']:
    tenant = str(uuid.UUID(f['tenant']))
    db = tenant_db(tenant)
    count = int(f['taskCount'])
    completed = str(uuid.uuid5(uuid.NAMESPACE_URL, tenant + '/completed'))
    db.execute('BEGIN IMMEDIATE')
    cols = columns(db, 'tasks')
    override = {'_id': "'synthetic-load-' || n", 'id': '1000000 + n', 'name': "'Synthetic load task ' || n"}
    select = ', '.join(override.get(c, f't.{ident(c)}') for c in cols)
    if not db.execute("SELECT 1 FROM tasks WHERE \"_id\" = 'synthetic-load-1'").fetchone():
        db.execute(f"""WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < ?)
            INSERT OR IGNORE INTO tasks ({', '.join(map(ident, cols))})
            SELECT {select} FROM (SELECT * FROM tasks LIMIT 1) AS t CROSS JOIN s""", (count - 50,))
    scols = columns(db, 'statuses')
    soverride = {'_id': '?1', 'name': "'Synthetic completed'", 'action': "'COMPLETED'", 'initial': '0', 'final': '1'}
    sselect = ', '.join(soverride.get(c, f's.{ident(c)}') for c in scols)
    db.execute(f"INSERT OR IGNORE INTO statuses ({', '.join(map(ident, scols))}) SELECT {sselect} FROM statuses AS s LIMIT 1", (completed,))
    db.execute('UPDATE "statusTransitionGroups" SET "allowAnyTransition" = 1')
    finalize(db, 'load-lab-fill')
    db.execute('COMMIT')
    db.execute('ANALYZE')
    f['completedStatusId'] = completed
    actual = db.execute('SELECT count(*) FROM tasks').fetchone()[0]
    if actual != count:
        raise ValueError(f'fixture expected {count}, has {actual}')
    print(json.dumps({'tasks': actual, 'databaseVerified': True}), flush=True)
json.dump(data, open(state, 'w'))
