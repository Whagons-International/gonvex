#!/usr/bin/env python3
"""Populate disposable SQLite tenant files to the target task counts."""
import json, sys, uuid
from sqlitelab import require_stopped, tenant_db, finalize, columns, ident
from fixture import structure

# Visibility structure (load-realism audit 2026-11-03): real tenants have several
# teams, workspaces and spots, so masks must evaluate more than "see all".
SPOT_PARENTS, SPOT_CHILDREN = 4, 3
DESCRIPTION = ("Guest reported the issue at the front desk; please check the fixture, confirm the "
               "parts on hand and update the guest before the end of the shift. Notes from the "
               "previous visit: the unit was serviced last month and the warranty covers labour. ")


def copy_row(db, table, source_id, overrides):
    """Inserts a copy of row `source_id` with `overrides` (column -> SQL expression)."""
    cols = columns(db, table)
    select = ', '.join(overrides.get(c, ident(c)) for c in cols)
    db.execute(f'INSERT OR IGNORE INTO {ident(table)} ({", ".join(map(ident, cols))}) '
               f'SELECT {select} FROM {ident(table)} WHERE "_id" = ?', (source_id,))


def structure(db, f):
    """Spots (4 parents x 3 children), teams T1-T3, a second workspace B with its
    own category, and an approval category in workspace A. Workspace A is
    linked to the seed team T0 and T1; B to T2 and T3."""
    for a in range(SPOT_PARENTS):
        db.execute('INSERT OR IGNORE INTO spots ("_id", name, "parentId") VALUES (?, ?, NULL)',
                   (f'load-spot-p{a}', f'Building {a}'))
        for b in range(SPOT_CHILDREN):
            db.execute('INSERT OR IGNORE INTO spots ("_id", name, "parentId") VALUES (?, ?, ?)',
                       (f'load-spot-c{a}{b}', f'Building {a} floor {b}', f'load-spot-p{a}'))
    teams = [f['teamId']] + [f'load-team-{k}' for k in (1, 2, 3)]
    for k, team in enumerate(teams[1:], 1):
        copy_row(db, 'teams', f['teamId'], {'_id': f"'{team}'", 'name': f"'Load team {k}'", 'slug': f"'load-team-{k}'"})
    copy_row(db, 'priorities', f['priorityId'], {'_id': "'load-priority-2'", 'name': "'Load priority 2'"})
    copy_row(db, 'workspaces', f['workspaceId'], {'_id': "'load-ws-b'", 'name': "'Load workspace B'", 'categoryId': "'load-cat-b'"})
    copy_row(db, 'categories', f['categoryId'], {'_id': "'load-cat-b'", 'name': "'Load category B'", 'workspaceId': "'load-ws-b'", 'teamId': f"'{teams[2]}'", 'approvalId': 'NULL'})
    db.execute("""INSERT OR IGNORE INTO approvals ("_id", name, "triggerType", "isActive", "requireAll", "minimumApprovals",
                  "requireRejectionComment", "requireSignature", "blockEditingDuringApproval")
                  VALUES ('load-approval', 'Load approval', 'ON_CREATE', 1, 0, 1, 0, 0, 0)""")
    copy_row(db, 'categories', f['categoryId'], {'_id': "'load-cat-approval'", 'name': "'Load approvals'", 'approvalId': "'load-approval'"})
    for workspace, team in [(f['workspaceId'], teams[1]), ('load-ws-b', teams[2]), ('load-ws-b', teams[3])]:
        if not db.execute('SELECT 1 FROM "workspaceTeams" WHERE "workspaceId" = ? AND "teamId" = ?', (workspace, team)).fetchone():
            db.execute('INSERT INTO "workspaceTeams" ("workspaceId", "teamId") VALUES (?, ?)', (workspace, team))
    # Synthetic tasks: 30% in workspace B, 15% without a spot, the rest spread
    # over the child spots; realistic description width (~400 B).
    db.execute(f"""UPDATE tasks SET
        "workspaceId" = CASE WHEN id % 10 < 3 THEN 'load-ws-b' ELSE "workspaceId" END,
        "categoryId" = CASE WHEN id % 10 < 3 THEN 'load-cat-b' ELSE "categoryId" END,
        "teamId" = CASE WHEN id % 10 < 3 THEN ? ELSE "teamId" END,
        "spotId" = CASE WHEN (id / 10) % 20 < 3 THEN NULL
                        ELSE 'load-spot-c' || ((id / 7) % {SPOT_PARENTS}) || ((id / 3) % {SPOT_CHILDREN}) END,
        description = ? || ' Ref #' || id
        WHERE "_id" LIKE 'synthetic-load-%'""", (teams[2], DESCRIPTION + DESCRIPTION[:120]))
    f.update(priorityIds=[f['priorityId'], 'load-priority-2'], teamIds=teams, workspaceIds=[f['workspaceId'], 'load-ws-b'], categoryIds=[f['categoryId'], 'load-cat-b'],
             approvalCategoryId='load-cat-approval', approvalId='load-approval',
             spotParents=[f'load-spot-p{a}' for a in range(SPOT_PARENTS)],
             spotChildren=[f'load-spot-c{a}{b}' for a in range(SPOT_PARENTS) for b in range(SPOT_CHILDREN)])


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
    structure(db, f)
    scols = columns(db, 'statuses')
    soverride = {'_id': '?1', 'name': "'Synthetic completed'", 'action': "'COMPLETED'", 'initial': '0', 'final': '1'}
    sselect = ', '.join(soverride.get(c, f's.{ident(c)}') for c in scols)
    db.execute(f"INSERT OR IGNORE INTO statuses ({', '.join(map(ident, scols))}) SELECT {sselect} FROM statuses AS s LIMIT 1", (completed,))
    db.execute('UPDATE "statusTransitionGroups" SET "allowAnyTransition" = 1')
    structure(db, f)
    finalize(db, 'load-lab-fill')
    db.execute('COMMIT')
    db.execute('ANALYZE')
    f['completedStatusId'] = completed
    actual = db.execute('SELECT count(*) FROM tasks').fetchone()[0]
    if actual != count:
        raise ValueError(f'fixture expected {count}, has {actual}')
    print(json.dumps({'tasks': actual, 'databaseVerified': True}), flush=True)
json.dump(data, open(state, 'w'))
