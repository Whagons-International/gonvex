#!/usr/bin/env python3
"""Set measured synthetic users to ordinary team members before load."""
import json, sys, uuid
from sqlitelab import require_stopped, control, tenant_db, finalize

require_stopped()
state = json.load(open(sys.argv[1]))
ctl = control()
accounts = [r[0] for r in ctl.execute("SELECT id FROM accounts WHERE email LIKE 'load-user-%@example.invalid'")]
if not accounts:
    raise ValueError('prepare synthetic accounts first')
fillers = [r[0] for r in ctl.execute("SELECT id FROM accounts WHERE email LIKE 'load-filler-%@example.invalid'")]
measured = json.dumps(accounts)
filler_list = json.dumps(fillers)
permissions = json.dumps({k: True for k in ['e2e', 'view-tasks', 'create-tasks', 'edit-tasks',
                                             'change-tasks-status', 'tag-tasks', 'comment-tasks']})
for rank, f in enumerate(state['fixtures']):
    db = tenant_db(str(uuid.UUID(f['tenant'])))
    db.execute('BEGIN IMMEDIATE')
    db.execute("""UPDATE members SET role = 'member', see_all_spots = 1, permissions = ?,
                  membership_revision = membership_revision + 1
                  WHERE account_id IN (SELECT value FROM json_each(?))""", (permissions, measured))
    db.execute("""INSERT OR IGNORE INTO "memberTeams" ("_id", "memberId", "teamId", "roleKey", "createdAt", "updatedAt")
                  SELECT 'load-team-' || id, id, ?, 'member', 0, 0 FROM members
                  WHERE account_id IN (SELECT value FROM json_each(?))""", (f['teamId'], measured))
    if rank < 3:
        active = db.execute("SELECT count(*) FROM members WHERE status = 'active'").fetchone()[0]
        excess = max(0, active - [95, 90, 85][rank])
        db.execute("""UPDATE members SET status = 'inactive', membership_revision = membership_revision + 1
                      WHERE id IN (SELECT id FROM members WHERE account_id IN (SELECT value FROM json_each(?))
                                   AND status = 'active' ORDER BY id LIMIT ?)""", (filler_list, excess))
    finalize(db, 'load-lab-staff')
    db.execute('COMMIT')
    print(json.dumps({
        'activeMembers': db.execute("SELECT count(*) FROM members WHERE status = 'active'").fetchone()[0],
        'members': db.execute('SELECT count(*) FROM members').fetchone()[0],
        'tasks': db.execute('SELECT count(*) FROM tasks').fetchone()[0]}), flush=True)
