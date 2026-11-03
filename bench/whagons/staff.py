#!/usr/bin/env python3
"""Give measured synthetic users realistic roles before load.

Per tenant, measured users (ordered by their load-user index) become:
- 2 admins: members.role 'admin' (settings rights for permission changes),
  every team, see_all_spots;
- 3 approvers of the tenant's approval rule (fill.py), team T0;
- everyone else an ordinary member with one or two teams and one or two
  parent spots; about 1 in 7 sees all spots.
Workspace A is linked to teams T0/T1 and workspace B to T2/T3 (fill.py), so
team membership decides the workspace and spots decide rows inside it.
The chosen roles are written back to the fixture for the generator."""
import json, re, sys, uuid
from sqlitelab import require_stopped, control, tenant_db, finalize

ADMINS, APPROVERS = 2, 3
TASK_PERMISSIONS = ['e2e', 'view-tasks', 'create-tasks', 'edit-tasks', 'assign-tasks', 'tag-tasks',
                    'comment-tasks', 'sign-tasks', 'change-tasks-status']

require_stopped()
state_path = sys.argv[1]
state = json.load(open(state_path))
ctl = control()
accounts = {r[1]: r[0] for r in ctl.execute(
    "SELECT id, email FROM accounts WHERE email LIKE 'load-user-%@example.invalid'")}
if not accounts:
    raise ValueError('prepare synthetic accounts first')
fillers = [r[0] for r in ctl.execute("SELECT id FROM accounts WHERE email LIKE 'load-filler-%@example.invalid'")]
permissions = json.dumps({k: True for k in TASK_PERMISSIONS})


def index(email):
    return int(re.match(r'load-user-(\d+)@', email).group(1))


for rank, f in enumerate(state['fixtures']):
    db = tenant_db(str(uuid.UUID(f['tenant'])))
    teams = f['teamIds']
    db.execute('BEGIN IMMEDIATE')
    members = db.execute("""SELECT id, account_id FROM members
                            WHERE account_id IN (SELECT value FROM json_each(?))""",
                         (json.dumps(list(accounts.values())),)).fetchall()
    email_of = {v: k for k, v in accounts.items()}
    members.sort(key=lambda m: index(email_of[m[1]]))
    db.execute("""DELETE FROM "memberTeams" WHERE "memberId" IN (SELECT value FROM json_each(?))""",
               (json.dumps([m[0] for m in members]),))
    db.execute("""DELETE FROM "memberSpots" WHERE "memberId" IN (SELECT value FROM json_each(?))""",
               (json.dumps([m[0] for m in members]),))
    admins, approvers = [], []
    for k, (member, _) in enumerate(members):
        if k < ADMINS:
            role, see_all, member_teams, spots = 'admin', 1, teams, []
            admins.append(member)
        else:
            if k < ADMINS + APPROVERS:
                approvers.append(member)
            role = 'member'
            see_all = 1 if k % 7 == 0 else 0
            member_teams = [[teams[0]], [teams[1]], [teams[2]], [teams[0], teams[3]]][k % 4]
            spots = [] if see_all else [f['spotParents'][k % 4]] + ([f['spotParents'][(k + 1) % 4]] if k % 3 == 0 else [])
        db.execute("""UPDATE members SET role = ?, see_all_spots = ?, permissions = ?,
                      membership_revision = membership_revision + 1 WHERE id = ?""",
                   (role, see_all, permissions, member))
        for team in member_teams:
            db.execute("""INSERT INTO "memberTeams" ("_id", "memberId", "teamId", "roleKey", "createdAt", "updatedAt")
                          VALUES (?, ?, ?, 'member', 0, 0)""", (f'load-mt-{member}-{team}', member, team))
        for spot in spots:
            db.execute("""INSERT INTO "memberSpots" ("_id", "memberId", "spotId", "createdAt", "updatedAt")
                          VALUES (?, ?, ?, 0, 0)""", (f'load-ms-{member}-{spot}', member, spot))
    db.execute('DELETE FROM "approvalApprovers" WHERE "approvalId" = ?', (f['approvalId'],))
    for order, member in enumerate(approvers):
        db.execute("""INSERT INTO "approvalApprovers" ("_id", "approvalId", "approverType", "approverId",
                      required, "orderIndex", scope) VALUES (?, ?, 'user', ?, 0, ?, 'global')""",
                   (f'load-approver-{member}', f['approvalId'], member, order))
    if rank < 3:
        active = db.execute("SELECT count(*) FROM members WHERE status = 'active'").fetchone()[0]
        excess = max(0, active - [95, 90, 85][rank])
        db.execute("""UPDATE members SET status = 'inactive', membership_revision = membership_revision + 1
                      WHERE id IN (SELECT id FROM members WHERE account_id IN (SELECT value FROM json_each(?))
                                   AND status = 'active' ORDER BY id LIMIT ?)""", (json.dumps(fillers), excess))
    finalize(db, 'load-lab-staff')
    db.execute('COMMIT')
    f.update(adminMemberIds=admins, approverMemberIds=approvers)
    print(json.dumps({
        'measured': len(members), 'admins': len(admins), 'approvers': len(approvers),
        'seeAllSpots': db.execute('SELECT count(*) FROM members WHERE see_all_spots = 1').fetchone()[0],
        'activeMembers': db.execute("SELECT count(*) FROM members WHERE status = 'active'").fetchone()[0],
        'tasks': db.execute('SELECT count(*) FROM tasks').fetchone()[0]}), flush=True)
json.dump(state, open(state_path, 'w'))
