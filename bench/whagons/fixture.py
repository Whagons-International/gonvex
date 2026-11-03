"""Synthetic fixture layout shared by fill/staff. No production data."""
import json, uuid
from sqlitelab import columns, ident

TEAM_COUNT = 3
SPOT_COUNT = 6
TASK_PERMISSIONS = ['view-tasks', 'create-tasks', 'edit-tasks', 'assign-tasks',
                    'change-tasks-status', 'tag-tasks', 'comment-tasks', 'sign-tasks',
                    'view-workspaces', 'view-users']
ADMIN_PERMISSIONS = TASK_PERMISSIONS + ['edit-users', 'manage-tenant', 'config.approvals',
                                     'create-approvals', 'edit-approvals', 'config.spots',
                                     'assign-spots', 'config.roles', 'settings.access']


def seed_id(f, name):
    return str(uuid.uuid5(uuid.UUID(f['tenant']), 'realism-v1/' + name))


def put(db, table, row):
    available = columns(db, table)
    unknown = set(row) - set(available)
    if unknown:
        raise ValueError(f'{table}: unknown fixture fields {unknown}')
    keys = list(row)
    db.execute(f'INSERT OR REPLACE INTO {ident(table)} ({",".join(map(ident, keys))}) '
               f'VALUES ({",".join("?" for _ in keys)})', [row[k] for k in keys])


def clone(db, table, source, changes):
    keys = columns(db, table)
    row = db.execute(f'SELECT * FROM {ident(table)} WHERE "_id" = ?', (source,)).fetchone()
    if row is None:
        raise ValueError(f'{table} fixture source missing')
    put(db, table, {**dict(zip(keys, row)), **changes})


def structure(db, f):
    f['teamIds'] = [f['teamId']] + [seed_id(f, f'team/{i}') for i in range(1, TEAM_COUNT)]
    f['workspaceIds'] = [f['workspaceId']] + [seed_id(f, f'workspace/{i}') for i in range(1, TEAM_COUNT)]
    f['categoryIds'] = [f['categoryId']] + [seed_id(f, f'category/{i}') for i in range(1, TEAM_COUNT)]
    f['spotIds'] = [seed_id(f, f'spot/{i}') for i in range(SPOT_COUNT)]
    f['operatorRoleId'] = seed_id(f, 'role/operator')
    f['supervisorRoleId'] = seed_id(f, 'role/supervisor')
    for i, team in enumerate(f['teamIds']):
        if i:
            clone(db, 'teams', f['teamId'], {'_id': team, 'name': f'Load team {i}', 'slug': f'load-{i}'})
            clone(db, 'workspaces', f['workspaceId'], {'_id': f['workspaceIds'][i], 'name': f'Load workspace {i}',
                  'teams': json.dumps([team]), 'categoryId': f['categoryIds'][i]})
            clone(db, 'categories', f['categoryId'], {'_id': f['categoryIds'][i], 'name': f'Load category {i}',
                  'teamId': team, 'workspaceId': f['workspaceIds'][i]})
        put(db, 'workspaceTeams', {'_id': seed_id(f, f'workspace-team/{i}'), 'workspaceId': f['workspaceIds'][i], 'teamId': team})
    for i, spot in enumerate(f['spotIds']):
        put(db, 'spots', {'_id': spot, 'name': f'Load area {i}', 'isBranch': False, 'order': i})
    for role, name in [(f['operatorRoleId'], 'Load operator'), (f['supervisorRoleId'], 'Load supervisor')]:
        put(db, 'roles', {'_id': role, 'name': name, 'scope': 'TEAM', 'guardName': 'sanctum'})
        for permission in TASK_PERMISSIONS:
            row = db.execute('SELECT "_id" FROM permissions WHERE name = ?', (permission,)).fetchone()
            pid = row[0] if row else seed_id(f, 'permission/' + permission)
            if not row:
                put(db, 'permissions', {'_id': pid, 'name': permission, 'guardName': 'sanctum'})
            put(db, 'rolePermissions', {'_id': seed_id(f, role + '/' + permission), 'roleId': role, 'permissionId': pid})
    f['customFieldId'] = seed_id(f, 'custom-field')
    put(db, 'customFields', {'_id': f['customFieldId'], 'name': 'Load inspection reference', 'fieldType': 'TEXT', 'slug': 'load-inspection-reference'})
    # Assign all rows, including the initial 50, across independent workspaces
    # and spots. Flat spots make the independent visibility expectation exact.
    rows = db.execute('SELECT "_id" FROM tasks ORDER BY id, "_id"').fetchall()
    description = 'Synthetic inspection: check equipment, record the condition and coordinate the repair with the next shift. '
    for n, (task,) in enumerate(rows):
        team = n % TEAM_COUNT
        spot = (n // TEAM_COUNT) % SPOT_COUNT
        db.execute('UPDATE tasks SET "workspaceId"=?, "teamId"=?, "categoryId"=?, "spotId"=?, description=? WHERE "_id"=?',
                   (f['workspaceIds'][team], f['teamIds'][team], f['categoryIds'][team], f['spotIds'][spot], description * 3, task))
        if n % 4 == 0:
            put(db, 'taskCustomFieldValues', {'_id': seed_id(f, 'field-value/' + task), 'fieldId': f['customFieldId'], 'taskId': task,
                                            'name': 'Load inspection reference', 'type': 'TEXT', 'value': f'INSPECT-{n:06}'})
    f['realismVersion'] = 1
    f['baseTasks'] = [dict(zip(['_id', 'id', 'workspaceId', 'spotId'], row)) for row in db.execute('SELECT "_id", id, "workspaceId", "spotId" FROM tasks')]
