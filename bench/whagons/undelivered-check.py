#!/usr/bin/env python3
"""Cross-check run.ts teardown diagnostics against the runtime's committed rows.

  undelivered-check.py <result.json> <data dir copied from the runtime volume>

For each distinct undelivered expectation (tenant, task, save path, args) it
reads the task row from the tenant's SQLite file and reports whether the
committed row carries the value the save set (server committed) and whether
the observers' replica rows differ from it (delivery gap), or the committed row
differs too (a later edit or a no-op save: harness expectation).
"""
import glob, json, os, sqlite3, sys
from collections import defaultdict

FIELDS = {'tasks.updateDescription': 'description', 'tasks.update': 'dueDate', 'tasks.rename': 'name',
          'tasks.setPriority': 'priorityId', 'tasks.reschedule': 'dueDate', 'tasks.start': 'statusId',
          'tasks.requestTransition': 'statusId', 'tasks.setSpot': 'spotId',
          'taskResources.recordTaskViewByTaskId': 'firstViewedAt'}

def main(result, data):
    r = json.load(open(result))
    t = r.get('taskExpectations') or {}
    print(json.dumps({k: v for k, v in t.items() if k != 'undelivered'}))
    ops = defaultdict(list)
    for u in t.get('undelivered', []):
        ops[(u['tenant'], u['task'], u['path'], u.get('args'), u['at'])].append(u)
    dbs = {os.path.basename(p)[:-3]: p for p in glob.glob(os.path.join(data, 'projects', '*', 'tenants', '*.db'))}
    summary = defaultdict(int)
    for (tenant, task, path, args, at), obs in sorted(ops.items(), key=lambda kv: kv[0][4]):
        field = FIELDS.get(path)
        con = sqlite3.connect(f'file:{dbs[tenant]}?mode=ro', uri=True)
        con.row_factory = sqlite3.Row
        row = con.execute('select * from tasks where _id=?', (task,)).fetchone()
        want = json.loads(args or '{}')
        want_value = want.get(field) if field not in ('dueDate',) else (want.get('dueDate') or want.get('updates', {}).get('dueDate'))
        committed = row[field] if row is not None and field else None
        if field == 'firstViewedAt':
            server_has = committed is not None and committed >= at
        else:
            server_has = committed == want_value
        replica = {o['row'].get(field) if field != 'description' else o['row'].get('description') for o in obs}
        kind = ('server-has-value: DELIVERY GAP' if server_has else
                'server-lacks-value: save did not set it / overwritten')
        summary[kind] += 1
        print(json.dumps({'tenant': tenant[:8], 'task': task[:8], 'path': path, 'field': field, 'at': at,
                          'savedAfterMs': (obs[0].get('savedAt') or at) - at, 'observers': len(obs),
                          'want': str(want_value)[:40], 'committed': str(committed)[:40],
                          'updatedAt': row['updatedAt'] if row is not None else None,
                          'replica': sorted(str(x)[:40] for x in replica), 'verdict': kind}))
    print(json.dumps(dict(summary)))

if __name__ == '__main__':
    main(*sys.argv[1:3])
