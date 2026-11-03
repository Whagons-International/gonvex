#!/usr/bin/env python3
"""Set measured synthetic users to ordinary team members before load."""
import os
PREFIX=os.environ.get("LOAD_CONTAINER_PREFIX","gvload-")
import json,subprocess,sys,urllib.parse,uuid
state=json.load(open(sys.argv[1]))
def sql(db,q):return subprocess.check_output(['docker','exec','-i',PREFIX+'pg','psql','-U','postgres','-At','-v','ON_ERROR_STOP=1','-d',db],input=q,text=True).strip()
def quote(s):return "'"+s.replace("'","''")+"'"
accounts=sql('gonvex_runtime',"SELECT id FROM accounts WHERE email LIKE 'load-user-%@example.invalid'").splitlines()
if not accounts:raise ValueError('prepare synthetic accounts first')
measured='account_id IN ('+','.join(quote(x) for x in accounts)+')'
filler_ids=sql('gonvex_runtime',"SELECT id FROM accounts WHERE email LIKE 'load-filler-%@example.invalid'").splitlines()
fillers='account_id IN ('+','.join(quote(x) for x in filler_ids)+')' if filler_ids else 'false'
for rank,f in enumerate(state['fixtures']):
 tenant=str(uuid.UUID(f['tenant']))
 db=urllib.parse.urlparse(sql('gonvex_runtime',f"SELECT database_url FROM gonvex_runtime_tenants WHERE tenant_id='{tenant}'")).path.strip('/')
 if not db.startswith('gonvex_'):raise ValueError('disposable database required')
 permissions={k:True for k in ['e2e','view-tasks','create-tasks','edit-tasks','change-tasks-status','tag-tasks','comment-tasks']}
 sql(db,f'''UPDATE members SET role='member',see_all_spots=true,permissions={quote(json.dumps(permissions))}::jsonb,membership_revision=membership_revision+1 WHERE {measured};
 INSERT INTO "memberTeams" ("_id","memberId","teamId","roleKey","createdAt","updatedAt")
 SELECT 'load-team-'||id,id,{quote(f['teamId'])},'member',0,0 FROM members WHERE {measured} ON CONFLICT DO NOTHING;''')
 if rank<3:
  active=int(sql(db,"SELECT count(*) FROM members WHERE status='active'"));excess=max(0,active-[95,90,85][rank])
  sql(db,f"UPDATE members SET status='inactive',membership_revision=membership_revision+1 WHERE id IN (SELECT id FROM members WHERE {fillers} AND status='active' ORDER BY id LIMIT {excess});")
 print(json.dumps({'activeMembers':int(sql(db,"SELECT count(*) FROM members WHERE status='active'")),'members':int(sql(db,'SELECT count(*) FROM members')),'tasks':int(sql(db,'SELECT count(*) FROM tasks'))}),flush=True)
