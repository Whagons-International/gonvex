#!/usr/bin/env python3
"""Populate disposable gvload databases only. Never accepts a database endpoint."""
import os
PREFIX=os.environ.get("LOAD_CONTAINER_PREFIX","gvload-")
import json,re,subprocess,sys,urllib.parse,uuid
state=sys.argv[1]; data=json.load(open(state))
def sql(db,q):
 return subprocess.check_output(['docker','exec','-i',PREFIX+'pg','psql','-U','postgres','-At','-v','ON_ERROR_STOP=1','-d',db],input=q,text=True).strip()
for f in data['fixtures']:
 tenant=str(uuid.UUID(f['tenant'])); project=str(uuid.UUID(data['projectId']))
 url=sql('gonvex_runtime',f"SELECT database_url FROM gonvex_runtime_tenants WHERE project_id='{project}' AND tenant_id='{tenant}';")
 db=urllib.parse.urlparse(url).path.strip('/')
 if not re.fullmatch(r'gonvex_[a-zA-Z0-9_]+',db): raise ValueError('not a disposable tenant database')
 count=int(f['taskCount']);completed=str(uuid.uuid5(uuid.NAMESPACE_URL,tenant+"/completed"))
 sql(db,f'''INSERT INTO tasks SELECT (jsonb_populate_record(NULL::tasks,to_jsonb(t)||jsonb_build_object('_id','synthetic-load-'||n,'id',1000000+n,'name','Synthetic load task '||n))).*
 FROM (SELECT * FROM tasks WHERE NOT EXISTS (SELECT 1 FROM tasks WHERE \"_id\"='synthetic-load-1') LIMIT 1) t CROSS JOIN generate_series(1,{count}-50) n ON CONFLICT DO NOTHING;
 INSERT INTO statuses SELECT (jsonb_populate_record(NULL::statuses,to_jsonb(s)||jsonb_build_object('_id','{completed}','name','Synthetic completed','action','COMPLETED','initial',false,'final',true))).* FROM statuses s LIMIT 1 ON CONFLICT DO NOTHING;
 UPDATE "statusTransitionGroups" SET "allowAnyTransition"=true;
 ANALYZE;''')
 f['completedStatusId']=completed
 actual=int(sql(db,'SELECT count(*) FROM tasks;'))
 if actual!=count: raise ValueError(f'fixture expected {count}, has {actual}')
 print(json.dumps({'tasks':actual,'databaseVerified':True}),flush=True)
json.dump(data,open(state,'w'))
