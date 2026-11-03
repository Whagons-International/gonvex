#!/usr/bin/env python3
"""Reset only exercised synthetic task state between comparison steps."""
import os
PREFIX=os.environ.get("LOAD_CONTAINER_PREFIX","gvload-")
import json,subprocess,sys,urllib.parse,uuid
state=json.load(open(sys.argv[1]))
def sql(db,q):return subprocess.check_output(['docker','exec','-i',PREFIX+'pg','psql','-U','postgres','-At','-v','ON_ERROR_STOP=1','-d',db],input=q,text=True).strip()
def quote(s):return "'"+s.replace("'","''")+"'"
for f in state['fixtures']:
 tenant=str(uuid.UUID(f['tenant']))
 db=urllib.parse.urlparse(sql('gonvex_runtime',f"SELECT database_url FROM gonvex_runtime_tenants WHERE tenant_id='{tenant}'")).path.strip('/')
 if not db.startswith('gonvex_'):raise ValueError('disposable database required')
 sql(db,f'''UPDATE tasks SET "statusId"={quote(f['statusId'])},"completedAt"=NULL,"firstViewedAt"=NULL,"updatedAt"=(extract(epoch FROM clock_timestamp())*1000)::bigint
 WHERE "workspaceId"={quote(f['workspaceId'])} AND ("statusId" IS DISTINCT FROM {quote(f['statusId'])} OR "completedAt" IS NOT NULL OR "firstViewedAt" IS NOT NULL);''')
sql('gonvex_runtime','CHECKPOINT;')
print('Synthetic task state reset; checkpoint complete')
