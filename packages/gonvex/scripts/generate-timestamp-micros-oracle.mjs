// Independent embedded-PostgreSQL oracle, synthetic values only. No endpoints.
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
const [input, output, engine] = process.argv.slice(2);
if (!engine) throw new Error('usage: SYNTHETIC_CASES OUTPUT_JSON PGLITE_ENTRY');
const { PGlite } = await import(pathToFileURL(path.resolve(engine)));
const source = JSON.parse(fs.readFileSync(input,'utf8'));
const db = new PGlite();
try {
  await db.exec('CREATE TABLE synthetic_time(id text PRIMARY KEY,at timestamptz,aligned boolean)');
  for (const c of source.cases) await db.query('INSERT INTO synthetic_time VALUES($1,$2::timestamptz,$3)', [c.id,c.wire,BigInt(c.micros)%1000n===0n]);
  await db.exec("INSERT INTO synthetic_time VALUES('synthetic-null',NULL,true)");
  const exact = (await db.query("SELECT id,(extract(epoch FROM at)*1000000)::bigint::text AS micros FROM synthetic_time WHERE at IS NOT NULL ORDER BY id")).rows;
  if (JSON.stringify(exact) !== JSON.stringify(source.cases.map(c=>({id:c.id,micros:c.micros})))) throw new Error('PostgreSQL source precision mismatch');
  const queries=[];
  for(const aligned of [false,true]) for(const cutoff of ['-1','0','1','1001','1700000000123456']) for(const op of ['=','>','<','>=','<=']) {
    const wire=source.cases.find(c=>c.micros===cutoff).wire;
    const ids=(await db.query('SELECT id FROM synthetic_time WHERE at '+op+' $1::timestamptz'+(aligned?' AND aligned=true':'')+' ORDER BY id',[wire])).rows.map(r=>r.id);
    queries.push({aligned,cutoffWire:wire,operator:op,expectedIds:ids});
  }
  const orders=[];
  for(const direction of ['ASC','DESC']) for(const limit of [1,3,21]) {
    const ids=(await db.query(`SELECT id FROM synthetic_time ORDER BY at ${direction} NULLS ${direction==='ASC'?'FIRST':'LAST'},id ${direction} LIMIT ${limit}`)).rows.map(r=>r.id);
    orders.push({direction,limit,expectedIds:ids});
  }
  const nullIds=(await db.query('SELECT id FROM synthetic_time WHERE at IS NULL ORDER BY id')).rows.map(r=>r.id);
  await db.exec('BEGIN');
  const stamp=async fn=>(await db.query(`SELECT (extract(epoch FROM ${fn}())*1000000)::bigint::text AS micros`)).rows[0].micros;
  const begin=await stamp('now');await new Promise(r=>setTimeout(r,5));
  if(await stamp('now')!==begin || BigInt(await stamp('clock_timestamp'))<BigInt(begin))throw new Error('PG clock contract mismatch');
  await db.exec('SAVEPOINT nested');await db.exec('ROLLBACK TO nested');await db.exec('RELEASE nested');
  if(await stamp('now')!==begin)throw new Error('PG savepoint clock changed');
  await db.exec('ROLLBACK');
  fs.writeFileSync(output,JSON.stringify({format:'synthetic-postgres-timestamp-oracle-v1',sourceCandidate:source.sourceCandidate,cases:source.cases,queries,orders,nullIds,postgresClockProperties:{transactionStartStable:true,savepointsPreserveStart:true,clockTimestampRealtime:true},qualification:'SYNTHETIC; not all app routes, production data, or above-six-digit rounding'},null,2)+'\n');
  process.stdout.write(JSON.stringify({cases:source.cases.length,typedFilterQueries:queries.length,orderLimitQueries:orders.length,nullQueries:1,postgresClockProperties:true,productionQualified:0})+'\n');
} finally { await db.close(); }
