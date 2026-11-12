// Derived SQLite inspection for diagnostics, not an authoring registry. Rebuilt
// by codegen using the same connection that produces local-schema.ts.
import { createHash } from 'node:crypto';
import type { DatabaseSync } from 'node:sqlite';
import type { LocalSchema } from '@gonvex/local-runtime/schema';

type Source = { file: string; line: number };
type Index = { name: string; unique: boolean; keys: string[]; predicate: string; source?: Source };
type Machinery = { name: string; type: 'trigger' | 'view'; bytes: number; hash: string; broadUpdate: boolean; source?: Source };
export type SchemaInspection = {
  version: 1;
  tables: Record<string, { columns: Record<string, { type: string; declared: boolean; primary: boolean; source?: Source }>; indexes: Index[] }>;
  machinery: Machinery[];
};
const same = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);
// Index source locations once: no migration rescans per column. Locations are
// best-effort DDL locations, never a second parser deciding schema semantics.
function sourcesFor(migrations: readonly {name:string;sql:string}[], schema:LocalSchema) {
  const locations=new Map<string,Source>();
  for(const migration of migrations){
    let table:string|undefined;
    const lines=migration.sql.split(/\r?\n/);
    for(let i=0;i<lines.length;i++){
      const line=lines[i]!,source={file:`migrations/${migration.name}`,line:i+1};
      const create=/\bCREATE\s+(?:UNIQUE\s+)?(?:VIRTUAL\s+)?(TABLE|VIEW|TRIGGER|INDEX)\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"((?:[^"]|"")+)"|`([^`]+)`|\[([^\]]+)\]|([\w]+))/i.exec(line);
      if(create){
        const name=(create[2]?.replaceAll('""','"')??create[3]??create[4]??create[5])!;
        const kind=create[1]!.toLowerCase();
        locations.set(`${kind}:${name}`,source);
        table=kind==='table'?name:undefined;
        if(table)for(const column of Object.keys(schema[table]?.columns??{}))locations.set(`column:${table}.${column}`,source);
      }
      if(table){
        // Each column declaration starts a line in generated migrations. Inline
        // declarations retain the table location as an honest fallback.
        const column=/^\s*(?:"([^"]+)"|`([^`]+)`|\[([^\]]+)\]|([\w]+))\s+(?:TEXT|INT\w*|BIGINT|BOOL\w*|REAL|DOUBLE|JSON|BLOB|NUMERIC)\b/i.exec(line);
        const name=column?.[1]??column?.[2]??column?.[3]??column?.[4];
        if(name&&schema[table]?.columns[name])locations.set(`column:${table}.${name}`,source);
      }
      if(line.includes(';'))table=undefined;
      const alter=/\bALTER\s+TABLE\s+(?:"([^"]+)"|([\w]+))\s+ADD\s+(?:COLUMN\s+)?(?:"([^"]+)"|([\w]+))/i.exec(line);
      if(alter)locations.set(`column:${alter[1]??alter[2]}.${alter[3]??alter[4]}`,source);
    }
  }
  return (kind:string,name:string,column?:string)=>locations.get(column?`column:${name}.${column}`:`${kind}:${name}`)??locations.get(`${kind}:${name}`)??locations.get(`view:${name}`);
}

export function inspectSchemaDesign(db: DatabaseSync, schema: LocalSchema, migrations: readonly { name: string; sql: string }[]): SchemaInspection {
  const explicit = new Set<string>();
  const sourceFor=sourcesFor(migrations,schema);
  // Reuse the existing runtime request/catalog, including declarations that
  // local compilation has not installed. Discovery is not proof of enforcement.
  for (const table of ['_gonvex_reference_catalog','_gonvex_reference_requests']) {
    if (!db.prepare('SELECT 1 FROM sqlite_schema WHERE type=\'table\' AND name=?').get(table)) continue;
    for (const row of db.prepare(`SELECT declaration FROM ${table}`).all() as {declaration:string}[]) {
      const catalog = JSON.parse(row.declaration) as {references?: {source:string; path?:string; at?:string}[]};
      for (const ref of catalog.references ?? []) {
        const column = /^\$\.(?:"([^"]+)"|([\w]+))/.exec(ref.at ?? ref.path ?? '');
        if (column) explicit.add(`${ref.source}.${column[1] ?? column[2]}`);
      }
    }
  }
  const tables: SchemaInspection['tables'] = {};
  for (const [name, table] of Object.entries(schema)) {
    const indexes = (db.prepare('SELECT name,"unique" FROM pragma_index_list(?)').all(name) as {name:string;unique:number}[]).map(index => {
      const sql = (db.prepare('SELECT sql FROM sqlite_schema WHERE name=?').get(index.name) as {sql:string|null})?.sql ?? '';
      const columns = db.prepare('SELECT cid,name,coll,desc FROM pragma_index_xinfo(?) WHERE key=1 ORDER BY seqno').all(index.name) as {cid:number;name:string|null;coll:string;desc:number}[];
      return {name:index.name,unique:!!index.unique,keys:columns.map(c=>JSON.stringify(c)),predicate:sql.match(/\bWHERE\b[\s\S]*/i)?.[0] ?? '',source:sourceFor('index',index.name)};
    });
    tables[name] = { columns: Object.fromEntries(Object.entries(table.columns).map(([column, shape])=>[column, {
      type:shape.type,primary:column===table.key,declared:explicit.has(`${name}.${column}`)||!!table.foreignKeys?.some(f=>f.columns.includes(column)),source:sourceFor('table',name,column),
    }])), indexes };
  }
  const machinery = (db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE type IN ('trigger','view') AND name NOT GLOB '_gonvex*' AND name NOT GLOB 'gonvex_*'").all() as {name:string;type:'trigger'|'view';sql:string}[]).map(row=>({
    name:row.name,type:row.type,bytes:Buffer.byteLength(row.sql),hash:createHash('sha256').update(row.sql).digest('hex'),
    broadUpdate:row.type==='trigger'&&/\bUPDATE\s+ON\b/i.test(row.sql)&&!/\bWHEN\b/i.test(row.sql)&&/\b(?:SELECT|EXISTS)\b/i.test(row.sql),source:sourceFor(row.type,row.name),
  }));
  return {version:1,tables,machinery};
}

export type SchemaFinding = { code: string; source?: Source; concern: string; change: string; suggestion: string; evidence: 'heuristic' | 'schema measurement' };
/** Compare inspection facts, not a score or count limit. Unchanged backlog is omitted. */
export function schemaFindings(before: SchemaInspection, after: SchemaInspection): SchemaFinding[] {
  const findings: SchemaFinding[] = [];
  for (const [name, table] of Object.entries(after.tables)) {
    const previous=before.tables[name];
    for (const [column, shape] of Object.entries(table.columns)) {
      const old=previous?.columns[column];
      if (!shape.primary && !shape.declared && /(?:Id|Ids|ID|IDs|By)$|(?:^|_)ids?$/.test(column) && (!old||old.declared)) findings.push({code:'relationship-declaration',source:shape.source,concern:`${name}.${column} looks like an identity but has no FK or explicit runtime relationship declaration. No target is inferred.`,change:old?'declaration removed':'column added',suggestion:'Declare its relationship semantics, or explain its external, historical or non-identity meaning. Metadata does not prove enforcement.',evidence:'heuristic'});
      if (column.endsWith('Ids') && table.columns[column.slice(0,-1)] && (!old||!previous?.columns[column.slice(0,-1)])) findings.push({code:'duplicate-representation',source:shape.source,concern:`${name}.${column} and ${column.slice(0,-1)} may represent the same fact.`,change:'scalar/list pair added',suggestion:'Choose one authority, or document the copy\'s access path, maintenance owner and rebuild path.',evidence:'heuristic'});
    }
    for (let i=0;i<table.indexes.length;i++) for (let j=i+1;j<table.indexes.length;j++) {
      const a=table.indexes[i]!,b=table.indexes[j]!;
      const changed=[a,b].filter(index=>!previous?.indexes.some(old=>old.name===index.name&&same({...old,source:undefined},{...index,source:undefined})));
      // Expression, collation, sort direction, partial predicate and uniqueness
      // matter. Do not call unlike constraints redundant.
      if (!changed.length||a.unique!==b.unique||a.predicate!==b.predicate||!a.keys.length||!b.keys.length||[...a.keys,...b.keys].some(key=>JSON.parse(key).cid<0)) continue;
      const [short,long]=a.keys.length<=b.keys.length?[a,b]:[b,a];
      if ((short.unique&&short.keys.length!==long.keys.length)||!short.keys.every((key,k)=>key===long.keys[k])) continue;
      findings.push({code:'overlapping-indexes',source:changed[0]!.source,concern:`${name}: ${short.name} overlaps the leading keys of ${long.name}.`,change:`${changed.map(index=>index.name).join(', ')} added or changed`,suggestion:'Review EXPLAIN QUERY PLAN and write cost; remove the redundant index or explain why both access paths are needed. Preserve uniqueness and partial-index semantics.',evidence:'heuristic'});
    }
  }
  for (const kind of ['trigger','view'] as const) {
    const old=before.machinery.filter(o=>o.type===kind), next=after.machinery.filter(o=>o.type===kind);
    const bytes=(items:Machinery[])=>items.reduce((n,item)=>n+item.bytes,0);
    if (next.length>old.length||bytes(next)>bytes(old)) findings.push({code:'generated-machinery',source:next.find(item=>!old.some(prior=>prior.name===item.name&&prior.hash===item.hash))?.source,concern:`${kind} definitions grew from ${old.length} / ${bytes(old)} SQL bytes to ${next.length} / ${bytes(next)} SQL bytes.`,change:`${next.length-old.length} objects, ${bytes(next)-bytes(old)} SQL bytes`,suggestion:'Explain the access path and measure parse memory per connection and write work. Prefer native constraints or bounded shared runtime validation where simpler. SQL bytes are measured; runtime cost is not.',evidence:'schema measurement'});
  }
  for (const trigger of after.machinery.filter(o=>o.broadUpdate)) if (!before.machinery.some(old=>old.name===trigger.name&&old.hash===trigger.hash)) findings.push({code:'broad-validation',source:trigger.source,concern:`${trigger.name} runs SELECT work on every UPDATE without an UPDATE OF or WHEN guard.`,change:'trigger added or changed',suggestion:'Check the query plan for a one-field edit; scope validation to changed owners/fields and indexed endpoints. This heuristic does not prove a full scan.',evidence:'heuristic'});
  return findings;
}
