/** REAL LOCAL SQLITE HANDLER FIXTURE ONLY. Not Whagons, not Gonvex SDK/workerd/deployed proof.
 * Separate authorization, transaction and reader application boundaries for scenario tests.
 */
import { DatabaseSync } from 'node:sqlite';
export function sqliteApplication(filename, fault = null) {
  const writer = 'synthetic-writer', reader = 'synthetic-reader';
  const db = new DatabaseSync(filename);
  db.exec('PRAGMA journal_mode=WAL; CREATE TABLE tasks(id TEXT PRIMARY KEY, kind TEXT, revision INTEGER); CREATE TABLE sequence(n INTEGER); INSERT INTO sequence VALUES(0)');
  const replica = new Map(); let closed=false;
  const check = (principal, expected, signal) => { if(signal?.aborted) throw new Error('aborted'); if(principal!==expected) throw new Error('unauthorized'); };
  return {
    mutationBoundary:'local-sqlite-handler',
    async applicationRead(principal,{signal}) {
      check(principal,reader,signal);
      const fresh=new DatabaseSync(filename); try { fresh.prepare('SELECT COUNT(*) FROM tasks').get(); } finally { fresh.close(); }
      return { authenticated:true,storage:'tenant-sqlite',fresh:true,expected:true };
    },
    async realtimeAuth(principal,{signal}) { check(principal,reader,signal); return {authenticated:true,protocolReady:true}; },
    async openReader(principal,{signal}) {
      check(principal,reader,signal);
      return { principal,async waitApplied({fixtureId,kind,revision,signal}) { check(principal,reader,signal); const applied=replica.get(fixtureId); return { principal,revision:applied?.revision,matches:applied?.kind===kind,applied:!!applied }; },async close() {} };
    },
    async mutate(principal,{kind,fixtureId,operationId,signal}) {
      check(principal,writer,signal);
      if(fault==='write') throw new Error('rejected');
      db.exec('BEGIN IMMEDIATE');
      let revision;
      try {
        revision=Number(db.prepare('UPDATE sequence SET n=n+1 RETURNING n').get().n);
        if(kind==='create') db.prepare('INSERT INTO tasks VALUES(?,?,?)').run(fixtureId,kind,revision);
        else db.prepare('UPDATE tasks SET kind=?,revision=? WHERE id=?').run(kind,revision,fixtureId);
        db.exec('COMMIT');
      } catch(e) { db.exec('ROLLBACK'); throw e; }
      // Local reader applies a post-commit change; fault tests deliberately stall this boundary.
      if(fault!=='stalled-sync') replica.set(fixtureId,{kind,revision});
      if(fault==='lost-reply') throw new Error('ambiguous');
      return {durable:true,operationId,revision};
    },
    async freshRead(principal,{fixtureId,kind,signal}) {
      check(principal,reader,signal);
      const fresh=new DatabaseSync(filename); let row;
      try { row=fresh.prepare('SELECT * FROM tasks WHERE id=?').get(fixtureId); } finally { fresh.close(); }
      return { storage:'tenant-sqlite',fresh:true,matches:row?.kind===kind,revision:row?.revision };
    },
    async cleanup(principal,{fixtureId}) { check(principal,writer); if(fault==='cleanup') return false; db.prepare('DELETE FROM tasks WHERE id=?').run(fixtureId);replica.delete(fixtureId);return true; },
    rowCount:()=>Number(db.prepare('SELECT COUNT(*) AS n FROM tasks').get().n),
    close:()=>{ if(!closed) { closed=true;db.close(); } },
  };
}
