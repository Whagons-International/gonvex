// A generated tenant-shaped SQLite file that exercises every transport edge:
// storage classes, invalid UTF-8, big values, a wide table that needs several
// INSERTs per chunk, rowid/alias/WITHOUT ROWID keys, children inserted before
// parents, AUTOINCREMENT above max(id), external-content FTS5 (trigram) kept
// by triggers, a projection maintained by a trigger, and a view.

import { openFile } from '../src/file.mjs';

export function writeFixture(path, { rows = 1200 } = {}) {
  const file = openFile(path);
  const db = file.db;
  db.exec(`
    PRAGMA journal_mode = WAL;
    CREATE TABLE child (id TEXT PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parent(id), note TEXT);
    CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT NOT NULL, data JSON, flag BOOLEAN, score REAL, raw BLOB);
    CREATE TABLE pairs (a TEXT, b INTEGER, v ANY, PRIMARY KEY (b, a)) WITHOUT ROWID;
    CREATE TABLE seq (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
    CREATE TABLE docs (title TEXT, body TEXT);
    CREATE VIRTUAL TABLE docs_fts USING fts5(title, body, content='docs', content_rowid='rowid', tokenize='trigram');
    CREATE TRIGGER docs_fts_insert AFTER INSERT ON docs BEGIN
      INSERT INTO docs_fts(rowid, title, body) VALUES (NEW.rowid, NEW.title, NEW.body);
    END;
    CREATE TRIGGER docs_fts_delete AFTER DELETE ON docs BEGIN
      INSERT INTO docs_fts(docs_fts, rowid, title, body) VALUES ('delete', OLD.rowid, OLD.title, OLD.body);
    END;
    CREATE TABLE parent_counts (parent_id INTEGER PRIMARY KEY REFERENCES parent(id), children INTEGER NOT NULL);
    CREATE TRIGGER child_count AFTER INSERT ON child BEGIN
      INSERT INTO parent_counts VALUES (NEW.parent_id, 1)
        ON CONFLICT (parent_id) DO UPDATE SET children = children + 1;
    END;
    CREATE INDEX child_parent ON child (parent_id);
    CREATE VIEW parent_summary AS SELECT p.id, p.name, c.children FROM parent p LEFT JOIN parent_counts c ON c.parent_id = p.id;
  `);
  const wide = Array.from({ length: 43 }, (_, i) => `c${i} ${i % 4 === 0 ? 'INTEGER' : i % 4 === 1 ? 'REAL' : 'TEXT'}`);
  db.exec(`CREATE TABLE wide (id INTEGER PRIMARY KEY, ${wide.join(', ')})`);

  db.exec('BEGIN');
  const parent = db.prepare('INSERT INTO parent VALUES (?, ?, ?, ?, ?, ?)');
  const child = db.prepare('INSERT INTO child VALUES (?, ?, ?)');
  for (let i = 1; i <= rows; i++) {
    parent.run(i, `parent ${i} ✓`, JSON.stringify({ i, tags: ['a', 'b'] }), i % 2, i % 5 === 0 ? i : i / 7, i % 9 === 0 ? new Uint8Array([i & 255, 0, 255]) : null);
  }
  for (let i = 1; i <= rows * 2; i++) child.run(`c-${i}`, 1 + (i % rows), i % 3 ? `note ${i}` : null);
  const pairs = db.prepare('INSERT INTO pairs VALUES (?, ?, ?)');
  const odd = [9007199254740993n, -9223372036854775808n, 3.0, -0.0, Infinity, '', 'é', new Uint8Array([]), null];
  for (let i = 0; i < rows; i++) pairs.run(`k${i % 13}`, i, odd[i % odd.length]);
  db.exec("INSERT INTO pairs VALUES ('bad-utf8', -1, CAST(X'FF00FE' AS TEXT))");
  db.exec("INSERT INTO pairs VALUES ('big', -2, '" + 'x'.repeat(500_000) + "')");
  const seq = db.prepare('INSERT INTO seq (v) VALUES (?)');
  for (let i = 0; i < 50; i++) seq.run(`s${i}`);
  db.exec("DELETE FROM seq WHERE id > 40; UPDATE sqlite_sequence SET seq = 1000 WHERE name = 'seq'");
  const docs = db.prepare('INSERT INTO docs VALUES (?, ?)');
  for (let i = 0; i < 300; i++) docs.run(`Title ${i}`, `searchable body text number ${i} with trigram matching`);
  db.exec('DELETE FROM docs WHERE rowid % 10 = 0');
  const wideInsert = db.prepare(`INSERT INTO wide VALUES (${Array.from({ length: 44 }, () => '?').join(', ')})`);
  for (let r = 1; r <= 300; r++) {
    wideInsert.run(r, ...Array.from({ length: 43 }, (_, i) => (i % 4 === 0 ? r * i : i % 4 === 1 ? r / (i + 1) : `r${r}c${i}`)));
  }
  db.exec('COMMIT');
  file.close();
}
