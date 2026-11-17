-- gonvex:scope tenant
-- No BLOB columns: change capture and the reference ledger encode rows with
-- json_object, which rejects BLOB values.
CREATE TABLE tasks (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  done BOOLEAN NOT NULL DEFAULT 0,
  due_at INTEGER,
  due_on INTEGER,
  meta JSON NOT NULL DEFAULT '{}',
  tags JSON NOT NULL DEFAULT '[]',
  price REAL,
  price_text TEXT,
  score REAL,
  parent_id TEXT,
  note TEXT
);
CREATE TABLE labels (
  id INTEGER PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  name TEXT NOT NULL
);
-- Seed rows are replaced by the copied data.
INSERT INTO tasks (id, title) VALUES ('seed', 'seeded by migration');
INSERT INTO _gonvex_reference_requests(declaration) VALUES('{
  "version":1,
  "tables":[{"name":"tasks","key":"id","live":"true"}],
  "references":[
    {"name":"task.parent","source":"tasks","path":"$.parent_id","target":"tasks","state":"exists"}
  ]
}');
