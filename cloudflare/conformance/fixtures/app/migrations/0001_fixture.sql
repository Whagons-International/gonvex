-- gonvex:scope tenant
-- Conformance fixture tables. No clock columns, so table checksums are exact.
CREATE TABLE IF NOT EXISTS board (
  id TEXT PRIMARY KEY NOT NULL,
  title TEXT NOT NULL,
  position INTEGER NOT NULL DEFAULT 0,
  bumps INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS board_by_position ON board (position);

-- Rows visible only to the Member in owner_id.
CREATE TABLE IF NOT EXISTS notes (
  id TEXT PRIMARY KEY NOT NULL,
  owner_id TEXT NOT NULL,
  body TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS notes_by_owner ON notes (owner_id);

-- A share makes one note visible to one more Member (row gain and loss).
CREATE TABLE IF NOT EXISTS note_shares (
  id TEXT PRIMARY KEY NOT NULL,
  note_id TEXT NOT NULL,
  member_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS note_shares_by_member ON note_shares (member_id);

-- Storage layer (K01): declared types, a deferred foreign key and document
-- numbers for the `storage-sql` scenario. Not checksummed.
CREATE TABLE IF NOT EXISTS storage_values (
  id TEXT PRIMARY KEY NOT NULL,
  label TEXT,
  meta JSON,
  flag BOOLEAN,
  n INTEGER,
  data BLOB
);
CREATE TABLE IF NOT EXISTS storage_parents (id TEXT PRIMARY KEY NOT NULL);
CREATE TABLE IF NOT EXISTS storage_children (
  id TEXT PRIMARY KEY NOT NULL,
  parent_id TEXT REFERENCES storage_parents (id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE IF NOT EXISTS storage_documents (id TEXT PRIMARY KEY NOT NULL, number TEXT NOT NULL);
-- Change capture (K03): the value shapes, key shapes and capture classes the
-- change log encodes. Rows are written by capture.apply (gonvex/capture.ts).
CREATE TABLE IF NOT EXISTS capture_values (
  id TEXT PRIMARY KEY NOT NULL,
  label TEXT,
  n INTEGER,
  amount REAL,
  flag BOOLEAN,
  meta JSON,
  note TEXT
);
-- Maintained by triggers on capture_values: captured like any entity table.
CREATE TABLE IF NOT EXISTS capture_totals (label TEXT PRIMARY KEY NOT NULL, count INTEGER NOT NULL);
CREATE TRIGGER IF NOT EXISTS capture_values_total_insert AFTER INSERT ON capture_values WHEN NEW.label IS NOT NULL BEGIN
  INSERT INTO capture_totals (label, count) VALUES (NEW.label, 1) ON CONFLICT (label) DO UPDATE SET count = count + 1;
END;
CREATE TRIGGER IF NOT EXISTS capture_values_total_delete AFTER DELETE ON capture_values WHEN OLD.label IS NOT NULL BEGIN
  UPDATE capture_totals SET count = count - 1 WHERE label = OLD.label;
  DELETE FROM capture_totals WHERE label = OLD.label AND count = 0;
END;
-- Integer key (rowid alias), and an app trigger that updates the row it fired for.
CREATE TABLE IF NOT EXISTS capture_counters (id INTEGER PRIMARY KEY, total INTEGER NOT NULL DEFAULT 0, stamp INTEGER NOT NULL DEFAULT 0);
CREATE TRIGGER IF NOT EXISTS capture_counters_stamp AFTER UPDATE OF total ON capture_counters BEGIN
  UPDATE capture_counters SET stamp = stamp + 1 WHERE id = NEW.id;
END;
-- Composite key, WITHOUT ROWID, written by a trigger (Rust's module host refuses module writes to a composite key).
-- Rust's entity capture reads NEW.rowid, so on Rust every write that reaches capture_pairs fails (note 0026).
CREATE TABLE IF NOT EXISTS capture_pairs (a TEXT NOT NULL, b TEXT NOT NULL, weight INTEGER, PRIMARY KEY (a, b)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS capture_pair_sources (id TEXT PRIMARY KEY NOT NULL, weight INTEGER);
CREATE TRIGGER IF NOT EXISTS capture_pair_sources_insert AFTER INSERT ON capture_pair_sources BEGIN
  INSERT INTO capture_pairs (a, b, weight) VALUES ('source', NEW.id, NEW.weight);
END;
-- Derived: no capture (derived-table-capture-v1).
CREATE TABLE IF NOT EXISTS capture_edges (id TEXT PRIMARY KEY NOT NULL, owner TEXT NOT NULL REFERENCES capture_values (id) ON DELETE CASCADE);
INSERT INTO _gonvex_table_options (name, capture, owner_table, owner_column) VALUES ('capture_edges', 'derived', 'capture_values', 'owner') ON CONFLICT (name) DO NOTHING;
-- A logical record over storage, with an owned table and a record dependency.
CREATE TABLE IF NOT EXISTS capture_labels (id TEXT PRIMARY KEY NOT NULL, text TEXT NOT NULL, color TEXT);
CREATE TABLE IF NOT EXISTS capture_doc_storage (_id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL, done BOOLEAN NOT NULL DEFAULT 0, label_id TEXT REFERENCES capture_labels (id));
CREATE TABLE IF NOT EXISTS capture_doc_tags (id TEXT PRIMARY KEY NOT NULL, doc_id TEXT NOT NULL REFERENCES capture_doc_storage (_id) ON DELETE CASCADE, tag TEXT NOT NULL);
CREATE VIEW IF NOT EXISTS capture_docs AS
  SELECT s._id, s.title, s.done,
         (SELECT text FROM capture_labels l WHERE l.id = s.label_id) AS label,
         (SELECT json_group_array(tag) FROM (SELECT tag FROM capture_doc_tags t WHERE t.doc_id = s._id ORDER BY tag)) AS tags
  FROM capture_doc_storage s;
INSERT INTO _gonvex_record_views (name, storage_table, key_column, schema) VALUES ('capture_docs', 'capture_doc_storage', '_id',
  '{"key":"_id","columns":{"_id":{"type":"text"},"title":{"type":"text"},"done":{"type":"boolean"},"label":{"type":"text"},"tags":{"type":"json"}}}')
  ON CONFLICT (name) DO NOTHING;
INSERT INTO _gonvex_table_options (name, capture, owner_table, owner_column, owner_direction) VALUES ('capture_doc_tags', 'owned', 'capture_doc_storage', 'doc_id', 'outbound') ON CONFLICT (name) DO NOTHING;
INSERT INTO _gonvex_record_dependencies (source_table, source_column, owner_storage, owner_column, owner_direction, watch_columns)
  VALUES ('capture_labels', 'id', 'capture_doc_storage', 'label_id', 'inbound', '["text"]') ON CONFLICT DO NOTHING;
-- Visibility masks v2 (K09): permission tables, a grant source, data and inheriting children.
CREATE TABLE IF NOT EXISTS mask_teams (id TEXT PRIMARY KEY NOT NULL, member_id TEXT NOT NULL, team_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS mask_workspace_teams (id TEXT PRIMARY KEY NOT NULL, team_id TEXT NOT NULL, workspace_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS mask_tasks (id TEXT PRIMARY KEY NOT NULL, workspace_id TEXT NOT NULL, creator_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS mask_task_users (id TEXT PRIMARY KEY NOT NULL, task_id TEXT NOT NULL, member_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS mask_notes (id TEXT PRIMARY KEY NOT NULL, task_id TEXT NOT NULL, author_id TEXT NOT NULL);
