-- Framework-owned, tenant-local reference ledger (SQLite). Application policy
-- is installed only by a declaration (see reference_integrity.rs).
CREATE TABLE IF NOT EXISTS _gonvex_reference_catalog (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1), declaration JSON NOT NULL
);
CREATE TABLE IF NOT EXISTS _gonvex_reference_tables (
  table_name TEXT PRIMARY KEY, key_column TEXT NOT NULL, live_when TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS _gonvex_reference_rules (
  name TEXT PRIMARY KEY, source_table TEXT NOT NULL, declaration JSON NOT NULL
);
CREATE INDEX IF NOT EXISTS _gonvex_reference_rules_source ON _gonvex_reference_rules(source_table);
-- A key can be the primary ID, an alternate key, or a shared namespace
-- for an intentionally untagged union such as reaction targets. Counts keep
-- shared keys correct when multiple rows/tables own the same identity.
CREATE TABLE IF NOT EXISTS _gonvex_reference_keys (
  table_name TEXT NOT NULL, token_name TEXT NOT NULL, key_column TEXT NOT NULL,
  PRIMARY KEY(table_name,token_name)
);
CREATE TABLE IF NOT EXISTS _gonvex_reference_targets (
  table_name TEXT NOT NULL, row_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('exists','live')),
  owner_count INTEGER NOT NULL DEFAULT 1 CHECK (owner_count >= 0),
  PRIMARY KEY (table_name,row_id,state)
);
-- `enforced_*` equals the target for every edge the native deferred FK
-- protects. Edges inventoried with a missing target keep it NULL (the
-- PostgreSQL NOT VALID equivalent) until that target appears.
CREATE TABLE IF NOT EXISTS _gonvex_reference_edges (
  rule_name TEXT NOT NULL, source_table TEXT NOT NULL, source_id TEXT NOT NULL,
  target_table TEXT NOT NULL, target_id TEXT NOT NULL,
  target_state TEXT NOT NULL CHECK (target_state IN ('exists','live')),
  enforced_table TEXT, enforced_id TEXT, enforced_state TEXT,
  PRIMARY KEY (rule_name,source_id,target_table,target_id,target_state),
  CONSTRAINT gonvex_reference_target FOREIGN KEY (enforced_table,enforced_id,enforced_state)
    REFERENCES _gonvex_reference_targets(table_name,row_id,state) DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX IF NOT EXISTS _gonvex_reference_edges_source ON _gonvex_reference_edges(source_table,source_id);
CREATE INDEX IF NOT EXISTS _gonvex_reference_edges_target ON _gonvex_reference_edges(target_table,target_id,target_state);
CREATE INDEX IF NOT EXISTS _gonvex_reference_edges_enforced
  ON _gonvex_reference_edges(enforced_table,enforced_id,enforced_state) WHERE enforced_table IS NOT NULL;
-- Configuration-selected domains bind to the exact parent configuration.
-- The ordinary target FK protects its version token across changes.
CREATE TABLE IF NOT EXISTS _gonvex_reference_bindings (
  name TEXT PRIMARY KEY, source_table TEXT NOT NULL, columns_ JSON NOT NULL
);
CREATE TABLE IF NOT EXISTS _gonvex_reference_configurations (
  binding_name TEXT NOT NULL, row_id TEXT NOT NULL, document JSON NOT NULL,
  version INTEGER NOT NULL, PRIMARY KEY(binding_name,row_id)
);
CREATE TABLE IF NOT EXISTS _gonvex_reference_sequence (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1), last_version INTEGER NOT NULL
);
INSERT INTO _gonvex_reference_sequence(singleton,last_version) VALUES(1,0)
  ON CONFLICT(singleton) DO NOTHING;
-- Always empty. Staged rows and declaration requests reference it through a
-- deferred FK, so a transaction cannot commit them unprocessed: a writer that
-- bypasses TenantTransaction (or reference_integrity::flush) fails closed.
CREATE TABLE IF NOT EXISTS _gonvex_reference_guard (id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS _gonvex_reference_pending (
  seq INTEGER PRIMARY KEY, table_name TEXT NOT NULL, old_doc JSON, new_doc JSON,
  guard INTEGER NOT NULL DEFAULT 0
    REFERENCES _gonvex_reference_guard(id) DEFERRABLE INITIALLY DEFERRED
);
-- Migrations declare references with
--   INSERT INTO _gonvex_reference_requests(declaration) VALUES('{…}');
-- The migration runner applies requests in order inside the same transaction.
CREATE TABLE IF NOT EXISTS _gonvex_reference_requests (
  seq INTEGER PRIMARY KEY, declaration JSON NOT NULL,
  guard INTEGER NOT NULL DEFAULT 0
    REFERENCES _gonvex_reference_guard(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE VIEW IF NOT EXISTS gonvex_reference_violations AS
  SELECT e.rule_name,e.source_table,e.source_id,e.target_table,e.target_id,
    CASE WHEN EXISTS (SELECT 1 FROM _gonvex_reference_targets x
      WHERE x.table_name=e.target_table AND x.row_id=e.target_id AND x.state='exists')
      THEN 'deleted' ELSE 'missing' END AS reason
  FROM _gonvex_reference_edges e
  LEFT JOIN _gonvex_reference_targets t ON t.table_name=e.target_table
    AND t.row_id=e.target_id AND t.state=e.target_state
  WHERE t.row_id IS NULL;
