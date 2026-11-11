
CREATE TABLE IF NOT EXISTS members (
  id TEXT PRIMARY KEY, account_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL DEFAULT 'active',
  display_name TEXT NOT NULL DEFAULT '', avatar_url TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL DEFAULT 'member', permissions JSON NOT NULL DEFAULT '{}',
  membership_revision INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE INDEX IF NOT EXISTS members_by_role ON members(role);
CREATE INDEX IF NOT EXISTS members_by_status ON members(status,id);
CREATE TABLE IF NOT EXISTS _gonvex_control_plane_membership_outbox (
  account_id TEXT PRIMARY KEY, member_id TEXT NOT NULL, status TEXT NOT NULL,
  membership_revision INTEGER NOT NULL,
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
DROP TRIGGER IF EXISTS gonvex_queue_control_plane_membership_insert;
DROP TRIGGER IF EXISTS gonvex_queue_control_plane_membership_update;
DROP TRIGGER IF EXISTS gonvex_queue_control_plane_membership_delete;
CREATE TRIGGER gonvex_queue_control_plane_membership_insert AFTER INSERT ON members BEGIN
  INSERT INTO _gonvex_control_plane_membership_outbox(account_id,member_id,status,membership_revision,updated_at)
  VALUES(NEW.account_id,NEW.id,NEW.status,NEW.membership_revision,CAST(unixepoch('subsec') * 1000 AS INTEGER))
  ON CONFLICT(account_id) DO UPDATE SET member_id=excluded.member_id,status=excluded.status,
    membership_revision=excluded.membership_revision,updated_at=excluded.updated_at
  WHERE excluded.membership_revision>=_gonvex_control_plane_membership_outbox.membership_revision;
END;
CREATE TRIGGER gonvex_queue_control_plane_membership_update AFTER UPDATE ON members BEGIN
  INSERT INTO _gonvex_control_plane_membership_outbox(account_id,member_id,status,membership_revision,updated_at)
  VALUES(NEW.account_id,NEW.id,NEW.status,NEW.membership_revision,CAST(unixepoch('subsec') * 1000 AS INTEGER))
  ON CONFLICT(account_id) DO UPDATE SET member_id=excluded.member_id,status=excluded.status,
    membership_revision=excluded.membership_revision,updated_at=excluded.updated_at
  WHERE excluded.membership_revision>=_gonvex_control_plane_membership_outbox.membership_revision;
END;
CREATE TRIGGER gonvex_queue_control_plane_membership_delete AFTER DELETE ON members BEGIN
  INSERT INTO _gonvex_control_plane_membership_outbox(account_id,member_id,status,membership_revision,updated_at)
  VALUES(OLD.account_id,OLD.id,'revoked',OLD.membership_revision+1,CAST(unixepoch('subsec') * 1000 AS INTEGER))
  ON CONFLICT(account_id) DO UPDATE SET member_id=excluded.member_id,status=excluded.status,
    membership_revision=excluded.membership_revision,updated_at=excluded.updated_at
  WHERE excluded.membership_revision>=_gonvex_control_plane_membership_outbox.membership_revision;
END;

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

CREATE TABLE IF NOT EXISTS _gonvex_schema_capabilities(name TEXT PRIMARY KEY);
INSERT INTO _gonvex_schema_capabilities(name) VALUES
  ('atomic-foreign-key-rebuild-v1'),('derived-table-capture-v1'),('logical-record-storage-v1') ON CONFLICT(name) DO NOTHING;
CREATE TABLE IF NOT EXISTS _gonvex_table_options(
  name TEXT PRIMARY KEY,
  capture TEXT NOT NULL CHECK(capture IN ('entity','derived','owned','metadata')),
  owner_table TEXT,
  owner_column TEXT,
  owner_direction TEXT NOT NULL DEFAULT 'outbound' CHECK(owner_direction IN ('outbound','inbound')),
  CHECK(capture NOT IN ('derived','owned') OR (owner_table IS NOT NULL AND owner_column IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS _gonvex_id_domains(_id INTEGER PRIMARY KEY,name TEXT NOT NULL UNIQUE) STRICT;
CREATE TABLE IF NOT EXISTS _gonvex_identity_keys(
  _id INTEGER PRIMARY KEY,domain INTEGER NOT NULL REFERENCES _gonvex_id_domains(_id),value TEXT NOT NULL CHECK(length(value)>0),UNIQUE(domain,value)
) STRICT;
CREATE TABLE IF NOT EXISTS _gonvex_interned_text(_id INTEGER PRIMARY KEY,value TEXT NOT NULL UNIQUE) STRICT;
CREATE TABLE IF NOT EXISTS _gonvex_record_views(
  name TEXT PRIMARY KEY,storage_table TEXT NOT NULL UNIQUE,key_column TEXT NOT NULL,
  schema TEXT NOT NULL CHECK(json_valid(schema))
) STRICT;
CREATE TRIGGER IF NOT EXISTS _gonvex_identity_key_immutable BEFORE UPDATE ON _gonvex_identity_keys WHEN NEW._id IS NOT OLD._id OR NEW.domain IS NOT OLD.domain OR NEW.value IS NOT OLD.value BEGIN SELECT RAISE(ABORT,'public identity mappings are immutable'); END;
CREATE TRIGGER IF NOT EXISTS _gonvex_identity_key_retained BEFORE DELETE ON _gonvex_identity_keys BEGIN SELECT RAISE(ABORT,'public identity mappings remain stable after record deletion'); END;
CREATE TRIGGER IF NOT EXISTS _gonvex_interned_text_immutable BEFORE UPDATE ON _gonvex_interned_text WHEN NEW.value IS NOT OLD.value OR NEW._id IS NOT OLD._id BEGIN SELECT RAISE(ABORT,'interned text is immutable; assign a different key'); END;
CREATE TRIGGER IF NOT EXISTS _gonvex_interned_text_retained BEFORE DELETE ON _gonvex_interned_text BEGIN SELECT RAISE(ABORT,'interned text preserves immutable snapshots'); END;

CREATE TABLE IF NOT EXISTS _gonvex_record_guard(_id INTEGER PRIMARY KEY);
CREATE TRIGGER IF NOT EXISTS _gonvex_record_guard_empty BEFORE INSERT ON _gonvex_record_guard BEGIN SELECT RAISE(ABORT,'record changes must finalize before commit'); END;
CREATE TABLE IF NOT EXISTS _gonvex_record_pending(
 table_name TEXT NOT NULL,row_id TEXT NOT NULL,old_value TEXT,guard INTEGER NOT NULL DEFAULT 0 REFERENCES _gonvex_record_guard(_id) DEFERRABLE INITIALLY DEFERRED,PRIMARY KEY(table_name,row_id)
) STRICT, WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _gonvex_record_dependencies(
 source_table TEXT NOT NULL,source_column TEXT NOT NULL,
 owner_storage TEXT NOT NULL,owner_column TEXT NOT NULL,owner_direction TEXT NOT NULL DEFAULT 'inbound' CHECK(owner_direction IN ('inbound','outbound')),
 watch_columns TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(watch_columns) AND json_type(watch_columns)='array'),
 PRIMARY KEY(source_table,source_column,owner_storage,owner_column)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS gonvex_migrations (
             name TEXT PRIMARY KEY,
             checksum TEXT NOT NULL,
             applied_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
             duration_ms INTEGER NOT NULL
           );
