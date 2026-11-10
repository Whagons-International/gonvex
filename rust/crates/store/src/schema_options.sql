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
