-- Experimental LOCAL-only schema. Never part of tenant provisioning/migrations.
-- Installed explicitly only inside the dedicated permission fixture directory.
CREATE TABLE IF NOT EXISTS _gonvex_permission_identity (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), instance_id TEXT NOT NULL,
 physical_key TEXT NOT NULL,
 schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version=1)
);
CREATE TABLE IF NOT EXISTS _gonvex_permission_scopes (
 scope_id INTEGER PRIMARY KEY, canonical_scope BLOB NOT NULL UNIQUE,
 instance_id TEXT NOT NULL, epoch TEXT NOT NULL, table_id TEXT NOT NULL COLLATE BINARY,
 generation INTEGER NOT NULL CHECK(generation>0), phase TEXT NOT NULL CHECK(phase IN ('building','pending','ready','failed')),
 covered_revision INTEGER NOT NULL DEFAULT -1,
 change_serial INTEGER NOT NULL DEFAULT 0 CHECK(typeof(change_serial)='integer' AND change_serial>=0)
);
-- A backfill declares its normalized sources BEFORE loading them. An empty
-- replacement still completes a source (revoke); missing loads never do.
CREATE TABLE IF NOT EXISTS _gonvex_permission_sources (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('grant','row','membership')), source_id TEXT NOT NULL COLLATE BINARY,
 complete INTEGER NOT NULL DEFAULT 0 CHECK(complete IN (0,1)),
 PRIMARY KEY(scope_id,generation,kind,source_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _gonvex_permission_atoms (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL, atom_id INTEGER NOT NULL,
 canonical_key BLOB NOT NULL,
 PRIMARY KEY(scope_id,generation,atom_id), UNIQUE(scope_id,generation,canonical_key)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _gonvex_permission_supports (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL,
 source_id TEXT NOT NULL COLLATE BINARY,
 target_kind TEXT NOT NULL CHECK(target_kind IN ('member','group')),
 target_id TEXT NOT NULL COLLATE BINARY, atom_id INTEGER NOT NULL,
 PRIMARY KEY(scope_id,generation,source_id,target_kind,target_id,atom_id),
 FOREIGN KEY(scope_id,generation,atom_id) REFERENCES _gonvex_permission_atoms(scope_id,generation,atom_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _gonvex_permission_support_set ON _gonvex_permission_supports(scope_id,generation,target_kind,target_id,atom_id);
CREATE INDEX IF NOT EXISTS _gonvex_permission_support_atom ON _gonvex_permission_supports(scope_id,generation,atom_id,target_kind,target_id);
CREATE TABLE IF NOT EXISTS _gonvex_permission_memberships (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL,
 source_id TEXT NOT NULL COLLATE BINARY, group_id TEXT NOT NULL COLLATE BINARY,
 member_kind TEXT NOT NULL CHECK(member_kind IN ('member','group')), member_id TEXT NOT NULL COLLATE BINARY,
 PRIMARY KEY(scope_id,generation,source_id,group_id,member_kind,member_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _gonvex_permission_group_members ON _gonvex_permission_memberships(scope_id,generation,group_id,member_kind,member_id);
CREATE INDEX IF NOT EXISTS _gonvex_permission_member_groups ON _gonvex_permission_memberships(scope_id,generation,member_kind,member_id,group_id);
-- UNION deduplicates reachable subjects across alternate group paths. Grant
-- support counts count distinct original sources, not paths through a DAG.
CREATE VIEW IF NOT EXISTS _gonvex_permission_set_atoms AS
 WITH RECURSIVE reach(scope_id,generation,group_id,member_id) AS (
  SELECT scope_id,generation,group_id,member_id FROM _gonvex_permission_memberships WHERE member_kind='member'
  UNION
  SELECT m.scope_id,m.generation,m.group_id,r.member_id FROM _gonvex_permission_memberships m
  JOIN reach r ON r.scope_id=m.scope_id AND r.generation=m.generation AND r.group_id=m.member_id
  WHERE m.member_kind='group'
 ), effective(scope_id,generation,set_id,atom_id,source_id) AS (
  SELECT scope_id,generation,target_id,atom_id,source_id FROM _gonvex_permission_supports WHERE target_kind='member'
  UNION ALL
  SELECT s.scope_id,s.generation,r.member_id,s.atom_id,s.source_id FROM _gonvex_permission_supports s
  JOIN reach r ON r.scope_id=s.scope_id AND r.generation=s.generation AND r.group_id=s.target_id WHERE s.target_kind='group'
 )
 SELECT scope_id,generation,set_id,atom_id,count(DISTINCT source_id) AS support_count
 FROM effective GROUP BY scope_id,generation,set_id,atom_id;
CREATE TABLE IF NOT EXISTS _gonvex_permission_row_supports (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL, source_id TEXT NOT NULL COLLATE BINARY,
 table_id TEXT NOT NULL COLLATE BINARY, universe_id TEXT NOT NULL COLLATE BINARY,
 row_key TEXT NOT NULL COLLATE BINARY,
 PRIMARY KEY(scope_id,generation,source_id,table_id,universe_id,row_key)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _gonvex_permission_universe ON _gonvex_permission_row_supports(scope_id,generation,table_id,universe_id,row_key);
CREATE VIEW IF NOT EXISTS _gonvex_permission_rows AS
 SELECT DISTINCT scope_id,generation,table_id,universe_id,row_key FROM _gonvex_permission_row_supports;
CREATE TABLE IF NOT EXISTS _gonvex_permission_row_atom_supports (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL, source_id TEXT NOT NULL COLLATE BINARY,
 table_id TEXT NOT NULL COLLATE BINARY, row_key TEXT NOT NULL COLLATE BINARY,
 slot TEXT NOT NULL COLLATE BINARY, atom_id INTEGER NOT NULL,
 PRIMARY KEY(scope_id,generation,source_id,table_id,row_key,slot,atom_id),
 FOREIGN KEY(scope_id,generation,atom_id) REFERENCES _gonvex_permission_atoms(scope_id,generation,atom_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _gonvex_permission_atom_row ON _gonvex_permission_row_atom_supports(scope_id,generation,table_id,slot,atom_id,row_key);
CREATE INDEX IF NOT EXISTS _gonvex_permission_row_atom ON _gonvex_permission_row_atom_supports(scope_id,generation,table_id,row_key,slot,atom_id);
CREATE VIEW IF NOT EXISTS _gonvex_permission_row_atoms AS
 SELECT DISTINCT scope_id,generation,table_id,row_key,slot,atom_id FROM _gonvex_permission_row_atom_supports;
