-- Explicit local normalized decision program storage; never tenant provisioning.
CREATE TABLE IF NOT EXISTS _gonvex_permission_decision_nodes (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL, node_id INTEGER NOT NULL,
 atom_id INTEGER NOT NULL, low_id INTEGER NOT NULL, high_id INTEGER NOT NULL,
 PRIMARY KEY(scope_id,generation,node_id),
 UNIQUE(scope_id,generation,atom_id,low_id,high_id), CHECK(low_id<>high_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _gonvex_permission_decision_atom
 ON _gonvex_permission_decision_nodes(scope_id,generation,atom_id,node_id);
CREATE TABLE IF NOT EXISTS _gonvex_permission_decision_edges (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL, child_id INTEGER NOT NULL,
 node_id INTEGER NOT NULL, atom_id INTEGER NOT NULL, branch INTEGER NOT NULL CHECK(branch IN (0,1)),
 PRIMARY KEY(scope_id,generation,child_id,node_id,branch)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _gonvex_permission_programs (
 scope_id INTEGER PRIMARY KEY, generation INTEGER NOT NULL, program BLOB NOT NULL,
 canonical_authority BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS _gonvex_permission_program_rows (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL, row_key TEXT NOT NULL COLLATE BINARY,
 root_id INTEGER NOT NULL, PRIMARY KEY(scope_id,generation,row_key)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _gonvex_permission_program_root
 ON _gonvex_permission_program_rows(scope_id,generation,root_id,row_key);
CREATE TABLE IF NOT EXISTS _gonvex_permission_program_changes (
 event_id INTEGER PRIMARY KEY, scope_id INTEGER NOT NULL, source_id TEXT NOT NULL,
 table_name TEXT NOT NULL, old_key TEXT, new_key TEXT, old_image JSON, new_image JSON
);
CREATE INDEX IF NOT EXISTS _gonvex_permission_program_pending
 ON _gonvex_permission_program_changes(scope_id,event_id);
CREATE TABLE IF NOT EXISTS _gonvex_permission_program_row_deltas (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL, revision INTEGER NOT NULL,
 row_key TEXT NOT NULL COLLATE BINARY, before_root INTEGER, after_root INTEGER,
 PRIMARY KEY(scope_id,generation,revision,row_key)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _gonvex_permission_program_principal_deltas (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL, revision INTEGER NOT NULL,
 principal TEXT NOT NULL, atom_id INTEGER NOT NULL, before_present INTEGER NOT NULL,
 after_present INTEGER NOT NULL,
 PRIMARY KEY(scope_id,generation,revision,principal,atom_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _gonvex_permission_decision_anchors (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL, atom_id INTEGER NOT NULL,
 root_id INTEGER NOT NULL, PRIMARY KEY(scope_id,generation,atom_id,root_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _gonvex_permission_decision_unanchored (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL, root_id INTEGER NOT NULL,
 PRIMARY KEY(scope_id,generation,root_id)
) WITHOUT ROWID;
-- Reverse dependencies are qualified by a root's positive cover. A shared
-- negative atom must not walk every other member's positive row root before
-- filtering them out. cover_atom=0 is the explicit all-absent universe bucket.
CREATE TABLE IF NOT EXISTS _gonvex_permission_decision_dependencies (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL,
 dependency_atom INTEGER NOT NULL, cover_atom INTEGER NOT NULL, root_id INTEGER NOT NULL,
 PRIMARY KEY(scope_id,generation,dependency_atom,cover_atom,root_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _gonvex_permission_program_principals (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL, principal TEXT NOT NULL,
 PRIMARY KEY(scope_id,generation,principal)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _gonvex_permission_program_resets (
 scope_id INTEGER NOT NULL, generation INTEGER NOT NULL, revision INTEGER NOT NULL,
 PRIMARY KEY(scope_id,generation,revision)
) WITHOUT ROWID;
