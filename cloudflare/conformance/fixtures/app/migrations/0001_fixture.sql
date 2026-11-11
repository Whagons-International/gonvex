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
