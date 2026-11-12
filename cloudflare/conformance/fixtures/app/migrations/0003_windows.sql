-- gonvex:scope tenant
-- Replica windows (K10): row and byte budgets, truncated windows, argument
-- filters, exclusions and multi-page snapshots. No clock columns.
CREATE TABLE IF NOT EXISTS win_items (
  id TEXT PRIMARY KEY NOT NULL,
  grp TEXT NOT NULL,
  rank INTEGER NOT NULL,
  label TEXT NOT NULL,
  archived_at INTEGER
);
CREATE INDEX IF NOT EXISTS win_items_by_rank ON win_items (rank);
CREATE INDEX IF NOT EXISTS win_items_by_grp ON win_items (grp, rank);
