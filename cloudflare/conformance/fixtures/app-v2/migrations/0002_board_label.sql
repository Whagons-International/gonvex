-- gonvex:scope tenant
-- v2 adds a column; existing rows take the default.
ALTER TABLE board ADD COLUMN label TEXT NOT NULL DEFAULT 'v1';
