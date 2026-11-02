-- gonvex:scope tenant
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY NOT NULL,
  body TEXT NOT NULL,
  author TEXT NOT NULL,
  -- ISO-8601 UTC text (schema.datetime()); sorts chronologically.
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS by_created_at ON messages (created_at);
