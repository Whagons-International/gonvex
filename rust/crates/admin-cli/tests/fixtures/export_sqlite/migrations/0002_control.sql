-- gonvex:scope control-plane
CREATE TABLE IF NOT EXISTS app_settings (
  key TEXT PRIMARY KEY,
  value JSON NOT NULL,
  updated_at INTEGER NOT NULL
);
