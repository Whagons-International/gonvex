-- gonvex:scope tenant
ALTER TABLE messages
  ALTER COLUMN created_at SET DEFAULT now();
