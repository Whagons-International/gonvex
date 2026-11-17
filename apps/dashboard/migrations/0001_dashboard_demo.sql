-- gonvex:scope tenant
-- SQLite. Timestamps are ISO-8601 UTC TEXT, the format the demo grid parses
-- and sorts. Text search scans with LIKE; there are no trigram indexes.

CREATE TABLE IF NOT EXISTS dashboard_demo_tasks (
  id TEXT PRIMARY KEY NOT NULL,
  tenant_id TEXT,
  pg_id INTEGER,
  name TEXT,
  title TEXT NOT NULL,
  status TEXT NOT NULL,
  description TEXT,
  audience TEXT,
  audience_user_ids JSON,
  audience_team_ids JSON,
  requires_acknowledgment BOOLEAN,
  workspace_id TEXT,
  category_id TEXT,
  team_id TEXT,
  template_id TEXT,
  spot_id TEXT,
  status_id TEXT,
  priority_id TEXT,
  status_name TEXT,
  status_color TEXT,
  status_action TEXT,
  status_icon TEXT,
  status_working_animation TEXT,
  status_initial BOOLEAN,
  priority_name TEXT,
  priority_color TEXT,
  category_name TEXT,
  category_icon TEXT,
  category_color TEXT,
  tag_names TEXT,
  tag_colors TEXT,
  attachment_count INTEGER,
  view_count INTEGER,
  sla_id TEXT,
  approval_id TEXT,
  form_id TEXT,
  requires_signature BOOLEAN,
  asset_id TEXT,
  created_by TEXT,
  reported_by_team_id TEXT,
  reported_by_user_id TEXT,
  recurrence_id TEXT,
  workplan_id TEXT,
  workplan_item_id TEXT,
  workplan_instance_id TEXT,
  occurrence_date TEXT,
  expected_duration INTEGER,
  flag_color TEXT,
  spot_name TEXT,
  workspace_name TEXT,
  assignee_names TEXT,
  assignee_ids TEXT,
  assignee_avatar_urls TEXT,
  all_user_names TEXT,
  all_user_avatar_urls TEXT,
  notes_count INTEGER,
  sla_started_at TEXT,
  sla_response_deadline TEXT,
  sla_resolution_deadline TEXT,
  sla_responded_at TEXT,
  sla_resolved_at TEXT,
  sla_paused_duration INTEGER,
  due_date TEXT,
  start_date TEXT,
  completed_at TEXT,
  latitude REAL,
  longitude REAL,
  first_viewed_at TEXT,
  deleted_at TEXT,
  row_hash TEXT,
  priority TEXT,
  assignee TEXT,
  project TEXT,
  label TEXT,
  due_at TEXT,
  completed BOOLEAN,
  estimate_minutes INTEGER,
  progress INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT
);

CREATE INDEX IF NOT EXISTS by_status ON dashboard_demo_tasks (status);
CREATE INDEX IF NOT EXISTS by_pg_id ON dashboard_demo_tasks (pg_id);
CREATE INDEX IF NOT EXISTS by_name ON dashboard_demo_tasks (name);
CREATE INDEX IF NOT EXISTS by_tenant_pg_id ON dashboard_demo_tasks (tenant_id, pg_id);
CREATE INDEX IF NOT EXISTS by_workspace ON dashboard_demo_tasks (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS by_status_id ON dashboard_demo_tasks (tenant_id, status_id);
CREATE INDEX IF NOT EXISTS by_category ON dashboard_demo_tasks (tenant_id, category_id);
CREATE INDEX IF NOT EXISTS by_priority_id ON dashboard_demo_tasks (tenant_id, priority_id);
CREATE INDEX IF NOT EXISTS by_status_name ON dashboard_demo_tasks (status_name);
CREATE INDEX IF NOT EXISTS by_priority_name ON dashboard_demo_tasks (priority_name);
CREATE INDEX IF NOT EXISTS by_team ON dashboard_demo_tasks (tenant_id, team_id);
CREATE INDEX IF NOT EXISTS by_spot ON dashboard_demo_tasks (tenant_id, spot_id);
CREATE INDEX IF NOT EXISTS by_spot_name ON dashboard_demo_tasks (spot_name);
CREATE INDEX IF NOT EXISTS by_workspace_name ON dashboard_demo_tasks (workspace_name);
CREATE INDEX IF NOT EXISTS by_assignee_names ON dashboard_demo_tasks (assignee_names);
CREATE INDEX IF NOT EXISTS by_flag_color ON dashboard_demo_tasks (flag_color);
CREATE INDEX IF NOT EXISTS by_priority ON dashboard_demo_tasks (priority);
CREATE INDEX IF NOT EXISTS by_assignee ON dashboard_demo_tasks (assignee);
CREATE INDEX IF NOT EXISTS by_project ON dashboard_demo_tasks (project);
CREATE INDEX IF NOT EXISTS by_due_date ON dashboard_demo_tasks (due_date);
CREATE INDEX IF NOT EXISTS by_due_at ON dashboard_demo_tasks (due_at);
CREATE INDEX IF NOT EXISTS by_created_at ON dashboard_demo_tasks (created_at);
CREATE INDEX IF NOT EXISTS by_created_at_id ON dashboard_demo_tasks (created_at, id);
CREATE INDEX IF NOT EXISTS by_updated_at ON dashboard_demo_tasks (updated_at);
CREATE INDEX IF NOT EXISTS by_active_created_at ON dashboard_demo_tasks (deleted_at, created_at, id);
CREATE INDEX IF NOT EXISTS by_active_pg_id ON dashboard_demo_tasks (deleted_at, pg_id, id);
CREATE INDEX IF NOT EXISTS by_workspace_created_at ON dashboard_demo_tasks (tenant_id, workspace_id, deleted_at, created_at, id);
CREATE INDEX IF NOT EXISTS by_workspace_pg_id ON dashboard_demo_tasks (tenant_id, workspace_id, deleted_at, pg_id, id);
CREATE INDEX IF NOT EXISTS by_status_created_at ON dashboard_demo_tasks (tenant_id, status_id, deleted_at, created_at, id);
CREATE INDEX IF NOT EXISTS by_priority_created_at ON dashboard_demo_tasks (tenant_id, priority_id, deleted_at, created_at, id);
CREATE INDEX IF NOT EXISTS by_assignee_created_at ON dashboard_demo_tasks (tenant_id, assignee, deleted_at, created_at, id);
CREATE INDEX IF NOT EXISTS by_due_created_at ON dashboard_demo_tasks (tenant_id, due_at, deleted_at, created_at, id);

CREATE TABLE IF NOT EXISTS dashboard_demo_files (
  id TEXT PRIMARY KEY NOT NULL,
  key TEXT NOT NULL,
  content_type TEXT,
  size INTEGER,
  created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS dashboard_demo_files_by_key ON dashboard_demo_files (key);
