-- gonvex:scope tenant
-- Live Queries (K08): the tables Whagons' task grid plan (`tasks.grid`, whagons5-client `2457bb65f` gonvex/tasks/gridQueries.ts) reads,
-- with the columns it names and Whagons' declared types. The fixture's
-- Reducers maintain the grid projection and the search documents
-- explicitly (Whagons uses triggers), so the change log alone replays them.
-- No clock columns: timestamps come from the scenario.
CREATE TABLE IF NOT EXISTS tasks (
  "_id" TEXT PRIMARY KEY NOT NULL,
  "_creationTime" REAL,
  "id" INTEGER,
  "name" TEXT,
  "description" TEXT,
  "workspaceId" TEXT,
  "statusId" TEXT,
  "priorityId" TEXT,
  "categoryId" TEXT,
  "spotId" TEXT,
  "teamId" TEXT,
  "templateId" TEXT,
  "createdBy" TEXT,
  "attributedCreatorId" TEXT,
  "createdVia" TEXT,
  "createdByWorkflowId" TEXT,
  "createdByWorkflowRunId" TEXT,
  "workflowTriggeredByUserId" TEXT,
  "approvalId" TEXT,
  "slaId" TEXT,
  "formId" TEXT,
  "formVersionId" TEXT,
  "requiresSignature" BOOLEAN,
  "assetId" TEXT,
  "fleetVehicleId" TEXT,
  "fleetLeaderId" TEXT,
  "fleetCompanionId" TEXT,
  "requiresAcknowledgment" BOOLEAN,
  "audienceRules" JSON,
  "reportedByGuest" BOOLEAN,
  "guestId" TEXT,
  "guestVip" BOOLEAN,
  "guestReportSeenAt" INTEGER,
  "reportedByTeamId" TEXT,
  "reportedByUserId" TEXT,
  "firstViewedAt" INTEGER,
  "dueDate" INTEGER,
  "startDate" INTEGER,
  "latitude" REAL,
  "longitude" REAL,
  "completedAt" INTEGER,
  "createdAt" INTEGER,
  "updatedAt" INTEGER,
  "flagColor" TEXT,
  "rowHash" TEXT,
  "recurrenceId" TEXT,
  "workplanId" TEXT,
  "workplanItemId" TEXT,
  "workplanInstanceId" TEXT,
  "occurrenceDate" TEXT,
  "deletedAt" INTEGER,
  "deletedBy" TEXT
);
CREATE INDEX IF NOT EXISTS tasks_by_workspace ON tasks ("workspaceId", "id");

-- The grid's one-to-one read projection (Whagons: maintained by triggers).
CREATE TABLE IF NOT EXISTS "taskGridRows" (
  "taskId" TEXT PRIMARY KEY NOT NULL,
  "assetId" TEXT,
  "fleetVehicleId" TEXT,
  "fleetLeaderId" TEXT,
  "fleetCompanionId" TEXT,
  "reportedByGuest" BOOLEAN,
  "guestVip" BOOLEAN,
  "id" INTEGER,
  "workspaceId" TEXT,
  "name" TEXT,
  "description" TEXT,
  "statusId" TEXT,
  "priorityId" TEXT,
  "categoryId" TEXT,
  "spotId" TEXT,
  "teamId" TEXT,
  "templateId" TEXT,
  "createdBy" TEXT,
  "dueDate" INTEGER,
  "startDate" INTEGER,
  "completedAt" INTEGER,
  "createdAt" INTEGER,
  "updatedAt" INTEGER,
  "_creationTime" REAL,
  "flagColor" TEXT,
  "deletedAt" INTEGER,
  "statusOrder" INTEGER,
  "statusName" TEXT,
  "priorityOrder" INTEGER,
  "priorityName" TEXT,
  "searchText" TEXT
);
CREATE INDEX IF NOT EXISTS task_grid_rows_by_workspace ON "taskGridRows" ("workspaceId", "id");

CREATE TABLE IF NOT EXISTS "taskRelatedSearchDocuments" (
  "taskId" TEXT PRIMARY KEY NOT NULL,
  "searchText" TEXT
);

CREATE TABLE IF NOT EXISTS "taskWorkspaceContexts" (
  "_id" TEXT PRIMARY KEY NOT NULL,
  "taskId" TEXT NOT NULL,
  "workspaceId" TEXT NOT NULL,
  "kind" TEXT,
  "recipientUserIds" JSON,
  "targetUserId" TEXT,
  "targetTeamId" TEXT,
  "revokedAt" INTEGER
);

CREATE TABLE IF NOT EXISTS "memberTeams" (
  "_id" TEXT PRIMARY KEY NOT NULL,
  "memberId" TEXT NOT NULL,
  "teamId" TEXT NOT NULL,
  "deletedAt" INTEGER
);

CREATE TABLE IF NOT EXISTS "taskUsers" (
  "_id" TEXT PRIMARY KEY NOT NULL,
  "taskId" TEXT NOT NULL,
  "userId" TEXT NOT NULL,
  "deletedAt" INTEGER
);

CREATE TABLE IF NOT EXISTS "taskFindings" (
  "_id" TEXT PRIMARY KEY NOT NULL,
  "linkedTaskId" TEXT,
  "deletedAt" INTEGER
);

CREATE TABLE IF NOT EXISTS statuses (
  "_id" TEXT PRIMARY KEY NOT NULL,
  "name" TEXT NOT NULL,
  "order" INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS priorities (
  "_id" TEXT PRIMARY KEY NOT NULL,
  "name" TEXT NOT NULL,
  "order" INTEGER NOT NULL
);
