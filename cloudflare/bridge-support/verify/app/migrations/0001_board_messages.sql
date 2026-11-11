-- gonvex:scope tenant
-- The columns of Whagons' boardMessages that the legacy bridge writes, plus the
-- caller and channel the runtime gave the Reducer (verification only).
CREATE TABLE IF NOT EXISTS "boardMessages" (
  "_id" TEXT PRIMARY KEY NOT NULL,
  "_creationTime" INTEGER NOT NULL,
  "boardId" TEXT NOT NULL,
  "createdBy" TEXT NOT NULL,
  "title" TEXT,
  "content" TEXT,
  "isPinned" BOOLEAN NOT NULL DEFAULT 0,
  "startsAt" INTEGER,
  "endsAt" INTEGER,
  "caller" TEXT NOT NULL,
  "channel" TEXT NOT NULL
);
