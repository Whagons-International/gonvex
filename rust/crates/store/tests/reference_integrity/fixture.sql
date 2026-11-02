CREATE TABLE teams(_id text PRIMARY KEY,"deletedAt" bigint);
CREATE TABLE workspaces(_id text PRIMARY KEY,teams jsonb,"deletedAt" bigint,note text);
CREATE TABLE approvals(_id text PRIMARY KEY,"deletedAt" bigint,selectors jsonb);
CREATE TABLE history(_id text PRIMARY KEY,"teamId" text);
INSERT INTO teams VALUES('before',NULL);
INSERT INTO workspaces VALUES('before','["before"]',NULL,'before');
-- Demonstrate the reported invariant fails on the original unconstrained schema.
DO $$ BEGIN
  BEGIN
    UPDATE teams SET "deletedAt"=1 WHERE _id='before';
    IF NOT EXISTS (SELECT FROM workspaces w JOIN teams t ON w.teams ? t._id WHERE t."deletedAt" IS NOT NULL) THEN
      RAISE EXCEPTION 'before fixture did not reproduce dangling live reference';
    END IF;
    RAISE EXCEPTION USING ERRCODE='P0004', MESSAGE='rollback reproduced orphan';
  EXCEPTION WHEN assert_failure THEN NULL;
  END;
END $$;
INSERT INTO workspaces VALUES('legacy','["missing"]',NULL,'unchanged');
SELECT gonvex_declare_references($catalog${
  "version":1,
  "tables":[
    {"name":"teams","key":"_id","live":"$.deletedAt == null"},
    {"name":"workspaces","key":"_id","live":"$.deletedAt == null"},
    {"name":"approvals","key":"_id","live":"$.deletedAt == null"},
    {"name":"history","key":"_id","live":"true"}
  ],
  "references":[
    {"name":"workspace.teams","source":"workspaces","path":"$.teams[*]","target":"teams","state":"live"},
    {"name":"approval.selectors","source":"approvals","at":"$.selectors[*]","path":"$.id","discriminator":"$.type","targets":{"team":"teams"},"state":"live"},
    {"name":"history.team","source":"history","path":"$.teamId","target":"teams","state":"exists","sourceWhen":"true"}
  ]
}$catalog$::jsonb);
