CREATE TABLE teams(_id TEXT PRIMARY KEY,"deletedAt" INTEGER);
CREATE TABLE workspaces(_id TEXT PRIMARY KEY,teams JSON,"deletedAt" INTEGER,note TEXT);
CREATE TABLE approvals(_id TEXT PRIMARY KEY,"deletedAt" INTEGER,selectors JSON);
CREATE TABLE history(_id TEXT PRIMARY KEY,"teamId" TEXT);
INSERT INTO teams VALUES('before',NULL);
INSERT INTO workspaces VALUES('before','["before"]',NULL,'before');
INSERT INTO workspaces VALUES('legacy','["missing"]',NULL,'unchanged');
INSERT INTO _gonvex_reference_requests(declaration) VALUES('{
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
}');
