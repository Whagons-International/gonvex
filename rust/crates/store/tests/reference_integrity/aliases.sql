CREATE TABLE "aliasTeams"(_id TEXT PRIMARY KEY,code TEXT UNIQUE,"deletedAt" INTEGER);
CREATE TABLE "aliasBackups"(_id TEXT PRIMARY KEY,"deletedAt" INTEGER);
CREATE TABLE "aliasLinks"(_id TEXT PRIMARY KEY,ref TEXT,alias TEXT,payload JSON,"deletedAt" INTEGER);
INSERT INTO "aliasTeams" VALUES('one','alpha',NULL);
INSERT INTO "aliasBackups" VALUES('one',NULL);
INSERT INTO "aliasLinks" VALUES('link','one','alpha','{"first":"one","other":"builtin"}',NULL);
INSERT INTO _gonvex_reference_requests(declaration) VALUES('{
 "version":1,
 "tables":[
  {"name":"aliasTeams","key":"_id","live":"$.deletedAt == null","targetKeys":[{"name":"aliasTeams.code","column":"code"},{"name":"shared","column":"_id"}]},
  {"name":"aliasBackups","key":"_id","live":"$.deletedAt == null","targetKeys":[{"name":"shared","column":"_id"}]},
  {"name":"aliasLinks","key":"_id","live":"$.deletedAt == null"}
 ],
 "references":[
  {"name":"aliasLinks.ref","source":"aliasLinks","column":"ref","path":"$.ref","target":"shared","state":"live"},
  {"name":"aliasLinks.alias","source":"aliasLinks","column":"alias","path":"$.alias","target":"aliasTeams.code","state":"live"},
  {"name":"aliasLinks.payload","source":"aliasLinks","column":"payload","encoding":"json-or-value","paths":["$.payload.first","$.payload.other"],"target":"aliasTeams","state":"live","ignoreValues":["builtin"],"ignoreReason":"A predefined catalog literal, not a tenant entity."}
 ],"exclusions":[
  {"source":"members","column":"account_id","kind":"external","reason":"Framework account identity belongs to the control plane."},
  {"source":"members","column":"status","kind":"lifecycle","reason":"Framework member admission lifecycle vocabulary."},
  {"source":"members","column":"display_name","kind":"content","reason":"Framework member presentation text, not a selector."},
  {"source":"members","column":"avatar_url","kind":"external","reason":"Framework member avatar asset URL, not a tenant row."},
  {"source":"members","column":"role","kind":"enum","reason":"Framework member admission role vocabulary."},
  {"source":"members","column":"permissions","kind":"enum","reason":"Framework permission names mapped to grant booleans."}
 ]
}');
