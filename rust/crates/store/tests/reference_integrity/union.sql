CREATE TABLE union_owners_a(_id TEXT PRIMARY KEY,code TEXT,"deletedAt" INTEGER);
CREATE TABLE union_owners_b(_id TEXT PRIMARY KEY,"deletedAt" INTEGER);
CREATE TABLE union_links(_id TEXT PRIMARY KEY,ref TEXT,alias TEXT);
INSERT INTO _gonvex_reference_requests(declaration) VALUES('{
 "version":1,
 "tables":[
  {"name":"union_owners_a","key":"_id","live":"$.deletedAt == null","targetKeys":[{"name":"shared","column":"_id"},{"name":"shared_code","column":"code","multipleOwners":true,"reason":"Several owners may publish the same selector value."}]},
  {"name":"union_owners_b","key":"_id","live":"$.deletedAt == null","targetKeys":[{"name":"shared","column":"_id"}]},
  {"name":"union_links","key":"_id","live":"true"}
 ],
 "references":[
  {"name":"union_links.ref","source":"union_links","path":"$.ref","target":"shared","state":"live"},
  {"name":"union_links.alias","source":"union_links","path":"$.alias","target":"shared_code","state":"live"}
 ]
}');
