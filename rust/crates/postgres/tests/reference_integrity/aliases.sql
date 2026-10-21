CREATE TABLE "aliasTeams"(_id text PRIMARY KEY,code text UNIQUE,"deletedAt" bigint);
CREATE TABLE "aliasBackups"(_id text PRIMARY KEY,"deletedAt" bigint);
CREATE TABLE "aliasLinks"(_id text PRIMARY KEY,ref text,alias text,payload jsonb,"deletedAt" bigint);
INSERT INTO "aliasTeams" VALUES('one','alpha',NULL);
INSERT INTO "aliasBackups" VALUES('one',NULL);
INSERT INTO "aliasLinks" VALUES('link','one','alpha','{"first":"one","other":"builtin"}',NULL);
SELECT gonvex_declare_references($catalog${
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
}$catalog$::jsonb);
DO $$ BEGIN
  BEGIN
    UPDATE "aliasTeams" SET code='beta' WHERE _id='one';
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'renaming a referenced unique key was accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
  BEGIN
    UPDATE "aliasTeams" SET "deletedAt"=1 WHERE _id='one';
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'removing a referenced literal-path target was accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
END $$;
DO $$ BEGIN
  BEGIN
    PERFORM * FROM _gonvex_reference_values('{"refs":[17]}'::jsonb,
      '{"name":"strict","path":"$.refs[*]","target":"aliasTeams"}'::jsonb);
    RAISE EXCEPTION 'numeric ID was accepted without a declared policy';
  EXCEPTION WHEN check_violation THEN NULL; END;
  IF (SELECT target_id FROM _gonvex_reference_values('{"refs":[17]}'::jsonb,
      '{"name":"legacy","path":"$.refs[*]","target":"aliasTeams","allowNumbers":true}'::jsonb))<>'17' THEN
    RAISE EXCEPTION 'declared numeric ID was not normalized to its consumer identity';
  END IF;
END $$;
-- The declared numeric policy still resolves through the native target FK.
SELECT gonvex_declare_references(jsonb_set(jsonb_set(declaration,
  '{references,2,allowNumbers}','true'),'{references,2,numberReason}',
  '"The legacy consumer explicitly normalizes numeric identifiers through String."'))
  FROM _gonvex_reference_catalog;
INSERT INTO "aliasTeams" VALUES('17','seventeen',NULL);
UPDATE "aliasLinks" SET payload='{"first":17}'::jsonb;
DO $$ BEGIN
  BEGIN
    DELETE FROM "aliasTeams" WHERE _id='17';
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'declared numeric ID bypassed target deletion enforcement';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
END $$;
-- JSON serialized inside a scalar must enforce the same reference paths.
UPDATE "aliasLinks" SET payload=to_jsonb('{"first":"one","other":"builtin"}'::text);
DO $$ BEGIN
  BEGIN
    UPDATE "aliasLinks" SET payload=to_jsonb('{"first":"missing"}'::text);
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'serialized JSON bypassed reference enforcement';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
  BEGIN
    UPDATE "aliasLinks" SET payload=to_jsonb('not-json'::text);
    RAISE EXCEPTION 'invalid serialized JSON bypassed extraction';
  EXCEPTION WHEN invalid_text_representation THEN NULL; END;
END $$;
UPDATE "aliasLinks" SET alias=NULL,payload=NULL;
-- Shared namespace remains live while either real table still owns it.
UPDATE "aliasTeams" SET "deletedAt"=2 WHERE _id='one';
DO $$ BEGIN
  BEGIN
    DELETE FROM "aliasBackups" WHERE _id='one';
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'deleting the last shared target was accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
END $$;
UPDATE "aliasLinks" SET ref=NULL;
DELETE FROM "aliasBackups" WHERE _id='one';
INSERT INTO "aliasLinks" VALUES('revoked','missing',NULL,NULL,3);
DO $$ BEGIN
  BEGIN
    UPDATE "aliasLinks" SET "deletedAt"=NULL WHERE _id='revoked';
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'reactivating a source with a missing target was accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
END $$;
-- Rebuild declarations without changing app data or losing alternate keys.
SELECT gonvex_declare_references(declaration) FROM _gonvex_reference_catalog;
DO $$ BEGIN
  IF EXISTS(SELECT FROM gonvex_reference_violations()) THEN RAISE EXCEPTION 'unexpected violation'; END IF;
  IF (SELECT code FROM "aliasTeams" WHERE _id='one')<>'alpha' THEN RAISE EXCEPTION 'failed rename changed application data'; END IF;
  IF (SELECT owner_count FROM _gonvex_reference_targets WHERE table_name='shared' AND row_id='one' AND state='exists')<>1 THEN RAISE EXCEPTION 'shared namespace count incorrect'; END IF;
  IF EXISTS(SELECT FROM _gonvex_reference_targets WHERE table_name='shared' AND row_id='one' AND state='live') THEN RAISE EXCEPTION 'shared live token survived the last live owner'; END IF;
  BEGIN
    ALTER TABLE "aliasLinks" ADD COLUMN "unreviewedId" text;
    PERFORM gonvex_check_reference_coverage((SELECT declaration FROM _gonvex_reference_catalog));
    RAISE EXCEPTION 'coverage lint accepted an unclassified ID';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM NOT LIKE 'unclassified reference candidate aliasLinks.unreviewedId%' THEN RAISE; END IF;
  END;
END $$;
DO $$ DECLARE catalog_ jsonb; BEGIN
  SELECT declaration INTO catalog_ FROM _gonvex_reference_catalog;
  BEGIN
    ALTER TABLE "aliasLinks" ADD COLUMN arbitrary_owner text;
    PERFORM gonvex_check_reference_coverage(catalog_);
    RAISE EXCEPTION 'coverage lint accepted a new field without an ID suffix';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM NOT LIKE 'unclassified reference candidate aliasLinks.arbitrary_owner%' THEN RAISE; END IF;
  END;
  BEGIN
    PERFORM gonvex_check_reference_coverage(jsonb_set(catalog_,'{references,0,column}','"alias"'));
    RAISE EXCEPTION 'coverage lint accepted a path in a different source column';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM NOT LIKE 'reference % selection must start at its declared column' THEN RAISE; END IF;
  END;
END $$;
SELECT 'alternate keys, union lifecycle, multiple paths, sentinels, reactivation, upgrade and coverage: passed' AS result;
