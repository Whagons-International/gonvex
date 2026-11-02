CREATE TABLE fields(_id text PRIMARY KEY,options jsonb,label text);
CREATE TABLE field_values(_id text PRIMARY KEY,"fieldId" text,value jsonb);
INSERT INTO fields VALUES('manual','{"source":"manual"}','Original');
INSERT INTO field_values VALUES('old','manual','"arbitrary text"'),('legacy-config','absent-legacy','"unknown domain"');
SELECT gonvex_declare_references(
  (SELECT declaration FROM _gonvex_reference_catalog) || $catalog${
    "tables":[
      {"name":"teams","key":"_id","live":"$.deletedAt == null"},
      {"name":"workspaces","key":"_id","live":"$.deletedAt == null"},
      {"name":"approvals","key":"_id","live":"$.deletedAt == null"},
      {"name":"history","key":"_id","live":"true"},
      {"name":"fields","key":"_id","live":"true"},
      {"name":"field_values","key":"_id","live":"true"}
    ],
    "bindings":[{"name":"fieldDomain","source":"fields","columns":["options"]}],
    "references":[{
      "name":"value.domain","source":"field_values","column":"value","path":"$.value[*]",
      "lookup":{"binding":"fieldDomain","key":"$.fieldId","when":"$.options.source == \"entity\""},
      "discriminator":"$.options.entity","targets":{"teams":"teams","workspaces":"workspaces"},"state":"live"
    },{
      "name":"nested.domain","source":"field_values","column":"value","at":"$.value[*] ? (exists(@.fieldId))","path":"$.selected[*]",
      "lookup":{"binding":"fieldDomain","key":"$.fieldId","when":"$.options.source == \"entity\""},
      "discriminator":"$.options.entity","targets":{"teams":"teams","workspaces":"workspaces"},"state":"live"
    }]
  }$catalog$::jsonb
);
DO $$ BEGIN
  BEGIN
    INSERT INTO fields VALUES('absent-legacy','{"source":"entity","entity":"teams"}','Previously missing');
    RAISE EXCEPTION 'a legacy value acquired an unchecked domain';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
END $$;
DELETE FROM field_values WHERE _id='legacy-config';
-- Unrelated parent text can change without releasing the domain binding.
UPDATE fields SET label='Renamed' WHERE _id='manual';
DO $$ BEGIN
  BEGIN
    UPDATE fields SET options='{"source":"entity","entity":"teams"}' WHERE _id='manual';
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'manual values were reinterpreted as references';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
END $$;
INSERT INTO fields VALUES('entity','{"source":"entity","entity":"teams"}','Team');
INSERT INTO teams VALUES('selected',NULL);
INSERT INTO field_values VALUES('entity-value','entity','["selected"]');
-- Each nested container resolves its own parent; manual and entity values can
-- coexist without a missing discriminator ending extraction for later items.
INSERT INTO field_values VALUES('nested-value',NULL,
  '[{"fieldId":"manual","selected":"free text"},{"fieldId":"entity","selected":"selected"}]');
DO $$ BEGIN
  BEGIN
    DELETE FROM teams WHERE _id='selected';
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'configuration-selected target deletion succeeded';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
  BEGIN
    UPDATE fields SET options='{"source":"entity","entity":"workspaces"}' WHERE _id='entity';
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'target domain changed while values depended on it';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
  BEGIN
    INSERT INTO field_values VALUES('missing','absent','"selected"');
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'missing configuration parent accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
END $$;
-- Clear dependents and change configuration atomically.
BEGIN;
DELETE FROM field_values WHERE "fieldId"='entity' OR _id='nested-value';
UPDATE fields SET options='{"source":"entity","entity":"workspaces"}' WHERE _id='entity';
COMMIT;
-- A fresh declaration rebuilds configurations before extracting child edges.
SELECT gonvex_declare_references((SELECT declaration FROM _gonvex_reference_catalog));
DO $$ BEGIN
  IF EXISTS(SELECT FROM gonvex_reference_violations()) THEN
    RAISE EXCEPTION 'configuration binding ledger has an orphan';
  END IF;
END $$;

-- Large parent configurations must never become PostgreSQL index keys.
INSERT INTO fields VALUES('large',jsonb_build_object('source','manual','labels',repeat('long arbitrary configuration ',1000)),'Large');
INSERT INTO field_values VALUES('large-value','large','"free text"');
UPDATE fields SET label='Still large' WHERE _id='large';

DO $$ DECLARE actual jsonb; BEGIN
  SELECT jsonb_agg(v) INTO actual FROM _gonvex_reference_decode('" [ 1, \"selected\", \" \" ] "','csv-or-json-list') v;
  IF actual<>'["1","selected"]'::jsonb THEN RAISE EXCEPTION 'JSON-string selector decoding: %',actual; END IF;
  SELECT jsonb_agg(v) INTO actual FROM _gonvex_reference_decode('" selected, other ,, "','csv-or-json-list') v;
  IF actual<>'["selected","other"]'::jsonb THEN RAISE EXCEPTION 'CSV selector decoding: %',actual; END IF;
  SELECT jsonb_agg(v) INTO actual FROM _gonvex_reference_decode('{"taskId":"selected","label":"A label"}','task-relation') v;
  IF actual<>'["selected"]'::jsonb THEN RAISE EXCEPTION 'Relation selector decoding: %',actual; END IF;
END $$;

-- Chained parent lookups select a domain by each object's local attribute key.
CREATE TABLE templates(_id text PRIMARY KEY,attributes jsonb);
ALTER TABLE workspaces ADD COLUMN "templateId" text;
ALTER TABLE field_values ADD COLUMN "workspaceId" text;
INSERT INTO templates VALUES('typed','[{"id":"place","reference":"teams"}]');
INSERT INTO workspaces(_id,"templateId") VALUES('parent','typed');
INSERT INTO teams VALUES('nested-target',NULL);
SELECT gonvex_declare_references((SELECT declaration FROM _gonvex_reference_catalog) || jsonb_build_object(
  'tables',(SELECT declaration->'tables' FROM _gonvex_reference_catalog)||'[{"name":"templates","key":"_id","live":"true"}]'::jsonb,
  'bindings',(SELECT declaration->'bindings' FROM _gonvex_reference_catalog)||'[
    {"name":"workspaceTemplate","source":"workspaces","columns":["templateId"]},
    {"name":"templateAttributes","source":"templates","columns":["attributes"]}]'::jsonb,
  'references',(SELECT declaration->'references' FROM _gonvex_reference_catalog)||$rule$[{
    "name":"value.attribute","source":"field_values","column":"value","state":"live",
    "at":"$.value ? (@.type() == \"object\").keyvalue() ? (@.value.kind == \"reference\")","path":"$.value.refId",
    "lookup":[{"binding":"workspaceTemplate","from":"document","key":"$.workspaceId","required":true},
      {"binding":"templateAttributes","from":"configuration","key":"$.templateId","required":true}],
    "discriminator":"$.attributes[*] ? (@.id == $entry.key).reference","targets":{"teams":"teams"}
  }]$rule$::jsonb));
INSERT INTO field_values(_id,value,"workspaceId") VALUES('attribute-value','{"place":{"kind":"reference","refId":"nested-target"}}','parent');
DO $$ BEGIN
  BEGIN
    DELETE FROM teams WHERE _id='nested-target';
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'chained configuration reference was lost';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
  BEGIN
    UPDATE templates SET attributes='[]' WHERE _id='typed';
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'attribute definition changed beneath its values';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
END $$;
