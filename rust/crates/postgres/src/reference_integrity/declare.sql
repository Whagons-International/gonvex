CREATE OR REPLACE FUNCTION gonvex_declare_references(catalog jsonb)
RETURNS void LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE entry jsonb; rule jsonb; alias_ jsonb; step_ jsonb; path_ text; name_ text; key_ text; actual_key text; target_ text;
BEGIN
  IF catalog->>'version' IS DISTINCT FROM '1'
    OR jsonb_typeof(catalog->'tables') IS DISTINCT FROM 'array'
    OR jsonb_typeof(catalog->'references') IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'invalid reference catalog';
  END IF;
  -- Catalog changes are DDL. The runtime calls this inside the serialized
  -- application migration, and these locks protect direct SQL installation too.
  PERFORM pg_advisory_xact_lock(hashtext('gonvex_reference_catalog:'||current_schema()));
  FOR name_ IN SELECT DISTINCT n FROM (
    SELECT table_name n FROM _gonvex_reference_tables
    UNION ALL SELECT e->>'name' FROM jsonb_array_elements(catalog->'tables') e
  ) names ORDER BY n LOOP
    IF name_ IS NULL OR name_ LIKE '\_gonvex\_%' ESCAPE '\' OR name_ LIKE 'gonvex\_%' ESCAPE '\' THEN
      RAISE EXCEPTION 'invalid application reference table';
    END IF;
    EXECUTE format('LOCK TABLE %I IN ACCESS EXCLUSIVE MODE',name_);
  END LOOP;
  FOR entry IN SELECT value FROM jsonb_array_elements(catalog->'tables') LOOP
    name_:=entry->>'name'; key_:=entry->>'key';
    SELECT CASE WHEN count(*)=1 THEN min(a.attname) END INTO actual_key
    FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey)
    WHERE i.indrelid=to_regclass(format('%I',name_)) AND i.indisprimary;
    IF key_ IS NULL OR key_ IS DISTINCT FROM actual_key THEN
      RAISE EXCEPTION 'reference table % must declare its single primary key',name_;
    END IF;
    IF entry ? 'targetKeys' AND jsonb_typeof(entry->'targetKeys') IS DISTINCT FROM 'array' THEN
      RAISE EXCEPTION 'invalid target key aliases for %',name_;
    END IF;
    FOR alias_ IN SELECT value FROM jsonb_array_elements(COALESCE(entry->'targetKeys','[]'::jsonb)) LOOP
      IF COALESCE(alias_->>'name','')='' OR alias_->>'name' LIKE '$configuration:%' OR EXISTS(
        SELECT FROM jsonb_array_elements(catalog->'tables') t WHERE t->>'name'=alias_->>'name'
      ) OR NOT EXISTS (
        SELECT FROM pg_attribute a WHERE a.attrelid=to_regclass(format('%I',name_))
          AND a.attname=alias_->>'column' AND a.attnum>0 AND NOT a.attisdropped
      ) THEN RAISE EXCEPTION 'target alias for % needs an existing key column and a separate namespace',name_; END IF;
      IF NOT EXISTS (
        SELECT FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey)
        WHERE i.indrelid=to_regclass(format('%I',name_)) AND i.indisunique AND i.indpred IS NULL
          AND i.indnkeyatts=1 AND a.attnum=i.indkey[0] AND a.attname=alias_->>'column'
      ) AND (alias_->'multipleOwners' IS DISTINCT FROM 'true'::jsonb
        OR length(trim(COALESCE(alias_->>'reason','')))<12) THEN
        RAISE EXCEPTION 'nonunique target alias for % needs an explicit multiple-owner policy and reason',name_;
      END IF;
    END LOOP;
    PERFORM (entry->>'live')::jsonpath;
    IF entry->>'live' IS NULL THEN RAISE EXCEPTION 'reference table % needs a lifecycle',name_; END IF;
  END LOOP;

  -- A prior statement in this migration may have queued deferred FK events.
  -- Drain those against the old declaration before rebuilding its token ledger.
  IF EXISTS(SELECT FROM pg_constraint WHERE conrelid='_gonvex_reference_edges'::regclass
    AND conname='gonvex_reference_target') THEN
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
  END IF;
  ALTER TABLE _gonvex_reference_edges DROP CONSTRAINT IF EXISTS gonvex_reference_target;
  FOR name_ IN SELECT table_name FROM _gonvex_reference_tables LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS gonvex_reference_changed ON %I',name_);
    EXECUTE format('DROP TRIGGER IF EXISTS gonvex_reference_no_truncate ON %I',name_);
  END LOOP;
  DELETE FROM _gonvex_reference_edges;
  DELETE FROM _gonvex_reference_targets;
  DELETE FROM _gonvex_reference_rules;
  DELETE FROM _gonvex_reference_keys;
  DELETE FROM _gonvex_reference_configurations;
  DELETE FROM _gonvex_reference_bindings;
  DELETE FROM _gonvex_reference_tables;
  INSERT INTO _gonvex_reference_tables(table_name,key_column,live_when)
    SELECT e->>'name',e->>'key',(e->>'live')::jsonpath FROM jsonb_array_elements(catalog->'tables') e;
  INSERT INTO _gonvex_reference_keys
    SELECT e->>'name',e->>'name',e->>'key' FROM jsonb_array_elements(catalog->'tables') e
    UNION ALL
    SELECT e->>'name',a->>'name',a->>'column' FROM jsonb_array_elements(catalog->'tables') e
      CROSS JOIN LATERAL jsonb_array_elements(COALESCE(e->'targetKeys','[]'::jsonb)) a;

  IF catalog ? 'bindings' AND jsonb_typeof(catalog->'bindings') IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'invalid configuration bindings';
  END IF;
  FOR entry IN SELECT value FROM jsonb_array_elements(COALESCE(catalog->'bindings','[]'::jsonb)) LOOP
    IF COALESCE(entry->>'name','')='' OR NOT EXISTS (
      SELECT FROM _gonvex_reference_tables WHERE table_name=entry->>'source'
    ) OR jsonb_typeof(entry->'columns') IS DISTINCT FROM 'array' OR entry->'columns'='[]'::jsonb THEN
      RAISE EXCEPTION 'invalid configuration binding';
    END IF;
    FOR path_ IN SELECT value FROM jsonb_array_elements_text(entry->'columns') LOOP
      IF NOT EXISTS(SELECT FROM pg_attribute WHERE attrelid=to_regclass(format('%I',entry->>'source'))
        AND attname=path_ AND attnum>0 AND NOT attisdropped) THEN
        RAISE EXCEPTION 'configuration binding % needs existing columns',entry->>'name';
      END IF;
    END LOOP;
    INSERT INTO _gonvex_reference_bindings VALUES(entry->>'name',entry->>'source',entry->'columns');
  END LOOP;

  FOR rule IN SELECT value FROM jsonb_array_elements(catalog->'references') LOOP
    IF rule->>'name' IS NULL OR rule->>'state' NOT IN ('exists','live') OR rule->>'state' IS NULL
      OR NOT EXISTS (SELECT FROM _gonvex_reference_tables WHERE table_name=rule->>'source')
      OR ((rule ? 'target') = (rule ? 'targets')) THEN
      RAISE EXCEPTION 'invalid reference declaration %',rule->>'name';
    END IF;
    IF ((rule ? 'path') = (rule ? 'paths'))
      OR (rule ? 'paths' AND (jsonb_typeof(rule->'paths') IS DISTINCT FROM 'array' OR rule->'paths'='[]'::jsonb)) THEN
      RAISE EXCEPTION 'reference % needs exactly one path or nonempty paths array',rule->>'name';
    END IF;
    FOR path_ IN SELECT value FROM jsonb_array_elements_text(COALESCE(rule->'paths',jsonb_build_array(rule->>'path'))) LOOP
      IF path_ IS NULL THEN RAISE EXCEPTION 'reference % has a null path',rule->>'name'; END IF;
      PERFORM path_::jsonpath;
    END LOOP;
    IF rule ? 'ignoreValues' AND (jsonb_typeof(rule->'ignoreValues') IS DISTINCT FROM 'array'
      OR COALESCE(length(trim(rule->>'ignoreReason')),0)<12
      OR EXISTS(SELECT FROM jsonb_array_elements(rule->'ignoreValues') v WHERE jsonb_typeof(v)<>'string')) THEN
      RAISE EXCEPTION 'reference % needs declared string sentinels and their meaning',rule->>'name';
    END IF;
    IF rule ? 'allowNumbers' AND (rule->'allowNumbers' IS DISTINCT FROM 'true'::jsonb
      OR length(trim(COALESCE(rule->>'numberReason','')))<12) THEN
      RAISE EXCEPTION 'reference % needs an explicit numeric selector policy and reason',rule->>'name';
    END IF;
    IF rule ? 'normalize' AND (rule->>'normalize' NOT IN ('csv-or-json-list','task-relation')
      OR length(trim(COALESCE(rule->>'normalizeReason','')))<12) THEN
      RAISE EXCEPTION 'reference % needs a supported selector decoder and reason',rule->>'name';
    END IF;
    IF rule ? 'encoding' AND (rule->>'encoding' IS DISTINCT FROM 'json-or-value'
      OR rule->>'column' IS NULL) THEN
      RAISE EXCEPTION 'reference % has unsupported storage encoding',rule->>'name';
    END IF;
    IF rule ? 'at' THEN PERFORM (rule->>'at')::jsonpath; END IF;
    IF rule ? 'sourceWhen' THEN PERFORM (rule->>'sourceWhen')::jsonpath; END IF;
    IF rule ? 'lookup' THEN
      IF jsonb_typeof(rule->'lookup') NOT IN ('array','object') OR rule->'lookup'='[]'::jsonb THEN
        RAISE EXCEPTION 'reference % needs configuration lookup steps',rule->>'name';
      END IF;
      FOR step_ IN SELECT value FROM jsonb_array_elements(CASE WHEN jsonb_typeof(rule->'lookup')='array'
        THEN rule->'lookup' ELSE jsonb_build_array(rule->'lookup') END) LOOP
        IF NOT EXISTS(SELECT FROM _gonvex_reference_bindings WHERE name=step_->>'binding')
          OR step_->>'key' IS NULL OR COALESCE(step_->>'from','container') NOT IN ('container','document','configuration') THEN
          RAISE EXCEPTION 'reference % needs a declared configuration binding and key',rule->>'name';
        END IF;
        PERFORM (step_->>'key')::jsonpath;
        IF step_ ? 'when' THEN PERFORM (step_->>'when')::jsonpath; END IF;
      END LOOP;
    END IF;
    IF rule ? 'targets' THEN
      PERFORM (rule->>'discriminator')::jsonpath;
      IF rule->>'discriminator' IS NULL OR jsonb_typeof(rule->'targets') IS DISTINCT FROM 'object'
        OR rule->'targets'='{}'::jsonb THEN
        RAISE EXCEPTION 'reference % needs discriminator variants',rule->>'name';
      END IF;
    END IF;
    FOR target_ IN SELECT rule->>'target' WHERE rule ? 'target'
      UNION ALL SELECT value FROM jsonb_each_text(COALESCE(rule->'targets','{}'::jsonb)) LOOP
      IF NOT EXISTS (SELECT FROM _gonvex_reference_keys WHERE token_name=target_) THEN
        RAISE EXCEPTION 'reference % has unknown target %',rule->>'name',target_;
      END IF;
    END LOOP;
    INSERT INTO _gonvex_reference_rules VALUES(rule->>'name',rule->>'source',rule);
  END LOOP;

  FOR entry IN SELECT value FROM jsonb_array_elements(catalog->'tables') LOOP
    name_:=entry->>'name'; key_:=entry->>'key';
    EXECUTE format('INSERT INTO _gonvex_reference_configurations(binding_name,row_id,document)
      SELECT b.name,r.%I::text,_gonvex_reference_configuration(to_jsonb(r),b.columns_)
      FROM %I r JOIN _gonvex_reference_bindings b ON b.source_table=%L',key_,name_,name_);
    EXECUTE format('INSERT INTO _gonvex_reference_targets(table_name,row_id,state,owner_count)
      SELECT d.token_name,d.row_id,d.state,count(*) FROM %I r
      CROSS JOIN LATERAL _gonvex_reference_row_targets(%L,to_jsonb(r)) d
      GROUP BY d.token_name,d.row_id,d.state
      ON CONFLICT(table_name,row_id,state) DO UPDATE
        SET owner_count=_gonvex_reference_targets.owner_count+EXCLUDED.owner_count',name_,name_);
    EXECUTE format('CREATE TRIGGER gonvex_reference_changed AFTER INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION _gonvex_reference_changed(%L)',name_,key_);
    EXECUTE format('CREATE TRIGGER gonvex_reference_no_truncate BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION _gonvex_reference_no_truncate()',name_);
  END LOOP;
  FOR entry IN SELECT value FROM jsonb_array_elements(catalog->'tables') LOOP
    name_:=entry->>'name'; key_:=entry->>'key';
    EXECUTE format('INSERT INTO _gonvex_reference_edges SELECT d.rule_name,%L,r.%I::text,d.target_table,d.target_id,d.target_state FROM %I r CROSS JOIN LATERAL _gonvex_reference_desired(%L,to_jsonb(r)) d',name_,key_,name_,name_);
  END LOOP;
  -- Backfill includes legacy violations. NOT VALID enforces every subsequent
  -- new edge and target deletion without changing any application data.
  ALTER TABLE _gonvex_reference_edges ADD CONSTRAINT gonvex_reference_target
    FOREIGN KEY(target_table,target_id,target_state)
    REFERENCES _gonvex_reference_targets(table_name,row_id,state)
    ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
  IF catalog ? 'exclusions' THEN PERFORM gonvex_check_reference_coverage(catalog); END IF;
  INSERT INTO _gonvex_reference_catalog VALUES(true,catalog)
    ON CONFLICT(singleton) DO UPDATE SET declaration=EXCLUDED.declaration;
END $$;
