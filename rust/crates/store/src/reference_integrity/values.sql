-- Explicit adapters for existing selector encodings. PostgreSQL uses the same
-- function for enforcement and snapshot audits; plain content never opts in.
CREATE OR REPLACE FUNCTION _gonvex_reference_decode(value_ jsonb,normalization text)
RETURNS SETOF jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE decoded jsonb; item jsonb; raw text;
BEGIN
  IF normalization IS NULL THEN RETURN NEXT value_; RETURN; END IF;
  IF value_ IS NULL OR value_='null'::jsonb THEN RETURN; END IF;
  IF normalization='task-relation' THEN
    RETURN NEXT CASE WHEN jsonb_typeof(value_)='object' THEN value_->'taskId' ELSE value_ END;
    RETURN;
  END IF;
  IF normalization<>'csv-or-json-list' THEN RAISE EXCEPTION 'unsupported selector normalization'; END IF;
  decoded:=value_;
  IF jsonb_typeof(value_)='string' THEN
    raw:=btrim(value_#>>'{}');
    IF left(raw,1)='[' THEN
      BEGIN
        decoded:=raw::jsonb;
        IF jsonb_typeof(decoded)<>'array' THEN decoded:=value_; END IF;
      EXCEPTION WHEN invalid_text_representation THEN decoded:=value_; END;
    END IF;
  END IF;
  IF jsonb_typeof(decoded)='array' THEN
    FOR item IN SELECT value FROM jsonb_array_elements(decoded) LOOP
      IF jsonb_typeof(item) IN ('string','number','boolean') THEN
        raw:=btrim(item#>>'{}');
        IF raw<>'' THEN RETURN NEXT to_jsonb(raw); END IF;
      ELSE RAISE EXCEPTION 'selector list needs scalar values' USING ERRCODE='23514'; END IF;
    END LOOP;
  ELSIF jsonb_typeof(decoded)='object' THEN
    FOR item IN SELECT value FROM jsonb_each(decoded) LOOP
      IF jsonb_typeof(item) IN ('string','number','boolean') THEN
        raw:=btrim(item#>>'{}');
        IF raw<>'' THEN RETURN NEXT to_jsonb(raw); END IF;
      ELSE RAISE EXCEPTION 'selector map needs scalar values' USING ERRCODE='23514'; END IF;
    END LOOP;
  ELSE
    FOR raw IN SELECT btrim(value) FROM unnest(string_to_array(decoded#>>'{}',',')) value LOOP
      IF raw<>'' THEN RETURN NEXT to_jsonb(raw); END IF;
    END LOOP;
  END IF;
END $$;

-- JSONPath is evaluated by PostgreSQL in both write enforcement and audits.
-- There is no second application-language implementation of extraction.
CREATE OR REPLACE FUNCTION _gonvex_reference_values(document jsonb, rule jsonb)
RETURNS TABLE(target_table text,target_id text)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE path_ jsonpath; container jsonb; identifier jsonb; discriminator jsonb; destination text;
  configuration_ jsonb; version_ bigint; parent_id text; lookup_ jsonb; step_ jsonb; lookup_document jsonb; vars_ jsonb;
BEGIN
  -- Some existing consumers accept both JSON values and JSON serialized in a
  -- text column. Decoding is declared per reference, never guessed globally.
  IF rule->>'encoding'='json-or-value'
    AND jsonb_typeof(document->(rule->>'column'))='string' THEN
    document:=jsonb_set(document,ARRAY[rule->>'column'],
      (document->>(rule->>'column'))::jsonb);
  END IF;
  <<containers>>
  FOR container IN SELECT jsonb_path_query(document,COALESCE(rule->>'at','$')::jsonpath) LOOP
    vars_:=jsonb_build_object('entry',container,'document',document);
    configuration_:=NULL;
    lookup_:=rule->'lookup';
    IF lookup_ IS NOT NULL THEN
      FOR step_ IN SELECT value FROM jsonb_array_elements(
        CASE WHEN jsonb_typeof(lookup_)='array' THEN lookup_ ELSE jsonb_build_array(lookup_) END) LOOP
      lookup_document:=CASE step_->>'from' WHEN 'document' THEN document
        WHEN 'configuration' THEN configuration_ ELSE container END;
      identifier:=jsonb_path_query_first(lookup_document,(step_->>'key')::jsonpath,vars_);
      IF identifier IS NULL OR identifier='null'::jsonb THEN
        IF step_->'required'='true'::jsonb THEN
          RAISE EXCEPTION 'reference % needs a configuration key',rule->>'name' USING ERRCODE='23514';
        END IF;
        CONTINUE containers;
      END IF;
      IF jsonb_typeof(identifier)<>'string'
        AND NOT COALESCE(jsonb_typeof(identifier)='number' AND rule->'allowNumbers'='true'::jsonb,false) THEN
        RAISE EXCEPTION 'reference % needs a string configuration key',rule->>'name' USING ERRCODE='23514';
      END IF;
      parent_id:=identifier#>>'{}';
      SELECT c.document,c.version INTO configuration_,version_ FROM _gonvex_reference_configurations c
        WHERE c.binding_name=step_->>'binding' AND c.row_id=parent_id;
      target_table:='$configuration:'||(step_->>'binding');
      target_id:=_gonvex_reference_binding_id(parent_id,version_);
      RETURN NEXT;
      -- Missing legacy parents remain auditable violations. New writes fail
      -- the native FK, including a stale snapshot of changed configuration.
      IF configuration_ IS NULL OR NOT COALESCE(jsonb_path_match(configuration_,
        COALESCE(step_->>'when','true')::jsonpath,vars_),false) THEN CONTINUE containers; END IF;
      END LOOP;
    END IF;
    FOR path_ IN SELECT value::jsonpath FROM jsonb_array_elements_text(
      COALESCE(rule->'paths',jsonb_build_array(rule->>'path'))) LOOP
    FOR identifier IN SELECT decoded FROM jsonb_path_query(container,path_) selected
      CROSS JOIN LATERAL _gonvex_reference_decode(selected,rule->>'normalize') decoded LOOP
      IF identifier='null'::jsonb THEN CONTINUE; END IF;
      IF jsonb_typeof(identifier)<>'string'
        AND NOT (jsonb_typeof(identifier)='number' AND COALESCE(rule->'allowNumbers'='true'::jsonb,false)) THEN
        RAISE EXCEPTION 'reference % must contain string IDs',rule->>'name' USING ERRCODE='23514';
      END IF;
      target_id:=identifier#>>'{}';
      IF target_id='' AND COALESCE((rule->>'allowEmpty')::boolean,false) THEN CONTINUE; END IF;
      IF target_id='' THEN
        RAISE EXCEPTION 'reference % contains an empty ID',rule->>'name' USING ERRCODE='23514';
      END IF;
      IF COALESCE(rule->'ignoreValues','[]'::jsonb) ? target_id THEN CONTINUE; END IF;
      destination:=rule->>'target';
      IF rule ? 'targets' THEN
        discriminator:=jsonb_path_query_first(
          CASE WHEN lookup_ IS NOT NULL THEN configuration_ ELSE container END,
          (rule->>'discriminator')::jsonpath,vars_);
        IF jsonb_typeof(discriminator) IS DISTINCT FROM 'string' THEN
          RAISE EXCEPTION 'reference % requires a target discriminator',rule->>'name' USING ERRCODE='23514';
        END IF;
        destination:=rule->'targets'->>(discriminator#>>'{}');
      END IF;
      IF destination IS NULL THEN
        RAISE EXCEPTION 'reference % has an undeclared target type',rule->>'name' USING ERRCODE='23514';
      END IF;
      target_table:=destination;
      RETURN NEXT;
    END LOOP;
    END LOOP;
  END LOOP;
END $$;

CREATE OR REPLACE FUNCTION _gonvex_reference_desired(source_name text,document jsonb)
RETURNS TABLE(rule_name text,target_table text,target_id text,target_state text)
LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
  SELECT r.name,v.target_table,v.target_id,r.declaration->>'state'
  FROM _gonvex_reference_rules r
  JOIN _gonvex_reference_tables t ON t.table_name=r.source_table
  CROSS JOIN LATERAL _gonvex_reference_values(document,r.declaration) v
  WHERE r.source_table=source_name
    AND COALESCE(jsonb_path_match(document,
      COALESCE((r.declaration->>'sourceWhen')::jsonpath,t.live_when)),false)
  GROUP BY r.name,v.target_table,v.target_id,r.declaration->>'state'
$$;

CREATE OR REPLACE FUNCTION gonvex_reference_violations()
RETURNS TABLE(rule_name text,source_table text,source_id text,target_table text,target_id text,reason text)
LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
  SELECT e.rule_name,e.source_table,e.source_id,e.target_table,e.target_id,
    CASE WHEN EXISTS (SELECT FROM _gonvex_reference_targets x
      WHERE x.table_name=e.target_table AND x.row_id=e.target_id AND x.state='exists')
      THEN 'deleted' ELSE 'missing' END
  FROM _gonvex_reference_edges e
  LEFT JOIN _gonvex_reference_targets t ON t.table_name=e.target_table
    AND t.row_id=e.target_id AND t.state=e.target_state
  WHERE t.row_id IS NULL
$$;
