-- A key can be the primary ID, an alternate key, or a shared namespace
-- for an intentionally untagged union such as reaction targets. Counts keep
-- shared keys correct when multiple rows/tables own the same identity.
CREATE TABLE IF NOT EXISTS _gonvex_reference_keys (
  table_name text NOT NULL, token_name text NOT NULL, key_column text NOT NULL,
  PRIMARY KEY(table_name,token_name)
);
ALTER TABLE _gonvex_reference_targets
  ADD COLUMN IF NOT EXISTS owner_count bigint NOT NULL DEFAULT 1 CHECK(owner_count>=0);

CREATE OR REPLACE FUNCTION _gonvex_reference_configuration(document jsonb,columns_ jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN document IS NOT NULL THEN
    (SELECT jsonb_object_agg(c,document->c) FROM jsonb_array_elements_text(columns_) c) END
$$;

CREATE OR REPLACE FUNCTION _gonvex_reference_binding_id(row_id text,version bigint)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT jsonb_build_array(row_id,version)::text
$$;

CREATE OR REPLACE FUNCTION _gonvex_reference_row_targets(source_name text,document jsonb)
RETURNS TABLE(token_name text,row_id text,state text)
LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
  SELECT k.token_name,_gonvex_reference_row_targets.document->>k.key_column,s.state
  FROM _gonvex_reference_keys k JOIN _gonvex_reference_tables t USING(table_name)
  CROSS JOIN (VALUES('exists'),('live')) s(state)
  WHERE k.table_name=source_name AND _gonvex_reference_row_targets.document IS NOT NULL
    AND _gonvex_reference_row_targets.document->>k.key_column IS NOT NULL
    AND (s.state='exists' OR COALESCE(jsonb_path_match(_gonvex_reference_row_targets.document,t.live_when),false))
  UNION ALL
  SELECT '$configuration:'||b.name,
    _gonvex_reference_binding_id(_gonvex_reference_row_targets.document->>t.key_column,c.version),s.state
  FROM _gonvex_reference_bindings b
  JOIN _gonvex_reference_tables t ON t.table_name=b.source_table
  JOIN _gonvex_reference_configurations c ON c.binding_name=b.name
    AND c.row_id=_gonvex_reference_row_targets.document->>t.key_column
    AND c.document=_gonvex_reference_configuration(_gonvex_reference_row_targets.document,b.columns_)
  CROSS JOIN (VALUES('exists'),('live')) s(state)
  WHERE b.source_table=source_name AND _gonvex_reference_row_targets.document IS NOT NULL
    AND (s.state='exists' OR COALESCE(jsonb_path_match(_gonvex_reference_row_targets.document,t.live_when),false))
$$;

CREATE OR REPLACE FUNCTION _gonvex_reference_sync_targets(source_name text,previous jsonb,document jsonb)
RETURNS void LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE token record; remaining bigint; previous_targets jsonb;
BEGIN
  -- Snapshot old tokens before updating the parent projection. Configuration
  -- versions come from a non-reusing sequence; arbitrary JSON never enters an
  -- index key. Comparing JSON retains the version for unrelated parent edits.
  SELECT COALESCE(jsonb_agg(to_jsonb(t)),'[]'::jsonb) INTO previous_targets
    FROM _gonvex_reference_row_targets(source_name,previous) t;
  DELETE FROM _gonvex_reference_configurations c USING _gonvex_reference_bindings b
    JOIN _gonvex_reference_tables t ON t.table_name=b.source_table
    WHERE c.binding_name=b.name AND b.source_table=source_name
      AND c.row_id=previous->>t.key_column
      AND (_gonvex_reference_sync_targets.document IS NULL
        OR previous->>t.key_column IS DISTINCT FROM _gonvex_reference_sync_targets.document->>t.key_column);
  -- Installing a formerly missing parent must not reinterpret legacy values
  -- whose domain was unknown at backfill. Clear/rebind those dependents first.
  IF previous IS NULL AND EXISTS (
    SELECT FROM _gonvex_reference_bindings b
    JOIN _gonvex_reference_tables t ON t.table_name=b.source_table
    JOIN _gonvex_reference_edges e ON e.target_table='$configuration:'||b.name
      AND e.target_id=_gonvex_reference_binding_id(_gonvex_reference_sync_targets.document->>t.key_column,NULL)
    WHERE b.source_table=source_name
  ) THEN
    RAISE EXCEPTION 'configuration has unresolved legacy dependents' USING ERRCODE='23503';
  END IF;
  INSERT INTO _gonvex_reference_configurations(binding_name,row_id,document)
    SELECT b.name,_gonvex_reference_sync_targets.document->>t.key_column,
      _gonvex_reference_configuration(_gonvex_reference_sync_targets.document,b.columns_)
    FROM _gonvex_reference_bindings b
    JOIN _gonvex_reference_tables t ON t.table_name=b.source_table
    WHERE b.source_table=source_name AND _gonvex_reference_sync_targets.document IS NOT NULL
    ON CONFLICT(binding_name,row_id) DO UPDATE SET document=EXCLUDED.document,
      version=CASE WHEN _gonvex_reference_configurations.document IS DISTINCT FROM EXCLUDED.document
        THEN EXCLUDED.version ELSE _gonvex_reference_configurations.version END;
  -- Native row updates serialize owner-count changes. REPEATABLE READ detects
  -- concurrent updates rather than consulting a stale cross-table snapshot.
  FOR token IN
    SELECT token_name,row_id,state,sum(delta)::bigint AS delta FROM (
      SELECT t.*,1 AS delta FROM _gonvex_reference_row_targets(source_name,document) t
      UNION ALL
      SELECT t.*,-1 AS delta FROM jsonb_to_recordset(previous_targets)
        AS t(token_name text,row_id text,state text)
    ) changes GROUP BY token_name,row_id,state HAVING sum(delta)<>0 ORDER BY token_name,row_id,state
  LOOP
    IF token.delta>0 THEN
      INSERT INTO _gonvex_reference_targets(table_name,row_id,state,owner_count)
        VALUES(token.token_name,token.row_id,token.state,token.delta)
        ON CONFLICT(table_name,row_id,state) DO UPDATE
          SET owner_count=_gonvex_reference_targets.owner_count+EXCLUDED.owner_count;
    ELSE
      UPDATE _gonvex_reference_targets SET owner_count=owner_count+token.delta
        WHERE table_name=token.token_name AND row_id=token.row_id AND state=token.state
        RETURNING owner_count INTO remaining;
      IF NOT FOUND THEN RAISE EXCEPTION 'reference target ledger is inconsistent'; END IF;
      IF remaining=0 THEN
        DELETE FROM _gonvex_reference_targets
          WHERE table_name=token.token_name AND row_id=token.row_id AND state=token.state AND owner_count=0;
      END IF;
    END IF;
  END LOOP;

END $$;
