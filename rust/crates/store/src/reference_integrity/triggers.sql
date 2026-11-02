CREATE OR REPLACE FUNCTION _gonvex_reference_sync_row(table_name_ text,row_id_ text,document jsonb,previous jsonb)
RETURNS void LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
  -- Preserve unchanged edges: a legacy NOT VALID violation must not be silently
  -- repaired or retried by an unrelated field update.
  DELETE FROM _gonvex_reference_edges e
  WHERE e.source_table=table_name_ AND e.source_id=row_id_
    AND NOT EXISTS (SELECT FROM _gonvex_reference_desired(table_name_,document) d
      WHERE d.rule_name=e.rule_name AND d.target_table=e.target_table
        AND d.target_id=e.target_id AND d.target_state=e.target_state);
  INSERT INTO _gonvex_reference_edges(rule_name,source_table,source_id,target_table,target_id,target_state)
    SELECT d.rule_name,table_name_,row_id_,d.target_table,d.target_id,d.target_state
    FROM _gonvex_reference_desired(table_name_,document) d ON CONFLICT DO NOTHING;

  PERFORM _gonvex_reference_sync_targets(table_name_,previous,document);
END $$;

CREATE OR REPLACE FUNCTION _gonvex_reference_changed()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE old_id text; new_id text;
BEGIN
  IF TG_OP<>'INSERT' THEN old_id:=to_jsonb(OLD)->>TG_ARGV[0]; END IF;
  IF TG_OP<>'DELETE' THEN new_id:=to_jsonb(NEW)->>TG_ARGV[0]; END IF;
  IF TG_OP='DELETE' OR (TG_OP='UPDATE' AND old_id IS DISTINCT FROM new_id) THEN
    PERFORM _gonvex_reference_sync_row(TG_TABLE_NAME,old_id,NULL,to_jsonb(OLD));
  END IF;
  IF TG_OP<>'DELETE' THEN
    PERFORM _gonvex_reference_sync_row(TG_TABLE_NAME,new_id,to_jsonb(NEW),
      CASE WHEN TG_OP='UPDATE' AND old_id=new_id THEN to_jsonb(OLD) ELSE NULL END);
    RETURN NEW;
  END IF;
  RETURN OLD;
END $$;

CREATE OR REPLACE FUNCTION _gonvex_reference_no_truncate()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'TRUNCATE would bypass declared references; use transactional DELETE'
    USING ERRCODE='23503';
END $$;
