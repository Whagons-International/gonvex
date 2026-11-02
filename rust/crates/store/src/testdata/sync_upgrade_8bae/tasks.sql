CREATE OR REPLACE FUNCTION "gonvex_sync_tasks_stage"() RETURNS trigger AS $$
DECLARE old_data jsonb; new_data jsonb; changed_columns text[]; row_key text;
BEGIN
  IF TG_OP='INSERT' THEN
    old_data:=NULL; new_data:=jsonb_build_object('id',NEW."id",'title',NEW."title");
    row_key:=NEW."id"::text; changed_columns:=ARRAY(SELECT jsonb_object_keys(new_data));
  ELSIF TG_OP='DELETE' THEN
    old_data:=jsonb_build_object('id',OLD."id",'title',OLD."title"); new_data:=NULL;
    row_key:=OLD."id"::text; changed_columns:=ARRAY(SELECT jsonb_object_keys(old_data));
  ELSE
    old_data:=jsonb_build_object('id',OLD."id",'title',OLD."title"); new_data:=jsonb_build_object('id',NEW."id",'title',NEW."title");
    row_key:=NEW."id"::text;
    changed_columns:=ARRAY(SELECT key FROM jsonb_object_keys(old_data||new_data) changed(key)
                           WHERE old_data->key IS DISTINCT FROM new_data->key ORDER BY key);
  END IF;
  INSERT INTO _gonvex_sync_changes
    (transaction_id,table_name,row_id,operation,old_value,new_value,changed_columns)
  VALUES(txid_current()::bigint,'tasks',row_key,lower(TG_OP),old_data,new_data,changed_columns);
  RETURN NULL;
END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS "gonvex_sync_tasks_stage_trigger" ON "tasks";
DROP TRIGGER IF EXISTS "gonvex_sync_tasks_finalize_trigger" ON "tasks";
CREATE TRIGGER "gonvex_sync_tasks_stage_trigger" AFTER INSERT OR UPDATE OR DELETE ON "tasks"
  FOR EACH ROW EXECUTE FUNCTION "gonvex_sync_tasks_stage"();
CREATE CONSTRAINT TRIGGER "gonvex_sync_tasks_finalize_trigger" AFTER INSERT OR UPDATE OR DELETE ON "tasks"
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION gonvex_sync_finalize_transaction();