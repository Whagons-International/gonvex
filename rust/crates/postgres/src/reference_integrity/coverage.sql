-- Coverage is separate from suffix-based inference: candidates need an explicit
-- relationship or a reasoned boundary classification owned by the application.
CREATE OR REPLACE FUNCTION gonvex_check_reference_coverage(catalog jsonb)
RETURNS void LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE item record; rule jsonb; entry jsonb; path_ text; anchor_ text; prefix_ text;
BEGIN
  IF jsonb_typeof(catalog->'exclusions') IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'reference coverage needs explicit exclusions';
  END IF;
  FOR rule IN SELECT value FROM jsonb_array_elements(catalog->'references') LOOP
    IF rule->>'column' IS NULL OR NOT EXISTS (
      SELECT FROM pg_attribute a WHERE a.attrelid=to_regclass(format('%I',rule->>'source'))
        AND a.attname=rule->>'column' AND a.attnum>0 AND NOT a.attisdropped
    ) THEN RAISE EXCEPTION 'reference % needs an existing source column',rule->>'name'; END IF;
    -- PostgreSQL canonicalizes member names to quoted JSONPath keys. Every
    -- selection must be anchored to the declared storage column; a sibling
    -- path cannot accidentally count as coverage for another column.
    prefix_:='$.'||to_jsonb(rule->>'column')::text;
    FOR path_ IN SELECT value FROM jsonb_array_elements_text(
      CASE WHEN rule ? 'at' AND (rule->>'at')::jsonpath::text<>'$'
        THEN jsonb_build_array(rule->>'at')
        ELSE COALESCE(rule->'paths',jsonb_build_array(rule->>'path')) END)
    LOOP
      anchor_:=path_::jsonpath::text;
      IF left(anchor_,length(prefix_))<>prefix_
        OR substr(anchor_,length(prefix_)+1,1) NOT IN ('','.','[',' ','?') THEN
        RAISE EXCEPTION 'reference % selection must start at its declared column',rule->>'name';
      END IF;
    END LOOP;
  END LOOP;
  FOR entry IN SELECT value FROM jsonb_array_elements(catalog->'exclusions') LOOP
    IF length(trim(COALESCE(entry->>'reason','')))<12
      OR entry->>'kind' NOT IN ('external','enum','local','content','snapshot','derived','lifecycle','identity')
      OR entry->>'kind' IS NULL OR NOT EXISTS (
        SELECT FROM pg_attribute a WHERE a.attrelid=to_regclass(format('%I',entry->>'source'))
          AND a.attname=entry->>'column' AND a.attnum>0 AND NOT a.attisdropped
      ) THEN RAISE EXCEPTION 'invalid reference exclusion for %.%',entry->>'source',entry->>'column'; END IF;
  END LOOP;
  FOR item IN
    SELECT c.relname AS table_name,a.attname AS column_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
    WHERE n.nspname=current_schema() AND c.relkind IN ('r','p')
      AND c.relname NOT LIKE '\_gonvex\_%' ESCAPE '\' AND c.relname NOT LIKE 'gonvex\_%' ESCAPE '\'
      AND NOT EXISTS(SELECT FROM pg_index i WHERE i.indrelid=c.oid AND i.indisprimary AND a.attnum=ANY(i.indkey))
      AND (a.atttypid IN ('text'::regtype,'varchar'::regtype,'json'::regtype,'jsonb'::regtype)
        OR EXISTS(SELECT FROM pg_type ty WHERE ty.oid=a.atttypid AND ty.typcategory='A'))
  LOOP
    IF NOT EXISTS (SELECT FROM jsonb_array_elements(catalog->'references') r
          WHERE r->>'source'=item.table_name AND r->>'column'=item.column_name)
       AND NOT EXISTS (SELECT FROM _gonvex_reference_keys k
          WHERE k.table_name=item.table_name AND k.key_column=item.column_name)
       AND NOT EXISTS (SELECT FROM jsonb_array_elements(catalog->'exclusions') e
          WHERE e->>'source'=item.table_name AND e->>'column'=item.column_name) THEN
      RAISE EXCEPTION 'unclassified reference candidate %.%',item.table_name,item.column_name;
    END IF;
  END LOOP;
END $$;
