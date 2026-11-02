
CREATE TABLE IF NOT EXISTS _gonvex_sync_clock (
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), epoch text NOT NULL,
  revision bigint NOT NULL DEFAULT 0, retained_revision bigint NOT NULL DEFAULT 0
);
INSERT INTO _gonvex_sync_clock(singleton,epoch,revision)
VALUES(true,md5(random()::text||clock_timestamp()::text),0) ON CONFLICT(singleton) DO NOTHING;
CREATE TABLE IF NOT EXISTS _gonvex_sync_changes (
  event_id bigserial PRIMARY KEY, transaction_id bigint NOT NULL, revision bigint, ordinal integer,
  command_id text, table_name text NOT NULL, row_id text NOT NULL, operation text NOT NULL,
  old_value jsonb, new_value jsonb, changed_columns text[] NOT NULL DEFAULT ARRAY[]::text[],
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS gonvex_sync_changes_revision ON _gonvex_sync_changes(revision,ordinal)
  WHERE revision IS NOT NULL;
CREATE INDEX IF NOT EXISTS gonvex_sync_changes_created_at ON _gonvex_sync_changes(created_at);
CREATE TABLE IF NOT EXISTS _gonvex_sync_transactions (
  revision bigint PRIMARY KEY, transaction_id bigint NOT NULL UNIQUE,
  root_command_id text NOT NULL, origin_command_id text NOT NULL,
  root_invocation_channel text NOT NULL DEFAULT 'system', invocation_channel text NOT NULL,
  actor_account_id text, actor_member_id text, on_behalf_of_member_id text,
  agent_execution_id text, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE _gonvex_sync_transactions ADD COLUMN IF NOT EXISTS root_invocation_channel text NOT NULL DEFAULT 'system';
CREATE TABLE IF NOT EXISTS _gonvex_action_outbox (
  id text PRIMARY KEY, action_path text NOT NULL, args jsonb NOT NULL, actor_user_id text,
  actor_email text, provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'pending' CHECK(status IN('pending','processing')),
  attempts integer NOT NULL DEFAULT 0, available_at timestamptz NOT NULL DEFAULT now(),
  locked_at timestamptz, last_error text, created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE _gonvex_action_outbox ADD COLUMN IF NOT EXISTS provenance jsonb NOT NULL DEFAULT '{}'::jsonb;
CREATE INDEX IF NOT EXISTS _gonvex_action_outbox_pending ON _gonvex_action_outbox(available_at,created_at)
  WHERE status='pending';
CREATE OR REPLACE FUNCTION gonvex_sync_finalize_transaction() RETURNS trigger AS $$
DECLARE revision_text text; next_revision bigint; current_epoch text; changed_tables text[]; notify_payload jsonb;
BEGIN
  revision_text:=current_setting('gonvex.sync_revision',true);
  IF revision_text IS NULL OR revision_text='' THEN
    UPDATE _gonvex_sync_clock SET revision=revision+1 WHERE singleton=true
      RETURNING revision,epoch INTO next_revision,current_epoch;
    PERFORM set_config('gonvex.sync_revision',next_revision::text,true);
    WITH ranked AS (
      SELECT event_id,row_number() OVER(ORDER BY event_id)::integer row_ordinal
      FROM _gonvex_sync_changes WHERE transaction_id=txid_current()::bigint AND revision IS NULL
    ) UPDATE _gonvex_sync_changes changes SET revision=next_revision,ordinal=ranked.row_ordinal,
      command_id=NULLIF(current_setting('gonvex.command_id',true),'')
      FROM ranked WHERE changes.event_id=ranked.event_id;
    SELECT array_agg(DISTINCT table_name ORDER BY table_name) INTO changed_tables
      FROM _gonvex_sync_changes WHERE transaction_id=txid_current()::bigint AND revision=next_revision;
    INSERT INTO _gonvex_sync_transactions
      (revision,transaction_id,root_command_id,origin_command_id,root_invocation_channel,invocation_channel,
       actor_account_id,actor_member_id,on_behalf_of_member_id,agent_execution_id)
    VALUES(
      next_revision,txid_current()::bigint,
      COALESCE(NULLIF(current_setting('gonvex.root_command_id',true),''),NULLIF(current_setting('gonvex.command_id',true),''),''),
      COALESCE(NULLIF(current_setting('gonvex.command_id',true),''),''),
      COALESCE(NULLIF(current_setting('gonvex.root_invocation_channel',true),''),NULLIF(current_setting('gonvex.invocation_channel',true),''),'system'),
      COALESCE(NULLIF(current_setting('gonvex.invocation_channel',true),''),'system'),
      NULLIF(current_setting('gonvex.actor_account_id',true),''),
      NULLIF(current_setting('gonvex.actor_member_id',true),''),
      NULLIF(current_setting('gonvex.on_behalf_of_member_id',true),''),
      NULLIF(current_setting('gonvex.agent_execution_id',true),'')
    ) ON CONFLICT(revision) DO NOTHING;
    notify_payload:=jsonb_build_object('epoch',current_epoch,'revision',next_revision,'tables',changed_tables);
    IF octet_length(notify_payload::text)>7000 THEN
      notify_payload:=jsonb_build_object('epoch',current_epoch,'revision',next_revision);
    END IF;
    PERFORM pg_notify('gonvex_change_feed',notify_payload::text);
  END IF;
  RETURN NULL;
END; $$ LANGUAGE plpgsql;
