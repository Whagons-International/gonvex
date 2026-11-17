-- Postgres tenant identity schema before the SQLite port (git 6a45b2e8, TENANT_IDENTITY_SQL).
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema=current_schema() AND table_name='members' AND column_name='user_id') THEN
    RAISE EXCEPTION 'identity-v2 migration required: tenant members still use user_id';
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS members (
  id text PRIMARY KEY, account_id text NOT NULL UNIQUE, status text NOT NULL DEFAULT 'active',
  display_name text NOT NULL DEFAULT '', avatar_url text NOT NULL DEFAULT '',
  role text NOT NULL DEFAULT 'member', permissions jsonb NOT NULL DEFAULT '{}'::jsonb,
  membership_revision bigint NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS members_by_role ON members(role);
CREATE INDEX IF NOT EXISTS members_by_status ON members(status,id);
CREATE TABLE IF NOT EXISTS _gonvex_control_plane_membership_outbox (
  account_id text PRIMARY KEY, member_id text NOT NULL, status text NOT NULL,
  membership_revision bigint NOT NULL, updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE OR REPLACE FUNCTION _gonvex_queue_control_plane_membership() RETURNS trigger AS $$
DECLARE projected_account_id text; projected_member_id text; projected_status text; projected_revision bigint;
BEGIN
  IF TG_OP='DELETE' THEN
    projected_account_id:=OLD.account_id; projected_member_id:=OLD.id;
    projected_status:='revoked'; projected_revision:=OLD.membership_revision+1;
  ELSE
    projected_account_id:=NEW.account_id; projected_member_id:=NEW.id;
    projected_status:=NEW.status; projected_revision:=NEW.membership_revision;
  END IF;
  INSERT INTO _gonvex_control_plane_membership_outbox(account_id,member_id,status,membership_revision,updated_at)
  VALUES(projected_account_id,projected_member_id,projected_status,projected_revision,now())
  ON CONFLICT(account_id) DO UPDATE SET member_id=EXCLUDED.member_id,status=EXCLUDED.status,
    membership_revision=EXCLUDED.membership_revision,updated_at=now()
  WHERE EXCLUDED.membership_revision>=_gonvex_control_plane_membership_outbox.membership_revision;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS gonvex_queue_control_plane_membership ON members;
CREATE TRIGGER gonvex_queue_control_plane_membership AFTER INSERT OR UPDATE OR DELETE ON members
  FOR EACH ROW EXECUTE FUNCTION _gonvex_queue_control_plane_membership();
