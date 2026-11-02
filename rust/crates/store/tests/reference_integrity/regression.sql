DO $$ BEGIN
  IF (SELECT count(*) FROM gonvex_reference_violations())<>1 THEN RAISE EXCEPTION 'legacy baseline lost'; END IF;
END $$;
UPDATE workspaces SET note='unrelated update' WHERE _id='legacy';
-- Force deferred checks inside a subtransaction, then prove rollback preserves
-- both the target and its security links.
DO $$ BEGIN
  BEGIN
    UPDATE teams SET "deletedAt"=2 WHERE _id='before';
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'soft deletion was accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
  IF (SELECT "deletedAt" FROM teams WHERE _id='before') IS NOT NULL THEN RAISE EXCEPTION 'delete partially committed'; END IF;
  IF (SELECT teams FROM workspaces WHERE _id='before')<>'["before"]'::jsonb THEN RAISE EXCEPTION 'security relationship changed'; END IF;
  BEGIN
    DELETE FROM teams WHERE _id='before';
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'hard deletion was accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
  BEGIN
    INSERT INTO workspaces VALUES('bad','["absent"]',NULL,NULL);
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'missing target was accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
  BEGIN
    INSERT INTO workspaces VALUES('bad-shape','[{}]',NULL,NULL);
    RAISE EXCEPTION 'malformed target was accepted';
  EXCEPTION WHEN check_violation THEN NULL; END;
  BEGIN
    INSERT INTO approvals VALUES('bad-type',NULL,'[{"type":"unknown","id":"before"}]');
    RAISE EXCEPTION 'unknown polymorphic target was accepted';
  EXCEPTION WHEN check_violation THEN NULL; END;
  BEGIN
    TRUNCATE teams;
    RAISE EXCEPTION 'truncate was accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
END $$;
INSERT INTO teams VALUES('missing',NULL);
DO $$ BEGIN
  BEGIN
    DELETE FROM teams WHERE _id='missing';
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'restored legacy target deletion was accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
END $$;
INSERT INTO teams VALUES('history-only',NULL);
INSERT INTO history VALUES('h','history-only');
UPDATE teams SET "deletedAt"=5 WHERE _id='history-only';
DO $$ BEGIN
  BEGIN
    DELETE FROM teams WHERE _id='history-only';
    SET CONSTRAINTS gonvex_reference_target IMMEDIATE;
    RAISE EXCEPTION 'historical identity hard deletion was accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
END $$;
-- Deferred FK allows explicit reassignment in either operation order.
BEGIN;
UPDATE teams SET "deletedAt"=3 WHERE _id='before';
UPDATE workspaces SET teams='[]' WHERE _id='before';
COMMIT;
DO $$ BEGIN
  IF EXISTS(SELECT FROM gonvex_reference_violations()) THEN RAISE EXCEPTION 'unexpected final invariant violation'; END IF;
END $$;
SELECT 'prototype PASS: before reproduced; soft/hard/new links/rollback/legacy restoration/polymorphism/history/TRUNCATE/reassignment';