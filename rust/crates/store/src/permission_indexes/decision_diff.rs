//! Exact current-commit diffs. Candidate keys come from changed row edges and
//! reverse decision dependencies, never a before/after per-member full scan.
use super::invalid;
use crate::DatabaseError;
use sqlx::{Row, SqliteConnection};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Delta {
    Added(String),
    Removed(String),
    Changed(String),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// A reset/gap cannot reconstruct its old authorization. Use the ordinary
    /// v2 resnapshot rather than disclosing a guessed incremental result.
    NormalV2,
    Exact {
        deltas: Vec<Delta>,
        candidates: i64,
    },
}

// ?1 scope, ?2 generation, ?3 revision, ?4 persisted member id. Atom 0 is an
// admission journal marker, not a permission atom. Both branches participate in
// reverse dependencies: an added negative atom can remove a row without adding
// any positive atom. A row delta's NULL root means absent, never constant FALSE.
pub const CTE: &str = r#"WITH RECURSIVE
pd AS MATERIALIZED (
 SELECT atom_id,before_present,after_present FROM _gonvex_permission_program_principal_deltas
 WHERE scope_id=?1 AND generation=?2 AND revision=?3 AND principal=?4
),
after_atoms(atom_id) AS MATERIALIZED (
 SELECT atom_id FROM _gonvex_permission_set_atoms WHERE scope_id=?1 AND generation=?2 AND set_id=?4
),
before_atoms(atom_id) AS MATERIALIZED (
 SELECT a.atom_id FROM after_atoms a WHERE NOT EXISTS(SELECT 1 FROM pd WHERE pd.atom_id=a.atom_id AND pd.before_present=0)
 UNION SELECT atom_id FROM pd WHERE before_present=1 AND atom_id<>0
),
admission(before_ok,after_ok) AS (
 SELECT coalesce((SELECT before_present FROM pd WHERE atom_id=0),EXISTS(SELECT 1 FROM _gonvex_permission_program_principals WHERE scope_id=?1 AND generation=?2 AND principal=?4)),
 EXISTS(SELECT 1 FROM _gonvex_permission_program_principals WHERE scope_id=?1 AND generation=?2 AND principal=?4)
),
cover_atoms(atom_id) AS MATERIALIZED (SELECT atom_id FROM before_atoms UNION SELECT atom_id FROM after_atoms),
affected(node_id) AS (
 SELECT d.root_id FROM pd CROSS JOIN cover_atoms p CROSS JOIN _gonvex_permission_decision_dependencies d
 WHERE pd.atom_id<>0 AND d.scope_id=?1 AND d.generation=?2 AND d.dependency_atom=pd.atom_id AND d.cover_atom=p.atom_id
 UNION
 SELECT d.root_id FROM pd CROSS JOIN _gonvex_permission_decision_dependencies d
 WHERE pd.atom_id<>0 AND d.scope_id=?1 AND d.generation=?2 AND d.dependency_atom=pd.atom_id AND d.cover_atom=0
),
rd AS MATERIALIZED (
 SELECT row_key,before_root,after_root FROM _gonvex_permission_program_row_deltas WHERE scope_id=?1 AND generation=?2 AND revision=?3
),
candidate_keys(row_key) AS (
 SELECT row_key FROM rd
 UNION SELECT r.row_key FROM affected a CROSS JOIN _gonvex_permission_program_rows r INDEXED BY _gonvex_permission_program_root
 WHERE r.scope_id=?1 AND r.generation=?2 AND r.root_id=a.node_id
 UNION SELECT r.row_key FROM pd CROSS JOIN _gonvex_permission_program_rows r WHERE pd.atom_id=0 AND r.scope_id=?1 AND r.generation=?2
),
roots(row_key,before_root,after_root,content_changed) AS MATERIALIZED (
 SELECT k.row_key,CASE WHEN d.row_key IS NULL THEN r.root_id ELSE d.before_root END,r.root_id,d.row_key IS NOT NULL
 FROM candidate_keys k LEFT JOIN rd d ON d.row_key=k.row_key
 LEFT JOIN _gonvex_permission_program_rows r ON r.scope_id=?1 AND r.generation=?2 AND r.row_key=k.row_key
),
walk(side,root_id,node_id) AS (
 SELECT 0,before_root,before_root FROM roots,admission WHERE before_root IS NOT NULL AND before_ok
 UNION SELECT 1,after_root,after_root FROM roots,admission WHERE after_root IS NOT NULL AND after_ok
 UNION ALL
 SELECT w.side,w.root_id,CASE WHEN (w.side=0 AND EXISTS(SELECT 1 FROM before_atoms WHERE atom_id=n.atom_id)) OR (w.side=1 AND EXISTS(SELECT 1 FROM after_atoms WHERE atom_id=n.atom_id)) THEN n.high_id ELSE n.low_id END
 FROM walk w JOIN _gonvex_permission_decision_nodes n ON n.scope_id=?1 AND n.generation=?2 AND n.node_id=w.node_id
),
accepted(side,root_id) AS MATERIALIZED (SELECT side,root_id FROM walk WHERE node_id=1),
result(row_key,was_visible,is_visible,content_changed) AS (
 SELECT row_key,EXISTS(SELECT 1 FROM accepted WHERE side=0 AND root_id=before_root),EXISTS(SELECT 1 FROM accepted WHERE side=1 AND root_id=after_root),content_changed FROM roots
)
"#;

pub async fn current(
    c: &mut SqliteConnection,
    scope: i64,
    generation: i64,
    revision: i64,
    principal: &str,
) -> Result<Outcome, DatabaseError> {
    super::require_local(c).await?;
    if revision < 0 || generation <= 0 || principal.is_empty() {
        return Err(invalid("invalid indexed diff identity"));
    }
    let valid: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _gonvex_permission_scopes s JOIN _gonvex_permission_programs p ON p.scope_id=s.scope_id AND p.generation=s.generation JOIN _gonvex_sync_clock c ON c.singleton=1 WHERE s.scope_id=?1 AND s.generation=?2 AND s.phase='ready' AND s.covered_revision=?3 AND c.revision=?3 AND NOT EXISTS(SELECT 1 FROM _gonvex_permission_program_changes WHERE scope_id=?1) AND NOT EXISTS(SELECT 1 FROM _gonvex_permission_program_resets WHERE scope_id=?1 AND generation=?2 AND revision=?3))").bind(scope).bind(generation).bind(revision).fetch_one(&mut *c).await?;
    if valid == 0 {
        return Ok(Outcome::NormalV2);
    }
    let sql = format!("{CTE} SELECT row_key,was_visible,is_visible,(SELECT count(*) FROM candidate_keys) AS candidates FROM result WHERE was_visible<>is_visible OR (was_visible AND content_changed) UNION ALL SELECT NULL,0,0,count(*) FROM candidate_keys ORDER BY row_key");
    let rows = sqlx::query(&sql)
        .bind(scope)
        .bind(generation)
        .bind(revision)
        .bind(principal)
        .fetch_all(c)
        .await?;
    let mut deltas = Vec::new();
    let mut candidates = 0;
    for row in rows {
        candidates = row.try_get(3)?;
        let Some(key) = row.try_get::<Option<String>, _>(0)? else {
            continue;
        };
        let before: bool = row.try_get(1)?;
        let after: bool = row.try_get(2)?;
        deltas.push(match (before, after) {
            (false, true) => Delta::Added(key),
            (true, false) => Delta::Removed(key),
            (true, true) => Delta::Changed(key),
            _ => return Err(invalid("invalid indexed visibility delta")),
        });
    }
    Ok(Outcome::Exact { deltas, candidates })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permission_indexes::decision::Builder;
    use sqlx::Connection;
    use std::collections::BTreeSet;
    #[derive(Clone)]
    enum F {
        Atom(i64),
        All(Box<F>, Box<F>),
        Any(Box<F>, Box<F>),
        Not(Box<F>),
    }
    impl F {
        fn eval(&self, set: u64) -> bool {
            match self {
                Self::Atom(a) => set & (1 << a) != 0,
                Self::All(a, b) => a.eval(set) && b.eval(set),
                Self::Any(a, b) => a.eval(set) || b.eval(set),
                Self::Not(a) => !a.eval(set),
            }
        }
        fn lower(&self, b: &mut Builder) -> i64 {
            match self {
                Self::Atom(a) => b.atom(*a),
                Self::All(a, c) => {
                    let a = a.lower(b);
                    let c = c.lower(b);
                    b.combine(true, a, c)
                }
                Self::Any(a, c) => {
                    let a = a.lower(b);
                    let c = c.lower(b);
                    b.combine(false, a, c)
                }
                Self::Not(a) => {
                    let a = a.lower(b);
                    b.negate(a)
                }
            }
        }
    }
    fn random(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        *seed >> 32
    }
    fn formula(seed: &mut u64, depth: usize) -> F {
        if depth == 0 {
            return F::Atom((random(seed) % 8 + 1) as i64);
        }
        match random(seed) % 4 {
            0 => F::Atom((random(seed) % 8 + 1) as i64),
            1 => F::All(
                Box::new(formula(seed, depth - 1)),
                Box::new(formula(seed, depth - 1)),
            ),
            2 => F::Any(
                Box::new(formula(seed, depth - 1)),
                Box::new(formula(seed, depth - 1)),
            ),
            _ => F::Not(Box::new(formula(seed, depth - 1))),
        }
    }
    #[tokio::test]
    async fn indexed_candidates_match_independent_boolean_oracle_for_atom_and_row_changes() {
        let mut c = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(concat!(
            include_str!("schema.sql"),
            "\n",
            include_str!("decision.sql")
        ))
        .execute(&mut c)
        .await
        .unwrap();
        sqlx::raw_sql("WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<8) INSERT INTO _gonvex_permission_atoms SELECT 1,1,n,CAST(n AS BLOB) FROM seq; INSERT INTO _gonvex_permission_program_principals VALUES(1,1,'member')").execute(&mut c).await.unwrap();
        let mut seed = 829137;
        let fs = (0..100).map(|_| formula(&mut seed, 4)).collect::<Vec<_>>();
        let mut b = Builder::load(&mut c, 1, 1).await.unwrap();
        let roots = fs.iter().map(|f| f.lower(&mut b)).collect::<Vec<_>>();
        for (i, root) in roots.iter().enumerate() {
            assert!(b.register_root(&mut c, 1, 1, *root).await.unwrap());
            sqlx::query("INSERT INTO _gonvex_permission_program_rows VALUES(1,1,?1,?2)")
                .bind(format!("row-{i:03}"))
                .bind(root)
                .execute(&mut c)
                .await
                .unwrap();
        }
        b.persist(&mut c, 1, 1).await.unwrap();
        for revision in 1..=128 {
            let before = random(&mut seed) % 512;
            let after = before ^ (1 << (random(&mut seed) % 8 + 1));
            sqlx::query("DELETE FROM _gonvex_permission_supports")
                .execute(&mut c)
                .await
                .unwrap();
            for atom in 1..=8 {
                if after & (1 << atom) != 0 {
                    sqlx::query("INSERT INTO _gonvex_permission_supports VALUES(1,1,'grant','member','member',?1)").bind(atom as i64).execute(&mut c).await.unwrap();
                }
                if (before ^ after) & (1 << atom) != 0 {
                    sqlx::query("INSERT INTO _gonvex_permission_program_principal_deltas VALUES(1,1,?1,'member',?2,?3,?4)").bind(revision).bind(atom as i64).bind(i64::from(before&(1<<atom)!=0)).bind(i64::from(after&(1<<atom)!=0)).execute(&mut c).await.unwrap();
                }
            }
            // One simultaneous content/authorization-edge write, independent
            // of whether the changed principal atom touches this row's DAG.
            let changed = (random(&mut seed) % 100) as usize;
            let old = fs[changed].lower(&mut b);
            let new = fs[(changed + 1) % 100].lower(&mut b);
            sqlx::query("UPDATE _gonvex_permission_program_rows SET root_id=?2 WHERE row_key=?1")
                .bind(format!("row-{changed:03}"))
                .bind(new)
                .execute(&mut c)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO _gonvex_permission_program_row_deltas VALUES(1,1,?1,?2,?3,?4)",
            )
            .bind(revision)
            .bind(format!("row-{changed:03}"))
            .bind(old)
            .bind(new)
            .execute(&mut c)
            .await
            .unwrap();
            let rows=sqlx::query(&format!("{CTE} SELECT row_key,was_visible,is_visible FROM result WHERE was_visible<>is_visible OR (was_visible AND content_changed) ORDER BY row_key")).bind(1).bind(1).bind(revision).bind("member").fetch_all(&mut c).await.unwrap();
            let actual = rows
                .into_iter()
                .map(|r| {
                    (
                        r.get::<String, _>(0),
                        r.get::<bool, _>(1),
                        r.get::<bool, _>(2),
                    )
                })
                .collect::<BTreeSet<_>>();
            let expected = fs
                .iter()
                .enumerate()
                .filter_map(|(i, f)| {
                    let was = f.eval(before);
                    let is = if i == changed {
                        fs[(i + 1) % 100].eval(after)
                    } else {
                        f.eval(after)
                    };
                    ((was != is) || (was && i == changed)).then(|| (format!("row-{i:03}"), was, is))
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(
                actual, expected,
                "seed revision {revision}; before={before}; after={after}"
            );
            sqlx::query("UPDATE _gonvex_permission_program_rows SET root_id=?2 WHERE row_key=?1")
                .bind(format!("row-{changed:03}"))
                .bind(old)
                .execute(&mut c)
                .await
                .unwrap();
        }
    }
    #[tokio::test]
    async fn shared_negative_flip_seeks_only_member_cover_not_ten_thousand_unrelated_roots() {
        struct Own(std::path::PathBuf);
        impl Drop for Own {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        std::fs::create_dir_all(crate::permission_indexes::LOCAL_FIXTURE_ROOT).unwrap();
        let own = Own(
            std::path::Path::new(crate::permission_indexes::LOCAL_FIXTURE_ROOT)
                .join(format!("decision-vm-{}.db", uuid::Uuid::new_v4())),
        );
        let mut c = SqliteConnection::connect_with(
            &sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&own.0)
                .create_if_missing(true),
        )
        .await
        .unwrap();
        sqlx::raw_sql(concat!(
            include_str!("schema.sql"),
            "\n",
            include_str!("decision.sql")
        ))
        .execute(&mut c)
        .await
        .unwrap();
        sqlx::raw_sql("WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<10000) INSERT INTO _gonvex_permission_atoms SELECT 1,1,n,CAST(n AS BLOB) FROM seq; INSERT INTO _gonvex_permission_atoms VALUES(1,1,20001,X'ff'); INSERT INTO _gonvex_permission_supports VALUES(1,1,'owner','member','member',1),(1,1,'negative','member','member',20001); INSERT INTO _gonvex_permission_program_principals VALUES(1,1,'member'); INSERT INTO _gonvex_permission_program_principal_deltas VALUES(1,1,1,'member',20001,0,1)").execute(&mut c).await.unwrap();
        let mut b = Builder::load(&mut c, 1, 1).await.unwrap();
        let d = b.atom(20001);
        let not_d = b.negate(d);
        let mut steps = Vec::new();
        let mut scans = Vec::new();
        for (start, end) in [(1, 1000), (1001, 10000)] {
            sqlx::query("BEGIN").execute(&mut c).await.unwrap();
            for atom in start..=end {
                let a = b.atom(atom);
                let root = b.combine(true, a, not_d);
                assert!(b.register_root(&mut c, 1, 1, root).await.unwrap());
                sqlx::query("INSERT INTO _gonvex_permission_program_rows VALUES(1,1,?1,?2)")
                    .bind(format!("row-{atom:05}"))
                    .bind(root)
                    .execute(&mut c)
                    .await
                    .unwrap();
            }
            b.persist(&mut c, 1, 1).await.unwrap();
            sqlx::query("COMMIT").execute(&mut c).await.unwrap();
            let db = rusqlite::Connection::open(&own.0).unwrap();
            let mut statement=db.prepare(&format!("{CTE} SELECT row_key,was_visible,is_visible,(SELECT count(*) FROM candidate_keys) FROM result WHERE was_visible<>is_visible")).unwrap();
            let actual = statement
                .query_map(rusqlite::params![1, 1, 1, "member"], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, bool>(1)?,
                        r.get::<_, bool>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(actual, vec![("row-00001".into(), true, false, 1)]);
            steps.push(statement.get_status(rusqlite::StatementStatus::VmStep));
            scans.push(statement.get_status(rusqlite::StatementStatus::FullscanStep));
        }
        assert!(
            steps[1] <= steps[0] + 1000,
            "VM growth with unrelated roots: {steps:?}"
        );
        assert!(
            scans[1] <= scans[0] + 10,
            "unrelated fullscan steps {scans:?}"
        );
        eprintln!(
            "changed-negative diff VM instructions at 1k/10k roots: {steps:?}; candidate rows=1"
        );
        c.close().await.unwrap();
    }
}
