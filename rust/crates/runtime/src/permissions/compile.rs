//! Provisional SQL set compiler over normalized indexes. It does not accept
//! arbitrary row predicates, SQL fragments, JSON scalar coercions or role names.
//! The application binding and executable adapter remain gated pending review.
use super::model::{Atom, InvalidPrimitive};

#[derive(Clone, Debug)]
pub(crate) enum Expr {
    Deny,
    SlotAny(String),
    HasAtom(Atom),
    All(Vec<Expr>),
    Any(Vec<Expr>),
    /// Branch-local subtraction; never invents a global deny precedence rule.
    Subtract {
        universe: String,
        allow: Box<Expr>,
        remove: Box<Expr>,
    },
    Unsupported(String),
}

#[derive(Clone, Debug)]
pub(crate) enum Binding {
    Text(String),
    Blob(Vec<u8>),
}

pub(crate) struct CompiledSet {
    pub sql: String,
    /// Parameters 1..=4 are scope_id, table_id, index generation, admitted
    /// exact-set ID. Additional parameters are opaque slot/atom values.
    pub extra_bindings: Vec<Binding>,
}

/// `universe` names the finite allowed row-key relation supplied by the trusted
/// policy binder. Ordering, row projection and keyset pagination must be added
/// AFTER this set's deduplication by the eventual replica query adapter.
pub(crate) fn compile(universe: &str, expression: &Expr) -> Result<CompiledSet, InvalidPrimitive> {
    if universe.is_empty() {
        return Err(InvalidPrimitive::EmptyIdentity);
    }
    struct Builder<'a> {
        universe: &'a str,
        ctes: Vec<String>,
        bindings: Vec<Binding>,
    }
    impl Builder<'_> {
        fn bind(&mut self, value: Binding) -> String {
            self.bindings.push(value);
            format!("?{}", self.bindings.len() + 4)
        }
        fn node(&mut self, expression: &Expr) -> Result<String, InvalidPrimitive> {
            let select = match expression {
                Expr::Deny => "SELECT row_key FROM universe WHERE 0".to_owned(),
                Expr::SlotAny(slot) => {
                    if slot.is_empty() {
                        return Err(InvalidPrimitive::EmptyIdentity);
                    }
                    let slot = self.bind(Binding::Text(slot.clone()));
                    // Membership in the finite universe is a semijoin. IN gives
                    // SQLite a keyed ephemeral lookup instead of nested scans of
                    // two DISTINCT-backed views when estimates are too small.
                    // Row keys are non-null BINARY text; final DISTINCT preserves
                    // the existing support/set semantics. The principal relation is
                    // another semijoin: membership, not support multiplicity.
                    // Keep the DISTINCT-backed views and full scope/generation/
                    // principal binding; compiler SQL policy hashes change.
                    format!("SELECT ra.row_key FROM _gonvex_permission_row_atoms ra WHERE ra.row_key IN (SELECT row_key FROM universe) AND ra.scope_id=?1 AND ra.table_id=?2 AND ra.generation=?3 AND ra.slot={slot} AND ra.atom_id IN (SELECT pa.atom_id FROM _gonvex_permission_set_atoms pa WHERE pa.scope_id=?1 AND pa.generation=?3 AND pa.set_id=?4)")
                }
                Expr::HasAtom(atom) => {
                    let atom = self.bind(Binding::Blob(atom.canonical()?));
                    format!("SELECT row_key FROM universe WHERE EXISTS (SELECT 1 FROM _gonvex_permission_set_atoms pa JOIN _gonvex_permission_atoms a ON a.scope_id=pa.scope_id AND a.atom_id=pa.atom_id AND a.generation=pa.generation WHERE pa.scope_id=?1 AND pa.generation=?3 AND pa.set_id=?4 AND a.canonical_key={atom})")
                }
                Expr::All(children) | Expr::Any(children) => {
                    if children.is_empty() {
                        return match expression {
                            Expr::All(_) => Err(InvalidPrimitive::EmptyConjunction),
                            _ => self.node(&Expr::Deny),
                        };
                    }
                    let operator = if matches!(expression, Expr::All(_)) {
                        " INTERSECT "
                    } else {
                        " UNION "
                    };
                    let mut parts = Vec::new();
                    for child in children {
                        parts.push(format!("SELECT row_key FROM {}", self.node(child)?));
                    }
                    parts.join(operator)
                }
                Expr::Subtract {
                    universe,
                    allow,
                    remove,
                } => {
                    if universe != self.universe {
                        return Err(InvalidPrimitive::WrongUniverse);
                    }
                    let allow = self.node(allow)?;
                    let remove = self.node(remove)?;
                    format!("SELECT row_key FROM {allow} EXCEPT SELECT row_key FROM {remove}")
                }
                Expr::Unsupported(kind) => return Err(InvalidPrimitive::Unsupported(kind.clone())),
            };
            let name = format!("p{}", self.ctes.len());
            self.ctes.push(format!("{name} AS ({select})"));
            Ok(name)
        }
    }
    let mut builder = Builder {
        universe,
        ctes: Vec::new(),
        bindings: Vec::new(),
    };
    let root = builder.node(expression)?;
    // The universe's identity is a bound value, never interpolated SQL.
    let universe_binding = builder.bind(Binding::Text(universe.to_owned()));
    let universe_sql = format!("universe AS (SELECT row_key FROM _gonvex_permission_rows WHERE scope_id=?1 AND table_id=?2 AND generation=?3 AND universe_id={universe_binding})");
    let sql = format!(
        "WITH {universe_sql}, {} SELECT DISTINCT row_key FROM {root}",
        builder.ctes.join(", ")
    );
    Ok(CompiledSet {
        sql,
        extra_bindings: builder.bindings,
    })
}

/// Bind a policy version to the exact compiled normalized expression/universe,
/// rather than trusting a mutable role label or caller-chosen version string.
/// Compiler changes conservatively require a new binding/backfill identity.
pub(crate) fn policy_version(
    universe: &str,
    expression: &Expr,
) -> Result<String, InvalidPrimitive> {
    use sha2::{Digest, Sha256};
    let compiled = compile(universe, expression)?;
    let mut hash = Sha256::new();
    hash.update(b"gonvex-normalized-policy-v1");
    hash.update((compiled.sql.len() as u64).to_be_bytes());
    hash.update(compiled.sql.as_bytes());
    for binding in compiled.extra_bindings {
        match binding {
            Binding::Text(value) => {
                hash.update([0]);
                hash.update((value.len() as u64).to_be_bytes());
                hash.update(value.as_bytes());
            }
            Binding::Blob(value) => {
                hash.update([1]);
                hash.update((value.len() as u64).to_be_bytes());
                hash.update(value);
            }
        }
    }
    Ok(hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// Full source-bound authority is separate from the mathematical set SQL.
/// Empty authority preserves only the old private normalized-fixture format.
pub(crate) fn policy_version_with_authority(
    universe: &str,
    expression: &Expr,
    authority: &[u8],
) -> Result<String, InvalidPrimitive> {
    let policy = policy_version(universe, expression)?;
    if authority.is_empty() {
        return Ok(policy);
    }
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"gonvex-source-policy-v1");
    hash.update((policy.len() as u64).to_be_bytes());
    hash.update(policy.as_bytes());
    hash.update((authority.len() as u64).to_be_bytes());
    hash.update(authority);
    Ok(hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unresolved_operators_and_wrong_negative_universe_are_errors() {
        assert!(matches!(
            compile("tenant-tasks", &Expr::Unsupported("globalDeny".into())),
            Err(InvalidPrimitive::Unsupported(_))
        ));
        assert!(matches!(
            compile(
                "tenant-tasks",
                &Expr::Subtract {
                    universe: "other-tenant".into(),
                    allow: Box::new(Expr::Deny),
                    remove: Box::new(Expr::Deny)
                }
            ),
            Err(InvalidPrimitive::WrongUniverse)
        ));
        assert!(matches!(
            compile("tenant-tasks", &Expr::All(vec![])),
            Err(InvalidPrimitive::EmptyConjunction)
        ));
    }
    #[tokio::test]
    async fn finite_universe_overlap_and_negative_sets_stay_linear_in_sql_work() {
        use sqlx::{Connection, Row};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let mut db = sqlx::SqliteConnection::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::raw_sql(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../store/src/permission_indexes/schema.sql"
        )))
        .execute(&mut db)
        .await
        .unwrap();
        sqlx::raw_sql("INSERT INTO _gonvex_permission_atoms VALUES(1,1,1,X'01'); INSERT INTO _gonvex_permission_supports VALUES(1,1,'first','member','alice',1),(1,1,'overlap','member','alice',1);
        WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) INSERT INTO _gonvex_permission_row_supports SELECT 1,1,'rows','tasks',CASE WHEN x<=9000 THEN 'allowed' ELSE 'foreign' END,printf('k%05d',x) FROM n;
        INSERT INTO _gonvex_permission_row_atom_supports SELECT scope_id,generation,'primary',table_id,row_key,'read',1 FROM _gonvex_permission_row_supports;
        INSERT INTO _gonvex_permission_row_atom_supports SELECT scope_id,generation,'backup',table_id,row_key,'read',1 FROM _gonvex_permission_row_supports WHERE row_key<='k00100';
        INSERT INTO _gonvex_permission_row_atom_supports SELECT scope_id,generation,'negative',table_id,row_key,'blocked',1 FROM _gonvex_permission_row_supports WHERE cast(substr(row_key,2) AS INTEGER)%3=0;").execute(&mut db).await.unwrap();
        let compiled = compile(
            "allowed",
            &Expr::Subtract {
                universe: "allowed".into(),
                allow: Box::new(Expr::SlotAny("read".into())),
                remove: Box::new(Expr::SlotAny("blocked".into())),
            },
        )
        .unwrap();
        // An instruction budget, not noisy host wall time. The former nested
        // scan exceeds this by two orders of magnitude at 9k row roots.
        let steps = Arc::new(AtomicUsize::new(0));
        let counter = steps.clone();
        db.lock_handle()
            .await
            .unwrap()
            .set_progress_handler(1000, move || {
                counter.fetch_add(1000, Ordering::Relaxed) < 5_000_000
            });
        let mut query = sqlx::query(&compiled.sql)
            .bind(1i64)
            .bind("tasks")
            .bind(1i64)
            .bind("alice");
        for value in &compiled.extra_bindings {
            query = match value {
                Binding::Text(s) => query.bind(s),
                Binding::Blob(b) => query.bind(b),
            };
        }
        let rows = query
            .fetch_all(&mut db)
            .await
            .expect("finite-universe SQL exceeded linear work budget");
        db.lock_handle().await.unwrap().remove_progress_handler();
        let mut actual = rows
            .iter()
            .map(|r| r.get::<String, _>(0))
            .collect::<Vec<_>>();
        actual.sort();
        let expected = (1..=9000)
            .filter(|n| n % 3 != 0)
            .map(|n| format!("k{n:05}"))
            .collect::<Vec<_>>();
        assert_eq!(
            actual, expected,
            "overlaps, negative branches or foreign universe changed authority"
        );
        assert!(steps.load(Ordering::Relaxed) < 5_000_000);
    }
}

#[cfg(test)]
#[path = "semijoin_tests.rs"]
mod semijoin_tests;
