//! Canonical reduced decision DAGs. Construction is principal-independent.
//! Reads start from an indexed positive cover and walk exact atom branches IN
//! SQL. They do not load/evaluate the collection in Rust for every principal.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sqlx::{Row, SqliteConnection};

use crate::DatabaseError;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum Formula {
    Constant {
        value: bool,
    },
    Atom {
        namespace: String,
        resource: String,
        #[serde(default)]
        principal: Option<String>,
    },
    All {
        children: Vec<Formula>,
    },
    Any {
        children: Vec<Formula>,
    },
    Not {
        child: Box<Formula>,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Node {
    atom: i64,
    low: i64,
    high: i64,
}
pub struct Builder {
    nodes: BTreeMap<i64, Node>,
    unique: BTreeMap<Node, i64>,
    next: i64,
    original: BTreeSet<i64>,
    apply: BTreeMap<(bool, i64, i64), i64>,
    not: BTreeMap<i64, i64>,
    exhausted: bool,
}
impl Default for Builder {
    fn default() -> Self {
        Self {
            nodes: BTreeMap::new(),
            unique: BTreeMap::new(),
            next: 2,
            original: BTreeSet::new(),
            apply: BTreeMap::new(),
            not: BTreeMap::new(),
            exhausted: false,
        }
    }
}
impl Builder {
    pub async fn load(
        c: &mut SqliteConnection,
        scope: i64,
        generation: i64,
    ) -> Result<Self, DatabaseError> {
        let mut out = Self {
            next: 2,
            ..Default::default()
        };
        for row in sqlx::query("SELECT node_id,atom_id,low_id,high_id FROM _gonvex_permission_decision_nodes WHERE scope_id=?1 AND generation=?2 ORDER BY node_id").bind(scope).bind(generation).fetch_all(c).await? {
            let id:i64=row.try_get(0)?;
            let n=Node{atom:row.try_get(1)?,low:row.try_get(2)?,high:row.try_get(3)?};
            if id<2 || id==i64::MAX || n.atom<=0 || n.low<0 || n.high<0 || n.low==n.high || out.nodes.contains_key(&id) {return Err(DatabaseError::Decode("invalid normalized decision DAG".into()));}
            out.next=out.next.max(id+1);out.unique.insert(n,id);out.nodes.insert(id,n);out.original.insert(id);
        }
        for n in out.nodes.values() {
            for child in [n.low, n.high] {
                if child >= 2 && out.nodes.get(&child).is_none_or(|c| c.atom <= n.atom) {
                    return Err(DatabaseError::Decode(
                        "decision DAG cycle/order mismatch".into(),
                    ));
                }
            }
        }
        Ok(out)
    }
    fn node(&mut self, atom: i64, low: i64, high: i64) -> i64 {
        if low == high {
            return low;
        }
        let node = Node { atom, low, high };
        if let Some(id) = self.unique.get(&node) {
            return *id;
        }
        if self.nodes.len() >= 131_072 || self.apply.len() >= 1_048_576 {
            self.exhausted = true;
            return 0; // Internal placeholder only: persist MUST reject it.
        }
        let id = self.next;
        self.next += 1;
        self.nodes.insert(id, node);
        self.unique.insert(node, id);
        id
    }
    pub fn atom(&mut self, atom: i64) -> i64 {
        if atom <= 0 {
            self.exhausted = true;
            return 0;
        }
        self.node(atom, 0, 1)
    }
    pub fn exhausted(&self) -> bool {
        self.exhausted
    }
    pub fn negate(&mut self, id: i64) -> i64 {
        let mut pending = vec![(id, false)];
        while let Some((id, visited)) = pending.pop() {
            if id < 2 || self.not.contains_key(&id) {
                continue;
            }
            let n = self.nodes[&id];
            if !visited {
                pending.push((id, true));
                pending.push((n.high, false));
                pending.push((n.low, false));
                continue;
            }
            let invert = |id| if id < 2 { 1 - id } else { self.not[&id] };
            let low = invert(n.low);
            let high = invert(n.high);
            let result = self.node(n.atom, low, high);
            self.not.insert(id, result);
            if self.exhausted {
                return 0;
            }
        }
        if id < 2 {
            1 - id
        } else {
            self.not[&id]
        }
    }
    fn pair(a: i64, b: i64) -> (i64, i64) {
        if a <= b {
            (a, b)
        } else {
            (b, a)
        }
    }
    fn simple(&self, and: bool, a: i64, b: i64) -> Option<i64> {
        if a == b {
            Some(a)
        } else if a == 0 {
            Some(if and { 0 } else { b })
        } else if a == 1 {
            Some(if and { b } else { 1 })
        } else {
            self.apply.get(&(and, a, b)).copied()
        }
    }
    fn split(&self, a: i64, b: i64) -> (i64, (i64, i64), (i64, i64)) {
        let x = self.nodes[&a];
        let y = self.nodes[&b];
        let atom = x.atom.min(y.atom);
        let (al, ah) = if x.atom == atom {
            (x.low, x.high)
        } else {
            (a, a)
        };
        let (bl, bh) = if y.atom == atom {
            (y.low, y.high)
        } else {
            (b, b)
        };
        (atom, Self::pair(al, bl), Self::pair(ah, bh))
    }
    pub fn combine(&mut self, and: bool, a: i64, b: i64) -> i64 {
        let (a, b) = Self::pair(a, b);
        let mut pending = vec![(a, b, false)];
        while let Some((a, b, visited)) = pending.pop() {
            if self.simple(and, a, b).is_some() {
                continue;
            }
            let (atom, (al, bl), (ah, bh)) = self.split(a, b);
            if !visited {
                pending.push((a, b, true));
                pending.push((ah, bh, false));
                pending.push((al, bl, false));
                continue;
            }
            let low = self
                .simple(and, al, bl)
                .expect("ordered DAG low dependency");
            let high = self
                .simple(and, ah, bh)
                .expect("ordered DAG high dependency");
            let result = self.node(atom, low, high);
            self.apply.insert((and, a, b), result);
            if self.exhausted {
                return 0;
            }
        }
        self.simple(and, a, b).expect("ordered DAG root")
    }
    pub async fn persist(
        &mut self,
        c: &mut SqliteConnection,
        scope: i64,
        generation: i64,
    ) -> Result<(), DatabaseError> {
        if self.exhausted {
            return Err(DatabaseError::Decode(
                "decision construction budget exhausted; index cannot publish".into(),
            ));
        }
        for (id, n) in &self.nodes {
            if self.original.contains(id) {
                continue;
            }
            sqlx::query("INSERT INTO _gonvex_permission_decision_nodes VALUES(?1,?2,?3,?4,?5,?6)")
                .bind(scope)
                .bind(generation)
                .bind(id)
                .bind(n.atom)
                .bind(n.low)
                .bind(n.high)
                .execute(&mut *c)
                .await?;
            for (branch, child) in [(0, n.low), (1, n.high)] {
                sqlx::query(
                    "INSERT INTO _gonvex_permission_decision_edges VALUES(?1,?2,?3,?4,?5,?6)",
                )
                .bind(scope)
                .bind(generation)
                .bind(child)
                .bind(id)
                .bind(n.atom)
                .bind(branch)
                .execute(&mut *c)
                .await?;
            }
        }
        self.original.extend(self.nodes.keys().copied());
        Ok(())
    }
}
/// A positive cover is derived from LOW paths, with HIGH branches anchored
/// by their own atom. Thus (owner AND NOT revoked) anchors on owner even when
/// revoked was interned first. Only formulas true at the all-absent assignment
/// need their explicit finite-universe bucket; FALSE has no candidates.
impl Builder {
    fn cover(&self, root: i64, out: &mut BTreeSet<i64>) -> bool {
        let mut id = root;
        while id >= 2 {
            let n = self.nodes[&id];
            if n.high != 0 {
                out.insert(n.atom);
            }
            id = n.low;
        }
        id == 1
    }

    pub async fn register_root(
        &mut self,
        c: &mut SqliteConnection,
        scope: i64,
        generation: i64,
        root: i64,
    ) -> Result<bool, DatabaseError> {
        let mut atoms = BTreeSet::new();
        let unanchored = self.cover(root, &mut atoms);
        let mut dependencies = BTreeSet::new();
        let mut seen = BTreeSet::new();
        let mut pending = vec![root];
        while let Some(id) = pending.pop() {
            if id < 2 || !seen.insert(id) {
                continue;
            }
            let n = self.nodes[&id];
            dependencies.insert(n.atom);
            pending.extend([n.low, n.high]);
        }
        let size = dependencies
            .len()
            .checked_mul(atoms.len() + usize::from(unanchored));
        if size.is_none_or(|n| n > 65_536) {
            self.exhausted = true;
            return Ok(false);
        }
        for atom in &atoms {
            sqlx::query("INSERT INTO _gonvex_permission_decision_anchors VALUES(?1,?2,?3,?4) ON CONFLICT DO NOTHING").bind(scope).bind(generation).bind(atom).bind(root).execute(&mut *c).await?;
        }
        if unanchored {
            sqlx::query("INSERT INTO _gonvex_permission_decision_unanchored VALUES(?1,?2,?3) ON CONFLICT DO NOTHING").bind(scope).bind(generation).bind(root).execute(&mut *c).await?;
            atoms.insert(0);
        }
        for dependency in dependencies {
            for cover in &atoms {
                sqlx::query("INSERT INTO _gonvex_permission_decision_dependencies VALUES(?1,?2,?3,?4,?5) ON CONFLICT DO NOTHING").bind(scope).bind(generation).bind(dependency).bind(cover).bind(root).execute(&mut *c).await?;
            }
        }
        Ok(true)
    }
}
/// Candidates start at admitted principal atoms and their covering root index.
/// A forward walk chooses exactly ONE branch at each decision. Seeding reverse
/// TRUE/FALSE edges would inspect every unrelated leaf and is deliberately not
/// used. Public/negative-only roots use their explicit finite-universe bucket.
pub const WALK_CTE: &str = "WITH RECURSIVE candidates(root_id) AS (SELECT a.root_id FROM _gonvex_permission_set_atoms p CROSS JOIN _gonvex_permission_decision_anchors a WHERE p.scope_id=?1 AND p.generation=?2 AND p.set_id=?3 AND a.scope_id=p.scope_id AND a.generation=p.generation AND a.atom_id=p.atom_id UNION SELECT root_id FROM _gonvex_permission_decision_unanchored WHERE scope_id=?1 AND generation=?2), walk(root_id,node_id) AS (SELECT root_id,root_id FROM candidates c WHERE EXISTS(SELECT 1 FROM _gonvex_permission_program_rows r WHERE r.scope_id=?1 AND r.generation=?2 AND r.root_id=c.root_id) UNION ALL SELECT w.root_id,CASE WHEN EXISTS(SELECT 1 FROM _gonvex_permission_set_atoms p WHERE p.scope_id=?1 AND p.generation=?2 AND p.set_id=?3 AND p.atom_id=n.atom_id) THEN n.high_id ELSE n.low_id END FROM walk w JOIN _gonvex_permission_decision_nodes n ON n.scope_id=?1 AND n.generation=?2 AND n.node_id=w.node_id) ";
pub fn select_rows() -> String {
    format!("{WALK_CTE} SELECT r.row_key FROM walk w JOIN _gonvex_permission_program_rows r INDEXED BY _gonvex_permission_program_root ON r.scope_id=?1 AND r.generation=?2 AND r.root_id=w.root_id WHERE w.node_id=1")
}

#[cfg(test)]
mod tests {
    use super::*;
    fn eval(b: &Builder, id: i64, set: u64) -> bool {
        if id < 2 {
            return id == 1;
        }
        let n = b.nodes[&id];
        eval(
            b,
            if set & (1 << n.atom) != 0 {
                n.high
            } else {
                n.low
            },
            set,
        )
    }
    #[test]
    fn default_builder_reserves_terminals_and_invalid_atoms_cannot_publish() {
        let mut b = Builder::default();
        let a = b.atom(1);
        assert_eq!(a, 2);
        assert!(!eval(&b, a, 0));
        assert!(eval(&b, a, 2));
        b.atom(0);
        assert!(b.exhausted());
    }
    #[test]
    fn decision_dag_preserves_and_or_negative_and_overlapping_supports() {
        let mut b = Builder {
            next: 2,
            ..Default::default()
        };
        let a = b.atom(1);
        let c = b.atom(2);
        let d = b.atom(3);
        let ac = b.combine(false, a, c);
        let nd = b.negate(d);
        let root = b.combine(true, ac, nd);
        for set in 0..16 {
            assert_eq!(
                eval(&b, root, set),
                (set & 2 != 0 || set & 4 != 0) && set & 8 == 0
            );
        }
        let ca = b.combine(false, c, a);
        assert_eq!(ac, ca);
        let nn = b.negate(nd);
        assert_eq!(nn, d);
    }
    #[tokio::test]
    async fn deep_decisions_use_explicit_stacks_and_over_budget_roots_cannot_publish() {
        use sqlx::Connection;
        let mut b = Builder {
            next: 2,
            ..Default::default()
        };
        let mut root = 0;
        for atom in (1..=10_000).rev() {
            let leaf = b.atom(atom);
            root = b.combine(false, leaf, root);
        }
        let negative = b.negate(root);
        assert_eq!(b.negate(negative), root);
        let mut c = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        // Cross product of dependency and covering atoms would be 100M rows.
        // Reject before writing any index statement, never approximate to allow.
        assert!(!b.register_root(&mut c, 1, 1, root).await.unwrap());
        assert!(b.exhausted());
        assert!(b.persist(&mut c, 1, 1).await.is_err());
        let mut b = Builder {
            next: 2,
            ..Default::default()
        };
        for atom in 1..=131_073 {
            b.atom(atom);
        }
        assert!(b.exhausted());
        assert!(b.persist(&mut c, 1, 1).await.is_err());
    }
    #[tokio::test]
    async fn sql_walk_starts_at_positive_cover_not_unrelated_false_edges() {
        use sqlx::Connection;
        let mut c = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(concat!(
            include_str!("schema.sql"),
            "\n",
            include_str!("decision.sql")
        ))
        .execute(&mut c)
        .await
        .unwrap();
        sqlx::raw_sql("WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<20001) INSERT INTO _gonvex_permission_atoms SELECT 1,1,n,CAST(n AS BLOB) FROM seq; INSERT INTO _gonvex_permission_atoms VALUES(1,1,9000000,X'fe'); INSERT INTO _gonvex_permission_supports VALUES(1,1,'first','member','one',3)").execute(&mut c).await.unwrap();
        let mut b = Builder::load(&mut c, 1, 1).await.unwrap();
        for i in 1..=10000 {
            let negative = b.atom(i * 2);
            let positive = b.atom(i * 2 + 1);
            let allowed_negative = b.negate(negative);
            let root = b.combine(true, positive, allowed_negative);
            b.register_root(&mut c, 1, 1, root).await.unwrap();
            sqlx::query("INSERT INTO _gonvex_permission_program_rows VALUES(1,1,?1,?2)")
                .bind(format!("row-{i:05}"))
                .bind(root)
                .execute(&mut c)
                .await
                .unwrap();
        }
        b.persist(&mut c, 1, 1).await.unwrap();
        let counts = sqlx::query(&format!(
            "{WALK_CTE} SELECT (SELECT count(*) FROM candidates),count(*) FROM walk"
        ))
        .bind(1)
        .bind(1)
        .bind("one")
        .fetch_one(&mut c)
        .await
        .unwrap();
        assert_eq!(counts.get::<i64, _>(0), 1);
        assert_eq!(counts.get::<i64, _>(1), 3);
        let keys: Vec<String> = sqlx::query_scalar(&select_rows())
            .bind(1)
            .bind(1)
            .bind("one")
            .fetch_all(&mut c)
            .await
            .unwrap();
        assert_eq!(keys, vec!["row-00001"]);
        let plans = sqlx::query(&format!("EXPLAIN QUERY PLAN {}", select_rows()))
            .bind(1)
            .bind(1)
            .bind("one")
            .fetch_all(&mut c)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get::<String, _>(3))
            .collect::<Vec<_>>();
        assert!(
            plans
                .iter()
                .any(|p| p.contains("SEARCH a") && p.contains("atom_id=?")),
            "{plans:?}"
        );
        assert!(
            plans
                .iter()
                .any(|p| p.contains("_gonvex_permission_program_root")),
            "{plans:?}"
        );
        let atom = b.atom(9000000);
        let root = b.negate(atom);
        b.register_root(&mut c, 1, 1, root).await.unwrap();
        b.persist(&mut c, 1, 1).await.unwrap();
        sqlx::query("INSERT INTO _gonvex_permission_program_rows VALUES(1,1,'negative-only',?1)")
            .bind(root)
            .execute(&mut c)
            .await
            .unwrap();
        let before: Vec<String> = sqlx::query_scalar(&select_rows())
            .bind(1)
            .bind(1)
            .bind("one")
            .fetch_all(&mut c)
            .await
            .unwrap();
        assert!(before.contains(&"negative-only".into()));
        sqlx::query(
            "INSERT INTO _gonvex_permission_supports VALUES(1,1,'negative','member','one',9000000)",
        )
        .execute(&mut c)
        .await
        .unwrap();
        let after: Vec<String> = sqlx::query_scalar(&select_rows())
            .bind(1)
            .bind(1)
            .bind("one")
            .fetch_all(&mut c)
            .await
            .unwrap();
        assert_eq!(after, vec!["row-00001"]);
    }
}
