//! Shared visibility test corpus: plans × principals × rows, checked against
//! the SQL compiler (run on SQLite), the interpreted evaluator
//! (`visibility::row_matches`) and the compiled class program
//! (`visibility_class`). Any disagreement is a leaked or a missing row.
//!
//! Reuse (workstream 5 and later evaluators): `corpus()` returns the plans,
//! principals (including principal changes) and rows; `sql_visible` runs one
//! plan for one principal in SQL; `check_case` asserts that an evaluator
//! agrees with SQL on every row.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::{json, Value};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::Row;

use crate::visibility::{
    compile_predicate, row_matches, InheritedParent, ResolvedVisibility, VisibilityPlan,
};
use crate::visibility_class::{member_visibility, EvalMemo, MemberHandle, RowView};
use crate::visibility_index::ParentAclIndex;

pub(crate) const TASK_COLUMNS: [&str; 6] = [
    "_id",
    "workspaceId",
    "spotId",
    "createdBy",
    "createdVia",
    "deletedAt",
];
pub(crate) const NOTE_COLUMNS: [&str; 3] = ["_id", "taskId", "authorId"];

pub(crate) struct CorpusPlan {
    pub name: &'static str,
    pub plan: VisibilityPlan,
    /// Whether the compiled evaluator is expected to fall back.
    pub fallback: bool,
}

pub(crate) struct Principal {
    pub name: String,
    pub role: String,
    pub permissions: Value,
    pub sets: BTreeMap<String, BTreeSet<String>>,
}

pub(crate) struct Corpus {
    pub plans: Vec<CorpusPlan>,
    pub principals: Vec<Principal>,
    pub tasks: Vec<Value>,
    pub notes: Vec<Value>,
}

fn plan(value: Value) -> VisibilityPlan {
    serde_json::from_value(value).expect("corpus plan")
}

fn tasks_sets() -> Value {
    json!({
        "workspaces": {"kind": "principal", "table": "workspaceTeams", "select": "workspaceId",
            "joins": [{"table": "memberTeams", "leftColumn": "teamId", "rightColumn": "teamId"}],
            "where": [{"table": "memberTeams", "column": "memberId", "context": "member.id"}]},
        "spots": {"kind": "principal", "table": "memberSpots", "select": "spotId", "joins": [],
            "where": [{"table": "memberSpots", "column": "memberId", "context": "member.id"}]},
        "allSpots": {"kind": "principal", "table": "members", "select": "id", "joins": [],
            "where": [{"table": "members", "column": "id", "context": "member.id"},
                      {"table": "members", "column": "see_all_spots", "value": {"literal": true}}]},
        "personalWorkspaces": {"kind": "principal", "table": "workspaces", "select": "_id", "joins": [],
            "where": [{"table": "workspaces", "column": "type", "value": {"literal": "PERSONAL"}}]},
        "ownedPersonal": {"kind": "principal", "table": "workspaces", "select": "_id", "joins": [],
            "where": [{"table": "workspaces", "column": "type", "value": {"literal": "PERSONAL"}},
                      {"table": "workspaces", "column": "createdBy", "context": "member.id"}]},
        "assigned": {"kind": "grant", "table": "taskUsers", "select": "taskId", "joins": [],
            "where": [{"table": "taskUsers", "column": "userId", "context": "member.id"}]}
    })
}

/// The Whagons task rule, reduced to every operator shape it uses.
pub(crate) fn tasks_plan() -> VisibilityPlan {
    plan(json!({
        "version": 2, "table": "tasks", "key": "_id",
        "permissionTables": ["memberTeams", "workspaceTeams", "memberSpots", "members", "workspaces"],
        "sets": tasks_sets(),
        "where": {"operator": "or", "children": [
            {"operator": "inSet", "column": "_id", "set": "assigned"},
            {"operator": "and", "children": [
                {"operator": "inSet", "column": "workspaceId", "set": "ownedPersonal"}]},
            {"operator": "and", "children": [
                {"operator": "not", "children": [
                    {"operator": "inSet", "column": "workspaceId", "set": "personalWorkspaces"}]},
                {"operator": "or", "children": [
                    {"operator": "permission", "value": "tasks.viewAll"},
                    {"operator": "role", "value": "system_admin"},
                    {"operator": "and", "children": [
                        {"operator": "eq", "column": "spotId", "value": {"literal": null}},
                        {"operator": "inSet", "column": "workspaceId", "set": "workspaces"}]},
                    {"operator": "and", "children": [
                        {"operator": "eq", "column": "spotId", "value": {"literal": ""}},
                        {"operator": "inSet", "column": "workspaceId", "set": "workspaces"}]},
                    {"operator": "and", "children": [
                        {"operator": "inSet", "column": "workspaceId", "set": "workspaces"},
                        {"operator": "inSet", "column": "spotId", "set": "spots"}]},
                    {"operator": "and", "children": [
                        {"operator": "inSet", "column": "workspaceId", "set": "workspaces"},
                        {"operator": "nonEmpty", "set": "allSpots"}]},
                    {"operator": "and", "children": [
                        {"operator": "eqContext", "column": "createdBy", "context": "member.id"},
                        {"operator": "eq", "column": "createdVia", "value": {"literal": "finding"}}]}
                ]}
            ]}
        ]}
    }))
}

pub(crate) fn corpus() -> Corpus {
    let tasks_plan = tasks_plan();
    let tasks = |predicate: Value| {
        plan(json!({
            "version": 2, "table": "tasks", "key": "_id",
            "permissionTables": ["memberTeams", "workspaceTeams", "memberSpots", "members", "workspaces"],
            "sets": tasks_sets(), "where": predicate
        }))
    };
    let notes = |predicate: Value| {
        plan(json!({
            "version": 2, "table": "taskNotes", "key": "_id",
            "permissionTables": ["memberTeams", "workspaceTeams", "memberSpots", "members", "workspaces"],
            "sets": tasks_sets(), "where": predicate
        }))
    };
    let plans = vec![
        CorpusPlan {
            name: "tasks",
            plan: tasks_plan,
            fallback: false,
        },
        CorpusPlan {
            name: "public",
            plan: tasks(json!({"operator": "public"})),
            fallback: false,
        },
        CorpusPlan {
            name: "roles",
            plan: tasks(json!({"operator": "and", "children": [
                {"operator": "or", "children": [
                    {"operator": "role", "value": "admin"},
                    {"operator": "permission", "value": "tasks.viewAll"}]},
                {"operator": "not", "children": [{"operator": "role", "value": "guest"}]}]})),
            fallback: false,
        },
        CorpusPlan {
            name: "owner",
            plan: tasks(json!({"operator": "eqContext", "column": "createdBy", "context": "member.id"})),
            fallback: false,
        },
        CorpusPlan {
            name: "notOwnerOrDeleted",
            plan: tasks(json!({"operator": "and", "children": [
                {"operator": "not", "children": [
                    {"operator": "eqContext", "column": "createdBy", "context": "member.id"}]},
                {"operator": "not", "children": [
                    {"operator": "eq", "column": "deletedAt", "value": {"literal": null}}]},
                {"operator": "not", "children": [{"operator": "not", "children": [
                    {"operator": "inSet", "column": "workspaceId", "set": "workspaces"}]}]}]})),
            fallback: false,
        },
        CorpusPlan {
            name: "emptyNonEmpty",
            plan: tasks(json!({"operator": "or", "children": [
                {"operator": "not", "children": [{"operator": "public"}]},
                {"operator": "nonEmpty", "set": "spots"}]})),
            fallback: false,
        },
        CorpusPlan {
            name: "noteInherits",
            plan: notes(json!({"operator": "or", "children": [
                {"operator": "inherits", "column": "taskId", "table": "tasks", "key": "_id",
                 "onMissing": {"operator": "or", "children": [
                    {"operator": "role", "value": "system_admin"},
                    {"operator": "inSet", "column": "taskId", "set": "assigned"}]}},
                {"operator": "eqContext", "column": "authorId", "context": "member.id"}]})),
            fallback: false,
        },
        CorpusPlan {
            name: "noteOverride",
            plan: notes(json!({"operator": "inherits", "column": "taskId", "table": "tasks", "key": "_id",
                "where": [{"column": "deletedAt", "value": {"literal": null}}],
                "predicate": {"operator": "or", "children": [
                    {"operator": "eqContext", "column": "createdBy", "context": "member.id"},
                    {"operator": "inSet", "column": "_id", "set": "assigned"},
                    {"operator": "and", "children": [
                        {"operator": "inSet", "column": "workspaceId", "set": "workspaces"},
                        {"operator": "not", "children": [
                            {"operator": "eq", "column": "spotId", "value": {"literal": "s2"}}]}]}]}})),
            fallback: false,
        },
    ];

    let set = |values: &[&str]| values.iter().map(|value| value.to_string()).collect();
    let principal = |name: &str,
                     role: &str,
                     permissions: Value,
                     workspaces: &[&str],
                     spots: &[&str],
                     all_spots: bool,
                     owned: &[&str],
                     assigned: &[&str]| Principal {
        name: name.to_owned(),
        role: role.to_owned(),
        permissions,
        sets: BTreeMap::from([
            ("workspaces".to_owned(), set(workspaces)),
            ("spots".to_owned(), set(spots)),
            (
                "allSpots".to_owned(),
                if all_spots { set(&[name]) } else { set(&[]) },
            ),
            ("personalWorkspaces".to_owned(), set(&["p1", "p2"])),
            ("ownedPersonal".to_owned(), set(owned)),
            ("assigned".to_owned(), set(assigned)),
        ]),
    };
    let principals = vec![
        principal("alice", "member", json!({}), &["w1"], &["s1"], false, &["p1"], &["t-w2-s2-bob-web-0"]),
        // Same shared tokens as alice: must share alice's class.
        principal("carol", "member", json!({}), &["w1"], &["s1"], false, &[], &[]),
        principal("bob", "member", json!({}), &["w1", "w2"], &[], true, &["p2"], &[]),
        principal("dave", "system_admin", json!({}), &["w1"], &["s1"], false, &[], &[]),
        principal("erin", "member", json!({"tasks.viewAll": true}), &[], &[], false, &[], &[]),
        principal("frank", "member", json!({}), &[], &[], false, &[], &[]),
        principal("gina", "guest", json!({"tasks.viewAll": "1"}), &["w2"], &["s2"], false, &[], &[]),
        principal("hank", "admin", json!({"tasks.viewAll": false}), &["w2"], &["s1", "s2"], false, &[], &["missing-task"]),
        // Principal changes: alice after joining w2, after a new grant, after
        // losing her spot.
        principal("alice", "member", json!({}), &["w1", "w2"], &["s1"], false, &["p1"], &["t-w2-s2-bob-web-0"]),
        principal("alice", "member", json!({}), &["w1"], &["s1"], false, &["p1"], &["t-w2-s2-bob-web-0", "t-w3-null-null-web-1"]),
        principal("alice", "member", json!({}), &["w1"], &[], false, &["p1"], &[]),
    ];

    let mut task_rows = Vec::new();
    for workspace in [Some("w1"), Some("w2"), Some("w3"), Some("p1"), Some("p2"), None] {
        for spot in [None, Some(""), Some("s1"), Some("s2")] {
            for creator in [Some("alice"), Some("bob"), None] {
                for (via, deleted) in [("finding", 0), ("web", 0), ("web", 1)] {
                    let id = format!(
                        "t-{}-{}-{}-{via}-{deleted}",
                        workspace.unwrap_or("null"),
                        spot.map_or("null", |spot| if spot.is_empty() { "blank" } else { spot }),
                        creator.unwrap_or("null"),
                    );
                    task_rows.push(json!({
                        "_id": id, "workspaceId": workspace, "spotId": spot,
                        "createdBy": creator, "createdVia": via,
                        "deletedAt": if deleted == 1 { json!("2026-10-01") } else { Value::Null },
                    }));
                }
            }
        }
    }
    let mut note_rows = Vec::new();
    let parents = task_rows
        .iter()
        .step_by(7)
        .map(|row| row["_id"].as_str().unwrap().to_owned())
        .chain(["missing-task".to_owned(), "t-gone".to_owned()])
        .map(Some)
        .chain([None]);
    for (index, parent) in parents.enumerate() {
        for author in ["alice", "bob"] {
            note_rows.push(json!({
                "_id": format!("n-{index}-{author}"), "taskId": parent, "authorId": author,
            }));
        }
    }
    Corpus {
        plans,
        principals,
        tasks: task_rows,
        notes: note_rows,
    }
}

/// The principal as the resolver would produce it for `plan`.
pub(crate) fn resolve(
    corpus: &Corpus,
    principal: &Principal,
    plan: &VisibilityPlan,
    tasks_index: &Arc<std::sync::RwLock<ParentAclIndex>>,
) -> ResolvedVisibility {
    let mut resolved = ResolvedVisibility {
        ephemeral_member: false,
        revision: 1,
        direct: BTreeMap::from([
            ("member.id".to_owned(), principal.name.clone()),
            ("account.id".to_owned(), format!("acct-{}", principal.name)),
            ("tenant.id".to_owned(), "t".to_owned()),
        ]),
        role: principal.role.clone(),
        permissions: principal.permissions.clone(),
        sets: principal.sets.clone(),
        fingerprint: String::new(),
        inherited: BTreeMap::new(),
        indexes: BTreeMap::new(),
    };
    if plan.table == "taskNotes" {
        let parent_plan = corpus.plans[0].plan.clone();
        let parent = resolve(corpus, principal, &parent_plan, tasks_index);
        resolved.inherited.insert(
            "tasks".to_owned(),
            InheritedParent {
                plan: Arc::new(parent_plan),
                resolved: Arc::new(parent),
                index: tasks_index.clone(),
            },
        );
    }
    resolved
}

pub(crate) fn tasks_index(corpus: &Corpus) -> Arc<std::sync::RwLock<ParentAclIndex>> {
    Arc::new(std::sync::RwLock::new(ParentAclIndex::from_snapshot(
        1,
        corpus
            .tasks
            .iter()
            .map(|row| (row["_id"].as_str().unwrap().to_owned(), row.clone()))
            .collect(),
    )))
}

/// An in-memory SQLite database holding the corpus rows and members.
pub(crate) async fn database(corpus: &Corpus) -> SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let table = |name: &str, columns: &[&str]| {
        format!(
            "CREATE TABLE \"{name}\" ({})",
            columns
                .iter()
                .map(|column| format!("\"{column}\" TEXT"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    for statement in [
        table("tasks", &TASK_COLUMNS),
        table("taskNotes", &NOTE_COLUMNS),
        "CREATE TABLE members (id TEXT, account_id TEXT, status TEXT, role TEXT, permissions TEXT)"
            .to_owned(),
    ] {
        sqlx::query(&statement).execute(&pool).await.unwrap();
    }
    for (name, columns, rows) in [
        ("tasks", &TASK_COLUMNS[..], &corpus.tasks),
        ("taskNotes", &NOTE_COLUMNS[..], &corpus.notes),
    ] {
        for row in rows {
            let statement = format!(
                "INSERT INTO \"{name}\" VALUES ({})",
                vec!["?"; columns.len()].join(", ")
            );
            let mut query = sqlx::query(&statement);
            for column in columns {
                query = query.bind(row[*column].as_str().map(str::to_owned));
            }
            query.execute(&pool).await.unwrap();
        }
    }
    let mut seen = BTreeSet::new();
    for principal in &corpus.principals {
        if !seen.insert(principal.name.clone()) {
            continue;
        }
        sqlx::query("INSERT INTO members VALUES (?, ?, 'active', ?, ?)")
            .bind(&principal.name)
            .bind(format!("acct-{}", principal.name))
            .bind(&principal.role)
            .bind(principal.permissions.to_string())
            .execute(&pool)
            .await
            .unwrap();
    }
    pool
}

/// Row keys `plan` admits for `resolved`, decided by the SQL compiler.
pub(crate) async fn sql_visible(
    pool: &SqlitePool,
    plan: &VisibilityPlan,
    resolved: &ResolvedVisibility,
) -> BTreeSet<String> {
    let mut parameters = Vec::new();
    let predicate = compile_predicate(plan, resolved, "r", &mut parameters).unwrap();
    let statement = format!(
        "SELECT r.\"_id\" FROM \"{}\" AS r WHERE {predicate}",
        plan.table
    );
    crate::sql_parameters::Parameters::scalars(&parameters)
        .unwrap()
        .bind(sqlx::query(&statement))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|error| panic!("{statement}: {error}"))
        .into_iter()
        .map(|row| row.get::<String, _>(0))
        .collect()
}

/// Assert that `visible` agrees with `expected` on every row; returns the
/// number of rows checked.
pub(crate) fn check_case(
    label: &str,
    rows: &[Value],
    expected: &BTreeSet<String>,
    mut visible: impl FnMut(&Value) -> bool,
) -> usize {
    let mut leaked = Vec::new();
    let mut missing = Vec::new();
    for row in rows {
        let id = row["_id"].as_str().unwrap();
        match (expected.contains(id), visible(row)) {
            (false, true) => leaked.push(id.to_owned()),
            (true, false) => missing.push(id.to_owned()),
            _ => {}
        }
    }
    assert!(
        leaked.is_empty() && missing.is_empty(),
        "{label}: leaked {leaked:?}, missing {missing:?}"
    );
    rows.len()
}

#[tokio::test]
async fn sql_interpreted_and_compiled_evaluators_agree_on_the_corpus() {
    let corpus = corpus();
    let pool = database(&corpus).await;
    let index = tasks_index(&corpus);
    let mut checks = 0;
    for corpus_plan in &corpus.plans {
        let plan = &corpus_plan.plan;
        plan.validate()
            .unwrap_or_else(|error| panic!("{}: {error}", corpus_plan.name));
        let rows = if plan.table == "tasks" {
            &corpus.tasks
        } else {
            &corpus.notes
        };
        // Scope per plan so classes of different plans never mix.
        let scope = format!("corpus-{}", corpus_plan.name);
        let mut memo = EvalMemo::default();
        for (number, principal) in corpus.principals.iter().enumerate() {
            let resolved = resolve(&corpus, principal, plan, &index);
            let expected = sql_visible(&pool, plan, &resolved).await;
            let label = format!("{}/{}#{number}", corpus_plan.name, principal.name);
            checks += check_case(&format!("{label}/interpreted"), rows, &expected, |row| {
                row_matches(plan, &resolved, row)
            });
            let handle = member_visibility(plan, &resolved, &scope);
            let member = match handle {
                MemberHandle::Compiled(member) => member,
                MemberHandle::Fallback => {
                    assert!(corpus_plan.fallback, "{label}: unexpected fallback");
                    continue;
                }
            };
            checks += check_case(&format!("{label}/compiled"), rows, &expected, |row| {
                member.visible(RowView::Value(row))
            });
            checks += check_case(&format!("{label}/compiled-json"), rows, &expected, |row| {
                let bytes = serde_json::to_vec(row).unwrap();
                member.visible(RowView::Json(&bytes))
            });
            checks += check_case(&format!("{label}/compiled-memo"), rows, &expected, |row| {
                member.visible_memo(&mut memo, row)
            });
        }
        assert!(
            memo.stats.class_evaluations <= memo.stats.compiled_checks,
            "{}: {:?}",
            corpus_plan.name,
            memo.stats
        );
    }
    assert!(checks > 50_000, "corpus too small: {checks}");
}

#[test]
fn compiled_evaluator_matches_the_interpreter_on_irregular_rows() {
    let corpus = corpus();
    let index = tasks_index(&corpus);
    let irregular = [
        json!({}),
        json!({"_id": "x"}),
        json!({"_id": "x", "workspaceId": "w1"}),
        json!({"_id": "x", "workspaceId": "w1", "spotId": "s1"}),
        json!({"_id": 5, "workspaceId": 5, "spotId": true, "createdBy": "alice", "createdVia": "finding"}),
        json!({"_id": "t-w2-s2-bob-web-0", "workspaceId": ["w1"], "spotId": {"a": 1}}),
        json!({"_id": "x", "workspaceId": "w1", "spotId": null, "createdBy": 1.5}),
        json!({"_id": "x", "workspaceId": "w\"1", "createdBy": "al\\u0069ce", "createdVia": "finding"}),
        json!({"_id": "n", "taskId": "t-w1-null-alice-web-0", "authorId": null}),
        json!({"_id": "n", "taskId": 7, "authorId": "alice"}),
        Value::Null,
        json!([1, 2]),
        json!("row"),
    ];
    for corpus_plan in &corpus.plans {
        for principal in &corpus.principals {
            let resolved = resolve(&corpus, principal, &corpus_plan.plan, &index);
            let MemberHandle::Compiled(member) =
                member_visibility(&corpus_plan.plan, &resolved, "irregular")
            else {
                continue;
            };
            for row in &irregular {
                let expected = row_matches(&corpus_plan.plan, &resolved, row);
                let bytes = serde_json::to_vec(row).unwrap();
                assert_eq!(
                    member.visible(RowView::Value(row)),
                    expected,
                    "{}/{} value {row}",
                    corpus_plan.name,
                    principal.name
                );
                assert_eq!(
                    member.visible(RowView::Json(&bytes)),
                    expected,
                    "{}/{} json {row}",
                    corpus_plan.name,
                    principal.name
                );
            }
        }
    }
    // Escaped keys and values decode from raw bytes too.
    let plan = &corpus.plans[3].plan; // owner
    let resolved = resolve(&corpus, &corpus.principals[0], plan, &index);
    let MemberHandle::Compiled(member) = member_visibility(plan, &resolved, "irregular") else {
        panic!("owner plan compiles");
    };
    assert!(member.visible(RowView::Json(br#"{"createdBy":"alice"}"#)));
    assert!(!member.visible(RowView::Json(br#"{"createdBy":"bob","x":[{"createdBy":"alice"}]}"#)));
    assert!(!member.visible(RowView::Json(b"not json")));
}

#[test]
fn identical_principals_share_a_class_and_principal_changes_move_members() {
    let corpus = corpus();
    let index = tasks_index(&corpus);
    let plan = &corpus.plans[0].plan;
    let bind = |number: usize| {
        let resolved = resolve(&corpus, &corpus.principals[number], plan, &index);
        match member_visibility(plan, &resolved, "classes") {
            MemberHandle::Compiled(member) => member,
            MemberHandle::Fallback => panic!("tasks plan compiles"),
        }
    };
    let alice = bind(0);
    let carol = bind(1);
    let bob = bind(2);
    assert!(
        Arc::ptr_eq(alice.class(), carol.class()),
        "grants, contexts and owned personal workspaces are member atoms"
    );
    assert!(!Arc::ptr_eq(alice.class(), bob.class()));
    let joined = bind(8);
    assert!(!Arc::ptr_eq(alice.class(), joined.class()), "team change moves alice");
    let granted = bind(9);
    assert!(
        Arc::ptr_eq(alice.class(), granted.class()),
        "a new grant changes the member, not the class"
    );
    let lost_spot = bind(10);
    assert!(Arc::ptr_eq(lost_spot.class(), bind(10).class()));
    assert!(!Arc::ptr_eq(alice.class(), lost_spot.class()));
    let stats = alice.scope().stats();
    assert_eq!(stats.classes, 4, "alice/carol, bob, joined, lost spot: {stats:?}");
    drop((joined, lost_spot));
    assert_eq!(alice.scope().stats().classes, 2, "dead classes leave the registry");
    // The admin class is ungated (sees rows without tokens); team classes are gated.
    assert!(alice.class().gated());
    assert!(!bind(3).class().gated());
}

#[test]
fn candidates_cover_every_member_that_sees_a_row() {
    let corpus = corpus();
    let index = tasks_index(&corpus);
    for corpus_plan in &corpus.plans {
        let plan = &corpus_plan.plan;
        let scope_name = format!("candidates-{}", corpus_plan.name);
        let members = corpus
            .principals
            .iter()
            .filter_map(|principal| {
                let resolved = resolve(&corpus, principal, plan, &index);
                match member_visibility(plan, &resolved, &scope_name) {
                    MemberHandle::Compiled(member) => Some(member),
                    MemberHandle::Fallback => None,
                }
            })
            .collect::<Vec<_>>();
        let Some(first) = members.first() else {
            continue;
        };
        let scope = first.scope().clone();
        for member in &members {
            scope.register(member);
        }
        let rows = if plan.table == "tasks" {
            &corpus.tasks
        } else {
            &corpus.notes
        };
        let mut visited = 0;
        let mut seen = 0;
        for (old, new) in rows.iter().zip(rows.iter().skip(1)) {
            let (old, new) = (scope.decode(RowView::Value(old)), scope.decode(RowView::Value(new)));
            let candidates = scope.candidates(Some(&old), Some(&new));
            visited += candidates.classes.len() + candidates.members.len();
            for member in &members {
                let in_class = candidates
                    .classes
                    .iter()
                    .any(|class| Arc::ptr_eq(class, member.class()));
                let in_members = candidates
                    .members
                    .iter()
                    .any(|candidate| Arc::ptr_eq(candidate, member));
                for row in [&old, &new] {
                    let visible = member.visible_row(row);
                    seen += visible as usize;
                    assert!(
                        !visible || in_members || in_class,
                        "{}: candidates miss a member that sees the row",
                        corpus_plan.name
                    );
                    if in_class && !in_members {
                        assert_eq!(visible, member.class().visible_default(row));
                    }
                }
            }
        }
        assert!(visited > 0 && seen > 0, "{}", corpus_plan.name);
    }
}
