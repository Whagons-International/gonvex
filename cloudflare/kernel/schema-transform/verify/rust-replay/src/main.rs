//! gonvex-a01-replay <tenant.db> <writes.json> <out.json>
//!
//! writes.json: [{"id": "...", "statements": ["...", ...]}, ...]. Each write
//! runs in its own transaction on a writer connection set up as the runtime
//! sets one up (foreign keys on, REGEXP, capture hook and functions). After
//! the statements, `feed::finalize` flushes record images and assigns a
//! revision; then COMMIT. Any error rolls the write back. Output per write:
//! ok, error text, and the change rows of its revision in ordinal order.
use gonvex_store::{capture, feed};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, Connection, Row, SqliteConnection};

#[derive(Deserialize)]
struct Write {
    id: String,
    statements: Vec<String>,
}

#[derive(Serialize)]
struct Change {
    table_name: String,
    row_id: String,
    operation: String,
    old_value: Option<String>,
    new_value: Option<String>,
    changed_columns: String,
}

#[derive(Serialize)]
struct Outcome {
    id: String,
    ok: bool,
    error: Option<String>,
    changes: Vec<Change>,
}

async fn run(connection: &mut SqliteConnection, write: &Write) -> Result<Vec<Change>, String> {
    for statement in &write.statements {
        sqlx::raw_sql(statement).execute(&mut *connection).await.map_err(|e| e.to_string())?;
    }
    let notice = feed::finalize(connection, &feed::Provenance::default()).await.map_err(|e| e.to_string())?;
    let Some(notice) = notice else { return Ok(Vec::new()) };
    let rows = sqlx::query(
        "SELECT table_name, row_id, operation, old_value, new_value, changed_columns FROM _gonvex_sync_changes WHERE revision = ? ORDER BY ordinal",
    )
    .bind(notice.revision as i64)
    .fetch_all(&mut *connection)
    .await
    .map_err(|e| e.to_string())?;
    Ok(rows
        .into_iter()
        .map(|r| Change {
            table_name: r.get(0),
            row_id: r.get(1),
            operation: r.get(2),
            old_value: r.get(3),
            new_value: r.get(4),
            changed_columns: r.get(5),
        })
        .collect())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        eprintln!("usage: gonvex-a01-replay <tenant.db> <writes.json> <out.json>");
        std::process::exit(2);
    }
    let writes: Vec<Write> = serde_json::from_str(&std::fs::read_to_string(&args[2]).expect("read writes")).expect("parse writes");
    let options = SqliteConnectOptions::new().filename(&args[1]).foreign_keys(true).with_regexp().disable_statement_logging();
    let mut connection = SqliteConnection::connect_with(&options).await.expect("open tenant file");
    capture::install(&mut connection).await.expect("install capture");
    let mut out = Vec::with_capacity(writes.len());
    for write in &writes {
        sqlx::raw_sql("BEGIN IMMEDIATE").execute(&mut connection).await.expect("begin");
        let result = run(&mut connection, write).await;
        let outcome = match result {
            Ok(changes) => match sqlx::raw_sql("COMMIT").execute(&mut connection).await {
                Ok(_) => Outcome { id: write.id.clone(), ok: true, error: None, changes },
                Err(e) => {
                    let _ = sqlx::raw_sql("ROLLBACK").execute(&mut connection).await;
                    Outcome { id: write.id.clone(), ok: false, error: Some(e.to_string()), changes: Vec::new() }
                }
            },
            Err(error) => {
                sqlx::raw_sql("ROLLBACK").execute(&mut connection).await.expect("rollback");
                Outcome { id: write.id.clone(), ok: false, error: Some(error), changes: Vec::new() }
            }
        };
        out.push(outcome);
    }
    std::fs::write(&args[3], serde_json::to_string(&out).expect("encode")).expect("write output");
}
