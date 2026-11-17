//! Runs each statement of `WRITES` (one per line) in its own tenant write
//! transaction on `DB` and prints whether it committed. Used to compare a
//! tenant file whose triggers are writer-only with an unchanged copy.
use gonvex_store::{begin_transaction, PoolLimits, PoolRegistry};

#[tokio::main]
async fn main() {
    let db = std::env::var("DB").expect("DB");
    let writes = std::fs::read_to_string(std::env::var("WRITES").expect("WRITES")).unwrap();
    let pools = PoolRegistry::new(PoolLimits::default());
    let url = format!("sqlite://{db}");
    for statement in writes.lines().filter(|line| !line.trim().is_empty()) {
        let mut transaction = begin_transaction(&pools, &url, false).await.unwrap();
        let result = sqlx::query(statement)
            .execute(&mut **transaction.transaction())
            .await;
        match result {
            Ok(done) => match transaction.commit().await {
                Ok(()) => println!("ok rows={} :: {statement}", done.rows_affected()),
                Err(error) => println!("commit error {error} :: {statement}"),
            },
            Err(error) => println!("error {error} :: {statement}"),
        }
    }
    pools.close().await;
}
