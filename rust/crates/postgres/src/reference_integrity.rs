//! Tenant-local constraints. Applications own the catalog and deletion policy;
//! every ordinary SQL writer is governed by the same PostgreSQL triggers/FK.

use crate::{sql_script::split_statements, DatabaseError};

pub(crate) async fn install(connection: &mut sqlx::PgConnection) -> Result<(), DatabaseError> {
    let mut transaction = connection.begin().await?;
    for source in [
        include_str!("reference_integrity/schema.sql"),
        include_str!("reference_integrity/values.sql"),
        include_str!("reference_integrity/tokens.sql"),
        include_str!("reference_integrity/triggers.sql"),
        include_str!("reference_integrity/coverage.sql"),
        include_str!("reference_integrity/declare.sql"),
    ] {
        for statement in split_statements(source)? {
            sqlx::query(&statement).execute(&mut *transaction).await?;
        }
    }
    transaction.commit().await?;
    Ok(())
}

/// Applications opting into coverage must classify schema additions in the
/// same migration, so an unregistered storage field cannot commit unnoticed.
pub(crate) async fn check_coverage(
    connection: &mut sqlx::PgConnection,
) -> Result<(), DatabaseError> {
    sqlx::query("SELECT gonvex_check_reference_coverage(declaration) FROM _gonvex_reference_catalog WHERE declaration ? 'exclusions'")
        .execute(connection).await?;
    Ok(())
}

use sqlx::Connection;
