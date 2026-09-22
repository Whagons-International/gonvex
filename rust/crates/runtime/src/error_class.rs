//! Classify Reducer failures at their source so clients can tell a permanent
//! rejection from infrastructure trouble without parsing prose.
//!
//! The rule of thumb: when the tenant transaction rolled back because of
//! something outside the intent (database connectivity, pool exhaustion,
//! deadlines, a module-host restart or deploy), the same idempotency key may be
//! retried and the class is `transient`. When the Reducer itself, its argument
//! schema, authorization or a data conflict refused the intent, retrying cannot
//! help and the class is `rejected`.

use std::sync::atomic::{AtomicBool, Ordering};

use gonvex_module_host::protocol::codes;
use gonvex_postgres::DatabaseError;
pub use gonvex_protocol::ReducerErrorClass;

use crate::control::ControlError;
use crate::execution::ExecutionError;
use crate::module_host::ModuleHostError;

/// True when a SQL failure says nothing about the intent itself.
pub fn sql_error_is_transient(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::BeginFailed => true,
        sqlx::Error::Database(database) => database
            .code()
            .is_some_and(|code| sqlstate_is_transient(code.as_ref())),
        _ => false,
    }
}

/// PostgreSQL SQLSTATE codes whose failure is about the server, not the data.
pub fn sqlstate_is_transient(code: &str) -> bool {
    // Class 08: connection exceptions. Class 53: insufficient resources
    // (disk full, out of memory, too many connections). Class 58: system I/O.
    if code.starts_with("08") || code.starts_with("53") || code.starts_with("58") {
        return true;
    }
    matches!(
        code,
        // serialization_failure, deadlock_detected
        "40001" | "40P01"
        // query_canceled (statement_timeout), lock_not_available (lock_timeout)
        | "57014" | "55P03"
        // admin_shutdown, crash_shutdown, cannot_connect_now
        | "57P01" | "57P02" | "57P03"
        // read_only_sql_transaction: a failover promoted/demoted the primary
        | "25006"
    )
}

pub fn database_error_class(error: &DatabaseError) -> ReducerErrorClass {
    match error {
        DatabaseError::AdmissionTimeout => ReducerErrorClass::Transient,
        DatabaseError::Sql(sql) if sql_error_is_transient(sql) => ReducerErrorClass::Transient,
        DatabaseError::MissingProject
        | DatabaseError::MissingTenant
        | DatabaseError::MissingAccount
        | DatabaseError::InvalidSession
        | DatabaseError::SessionProjectMismatch => ReducerErrorClass::Unauthenticated,
        // Routing and provisioning are server configuration. They never mean
        // that the user's intent is wrong, so never let them delete it.
        DatabaseError::ProjectNotFound(_)
        | DatabaseError::TenantNotFound { .. }
        | DatabaseError::TenantDatabaseMissing { .. } => ReducerErrorClass::Transient,
        // The account lost access: a durable, user-visible refusal.
        DatabaseError::MemberNotFound(_) | DatabaseError::NoTenantMembership => {
            ReducerErrorClass::Rejected
        }
        DatabaseError::IdempotencyPathMismatch { .. } | DatabaseError::Sql(_) => {
            ReducerErrorClass::Rejected
        }
    }
}

pub fn module_host_error_class(error: &ModuleHostError) -> ReducerErrorClass {
    match error {
        ModuleHostError::Remote {
            code, retryable, ..
        } => {
            if *retryable {
                return ReducerErrorClass::Transient;
            }
            match code.as_str() {
                // The module host drained, restarted, was redeployed, hit the
                // invocation deadline, or lost its connection mid-call.
                codes::CANCELLED
                | codes::SHUTTING_DOWN
                | codes::BUDGET_EXCEEDED
                | codes::MODULE_NOT_LOADED
                | codes::UNKNOWN_GENERATION
                | codes::GENERATION_CONFLICT
                | codes::MODULE_LOAD_FAILED
                | codes::INVALID_ARTIFACT
                | codes::ARTIFACT_HASH_MISMATCH
                | codes::HOST_CALL_FAILED => ReducerErrorClass::Transient,
                // Application throws and schema validation are about the intent.
                _ => ReducerErrorClass::Rejected,
            }
        }
        // Every other variant is the host process or its transport failing.
        _ => ReducerErrorClass::Transient,
    }
}

impl ExecutionError {
    pub fn reducer_error_class(&self) -> ReducerErrorClass {
        match self {
            ExecutionError::Transient(_) => ReducerErrorClass::Transient,
            ExecutionError::ModuleMissing(_) => ReducerErrorClass::Transient,
            ExecutionError::Database(error) => database_error_class(error),
            ExecutionError::ModuleHost(error) => module_host_error_class(error),
            ExecutionError::StaleCatalog { .. }
            | ExecutionError::StaleReducerArtifact { .. }
            | ExecutionError::ClientUpdateRequired { .. } => ReducerErrorClass::UpdateRequired,
            ExecutionError::FunctionMissing(_)
            | ExecutionError::WrongKind { .. }
            | ExecutionError::InternalFunction(_)
            | ExecutionError::HostCall(_)
            | ExecutionError::CapabilityUnavailable { .. }
            | ExecutionError::NotInteractive(_)
            | ExecutionError::InvalidArguments { .. }
            | ExecutionError::InvalidResult { .. }
            | ExecutionError::InvocationDepth
            | ExecutionError::RecursiveAction(_) => ReducerErrorClass::Rejected,
        }
    }
}

impl ControlError {
    pub fn reducer_error_class(&self) -> ReducerErrorClass {
        match self {
            ControlError::AuthenticationRequired => ReducerErrorClass::Unauthenticated,
            ControlError::Database(error) => database_error_class(error),
            ControlError::Sql(error) if sql_error_is_transient(error) => {
                ReducerErrorClass::Transient
            }
            _ => ReducerErrorClass::Rejected,
        }
    }
}

/// Records whether any database work inside one invocation failed for a
/// transient reason. Host-call errors reach the module as plain strings, and
/// an application may rethrow them as its own error; this flag keeps the
/// infrastructure cause visible to the classification after the module
/// returns.
#[derive(Debug, Default)]
pub struct TransientFault(AtomicBool);

impl TransientFault {
    pub fn record(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn observed(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Stringify a SQL failure for the module, remembering transient causes.
    pub fn sql(&self, error: sqlx::Error) -> String {
        if sql_error_is_transient(&error) {
            self.record();
        }
        error.to_string()
    }

    /// Stringify a routed database failure, remembering transient causes.
    pub fn database(&self, error: DatabaseError) -> String {
        if database_error_class(&error) == ReducerErrorClass::Transient {
            self.record();
        }
        error.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_connection_and_resource_failures_as_transient() {
        for code in [
            "08000", "08006", "08P01", "40001", "40P01", "53300", "53200", "57014", "57P01",
            "57P03", "55P03", "25006", "58030",
        ] {
            assert!(sqlstate_is_transient(code), "{code} should be transient");
        }
        for code in ["23505", "23503", "22P02", "42501", "P0001", "42P01"] {
            assert!(!sqlstate_is_transient(code), "{code} should be rejected");
        }
        assert!(sql_error_is_transient(&sqlx::Error::PoolTimedOut));
        assert!(sql_error_is_transient(&sqlx::Error::PoolClosed));
        assert!(sql_error_is_transient(&sqlx::Error::Io(
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset",)
        )));
        assert!(!sql_error_is_transient(&sqlx::Error::RowNotFound));
    }

    #[test]
    fn classifies_database_errors() {
        assert_eq!(
            database_error_class(&DatabaseError::AdmissionTimeout),
            ReducerErrorClass::Transient
        );
        assert_eq!(
            database_error_class(&DatabaseError::Sql(sqlx::Error::PoolTimedOut)),
            ReducerErrorClass::Transient
        );
        assert_eq!(
            database_error_class(&DatabaseError::InvalidSession),
            ReducerErrorClass::Unauthenticated
        );
        assert_eq!(
            database_error_class(&DatabaseError::NoTenantMembership),
            ReducerErrorClass::Rejected
        );
        assert_eq!(
            database_error_class(&DatabaseError::IdempotencyPathMismatch {
                key: "k".to_owned(),
                stored_path: "a".to_owned(),
                requested_path: "b".to_owned(),
            }),
            ReducerErrorClass::Rejected
        );
        assert_eq!(
            database_error_class(&DatabaseError::Sql(sqlx::Error::RowNotFound)),
            ReducerErrorClass::Rejected
        );
    }

    #[test]
    fn classifies_module_host_failures() {
        let remote = |code: &str, retryable: bool| ModuleHostError::Remote {
            code: code.to_owned(),
            message: "m".to_owned(),
            retryable,
        };
        assert_eq!(
            module_host_error_class(&remote(codes::EXECUTION_FAILED, false)),
            ReducerErrorClass::Rejected
        );
        assert_eq!(
            module_host_error_class(&remote(codes::INVALID_ARGS, false)),
            ReducerErrorClass::Rejected
        );
        assert_eq!(
            module_host_error_class(&remote(codes::BUDGET_EXCEEDED, false)),
            ReducerErrorClass::Transient
        );
        assert_eq!(
            module_host_error_class(&remote(codes::CANCELLED, false)),
            ReducerErrorClass::Transient
        );
        assert_eq!(
            module_host_error_class(&remote("future_code", true)),
            ReducerErrorClass::Transient
        );
        assert_eq!(
            module_host_error_class(&ModuleHostError::NotReady),
            ReducerErrorClass::Transient
        );
        assert_eq!(
            module_host_error_class(&ModuleHostError::Timeout),
            ReducerErrorClass::Transient
        );
    }

    #[test]
    fn classifies_execution_errors() {
        assert_eq!(
            ExecutionError::StaleReducerArtifact {
                expected: "a".to_owned(),
                active: "b".to_owned()
            }
            .reducer_error_class(),
            ReducerErrorClass::UpdateRequired
        );
        assert_eq!(
            ExecutionError::ClientUpdateRequired {
                expected: 1,
                active: 2
            }
            .reducer_error_class(),
            ReducerErrorClass::UpdateRequired
        );
        assert_eq!(
            ExecutionError::InvalidArguments {
                path: "p".to_owned(),
                message: "m".to_owned()
            }
            .reducer_error_class(),
            ReducerErrorClass::Rejected
        );
        assert_eq!(
            ExecutionError::ModuleMissing("project".to_owned()).reducer_error_class(),
            ReducerErrorClass::Transient
        );
        // An application error raised after a transient host-call failure
        // keeps the application's message but the infrastructure class.
        let wrapped = ExecutionError::Transient(Box::new(ExecutionError::ModuleHost(
            ModuleHostError::Remote {
                code: codes::EXECUTION_FAILED.to_owned(),
                message: "Task not found".to_owned(),
                retryable: false,
            },
        )));
        assert_eq!(wrapped.reducer_error_class(), ReducerErrorClass::Transient);
        assert!(wrapped.to_string().contains("Task not found"));
        assert_eq!(
            ExecutionError::HostCall("commit failed".to_owned()).reducer_error_class(),
            ReducerErrorClass::Rejected
        );
    }

    #[test]
    fn classifies_control_errors() {
        assert_eq!(
            ControlError::AuthenticationRequired.reducer_error_class(),
            ReducerErrorClass::Unauthenticated
        );
        assert_eq!(
            ControlError::Sql(sqlx::Error::PoolTimedOut).reducer_error_class(),
            ReducerErrorClass::Transient
        );
        assert_eq!(
            ControlError::TenantAdminRequired.reducer_error_class(),
            ReducerErrorClass::Rejected
        );
    }

    #[test]
    fn transient_fault_remembers_only_transient_causes() {
        let fault = TransientFault::default();
        let _ = fault.sql(sqlx::Error::RowNotFound);
        assert!(!fault.observed());
        let _ = fault.database(DatabaseError::AdmissionTimeout);
        assert!(fault.observed());
    }
}
