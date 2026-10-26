//! Function-kind policy shared by module execution adapters.
//!
//! The host grant and the structural policy both restrict every capability.

use crate::{Capabilities, FunctionKind};

/// The maximum capability set for a function kind, before host grants apply.
#[inline]
pub fn structural_capabilities(kind: &FunctionKind) -> Capabilities {
    match kind {
        FunctionKind::Query => Capabilities {
            db_read: true,
            db_write: false,
            action_outbox: false,
            action_tools: false,
            scheduler: false,
            network: false,
            storage: false,
            sandbox: false,
            secrets: false,
            functions: false,
        },
        FunctionKind::Reducer => Capabilities {
            db_read: true,
            db_write: true,
            action_outbox: true,
            action_tools: false,
            scheduler: true,
            network: false,
            storage: true,
            sandbox: false,
            secrets: false,
            functions: false,
        },
        FunctionKind::Action => Capabilities {
            db_read: false,
            db_write: false,
            action_outbox: false,
            action_tools: true,
            scheduler: true,
            network: true,
            storage: true,
            sandbox: true,
            secrets: true,
            functions: true,
        },
    }
}

/// Restrict an invocation to the intersection of its kind and host grant.
#[inline]
pub fn effective_capabilities(kind: &FunctionKind, granted: &Capabilities) -> Capabilities {
    let structural = structural_capabilities(kind);
    Capabilities {
        db_read: structural.db_read && granted.db_read,
        db_write: structural.db_write && granted.db_write,
        action_outbox: structural.action_outbox && granted.action_outbox,
        action_tools: structural.action_tools && granted.action_tools,
        scheduler: structural.scheduler && granted.scheduler,
        network: structural.network && granted.network,
        storage: structural.storage && granted.storage,
        sandbox: structural.sandbox && granted.sandbox,
        secrets: structural.secrets && granted.secrets,
        functions: structural.functions && granted.functions,
    }
}
