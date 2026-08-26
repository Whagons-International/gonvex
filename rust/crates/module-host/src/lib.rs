//! Shared module-host transport and implementation.
//!
//! The replacement Rust server imports the same framing and message types as
//! the host. This prevents drift in the local TypeScript execution protocol.

pub mod artifact;
pub mod framing;
pub mod protocol;
pub mod session;
