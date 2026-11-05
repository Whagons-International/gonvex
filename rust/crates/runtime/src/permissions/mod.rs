//! Opt-in internal permission primitives. No application or wire API.
//!
//! The internal normalized query/diff path executes indexed SQL and checks
//! readiness in its SQLite snapshot. No replica/WebSocket route selects it yet;
//! application bindings and ordered live delivery remain gated. V2 is default.
#![allow(dead_code)]

pub(crate) mod compile;
mod delivery;
mod diff;
pub(crate) mod model;
pub(crate) mod provider;
pub(crate) mod query;
mod roles;
pub(crate) mod source;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) static LOCAL_TEST_ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());
