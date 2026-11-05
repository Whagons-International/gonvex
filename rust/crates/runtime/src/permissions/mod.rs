//! Opt-in internal permission primitives. No application or wire API.
//!
//! The internal normalized query/diff path executes indexed SQL and checks
//! readiness in its SQLite snapshot. No replica/WebSocket route selects it yet;
//! application bindings and ordered live delivery remain gated. V2 is default.
#![allow(dead_code)]

mod compile;
mod delivery;
mod diff;
mod model;
mod query;
mod roles;

#[cfg(test)]
mod tests;
