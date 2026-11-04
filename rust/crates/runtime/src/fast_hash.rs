//! Fast hash maps for per-event and per-row work (delivery fan-out, snapshot
//! cache deltas), where std's SipHash showed up at 3-6 % of CPU.
//!
//! `foldhash::fast::RandomState` is seeded randomly per process, so keys an
//! attacker picks (row ids, table names, argument JSON of one tenant) cannot
//! be chosen offline to collide. These maps also live for one event or one
//! open and are bounded by its rows. Do not use them for long-lived maps keyed
//! by untrusted input across tenants.

pub(crate) type FastMap<K, V> = std::collections::HashMap<K, V, foldhash::fast::RandomState>;
pub(crate) type FastSet<K> = std::collections::HashSet<K, foldhash::fast::RandomState>;
