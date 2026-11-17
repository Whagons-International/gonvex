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

#[cfg(test)]
mod tests {
    /// The vendored serde_json (`rust/vendor/serde_json`, foldhash map)
    /// keeps `preserve_order`: objects iterate and serialize in insertion
    /// order, which row texts and canonical hashes depend on.
    #[test]
    fn serde_json_objects_keep_insertion_order() {
        let text = r#"{"z":1,"_id":"a","m":{"y":true,"b":null},"a":[1,2]}"#;
        let value: serde_json::Value = serde_json::from_str(text).unwrap();
        let keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, ["z", "_id", "m", "a"]);
        assert_eq!(serde_json::to_string(&value).unwrap(), text);
        let mut map = serde_json::Map::with_capacity(4);
        for key in ["d", "c", "b", "a"] {
            map.insert(key.to_owned(), serde_json::Value::Null);
        }
        assert_eq!(map.get("c"), Some(&serde_json::Value::Null));
        assert_eq!(
            serde_json::to_string(&map).unwrap(),
            r#"{"d":null,"c":null,"b":null,"a":null}"#
        );
    }
}
