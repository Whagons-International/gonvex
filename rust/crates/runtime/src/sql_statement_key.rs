use std::borrow::Cow;
use std::fmt::Write;

// SQLx caches by SQL text. Prefix both operations so describe cannot populate
// an execution cache entry; execution also includes the actual bound OIDs.
pub(crate) fn statement<'a>(sql: &'a str, oids: &[u32]) -> Cow<'a, str> {
    let mut key = String::with_capacity(sql.len() + 40 + oids.len() * 4);
    key.push_str("/* gonvex.parameter-types.v1:");
    for oid in oids {
        write!(&mut key, "{oid},").expect("writing to a String cannot fail");
    }
    key.push_str(" */\n");
    key.push_str(sql);
    Cow::Owned(key)
}
pub(crate) fn description(sql: &str) -> String {
    format!("/* gonvex.description.v1 */\n{sql}")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn no_parameters_gets_execution_namespace() {
        assert_eq!(
            statement("SELECT 1", &[]),
            "/* gonvex.parameter-types.v1: */\nSELECT 1"
        );
    }
    #[test]
    fn types_partition_cache() {
        assert_ne!(statement("SELECT $1", &[20]), statement("SELECT $1", &[25]));
    }
    #[test]
    fn order_is_part_of_key() {
        assert_ne!(
            statement("SELECT $1,$2", &[20, 25]),
            statement("SELECT $1,$2", &[25, 20])
        );
    }
    #[test]
    fn framing_is_unambiguous() {
        assert_ne!(
            statement("SELECT $1,$2", &[1, 23]),
            statement("SELECT $1,$2", &[12, 3])
        );
    }
    #[test]
    fn trailing_line_comment_does_not_change_suffix() {
        assert_eq!(
            statement("SELECT $1--comment", &[20]),
            "/* gonvex.parameter-types.v1:20, */\nSELECT $1--comment"
        );
    }
    #[test]
    fn description_namespace_cannot_alias_execution() {
        assert_ne!(description("SELECT $1"), statement("SELECT $1", &[20]));
        assert_ne!(
            description(statement("SELECT $1", &[20]).as_ref()),
            statement("SELECT $1", &[20])
        );
    }
}
