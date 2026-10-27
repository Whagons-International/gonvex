use std::borrow::Cow;

// SQLx caches by SQL text. Prefix both operations so describe cannot populate
// an execution cache entry; execution also includes the actual bound OIDs.
pub(crate) fn statement<'a>(sql: &'a str, oids: &[u32]) -> Cow<'a, str> {
    let mut key = String::with_capacity(sql.len() + 40 + oids.len() * 4);
    append_execution(&mut key, sql, oids);
    Cow::Owned(key)
}
pub(crate) fn description(sql: &str) -> String {
    let mut key = String::with_capacity("/* gonvex.description.v1 */\n".len() + sql.len());
    append_description(&mut key, sql);
    key
}

// These append bodies are extracted by verification/verus/verify_sql_statement_key.py.
// Allocation, capacity arithmetic, and Cow packaging stay outside that proof.
fn append_decimal(key: &mut String, oid: u32) {
    let mut buffer = ['0'; 10];
    let mut index = 9;
    buffer[index] = (b'0' + (oid % 10) as u8) as char;
    let mut remain = oid / 10;
    while remain > 0 {
        index -= 1;
        buffer[index] = (b'0' + (remain % 10) as u8) as char;
        remain /= 10;
    }
    while index < 10 {
        key.push(buffer[index]);
        index += 1;
    }
}

fn append_execution(key: &mut String, sql: &str, oids: &[u32]) {
    key.push_str("/* gonvex.parameter-types.v1:");
    let mut index = 0;
    while index < oids.len() {
        append_decimal(key, oids[index]);
        key.push(',');
        index += 1;
    }
    key.push_str(" */\n");
    key.push_str(sql);
}

fn append_description(key: &mut String, sql: &str) {
    key.push_str("/* gonvex.description.v1 */\n");
    key.push_str(sql);
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

    // The old formatter is a native compatibility oracle, outside the Verus proof.
    fn old_statement(sql: &str, oids: &[u32]) -> String {
        use std::fmt::Write;
        let mut key = String::with_capacity(sql.len() + 40 + oids.len() * 4);
        key.push_str("/* gonvex.parameter-types.v1:");
        for oid in oids {
            write!(&mut key, "{oid},").expect("writing to a String cannot fail");
        }
        key.push_str(" */\n");
        key.push_str(sql);
        key
    }

    #[test]
    fn proposed_appenders_match_original_bytes() {
        let mut edges = vec![0, 1, u32::MAX];
        let mut power = 10u32;
        loop {
            edges.extend([power - 1, power, power + 1]);
            match power.checked_mul(10) {
                Some(next) => power = next,
                None => break,
            }
        }
        edges.extend([
            25, 16, 20, 701, 3802, 1009, 1005, 1007, 1016, 1021, 1022, 1000, 2951,
        ]);
        let mut cases = vec![vec![], vec![1, 23], vec![12, 3], edges.clone()];
        cases.push(edges.iter().rev().copied().collect());
        for n in edges {
            cases.extend([vec![n], vec![n, n], vec![0, n, u32::MAX, n]]);
        }
        for sql in [
            "",
            "SELECT 1",
            "SELECT $1--comment",
            "SELECT $1--comment\r\n",
            "SELECT 'é漢🙂'\n--終",
            "/* gonvex.parameter-types.v1:0, */\nSELECT 1",
            "/* gonvex.description.v1 */\nSELECT 1",
            "\0\r\n*/ ,",
        ] {
            for oids in &cases {
                assert_eq!(
                    statement(sql, oids).as_bytes(),
                    old_statement(sql, oids).as_bytes()
                );
            }
            assert_eq!(
                description(sql).as_bytes(),
                format!("/* gonvex.description.v1 */\n{sql}").as_bytes()
            );
        }
    }
}
