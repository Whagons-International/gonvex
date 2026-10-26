//! Keep invocation transaction ownership outside raw module SQL.
//!
//! Statement parsing and read-only Query admission remain in host_calls.
//! Savepoint rollback stays inside the existing outer transaction.

fn invocation_head_tokens(mut source: &str) -> [String; 3] {
    let mut tokens = [String::new(), String::new(), String::new()];
    for (slot, token) in tokens.iter_mut().enumerate() {
        loop {
            source = source.trim_start_matches(|ch: char| ch.is_whitespace() || ch == ';');
            if let Some(comment) = source.strip_prefix("--") {
                source = comment
                    .find(['\r', '\n'])
                    .map(|index| &comment[index..])
                    .unwrap_or("");
            } else if let Some(comment) = source.strip_prefix("/*") {
                let bytes = comment.as_bytes();
                let mut depth = 1usize;
                let mut index = 0usize;
                while index < bytes.len() && depth > 0 {
                    if bytes.get(index..index + 2) == Some(b"/*") {
                        depth += 1;
                        index += 2;
                    } else if bytes.get(index..index + 2) == Some(b"*/") {
                        depth -= 1;
                        index += 2;
                    } else {
                        index += 1;
                    }
                }
                if depth > 0 {
                    return tokens;
                }
                source = &comment[index..];
            } else {
                break;
            }
        }
        let size = source
            .char_indices()
            .find_map(|(index, ch)| {
                (!(ch.is_ascii_alphanumeric() || ch == '_' || ch == '$' || !ch.is_ascii()))
                    .then_some(index)
            })
            .unwrap_or(source.len());
        if size == 0 {
            // A typed prepared statement named `transaction` has a type list.
            if slot == 2 && source.starts_with('(') {
                *token = "(".to_owned();
            }
            break;
        }
        *token = source[..size].to_ascii_lowercase();
        source = &source[size..];
    }
    tokens
}

pub(crate) fn require_invocation_statement(statement: &str) -> Result<(), String> {
    let [first, second, third] = invocation_head_tokens(statement);
    let control = match first.as_str() {
        "commit" | "end" | "abort" | "begin" | "start" => true,
        "rollback" => {
            !(second == "to" || ((second == "work" || second == "transaction") && third == "to"))
        }
        "prepare" => second == "transaction" && third != "as" && third != "(",
        _ => false,
    };
    if control {
        Err("transaction boundaries belong to the invocation; raw transaction control is not allowed".to_owned())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invocation_transaction_boundaries() {
        for statement in [
            ";COMMIT",
            ";;/* prefix */ END",
            ";--prefix\rCOMMIT",
            "COMMIT",
            "COMMIT WORK",
            "COMMIT AND CHAIN",
            "END",
            "END WORK",
            "END TRANSACTION",
            "ABORT",
            "ROLLBACK",
            "ROLLBACK WORK",
            "ROLLBACK AND CHAIN",
            "BEGIN",
            "START TRANSACTION",
            "PREPARE TRANSACTION 'command'",
            "/* prefix */ COMMIT",
            "-- prefix\nEND WORK",
            "-- prefix\rCOMMIT",
            "/* outer /*inner*/ SELECT */ COMMIT",
            "\u{a0}cOmMiT;",
            "ROLLBACK /* reason */ WORK AND CHAIN",
        ] {
            assert!(
                require_invocation_statement(statement).is_err(),
                "{statement:?}"
            );
        }
    }

    #[test]
    fn preserves_savepoints_prepared_queries_and_keyword_boundaries() {
        for statement in [
            "SELECT 'COMMIT'",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "SAVEPOINT app",
            "RELEASE SAVEPOINT app",
            "ROLLBACK TO app",
            "ROLLBACK WORK TO SAVEPOINT app",
            "ROLLBACK TRANSACTION TO app",
            "ROLLBACK /* reason */ TO app",
            "PREPARE app AS SELECT 1",
            "PREPARE transaction AS SELECT 1",
            "PREPARE transaction(integer) AS SELECT 1",
            "PREPARE transaction /* reason */ (integer) AS SELECT 1",
            "EXECUTE app",
            "DEALLOCATE app",
            "COMMITMENTS",
            "commit$app",
            "/* outer /*inner*/ COMMIT */ SELECT 1",
        ] {
            assert!(
                require_invocation_statement(statement).is_ok(),
                "{statement:?}"
            );
        }
    }
}
