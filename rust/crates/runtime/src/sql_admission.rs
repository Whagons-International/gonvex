//! Admission for module-provided SQL run on the invocation transaction.
//!
//! SQLx runs every statement in a SQLite query string, so admitting exactly
//! one statement is a security boundary. The scanner below follows SQLite's
//! own tokenizer (`tokenize.c`) for everything that can hide a `;`: string
//! literals, quoted identifiers (`"…"`, `` `…` ``, `[…]`), blob literals,
//! TCL-style variables (`$name(…)`), `--` comments (ended only by `\n`) and
//! non-nesting `/* … */` comments. A disagreement with SQLite may only ever
//! see more statements than SQLite does, never fewer.
//!
//! Transaction boundaries belong to the invocation; savepoints stay inside
//! the existing outer transaction. Statements that reach outside the tenant
//! database file or change connection state (ATTACH, DETACH, PRAGMA, VACUUM)
//! are refused.

#[derive(Debug, PartialEq, Eq)]
enum Token {
    Word(String),
    Other(char),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LexError {
    Nul,
    Unterminated,
}

pub(crate) struct AdmittedStatement {
    /// The first token of the statement, lowercased (empty when it is not a
    /// word, for example a quoted identifier).
    pub(crate) first_word: String,
}

fn id_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || byte >= 0x80
}

fn space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\x0c' | b'\r')
}

/// Returns the head tokens (up to three) of every non-empty statement.
fn statements(source: &str) -> Result<Vec<Vec<Token>>, LexError> {
    let bytes = source.as_bytes();
    if bytes.contains(&0) {
        return Err(LexError::Nul);
    }
    let mut statements = Vec::new();
    let mut current: Option<Vec<Token>> = None;
    let mut index = 0;
    let push = |current: &mut Option<Vec<Token>>, token: Token| {
        let tokens = current.get_or_insert_with(Vec::new);
        if tokens.len() < 3 {
            tokens.push(token);
        }
    };
    // Quoted run that ends at `close`; a doubled `close` is an escape when
    // `doubling` is set.
    let quoted = |start: usize, close: u8, doubling: bool| -> Result<usize, LexError> {
        let mut index = start;
        while index < bytes.len() {
            if bytes[index] == close {
                if doubling && bytes.get(index + 1) == Some(&close) {
                    index += 2;
                    continue;
                }
                return Ok(index + 1);
            }
            index += 1;
        }
        Err(LexError::Unterminated)
    };
    while index < bytes.len() {
        let byte = bytes[index];
        match byte {
            _ if space(byte) => index += 1,
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index = bytes[index..]
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(bytes.len(), |offset| index + offset + 1);
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = source[index + 2..]
                    .find("*/")
                    .map(|offset| index + 2 + offset + 2)
                    .ok_or(LexError::Unterminated)?;
            }
            b';' => {
                if let Some(tokens) = current.take() {
                    statements.push(tokens);
                }
                index += 1;
            }
            b'\'' => {
                push(&mut current, Token::Other('\''));
                index = quoted(index + 1, b'\'', true)?;
            }
            b'"' | b'`' => {
                push(&mut current, Token::Other(byte as char));
                index = quoted(index + 1, byte, true)?;
            }
            b'[' => {
                push(&mut current, Token::Other('['));
                index = quoted(index + 1, b']', false)?;
            }
            b'x' | b'X' if bytes.get(index + 1) == Some(&b'\'') => {
                push(&mut current, Token::Other('\''));
                index = quoted(index + 2, b'\'', false)?;
            }
            b'$' | b'@' | b':' | b'#' => {
                // SQLite variables, including TCL-style `$a::b(any)` names.
                let mut end = index + 1;
                let mut named = 0usize;
                while end < bytes.len() {
                    let next = bytes[end];
                    if id_char(next) {
                        named += 1;
                        end += 1;
                    } else if next == b'(' && named > 0 {
                        end += 1;
                        while end < bytes.len() && !space(bytes[end]) && bytes[end] != b')' {
                            end += 1;
                        }
                        if bytes.get(end) == Some(&b')') {
                            end += 1;
                        }
                        break;
                    } else if next == b':' && bytes.get(end + 1) == Some(&b':') {
                        end += 2;
                    } else {
                        break;
                    }
                }
                push(&mut current, Token::Other(byte as char));
                index = end.max(index + 1);
            }
            _ if id_char(byte) => {
                let end = bytes[index..]
                    .iter()
                    .position(|byte| !id_char(*byte))
                    .map_or(bytes.len(), |offset| index + offset);
                push(
                    &mut current,
                    Token::Word(source[index..end].to_ascii_lowercase()),
                );
                index = end;
            }
            _ => {
                let character = source[index..].chars().next().unwrap_or('?');
                push(&mut current, Token::Other(character));
                index += character.len_utf8();
            }
        }
    }
    if let Some(tokens) = current {
        statements.push(tokens);
    }
    Ok(statements)
}

fn word(tokens: &[Token], index: usize) -> &str {
    match tokens.get(index) {
        Some(Token::Word(word)) => word,
        Some(Token::Other('(')) => "(",
        _ => "",
    }
}

fn require_head(tokens: &[Token]) -> Result<(), String> {
    let (first, second, third) = (word(tokens, 0), word(tokens, 1), word(tokens, 2));
    let control = match first {
        "commit" | "end" | "abort" | "begin" | "start" => true,
        "rollback" => {
            !(second == "to" || ((second == "work" || second == "transaction") && third == "to"))
        }
        "prepare" => second == "transaction" && third != "as" && third != "(",
        _ => false,
    };
    if control {
        return Err("transaction boundaries belong to the invocation; raw transaction control is not allowed".to_owned());
    }
    if matches!(first, "attach" | "detach" | "pragma" | "vacuum") {
        return Err(format!(
            "{} statements are not allowed in module SQL",
            first.to_ascii_uppercase()
        ));
    }
    Ok(())
}

/// Admits exactly one statement that does not control the invocation
/// transaction or reach outside the tenant database.
pub(crate) fn admit_statement(statement: &str) -> Result<AdmittedStatement, String> {
    let statement = statement.trim();
    if statement.is_empty() {
        return Err("a database statement is required".to_owned());
    }
    let statements = statements(statement).map_err(|error| match error {
        LexError::Nul => "a database statement may not contain NUL".to_owned(),
        LexError::Unterminated => {
            "a database statement contains an unterminated literal or comment".to_owned()
        }
    })?;
    if statements.len() > 1 {
        return Err("a database statement may not contain more than one statement".to_owned());
    }
    let tokens = statements.into_iter().next().unwrap_or_default();
    require_head(&tokens)?;
    Ok(AdmittedStatement {
        first_word: word(&tokens, 0).to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invocation_transaction_boundaries() {
        for statement in [
            ";COMMIT",
            ";;/* prefix */ END",
            ";--prefix\nCOMMIT",
            "COMMIT",
            "COMMIT TRANSACTION",
            "END",
            "END TRANSACTION",
            "ABORT",
            "ROLLBACK",
            "ROLLBACK TRANSACTION",
            "BEGIN",
            "BEGIN IMMEDIATE",
            "BEGIN EXCLUSIVE TRANSACTION",
            "START TRANSACTION",
            "PREPARE TRANSACTION 'command'",
            "/* prefix */ COMMIT",
            "-- prefix\nEND",
            "cOmMiT;",
            "ROLLBACK /* reason */ TRANSACTION",
        ] {
            assert!(admit_statement(statement).is_err(), "{statement:?}");
        }
    }

    #[test]
    fn preserves_savepoints_and_keyword_boundaries() {
        for statement in [
            "SELECT 'COMMIT'",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "SAVEPOINT app",
            "RELEASE SAVEPOINT app",
            "RELEASE app",
            "ROLLBACK TO app",
            "ROLLBACK TO SAVEPOINT app",
            "ROLLBACK TRANSACTION TO app",
            "ROLLBACK /* reason */ TO app",
            "COMMITMENTS",
            "commit$app",
            "SELECT 1; -- trailing\n",
            "-- note; with a semicolon\nSELECT 1",
        ] {
            assert!(admit_statement(statement).is_ok(), "{statement:?}");
        }
    }

    #[test]
    fn refuses_statements_that_reach_outside_the_tenant_database() {
        for statement in [
            "ATTACH DATABASE '/tmp/other.db' AS other",
            "attach '/tmp/other.db' as other",
            "DETACH other",
            "PRAGMA foreign_keys = OFF",
            "PRAGMA journal_mode",
            "VACUUM INTO '/tmp/copy.db'",
        ] {
            assert!(admit_statement(statement).is_err(), "{statement:?}");
        }
        assert!(admit_statement("SELECT * FROM pragma_table_info('tasks')").is_ok());
    }

    #[test]
    fn counts_statements_with_sqlite_lexical_rules() {
        let one = [
            "SELECT 'a'';b' AS n",
            "SELECT \"a;b\" FROM t",
            "SELECT `a;b` FROM t",
            "SELECT [a;b] FROM t",
            "SELECT x'00' AS n",
            "SELECT 1 /* ; */ AS n",
            // SQLite comments do not nest: the first */ ends this one.
            "/* outer /* inner */ SELECT 1",
            // A carriage return does not end a SQLite line comment.
            "SELECT 1 -- comment\r; DELETE FROM tasks",
            // TCL-style variables can contain quotes and semicolons.
            "SELECT $a(';')",
        ];
        for statement in one {
            assert!(admit_statement(statement).is_ok(), "{statement:?}");
        }
        let many = [
            "SELECT 1; SELECT 2",
            "SELECT 1 -- comment\n; DELETE FROM tasks",
            // Postgres escape strings do not exist in SQLite: E'a\' ends at \'.
            r"SELECT E'a\';ATTACH 'x' AS y;--'",
            // Postgres dollar quotes do not exist in SQLite.
            "SELECT $$;ATTACH 'x' AS y;$$",
            // Backtick identifiers can hide a quote from a Postgres lexer.
            "SELECT `'`;ATTACH 'x' AS y;--'",
            "SELECT 1 /* a /* b */ ; ATTACH 'x' AS y; -- */",
            "SELECT [a'];ATTACH 'x' AS y;--']",
            "SELECT $a(')\n;ATTACH 'x' AS y;--'",
        ];
        for statement in many {
            assert!(
                admit_statement(statement).is_err(),
                "{statement:?} must not be admitted as one statement"
            );
        }
        assert!(admit_statement("SELECT 'open").is_err());
        assert!(admit_statement("SELECT 1 /* open").is_err());
        assert!(admit_statement("SELECT [open").is_err());
        assert!(admit_statement("SELECT '\0'").is_err());
        assert!(admit_statement("  ").is_err());
    }

    #[test]
    fn reports_the_first_word() {
        assert_eq!(admit_statement("  sElEcT 1").unwrap().first_word, "select");
        assert_eq!(
            admit_statement(";;/* x */ WITH a AS (SELECT 1) SELECT * FROM a")
                .unwrap()
                .first_word,
            "with"
        );
        assert_eq!(admit_statement("\"select\" 1").unwrap().first_word, "");
    }
}
