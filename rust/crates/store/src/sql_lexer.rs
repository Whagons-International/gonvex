//! SQLite lexical boundaries shared by runtime statement admission and tests.
//! This scans lexical boundaries, not SQL grammar: `'…'` strings and
//! `"…"`, `` `…` `` and `[…]` identifiers (doubled quotes escape), `--` and
//! non-nesting `/* */` comments, and `CREATE TRIGGER … BEGIN … END` bodies,
//! whose inner semicolons do not end the statement.
#[derive(Debug, PartialEq, Eq)]
pub struct StatementInfo<'a> {
    pub count: usize,
    /// The first unquoted word, borrowed with its original spelling.
    pub first_word: Option<&'a str>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum LexError {
    Nul,
    UnterminatedQuote,
    UnterminatedComment,
}

fn word_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte >= 128
}
fn word_part(byte: u8) -> bool {
    word_start(byte) || byte.is_ascii_digit() || byte == b'$'
}

/// Index just past the closing delimiter; a doubled closer is an escape.
fn quoted(source: &str, index: usize, close: u8) -> Result<usize, LexError> {
    let bytes = source.as_bytes();
    let mut index = index;
    loop {
        // ASCII delimiters cannot occur inside a UTF-8 multibyte sequence.
        index += source[index..]
            .find(char::from(close))
            .ok_or(LexError::UnterminatedQuote)?;
        if close != b']' && bytes.get(index + 1) == Some(&close) {
            index += 2;
        } else {
            return Ok(index + 1);
        }
    }
}

/// Count nonempty statements and report the first unquoted keyword.
/// This does not parse SQL grammar or enforce an invocation policy.
pub fn analyze(source: &str) -> Result<StatementInfo<'_>, LexError> {
    scan(source, |_| {})
}

/// Return byte ranges of nonempty statements, including their terminating `;`.
/// Leading whitespace/comments belong to the next statement. A trailing
/// comment or empty statement does not create an execution unit.
pub fn statement_ranges(source: &str) -> Result<Vec<std::ops::Range<usize>>, LexError> {
    let mut ranges = Vec::new();
    scan(source, |range| ranges.push(range))?;
    Ok(ranges)
}

/// Tracks `CREATE [TEMP|TEMPORARY] TRIGGER … BEGIN … END` within a statement.
#[derive(Default)]
struct TriggerState {
    words: usize,
    create: bool,
    trigger: bool,
    in_body: bool,
    last_was_end: bool,
}

impl TriggerState {
    fn word(&mut self, word: &str) {
        let upper = word.to_ascii_uppercase();
        match self.words {
            0 => self.create = upper == "CREATE",
            1 | 2 if self.create && upper == "TRIGGER" => self.trigger = true,
            1 if upper == "TEMP" || upper == "TEMPORARY" => {}
            1 => self.create = false,
            _ => {}
        }
        if self.trigger && upper == "BEGIN" {
            self.in_body = true;
        }
        self.words += 1;
        self.last_was_end = upper == "END";
    }

    fn other_token(&mut self) {
        self.last_was_end = false;
    }

    /// Whether a `;` here ends the statement (as `sqlite3_complete` decides).
    fn ends_statement(&self) -> bool {
        !self.in_body || self.last_was_end
    }
}

fn scan(
    source: &str,
    mut on_statement: impl FnMut(std::ops::Range<usize>),
) -> Result<StatementInfo<'_>, LexError> {
    let bytes = source.as_bytes();
    if bytes.contains(&0) {
        return Err(LexError::Nul);
    }
    let mut index = 0;
    let mut count = 0;
    let mut start = 0;
    let mut active = false;
    let mut head_seen = false;
    let mut first_word = None;
    let mut trigger = TriggerState::default();
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
        } else if bytes[index..].starts_with(b"--") {
            index += 2;
            // SQLite ends a line comment only at `\n`; `\r` does not end it.
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
        } else if bytes[index..].starts_with(b"/*") {
            index = source[index + 2..]
                .find("*/")
                .map(|offset| index + 2 + offset + 2)
                .ok_or(LexError::UnterminatedComment)?;
        } else if byte == b';' && (!active || trigger.ends_statement()) {
            count += usize::from(active);
            if active {
                on_statement(start..index + 1);
            }
            active = false;
            trigger = TriggerState::default();
            index += 1;
            start = index;
        } else {
            active = true;
            let first = !head_seen;
            head_seen = true;
            match byte {
                b'\'' | b'"' | b'`' => {
                    index = quoted(source, index + 1, byte)?;
                    trigger.other_token();
                }
                b'[' => {
                    index = quoted(source, index + 1, b']')?;
                    trigger.other_token();
                }
                byte if word_start(byte) => {
                    let word_start_index = index;
                    index += 1;
                    while bytes.get(index).copied().is_some_and(word_part) {
                        index += 1;
                    }
                    let word = &source[word_start_index..index];
                    if first {
                        first_word = Some(word);
                    }
                    trigger.word(word);
                }
                _ => {
                    index += 1;
                    trigger.other_token();
                }
            }
        }
    }
    count += usize::from(active);
    if active {
        on_statement(start..source.len());
    }
    Ok(StatementInfo { count, first_word })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn literals_comments_and_empty_statements_keep_one_select() {
        let statements = [
            "SELECT 42 AS n; -- trailing comment",
            "-- leading comment\nSELECT 42 AS n",
            "SELECT 'it''s ; okay' AS n",
            "SELECT 'café'';okay' AS n",
            "SELECT 1 /* comment ; ' */ AS n",
            "SELECT 1 -- comment\n;",
            "SELECT 1 AS \"semi;colon\"",
            "SELECT 1 AS \"a\"\";b\"",
            "SELECT 1 AS \"é\"\";b\"",
            "SELECT 1 AS [semi;colon]",
            "SELECT 1 AS `semi;colon`",
            "SELECT X'3B' AS n",
        ];
        let prefixes = ["", "-- prefix\r\n", "-- prefix\n", "/* prefix */", "; ; "];
        let suffixes = ["", ";", "; -- after", "; /* after */ ; ;"];
        for statement in statements {
            for prefix in prefixes {
                for suffix in suffixes {
                    let source = format!("{prefix}{statement}\n{suffix}");
                    assert_eq!(
                        analyze(&source),
                        Ok(StatementInfo {
                            count: 1,
                            first_word: Some("SELECT")
                        }),
                        "{source:?}"
                    );
                }
            }
        }
    }
    #[test]
    fn statement_boundaries_match_sqlite_comments_and_strings() {
        assert_eq!(analyze("SELECT 1 -- comment\n; SELECT 2").unwrap().count, 2);
        // As in SQLite, `\r` does not end a line comment: nothing follows.
        assert_eq!(analyze("SELECT 1 -- comment\r; SELECT 2").unwrap().count, 1);
        assert_eq!(analyze("SELECT ';'; SELECT 2").unwrap().count, 2);
        assert_eq!(analyze("SELECT 1 /* a */; COMMIT").unwrap().count, 2);
        // SQLite block comments do not nest.
        assert_eq!(analyze("SELECT 1 /* a /* b */; SELECT 2").unwrap().count, 2);
        // Backslashes are ordinary characters in SQLite strings.
        assert_eq!(analyze(r"SELECT 'a\'; SELECT 2").unwrap().count, 2);
    }
    #[test]
    fn malformed_lexemes_are_errors() {
        for source in [
            "SELECT 'a",
            "SELECT \"a",
            "SELECT [a",
            "SELECT `a",
            "SELECT 1 /*outer",
            "SELECT 1\0",
        ] {
            assert!(analyze(source).is_err(), "{source:?}");
        }
    }
    #[test]
    fn quoted_first_token_cannot_become_read_keyword() {
        for source in ["\"select\" 42", "'select' 42", "[select] 42", "`select` 42"] {
            assert_eq!(analyze(source).unwrap().first_word, None);
        }
    }
    #[test]
    fn trigger_bodies_are_one_statement() {
        let trigger = "CREATE TRIGGER t AFTER INSERT ON a BEGIN INSERT INTO b VALUES(1); UPDATE c SET d=1; END;";
        assert_eq!(statement_ranges(trigger).unwrap().len(), 1);
        let temp = "create temp trigger \"x\" before delete on a begin select raise(abort,'no;'); end; SELECT 2;";
        assert_eq!(analyze(temp).unwrap().count, 2);
        // `BEGIN` outside a trigger is an ordinary statement.
        assert_eq!(analyze("BEGIN; SELECT 1; END;").unwrap().count, 3);
        assert_eq!(analyze("CREATE TABLE end_(x); SELECT 1").unwrap().count, 2);
        // Like sqlite3_complete, `END;` after a CASE ends the scan early: a
        // conservative over-count, never an under-count.
        let case =
            "CREATE TRIGGER t AFTER INSERT ON a BEGIN SELECT CASE WHEN 1 THEN 2 END; END; SELECT 3";
        assert_eq!(analyze(case).unwrap().count, 3);
    }
}
