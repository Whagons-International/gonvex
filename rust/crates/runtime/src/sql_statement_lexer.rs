//! PostgreSQL statement admission without a dependency on SQLx or the runtime.
//! This scans lexical boundaries, not SQL grammar. Ordinary strings assume
//! standard_conforming_strings=on; E strings always process backslash escapes.
#[derive(Debug, PartialEq, Eq)]
pub struct StatementInfo {
    pub count: usize,
    pub first_word: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum LexError {
    Nul,
    UnterminatedQuote,
    UnterminatedDollarQuote,
    UnterminatedComment,
}

fn word_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte >= 128
}
fn word_part(byte: u8) -> bool {
    word_start(byte) || byte.is_ascii_digit() || byte == b'$'
}
fn quoted(bytes: &[u8], mut index: usize, quote: u8, escapes: bool) -> Result<usize, LexError> {
    while index < bytes.len() {
        if escapes && bytes[index] == b'\\' {
            index += 2;
        } else if bytes[index] == quote {
            if bytes.get(index + 1) == Some(&quote) {
                index += 2;
            } else {
                return Ok(index + 1);
            }
        } else {
            index += 1;
        }
    }
    Err(LexError::UnterminatedQuote)
}
// PostgreSQL's quotecontinue rule permits whitespace/line comments containing
// a newline. The original string mode carries across the continuation.
fn string_literal(bytes: &[u8], mut index: usize, escapes: bool) -> Result<usize, LexError> {
    loop {
        let end = quoted(bytes, index, b'\'', escapes)?;
        let mut gap = end;
        let mut newline = false;
        while gap < bytes.len() {
            if bytes[gap].is_ascii_whitespace() {
                newline |= matches!(bytes[gap], b'\r' | b'\n');
                gap += 1;
            } else if bytes[gap..].starts_with(b"--") {
                gap += 2;
                while gap < bytes.len() && !matches!(bytes[gap], b'\r' | b'\n') {
                    gap += 1;
                }
            } else {
                break;
            }
        }
        if newline && bytes.get(gap) == Some(&b'\'') {
            index = gap + 1;
        } else {
            return Ok(end);
        }
    }
}

fn dollar_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut index = start + 1;
    if bytes.get(index) == Some(&b'$') {
        return Some(index + 1);
    }
    if !bytes.get(index).copied().is_some_and(word_start) {
        return None;
    }
    index += 1;
    while let Some(&byte) = bytes.get(index) {
        if byte == b'$' {
            return Some(index + 1);
        }
        if !(word_start(byte) || byte.is_ascii_digit()) {
            return None;
        }
        index += 1;
    }
    None
}

pub fn analyze(source: &str) -> Result<StatementInfo, LexError> {
    let bytes = source.as_bytes();
    if bytes.contains(&0) {
        return Err(LexError::Nul);
    }
    let mut index = 0;
    let mut count = 0;
    let mut active = false;
    let mut head_seen = false;
    let mut first_word = None;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
        } else if bytes[index..].starts_with(b"--") {
            index += 2;
            while index < bytes.len() && !matches!(bytes[index], b'\r' | b'\n') {
                index += 1;
            }
        } else if bytes[index..].starts_with(b"/*") {
            let mut depth = 1usize;
            index += 2;
            while index < bytes.len() && depth != 0 {
                if bytes[index..].starts_with(b"/*") {
                    depth += 1;
                    index += 2;
                } else if bytes[index..].starts_with(b"*/") {
                    depth -= 1;
                    index += 2;
                } else {
                    index += 1;
                }
            }
            if depth != 0 {
                return Err(LexError::UnterminatedComment);
            }
        } else if byte == b';' {
            count += usize::from(active);
            active = false;
            index += 1;
        } else {
            active = true;
            let first = !head_seen;
            head_seen = true;
            if matches!(byte, b'e' | b'E') && bytes.get(index + 1) == Some(&b'\'') {
                index = string_literal(bytes, index + 2, true)?;
            } else if byte == b'\'' {
                index = string_literal(bytes, index + 1, false)?;
            } else if byte == b'"' {
                index = quoted(bytes, index + 1, byte, false)?;
            } else if byte == b'$' {
                if let Some(end) = dollar_end(bytes, index) {
                    let tag = &source[index..end];
                    index = source[end..]
                        .find(tag)
                        .map(|offset| end + offset + tag.len())
                        .ok_or(LexError::UnterminatedDollarQuote)?;
                } else {
                    index += 1;
                }
            } else if word_start(byte) {
                let start = index;
                index += 1;
                while bytes.get(index).copied().is_some_and(word_part) {
                    index += 1;
                }
                if first {
                    first_word = Some(source[start..index].to_ascii_lowercase());
                }
            } else {
                index += 1;
            }
        }
    }
    count += usize::from(active);
    Ok(StatementInfo { count, first_word })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn literals_comments_and_empty_statements_keep_one_select() {
        let statements = [
            "SELECT $$semi;colon$$ AS n",
            "SELECT $$'$$ AS n",
            "SELECT 42 AS n; -- trailing comment",
            r"SELECT E'a\';b' AS n",
            "-- leading comment\nSELECT 42 AS n",
            "SELECT 'it''s ; okay' AS n",
            "SELECT $tag$quote ' ; /*comment*/$tag$ AS n",
            "SELECT $é$quote ' ; /*comment*/$é$ AS n",
            "SELECT 1 /*outer /*inner*/ ; ' */ AS n",
            "SELECT 1 -- comment\r;",
            "SELECT 1 AS \"semi;colon\"",
            "SELECT 1 AS \"a\"\";b\"",
        ];
        let prefixes = [
            "",
            "-- prefix\r",
            "-- prefix\n",
            "/* nested /*inner*/ prefix */",
            "; ; ",
        ];
        let suffixes = ["", ";", "; -- after", "; /* after */ ; ;"];
        for statement in statements {
            for prefix in prefixes {
                for suffix in suffixes {
                    let source = format!("{prefix}{statement}\n{suffix}");
                    assert_eq!(
                        analyze(&source),
                        Ok(StatementInfo {
                            count: 1,
                            first_word: Some("select".into())
                        }),
                        "{source:?}"
                    );
                }
            }
        }
    }
    #[test]
    fn rejects_batches_hidden_after_carriage_return_comments() {
        assert_eq!(analyze("SELECT 1 -- comment\r; SELECT 2").unwrap().count, 2);
        assert_eq!(analyze("SELECT ';'; SELECT 2").unwrap().count, 2);
        assert_eq!(analyze("SELECT $$;$$; COMMIT").unwrap().count, 2);
    }
    #[test]
    fn malformed_lexemes_are_errors() {
        for source in [
            "SELECT 'a",
            "SELECT \"a",
            "SELECT E'a\\",
            "SELECT $$a",
            "SELECT $tag$a",
            "SELECT 1 /*outer /*inner*/",
            "SELECT 1\0",
        ] {
            assert!(analyze(source).is_err(), "{source:?}");
        }
    }
    #[test]
    fn dollar_inside_identifier_is_not_a_string_delimiter() {
        assert_eq!(analyze("SELECT a$tag$b; SELECT 2").unwrap().count, 2);
        assert_eq!(analyze("SELECT a$$b; SELECT 2").unwrap().count, 2);
    }
    #[test]
    fn quoted_first_token_cannot_become_read_keyword() {
        for source in [
            "\"select\" 42",
            "'select' 42",
            "$$select$$ 42",
            "E'select' 42",
        ] {
            assert_eq!(analyze(source).unwrap().first_word, None);
        }
    }

    #[test]
    fn string_continuations_retain_the_original_escape_mode() {
        for separator in ["\n", "\r", " -- continuation\r", " -- continuation\n"] {
            let source = format!("SELECT E'a'{separator}'b\\';c' AS n");
            assert_eq!(analyze(&source).unwrap().count, 1, "{source:?}");
            assert_eq!(analyze(&(source + "; SELECT 2")).unwrap().count, 2);
        }
        assert_eq!(
            analyze("SELECT 'a'\n'b\\'; SELECT 2").unwrap().count,
            2,
            "ordinary strings keep standard_conforming_strings=on semantics"
        );
    }
}
