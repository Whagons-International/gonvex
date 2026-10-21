//! SQL script parsing shared by tenant migrations and framework installation.

use crate::DatabaseError;

pub(crate) fn split_statements(source: &str) -> Result<Vec<String>, DatabaseError> {
    let mut statements = Vec::new();
    let mut start = 0;
    let mut quote = None;
    let mut line_comment = false;
    let mut block_comment = 0_u32;
    let mut dollar_tag = String::new();
    let mut chars = source.char_indices().peekable();
    while let Some((index, character)) = chars.next() {
        if line_comment {
            if character == '\n' {
                line_comment = false;
            }
            continue;
        }
        if block_comment > 0 {
            if source[index..].starts_with("/*") {
                block_comment += 1;
                let _ = chars.next();
                continue;
            }
            if source[index..].starts_with("*/") {
                block_comment -= 1;
                let _ = chars.next();
                continue;
            }
            continue;
        }
        if !dollar_tag.is_empty() {
            if source[index..].starts_with(&dollar_tag) {
                for _ in 1..dollar_tag.len() {
                    let _ = chars.next();
                }
                dollar_tag.clear();
            }
            continue;
        }
        if let Some(active_quote) = quote {
            if character == active_quote as char {
                if chars
                    .peek()
                    .is_some_and(|(_, next_character)| *next_character == character)
                {
                    let _ = chars.next();
                    continue;
                }
                quote = None;
            }
            continue;
        }
        if source[index..].starts_with("--") {
            line_comment = true;
            let _ = chars.next();
            continue;
        }
        if source[index..].starts_with("/*") {
            block_comment = 1;
            let _ = chars.next();
            continue;
        }
        if matches!(character, '\'' | '"') {
            quote = Some(character as u8);
            continue;
        }
        if character == '$' {
            if let Some(relative_end) = source[index + 1..].find('$') {
                let end = index + relative_end + 2;
                let candidate = &source[index..end];
                if candidate
                    .strip_prefix('$')
                    .and_then(|tag| tag.strip_suffix('$'))
                    .is_some_and(|tag| {
                        tag.bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                    })
                {
                    dollar_tag = candidate.to_owned();
                    for _ in 1..dollar_tag.len() {
                        let _ = chars.next();
                    }
                    continue;
                }
            }
        }
        if character == ';' {
            let statement = source[start..=index].trim();
            if !statement.is_empty() {
                statements.push(statement.to_owned());
            }
            start = index + character.len_utf8();
        }
    }
    if quote.is_some() || !dollar_tag.is_empty() || block_comment != 0 {
        return Err(sqlx::Error::Protocol("unterminated SQL quote or comment".to_owned()).into());
    }
    let trailing = source[start..].trim();
    if !trailing.is_empty() {
        statements.push(trailing.to_owned());
    }
    Ok(statements)
}

#[cfg(test)]
mod split_statements_tests {
    use super::split_statements;

    #[test]
    fn handles_multibyte_text_inside_dollar_quoted_statement() {
        let source = "DO $$ BEGIN RAISE NOTICE 'migration café'; END $$;\n".to_owned()
            + "CREATE TABLE migration_text (value text);";

        assert_eq!(
            split_statements(&source).unwrap(),
            vec![
                "DO $$ BEGIN RAISE NOTICE 'migration café'; END $$;".to_owned(),
                "CREATE TABLE migration_text (value text);".to_owned(),
            ]
        );
    }
}
