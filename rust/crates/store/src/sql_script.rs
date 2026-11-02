//! SQL script execution units for migrations and framework installation.

use crate::{sql_lexer, DatabaseError};

pub(crate) fn split_statements(source: &str) -> Result<Vec<String>, DatabaseError> {
    let ranges = sql_lexer::statement_ranges(source)
        .map_err(|error| sqlx::Error::Protocol(format!("invalid SQL script: {error:?}")))?;
    Ok(ranges
        .into_iter()
        .map(|range| source[range].trim().to_owned())
        .collect())
}

#[cfg(test)]
mod split_statements_tests {
    use super::split_statements;

    #[test]
    fn postgres_literal_and_comment_boundaries_keep_two_statements() {
        for first in [
            "SELECT 1 -- comment\r;",
            r"SELECT E'a\';b';",
            "SELECT $é$semi;colon$é$;",
            "SELECT identifier$tag$part;",
            "SELECT E'a' -- continue\r'b\\';c';",
        ] {
            let source = format!("{first} SELECT 2;");
            assert_eq!(
                split_statements(&source).unwrap(),
                vec![first.to_owned(), "SELECT 2;".to_owned()],
                "{source:?}"
            );
        }
    }

    #[test]
    fn comments_and_empty_statements_do_not_create_work() {
        assert!(split_statements("; -- comment\r; /* comment */ ;")
            .unwrap()
            .is_empty());
        for source in ["SELECT 'a", "SELECT $é$a", "SELECT 1 /* a", "SELECT 1\0"] {
            assert!(split_statements(source).is_err(), "{source:?}");
        }
    }

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
