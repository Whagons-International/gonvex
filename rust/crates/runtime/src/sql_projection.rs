//! Propagate declared column kinds through SELECT aliases, joins, subqueries
//! and CTEs. This is result typing, not SQL admission: SQLite parses and executes
//! the statement. Unknown expressions retain SQLite's normal value decoding.
use gonvex_store::rows::JsonKind;
use std::collections::BTreeMap;
#[derive(Clone, Debug, PartialEq)]
enum Token {
    Id(String),
    Word(String),
    Symbol(char),
    Literal,
}
fn tokens(sql: &str) -> Option<Vec<Token>> {
    let mut out = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b if b.is_ascii_whitespace() => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = sql[i..].find('\n').map_or(bytes.len(), |n| i + n + 1)
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                i += sql[i..].find("*/")? + 2;
            }
            b'\'' | b'"' | b'`' | b'[' => {
                let close = if c == b'[' { b']' } else { c };
                let mut value = Vec::new();
                i += 1;
                loop {
                    let b = *bytes.get(i)?;
                    i += 1;
                    if b == close {
                        if close != b']' && bytes.get(i) == Some(&close) {
                            value.push(close);
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        value.push(b);
                    }
                }
                out.push(if c == b'\'' {
                    Token::Literal
                } else {
                    Token::Id(String::from_utf8(value).ok()?)
                });
            }
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                i += 1;
                while bytes
                    .get(i)
                    .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'$')
                {
                    i += 1;
                }
                out.push(Token::Word(sql[start..i].to_owned()));
            }
            _ => {
                out.push(Token::Symbol(c as char));
                i += 1;
            }
        }
    }
    Some(out)
}
fn name(token: &Token) -> Option<&str> {
    match token {
        Token::Id(s) | Token::Word(s) => Some(s),
        _ => None,
    }
}
fn word(token: &Token, s: &str) -> bool {
    matches!(token,Token::Word(t) if t.eq_ignore_ascii_case(s))
}
fn symbol(token: Option<&Token>, c: char) -> bool {
    token == Some(&Token::Symbol(c))
}
fn close(tokens: &[Token], start: usize) -> Option<usize> {
    if !symbol(tokens.get(start), '(') {
        return None;
    }
    let mut depth = 1;
    for (i, t) in tokens.iter().enumerate().skip(start + 1) {
        match t {
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}
fn split(tokens: &[Token]) -> Vec<&[Token]> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut depth = 0;
    for (i, t) in tokens.iter().enumerate() {
        match t {
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => depth -= 1,
            Token::Symbol(',') if depth == 0 => {
                out.push(&tokens[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&tokens[start..]);
    out
}
type Fields = Vec<(String, Option<JsonKind>)>;
struct Source {
    alias: String,
    table: String,
    fields: Fields,
    known: bool,
}
fn field(fields: &Fields, column: &str) -> Option<JsonKind> {
    fields
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(column))
        .and_then(|(_, kind)| *kind)
}
fn clause(t: &Token) -> bool {
    [
        "where",
        "group",
        "order",
        "having",
        "limit",
        "offset",
        "union",
        "intersect",
        "except",
        "window",
        "returning",
    ]
    .iter()
    .any(|w| word(t, w))
}
fn reserved(t: &Token) -> bool {
    clause(t)
        || [
            "join", "left", "right", "full", "inner", "outer", "cross", "natural", "on", "using",
            "as", "indexed", "not",
        ]
        .iter()
        .any(|w| word(t, w))
        || matches!(t, Token::Symbol(_))
}
fn expression(tokens: &[Token], sources: &[Source]) -> Option<JsonKind> {
    match tokens {
        [column] if name(column).is_some() => {
            let matches = sources
                .iter()
                .filter_map(|s| field(&s.fields, name(column).unwrap()))
                .collect::<Vec<_>>();
            if matches.len() == 1 {
                return Some(matches[0]);
            }
        }
        [alias, Token::Symbol('.'), column] => {
            let alias = name(alias)?;
            let source = sources.iter().find(|s| {
                s.alias.eq_ignore_ascii_case(alias) || s.table.eq_ignore_ascii_case(alias)
            })?;
            return field(&source.fields, name(column)?);
        }
        _ => {}
    }
    if tokens.first().is_some_and(|t| {
        word(t, "json")
            || word(t, "json_array")
            || word(t, "json_object")
            || word(t, "json_group_array")
            || word(t, "json_group_object")
    }) && symbol(tokens.get(1), '(')
        && close(tokens, 1) == Some(tokens.len() - 1)
    {
        return Some(JsonKind::Json);
    }
    if tokens
        .first()
        .is_some_and(|t| word(t, "coalesce") || word(t, "ifnull"))
        && symbol(tokens.get(1), '(')
        && close(tokens, 1) == Some(tokens.len() - 1)
    {
        let kinds = split(&tokens[2..tokens.len() - 1])
            .into_iter()
            .filter(|v| !matches!(*v,[ref t] if word(t,"null")))
            .map(|v| expression(v, sources))
            .collect::<Vec<_>>();
        if let Some(Some(kind)) = kinds.first() {
            if kinds.iter().all(|other| other == &Some(*kind)) {
                return Some(*kind);
            }
        }
    }
    None
}
fn infer<F>(
    tokens: &[Token],
    inherited: &BTreeMap<String, Fields>,
    resolve: &mut F,
) -> Result<Option<Fields>, String>
where
    F: FnMut(&str) -> Result<Fields, String>,
{
    let mut ctes = inherited.clone();
    let mut start = 0;
    if tokens.first().is_some_and(|t| word(t, "with")) {
        start = 1;
        if tokens.get(start).is_some_and(|t| word(t, "recursive")) {
            start += 1;
        }
        loop {
            let Some(cte) = tokens.get(start).and_then(name).map(str::to_lowercase) else {
                return Ok(None);
            };
            start += 1;
            let mut aliases = None;
            if symbol(tokens.get(start), '(') {
                let Some(end) = close(tokens, start) else {
                    return Ok(None);
                };
                aliases = Some(
                    split(&tokens[start + 1..end])
                        .into_iter()
                        .map(|p| {
                            if p.len() == 1 {
                                name(&p[0]).map(str::to_owned)
                            } else {
                                None
                            }
                        })
                        .collect::<Option<Vec<_>>>(),
                );
                start = end + 1;
            }
            if !tokens.get(start).is_some_and(|t| word(t, "as")) {
                return Ok(None);
            }
            start += 1;
            if tokens.get(start).is_some_and(|t| word(t, "not")) {
                start += 1;
            }
            if tokens.get(start).is_some_and(|t| word(t, "materialized")) {
                start += 1;
            }
            let Some(end) = close(tokens, start) else {
                return Ok(None);
            };
            let mut fields = infer(&tokens[start + 1..end], &ctes, resolve)?.unwrap_or_default();
            if let Some(Some(aliases)) = aliases {
                if aliases.len() != fields.len() {
                    return Ok(None);
                }
                for ((field, _), alias) in fields.iter_mut().zip(aliases) {
                    *field = alias;
                }
            }
            ctes.insert(cte, fields);
            start = end + 1;
            if symbol(tokens.get(start), ',') {
                start += 1;
            } else {
                break;
            }
        }
    }
    if !tokens.get(start).is_some_and(|t| word(t, "select")) {
        return Ok(None);
    }
    start += 1;
    if tokens
        .get(start)
        .is_some_and(|t| word(t, "distinct") || word(t, "all"))
    {
        start += 1;
    }
    let mut depth = 0;
    let mut from = None;
    let mut projection_end = tokens.len();
    for (i, t) in tokens.iter().enumerate().skip(start) {
        match t {
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => depth -= 1,
            _ if depth == 0 && word(t, "from") => {
                from = Some(i);
                projection_end = i;
                break;
            }
            _ if depth == 0 && clause(t) => {
                projection_end = i;
                break;
            }
            Token::Symbol(';') if depth == 0 => {
                projection_end = i;
                break;
            }
            _ => {}
        }
    }
    let mut sources = Vec::new();
    if let Some(from) = from {
        let mut i = from + 1;
        loop {
            let fields;
            let table;
            let known;
            if symbol(tokens.get(i), '(') {
                let Some(end) = close(tokens, i) else {
                    return Ok(None);
                };
                let nested = infer(&tokens[i + 1..end], &ctes, resolve)?;
                known = nested.is_some();
                fields = nested.unwrap_or_default();
                table = String::new();
                i = end + 1;
            } else {
                let Some(first) = tokens.get(i).and_then(name) else {
                    return Ok(None);
                };
                table = if symbol(tokens.get(i + 1), '.') {
                    let Some(second) = tokens.get(i + 2).and_then(name) else {
                        return Ok(None);
                    };
                    i += 3;
                    format!("{first}.{second}")
                } else {
                    i += 1;
                    first.to_owned()
                };
                if symbol(tokens.get(i), '(') {
                    let Some(end) = close(tokens, i) else {
                        return Ok(None);
                    };
                    i = end + 1;
                    fields = Vec::new();
                    known = false;
                } else if let Some(cte) = ctes.get(&table.to_lowercase()) {
                    fields = cte.clone();
                    known = true;
                } else {
                    fields = resolve(&table)?;
                    known = !fields.is_empty();
                }
            }
            if tokens.get(i).is_some_and(|t| word(t, "as")) {
                i += 1;
            }
            let alias = if let Some(token) = tokens.get(i).filter(|t| !reserved(t)) {
                if let Some(alias) = name(token) {
                    i += 1;
                    alias.to_owned()
                } else {
                    table.clone()
                }
            } else {
                table.clone()
            };
            sources.push(Source {
                alias,
                table,
                fields,
                known,
            });
            let mut depth = 0;
            let mut next = None;
            while let Some(t) = tokens.get(i) {
                match t {
                    Token::Symbol('(') => depth += 1,
                    Token::Symbol(')') => depth -= 1,
                    _ if depth == 0 && clause(t) => break,
                    _ if depth == 0 && (word(t, "join") || *t == Token::Symbol(',')) => {
                        next = Some(i + 1);
                        break;
                    }
                    _ => {}
                }
                i += 1;
            }
            match next {
                Some(at) => i = at,
                None => break,
            }
        }
    }
    let mut fields = Vec::new();
    for mut part in split(&tokens[start..projection_end]) {
        if part == [Token::Symbol('*')] {
            if sources.iter().any(|s| !s.known) {
                return Ok(None);
            }
            for source in &sources {
                fields.extend(source.fields.clone());
            }
            continue;
        }
        if let [alias, Token::Symbol('.'), Token::Symbol('*')] = part {
            let Some(source) = name(alias).and_then(|alias| {
                sources.iter().find(|s| {
                    s.alias.eq_ignore_ascii_case(alias) || s.table.eq_ignore_ascii_case(alias)
                })
            }) else {
                return Ok(None);
            };
            if !source.known {
                return Ok(None);
            }
            fields.extend(source.fields.clone());
            continue;
        }
        let mut alias = None;
        let mut depth = 0;
        let mut as_at = None;
        for (i, t) in part.iter().enumerate() {
            match t {
                Token::Symbol('(') => depth += 1,
                Token::Symbol(')') => depth -= 1,
                _ if depth == 0 && word(t, "as") => as_at = Some(i),
                _ => {}
            }
        }
        if let Some(at) = as_at {
            alias = part.get(at + 1).and_then(name).map(str::to_owned);
            part = &part[..at];
        } else if part.len() > 1
            && part.last().and_then(name).is_some()
            && part.get(part.len() - 2) != Some(&Token::Symbol('.'))
        {
            alias = name(part.last().unwrap()).map(str::to_owned);
            part = &part[..part.len() - 1];
        }
        let output = alias
            .or_else(|| match part {
                [column] => name(column).map(str::to_owned),
                [_, Token::Symbol('.'), column] => name(column).map(str::to_owned),
                _ => None,
            })
            .unwrap_or_default();
        fields.push((output, expression(part, &sources)));
    }
    Ok(Some(fields))
}
pub(super) fn kinds<F>(sql: &str, mut resolve: F) -> Result<Option<Vec<Option<JsonKind>>>, String>
where
    F: FnMut(&str) -> Result<Vec<(String, JsonKind)>, String>,
{
    let Some(tokens) = tokens(sql) else {
        return Ok(None);
    };
    let mut resolver = |name: &str| {
        resolve(name).map(|fields| {
            fields
                .into_iter()
                .map(|(name, kind)| (name, Some(kind)))
                .collect()
        })
    };
    Ok(infer(&tokens, &BTreeMap::new(), &mut resolver)?
        .map(|fields| fields.into_iter().map(|(_, kind)| kind).collect()))
}
#[cfg(test)]
mod tests {
    use super::*;
    fn resolve(table: &str) -> Result<Vec<(String, JsonKind)>, String> {
        Ok(match table.to_lowercase().as_str() {
            "tasks" => vec![
                ("_id".into(), JsonKind::Text),
                ("value".into(), JsonKind::Json),
                ("done".into(), JsonKind::Boolean),
            ],
            "other" => vec![
                ("id".into(), JsonKind::Integer),
                ("value".into(), JsonKind::Text),
            ],
            _ => vec![],
        })
    }
    #[test]
    fn aliases_joins_and_wildcards() {
        assert_eq!(kinds("SELECT t.value payload,t.done complete,o.value label,count(*) total FROM tasks t JOIN other o ON o.id=t._id WHERE t.done=1",resolve).unwrap(),Some(vec![Some(JsonKind::Json),Some(JsonKind::Boolean),Some(JsonKind::Text),None]));
        assert_eq!(
            kinds(
                "SELECT t.* FROM tasks t JOIN json_each($1) ids ON ids.value=t._id",
                resolve
            )
            .unwrap(),
            Some(vec![
                Some(JsonKind::Text),
                Some(JsonKind::Json),
                Some(JsonKind::Boolean)
            ])
        );
    }
    #[test]
    fn nested_cte_columns() {
        assert_eq!(kinds("WITH t AS (SELECT value,done FROM tasks), renamed(payload,complete) AS MATERIALIZED (SELECT * FROM t) SELECT q.payload,q.complete FROM (SELECT * FROM renamed) q JOIN other o ON 1",resolve).unwrap(),Some(vec![Some(JsonKind::Json),Some(JsonKind::Boolean)]));
    }
    #[test]
    fn unknown_expressions_keep_sqlite_decoding() {
        assert_eq!(
            kinds("SELECT 1 value, 'false' done FROM tasks", resolve).unwrap(),
            Some(vec![None, None])
        );
        assert_eq!(kinds("SELECT * FROM json_each($1)", resolve).unwrap(), None);
        assert_eq!(
            kinds("SELECT json('0') value", resolve).unwrap(),
            Some(vec![Some(JsonKind::Json)])
        );
    }
}
