//! The SQL/JSON path subset reference declarations use, evaluated in lax mode
//! with PostgreSQL's semantics (array auto-unwrapping, existential and
//! three-valued comparisons, `jsonb_path_match` for predicates).
//!
//! Supported: `$`, `@`, `$name` variables, `.key`, `."key"`, `.*`, `[*]`,
//! `[n]`, `[n to m]`, `? (filter)`, `.type()`, `.size()`, `.keyvalue()`,
//! `.string()`, literals, `== != <> < <= > >=`, `&&`, `||`, `!`,
//! `exists(…)`, `starts with` and an optional `lax`/`strict` prefix.

use std::cmp::Ordering;

use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct JsonPath {
    expr: Expr,
}

#[derive(Clone, Debug, PartialEq)]
enum Expr {
    Path(Primary, Vec<Step>),
    Compare(Box<Expr>, CompareOp, Box<Expr>),
    StartsWith(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Exists(Box<Expr>),
}

#[derive(Clone, Debug, PartialEq)]
enum Primary {
    Root,
    Current,
    Variable(String),
    Literal(Value),
    Nested(Box<Expr>),
}

#[derive(Clone, Debug, PartialEq)]
enum Step {
    Member(String),
    AnyMember,
    AnyElement,
    Elements(Vec<(i64, Option<i64>)>),
    Filter(Box<Expr>),
    Type,
    Size,
    KeyValue,
    String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Three-valued predicate result.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Truth {
    True,
    False,
    Unknown,
}

impl Truth {
    fn from_bool(value: bool) -> Self {
        if value {
            Self::True
        } else {
            Self::False
        }
    }
}

pub(crate) type Vars<'a> = Option<&'a Map<String, Value>>;

impl JsonPath {
    pub(crate) fn parse(source: &str) -> Result<Self, String> {
        let mut parser = Parser {
            tokens: tokenize(source)?,
            index: 0,
        };
        if matches!(parser.peek(), Some(Token::Word(word)) if word == "lax" || word == "strict") {
            parser.index += 1;
        }
        let expr = parser.expr()?;
        if parser.index != parser.tokens.len() {
            return Err(format!("unexpected input in JSON path {source:?}"));
        }
        Ok(Self { expr })
    }

    /// The first member accessor applied to `$`, if the path starts `$.key`.
    pub(crate) fn root_member(&self) -> Option<&str> {
        match &self.expr {
            Expr::Path(Primary::Root, steps) => match steps.first() {
                Some(Step::Member(name)) => Some(name),
                _ => None,
            },
            _ => None,
        }
    }

    /// `jsonb_path_query`: every selected item. Errors (such as an unknown
    /// variable) select nothing, as lax mode suppresses structural errors.
    pub(crate) fn query(&self, document: &Value, vars: Vars<'_>) -> Vec<Value> {
        let context = Context {
            root: document,
            vars,
        };
        match &self.expr {
            Expr::Path(..) => context.items(&self.expr, None).unwrap_or_default(),
            predicate => match context.truth(predicate, None) {
                Truth::True => vec![Value::Bool(true)],
                Truth::False => vec![Value::Bool(false)],
                Truth::Unknown => vec![Value::Null],
            },
        }
    }

    pub(crate) fn query_first(&self, document: &Value, vars: Vars<'_>) -> Option<Value> {
        self.query(document, vars).into_iter().next()
    }

    /// `jsonb_path_match`: `None` when the result is unknown or not a single
    /// boolean.
    pub(crate) fn matches(&self, document: &Value, vars: Vars<'_>) -> Option<bool> {
        let context = Context {
            root: document,
            vars,
        };
        match &self.expr {
            Expr::Path(..) => match context.items(&self.expr, None).ok()?.as_slice() {
                [Value::Bool(value)] => Some(*value),
                _ => None,
            },
            predicate => match context.truth(predicate, None) {
                Truth::True => Some(true),
                Truth::False => Some(false),
                Truth::Unknown => None,
            },
        }
    }
}

struct Context<'a> {
    root: &'a Value,
    vars: Vars<'a>,
}

impl Context<'_> {
    fn items(&self, expr: &Expr, current: Option<&Value>) -> Result<Vec<Value>, ()> {
        match expr {
            Expr::Path(primary, steps) => {
                let mut items = match primary {
                    Primary::Root => vec![self.root.clone()],
                    Primary::Current => vec![current.ok_or(())?.clone()],
                    Primary::Variable(name) => {
                        vec![self.vars.and_then(|vars| vars.get(name)).ok_or(())?.clone()]
                    }
                    Primary::Literal(value) => vec![value.clone()],
                    Primary::Nested(inner) => self.items(inner, current)?,
                };
                for step in steps {
                    items = self.step(step, items)?;
                }
                Ok(items)
            }
            predicate => Ok(match self.truth(predicate, current) {
                Truth::True => vec![Value::Bool(true)],
                Truth::False => vec![Value::Bool(false)],
                Truth::Unknown => vec![Value::Null],
            }),
        }
    }

    fn step(&self, step: &Step, items: Vec<Value>) -> Result<Vec<Value>, ()> {
        let mut output = Vec::new();
        match step {
            Step::Member(name) => {
                for item in unwrap(items) {
                    if let Value::Object(object) = item {
                        if let Some(value) = object.get(name) {
                            output.push(value.clone());
                        }
                    }
                }
            }
            Step::AnyMember => {
                for item in unwrap(items) {
                    if let Value::Object(object) = item {
                        output.extend(object.values().cloned());
                    }
                }
            }
            Step::AnyElement => {
                for item in items {
                    match item {
                        Value::Array(elements) => output.extend(elements),
                        other => output.push(other),
                    }
                }
            }
            Step::Elements(ranges) => {
                for item in items {
                    let elements = match item {
                        Value::Array(elements) => elements,
                        other => vec![other],
                    };
                    let length = elements.len() as i64;
                    for (start, end) in ranges {
                        let end = end.unwrap_or(*start);
                        for index in (*start).max(0)..=end.min(length - 1) {
                            output.push(elements[index as usize].clone());
                        }
                    }
                }
            }
            Step::Filter(predicate) => {
                for item in unwrap(items) {
                    if self.truth(predicate, Some(&item)) == Truth::True {
                        output.push(item);
                    }
                }
            }
            Step::Type => {
                for item in items {
                    output.push(Value::String(type_name(&item).to_owned()));
                }
            }
            Step::Size => {
                for item in items {
                    let size = match &item {
                        Value::Array(elements) => elements.len(),
                        _ => 1,
                    };
                    output.push(Value::from(size));
                }
            }
            Step::KeyValue => {
                for item in unwrap(items) {
                    match item {
                        Value::Object(object) => {
                            for (key, value) in object {
                                output.push(serde_json::json!({"key": key, "value": value}));
                            }
                        }
                        _ => return Err(()),
                    }
                }
            }
            Step::String => {
                for item in unwrap(items) {
                    output.push(match item {
                        Value::String(text) => Value::String(text),
                        Value::Number(number) => Value::String(number.to_string()),
                        Value::Bool(value) => Value::String(value.to_string()),
                        _ => return Err(()),
                    });
                }
            }
        }
        Ok(output)
    }

    fn truth(&self, expr: &Expr, current: Option<&Value>) -> Truth {
        match expr {
            Expr::And(left, right) => match self.truth(left, current) {
                Truth::False => Truth::False,
                left => match (left, self.truth(right, current)) {
                    (_, Truth::False) => Truth::False,
                    (Truth::True, Truth::True) => Truth::True,
                    _ => Truth::Unknown,
                },
            },
            Expr::Or(left, right) => match self.truth(left, current) {
                Truth::True => Truth::True,
                left => match (left, self.truth(right, current)) {
                    (_, Truth::True) => Truth::True,
                    (Truth::False, Truth::False) => Truth::False,
                    _ => Truth::Unknown,
                },
            },
            Expr::Not(inner) => match self.truth(inner, current) {
                Truth::True => Truth::False,
                Truth::False => Truth::True,
                Truth::Unknown => Truth::Unknown,
            },
            Expr::Exists(inner) => match self.items(inner, current) {
                Ok(items) => Truth::from_bool(!items.is_empty()),
                Err(()) => Truth::Unknown,
            },
            Expr::Compare(left, op, right) => {
                let (Ok(left), Ok(right)) = (self.items(left, current), self.items(right, current))
                else {
                    return Truth::Unknown;
                };
                let (left, right) = (unwrap(left), unwrap(right));
                let mut unknown = false;
                for left in &left {
                    for right in &right {
                        match compare(left, *op, right) {
                            Truth::True => return Truth::True,
                            Truth::Unknown => unknown = true,
                            Truth::False => {}
                        }
                    }
                }
                if unknown {
                    Truth::Unknown
                } else {
                    Truth::False
                }
            }
            Expr::StartsWith(left, right) => {
                let (Ok(left), Ok(right)) = (self.items(left, current), self.items(right, current))
                else {
                    return Truth::Unknown;
                };
                let [Value::String(prefix)] = right.as_slice() else {
                    return Truth::Unknown;
                };
                let mut unknown = false;
                for item in unwrap(left) {
                    match item {
                        Value::String(text) if text.starts_with(prefix.as_str()) => {
                            return Truth::True
                        }
                        Value::String(_) => {}
                        _ => unknown = true,
                    }
                }
                if unknown {
                    Truth::Unknown
                } else {
                    Truth::False
                }
            }
            Expr::Path(..) => match self.items(expr, current) {
                Ok(items) => match items.as_slice() {
                    [Value::Bool(value)] => Truth::from_bool(*value),
                    _ => Truth::Unknown,
                },
                Err(()) => Truth::Unknown,
            },
        }
    }
}

fn unwrap(items: Vec<Value>) -> Vec<Value> {
    let mut output = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Value::Array(elements) => output.extend(elements),
            other => output.push(other),
        }
    }
    output
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn compare(left: &Value, op: CompareOp, right: &Value) -> Truth {
    let ordering = match (left, right) {
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        (Value::Null, _) | (_, Value::Null) => {
            return Truth::from_bool(op == CompareOp::Ne);
        }
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        (Value::Number(left), Value::Number(right)) => match (left.as_f64(), right.as_f64()) {
            (Some(left), Some(right)) => left.partial_cmp(&right),
            _ => None,
        },
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        _ => None,
    };
    let Some(ordering) = ordering else {
        return Truth::Unknown;
    };
    Truth::from_bool(match op {
        CompareOp::Eq => ordering == Ordering::Equal,
        CompareOp::Ne => ordering != Ordering::Equal,
        CompareOp::Lt => ordering == Ordering::Less,
        CompareOp::Le => ordering != Ordering::Greater,
        CompareOp::Gt => ordering == Ordering::Greater,
        CompareOp::Ge => ordering != Ordering::Less,
    })
}

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Dollar,
    At,
    Variable(String),
    Dot,
    Star,
    OpenBracket,
    CloseBracket,
    OpenParen,
    CloseParen,
    Question,
    Comma,
    Minus,
    Op(CompareOp),
    And,
    Or,
    Bang,
    Str(String),
    Number(Value),
    Word(String),
}

fn tokenize(source: &str) -> Result<Vec<Token>, String> {
    let chars = source.chars().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut index = 0;
    let identifier = |c: char| c.is_alphanumeric() || c == '_';
    while index < chars.len() {
        let c = chars[index];
        let next = chars.get(index + 1).copied();
        match c {
            c if c.is_whitespace() => index += 1,
            '$' => {
                let start = index + 1;
                let mut end = start;
                while end < chars.len() && identifier(chars[end]) {
                    end += 1;
                }
                if end == start {
                    tokens.push(Token::Dollar);
                } else {
                    tokens.push(Token::Variable(chars[start..end].iter().collect()));
                }
                index = end;
            }
            '@' => {
                tokens.push(Token::At);
                index += 1;
            }
            '.' => {
                tokens.push(Token::Dot);
                index += 1;
            }
            '*' => {
                tokens.push(Token::Star);
                index += 1;
            }
            '[' => {
                tokens.push(Token::OpenBracket);
                index += 1;
            }
            ']' => {
                tokens.push(Token::CloseBracket);
                index += 1;
            }
            '(' => {
                tokens.push(Token::OpenParen);
                index += 1;
            }
            ')' => {
                tokens.push(Token::CloseParen);
                index += 1;
            }
            '?' => {
                tokens.push(Token::Question);
                index += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                index += 1;
            }
            '=' if next == Some('=') => {
                tokens.push(Token::Op(CompareOp::Eq));
                index += 2;
            }
            '!' if next == Some('=') => {
                tokens.push(Token::Op(CompareOp::Ne));
                index += 2;
            }
            '!' => {
                tokens.push(Token::Bang);
                index += 1;
            }
            '<' if next == Some('>') => {
                tokens.push(Token::Op(CompareOp::Ne));
                index += 2;
            }
            '<' if next == Some('=') => {
                tokens.push(Token::Op(CompareOp::Le));
                index += 2;
            }
            '<' => {
                tokens.push(Token::Op(CompareOp::Lt));
                index += 1;
            }
            '>' if next == Some('=') => {
                tokens.push(Token::Op(CompareOp::Ge));
                index += 2;
            }
            '>' => {
                tokens.push(Token::Op(CompareOp::Gt));
                index += 1;
            }
            '&' if next == Some('&') => {
                tokens.push(Token::And);
                index += 2;
            }
            '|' if next == Some('|') => {
                tokens.push(Token::Or);
                index += 2;
            }
            '-' => {
                tokens.push(Token::Minus);
                index += 1;
            }
            '"' => {
                let mut text = String::new();
                index += 1;
                loop {
                    let Some(&c) = chars.get(index) else {
                        return Err(format!("unterminated string in JSON path {source:?}"));
                    };
                    index += 1;
                    match c {
                        '"' => break,
                        '\\' => {
                            let escaped = chars.get(index).copied().ok_or_else(|| {
                                format!("unterminated escape in JSON path {source:?}")
                            })?;
                            index += 1;
                            text.push(match escaped {
                                'n' => '\n',
                                't' => '\t',
                                'r' => '\r',
                                'b' => '\u{8}',
                                'f' => '\u{c}',
                                'u' => {
                                    let hex: String =
                                        chars.get(index..index + 4).unwrap_or(&[]).iter().collect();
                                    index += 4;
                                    u32::from_str_radix(&hex, 16)
                                        .ok()
                                        .and_then(char::from_u32)
                                        .ok_or_else(|| {
                                            format!("invalid escape in JSON path {source:?}")
                                        })?
                                }
                                other => other,
                            });
                        }
                        other => text.push(other),
                    }
                }
                tokens.push(Token::Str(text));
            }
            c if c.is_ascii_digit() => {
                let start = index;
                while index < chars.len()
                    && (chars[index].is_ascii_digit()
                        || chars[index] == '.'
                            && chars.get(index + 1).is_some_and(char::is_ascii_digit)
                        || matches!(chars[index], 'e' | 'E'))
                {
                    if matches!(chars[index], 'e' | 'E')
                        && matches!(chars.get(index + 1), Some('+' | '-'))
                    {
                        index += 1;
                    }
                    index += 1;
                }
                let text: String = chars[start..index].iter().collect();
                let number = serde_json::from_str::<Value>(&text)
                    .map_err(|_| format!("invalid number {text:?} in JSON path"))?;
                tokens.push(Token::Number(number));
            }
            c if identifier(c) => {
                let start = index;
                while index < chars.len() && identifier(chars[index]) {
                    index += 1;
                }
                tokens.push(Token::Word(chars[start..index].iter().collect()));
            }
            other => return Err(format!("unexpected {other:?} in JSON path {source:?}")),
        }
    }
    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.index)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.index).cloned();
        self.index += usize::from(token.is_some());
        token
    }

    fn expect(&mut self, expected: Token) -> Result<(), String> {
        match self.next() {
            Some(token) if token == expected => Ok(()),
            other => Err(format!(
                "expected {expected:?} in JSON path, found {other:?}"
            )),
        }
    }

    fn expr(&mut self) -> Result<Expr, String> {
        let mut left = self.and()?;
        while self.peek() == Some(&Token::Or) {
            self.index += 1;
            left = Expr::Or(Box::new(left), Box::new(self.and()?));
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<Expr, String> {
        let mut left = self.unary()?;
        while self.peek() == Some(&Token::And) {
            self.index += 1;
            left = Expr::And(Box::new(left), Box::new(self.unary()?));
        }
        Ok(left)
    }

    fn unary(&mut self) -> Result<Expr, String> {
        if self.peek() == Some(&Token::Bang) {
            self.index += 1;
            return Ok(Expr::Not(Box::new(self.unary()?)));
        }
        if matches!(self.peek(), Some(Token::Word(word)) if word == "exists")
            && self.tokens.get(self.index + 1) == Some(&Token::OpenParen)
        {
            self.index += 2;
            let inner = self.expr()?;
            self.expect(Token::CloseParen)?;
            return Ok(Expr::Exists(Box::new(inner)));
        }
        let left = self.value()?;
        match self.peek() {
            Some(Token::Op(op)) => {
                let op = *op;
                self.index += 1;
                let right = self.value()?;
                Ok(Expr::Compare(Box::new(left), op, Box::new(right)))
            }
            Some(Token::Word(word)) if word == "starts" => {
                self.index += 1;
                match self.next() {
                    Some(Token::Word(word)) if word == "with" => {}
                    other => return Err(format!("expected `with`, found {other:?}")),
                }
                let right = self.value()?;
                Ok(Expr::StartsWith(Box::new(left), Box::new(right)))
            }
            _ => Ok(left),
        }
    }

    fn value(&mut self) -> Result<Expr, String> {
        let primary = match self.next() {
            Some(Token::Dollar) => Primary::Root,
            Some(Token::At) => Primary::Current,
            Some(Token::Variable(name)) => Primary::Variable(name),
            Some(Token::Str(text)) => Primary::Literal(Value::String(text)),
            Some(Token::Number(number)) => Primary::Literal(number),
            Some(Token::Minus) => match self.next() {
                Some(Token::Number(Value::Number(number))) => Primary::Literal(
                    serde_json::from_str(&format!("-{number}"))
                        .map_err(|error| error.to_string())?,
                ),
                other => return Err(format!("expected a number after `-`, found {other:?}")),
            },
            Some(Token::Word(word)) => match word.as_str() {
                "true" => Primary::Literal(Value::Bool(true)),
                "false" => Primary::Literal(Value::Bool(false)),
                "null" => Primary::Literal(Value::Null),
                other => return Err(format!("unexpected {other:?} in JSON path")),
            },
            Some(Token::OpenParen) => {
                let inner = self.expr()?;
                self.expect(Token::CloseParen)?;
                Primary::Nested(Box::new(inner))
            }
            other => return Err(format!("unexpected {other:?} in JSON path")),
        };
        let mut steps = Vec::new();
        loop {
            match self.peek() {
                Some(Token::Dot) => {
                    self.index += 1;
                    match self.next() {
                        Some(Token::Star) => steps.push(Step::AnyMember),
                        Some(Token::Str(key)) => steps.push(Step::Member(key)),
                        Some(Token::Word(word)) => {
                            if self.peek() == Some(&Token::OpenParen) {
                                self.index += 1;
                                self.expect(Token::CloseParen)?;
                                steps.push(match word.as_str() {
                                    "type" => Step::Type,
                                    "size" => Step::Size,
                                    "keyvalue" => Step::KeyValue,
                                    "string" => Step::String,
                                    other => {
                                        return Err(format!(
                                            "unsupported JSON path method {other}()"
                                        ))
                                    }
                                });
                            } else {
                                steps.push(Step::Member(word));
                            }
                        }
                        other => return Err(format!("unexpected {other:?} after `.`")),
                    }
                }
                Some(Token::OpenBracket) => {
                    self.index += 1;
                    if self.peek() == Some(&Token::Star) {
                        self.index += 1;
                        self.expect(Token::CloseBracket)?;
                        steps.push(Step::AnyElement);
                        continue;
                    }
                    let mut ranges = Vec::new();
                    loop {
                        let start = self.index_value()?;
                        let end = if matches!(self.peek(), Some(Token::Word(word)) if word == "to")
                        {
                            self.index += 1;
                            Some(self.index_value()?)
                        } else {
                            None
                        };
                        ranges.push((start, end));
                        match self.next() {
                            Some(Token::Comma) => continue,
                            Some(Token::CloseBracket) => break,
                            other => return Err(format!("unexpected {other:?} in subscript")),
                        }
                    }
                    steps.push(Step::Elements(ranges));
                }
                Some(Token::Question) => {
                    self.index += 1;
                    self.expect(Token::OpenParen)?;
                    let predicate = self.expr()?;
                    self.expect(Token::CloseParen)?;
                    steps.push(Step::Filter(Box::new(predicate)));
                }
                _ => break,
            }
        }
        Ok(Expr::Path(primary, steps))
    }

    fn index_value(&mut self) -> Result<i64, String> {
        match self.next() {
            Some(Token::Number(Value::Number(number))) => number
                .as_i64()
                .ok_or_else(|| format!("invalid subscript {number}")),
            other => Err(format!("expected a subscript, found {other:?}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn query(path: &str, document: Value) -> Vec<Value> {
        JsonPath::parse(path).unwrap().query(&document, None)
    }

    #[test]
    fn lax_accessors_unwrap_and_wrap() {
        assert_eq!(
            query("$.teams[*]", json!({"teams":["a","b"]})),
            [json!("a"), json!("b")]
        );
        assert_eq!(
            query("$.selected[*]", json!({"selected":"x"})),
            [json!("x")]
        );
        assert_eq!(query("$.missing", json!({})), Vec::<Value>::new());
        assert_eq!(
            query("$.items.id", json!({"items":[{"id":1},{"id":2}]})),
            [json!(1), json!(2)]
        );
        assert_eq!(
            query("$.\"payload\".first", json!({"payload":{"first":"one"}})),
            [json!("one")]
        );
        assert_eq!(query("$[0 to 1]", json!([1, 2, 3])), [json!(1), json!(2)]);
    }

    #[test]
    fn filters_methods_and_variables() {
        let document = json!({"value":[{"fieldId":"a","selected":"x"},{"other":1}]});
        assert_eq!(
            query("$.value[*] ? (exists(@.fieldId))", document),
            [json!({"fieldId":"a","selected":"x"})]
        );
        let document = json!({"value":{"place":{"kind":"reference","refId":"t"},"label":"x"}});
        let path =
            "$.value ? (@.type() == \"object\").keyvalue() ? (@.value.kind == \"reference\")";
        assert_eq!(
            query(path, document),
            [json!({"key":"place","value":{"kind":"reference","refId":"t"}})]
        );
        let vars = json!({"entry":{"key":"place"}});
        let configuration =
            json!({"attributes":[{"id":"place","reference":"teams"},{"id":"x","reference":"y"}]});
        assert_eq!(
            JsonPath::parse("$.attributes[*] ? (@.id == $entry.key).reference")
                .unwrap()
                .query_first(&configuration, vars.as_object()),
            Some(json!("teams"))
        );
    }

    #[test]
    fn predicates_follow_jsonb_path_match() {
        let live = JsonPath::parse("$.deletedAt == null").unwrap();
        assert_eq!(live.matches(&json!({"deletedAt":null}), None), Some(true));
        assert_eq!(live.matches(&json!({"deletedAt":1}), None), Some(false));
        assert_eq!(live.matches(&json!({}), None), Some(false));
        assert_eq!(
            JsonPath::parse("true").unwrap().matches(&json!({}), None),
            Some(true)
        );
        let entity = JsonPath::parse("$.options.source == \"entity\"").unwrap();
        assert_eq!(
            entity.matches(&json!({"options":{"source":"entity"}}), None),
            Some(true)
        );
        assert_eq!(entity.matches(&json!({"options":{"source":1}}), None), None);
        assert!(JsonPath::parse("$.a ? (").is_err());
        assert!(JsonPath::parse("$.a.unknown()").is_err());
        assert_eq!(
            JsonPath::parse("$.payload.first").unwrap().root_member(),
            Some("payload")
        );
    }
}
