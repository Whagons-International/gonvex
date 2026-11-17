//! The SQL/JSON path subset reference declarations use, evaluated in lax mode
//! with PostgreSQL's semantics (array auto-unwrapping, existential and
//! three-valued comparisons, `jsonb_path_match` for predicates).
//!
//! Supported: `$`, `@`, `$name` variables, `.key`, `."key"`, `.*`, `.**`, `[*]`,
//! `[n]`, `[n to m]`, `? (filter)`, `.type()`, `.size()`, `.keyvalue()`,
//! `.string()`, literals, `== != <> < <= > >=`, `&&`, `||`, `!`,
//! `exists(…)`, `starts with` and an optional `lax`/`strict` prefix.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeSet;

use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct JsonPath {
    expr: Expr,
    lax: bool,
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
    Descendants,
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
    /// Exact object-member projection; no filters, array traversal or inference.
    pub(crate) fn member_path(&self) -> Option<Vec<String>> {
        match &self.expr {
            Expr::Path(Primary::Root, steps) if !steps.is_empty() => steps
                .iter()
                .map(|step| match step {
                    Step::Member(name) => Some(name.clone()),
                    _ => None,
                })
                .collect(),
            _ => None,
        }
    }

    pub(crate) fn is_root(&self) -> bool {
        matches!(&self.expr, Expr::Path(Primary::Root, steps) if steps.is_empty())
    }

    /// Document fields this expression may read. An unconstrained root or
    /// wildcard needs the full row; caller must not guess a smaller projection.
    pub(crate) fn document_members(
        &self,
        root_is_document: bool,
        entry_is_document: bool,
    ) -> Option<BTreeSet<String>> {
        fn visit(expr: &Expr, root: bool, entry: bool, names: &mut BTreeSet<String>) -> Option<()> {
            match expr {
                Expr::Path(primary, steps) => {
                    let document = matches!(primary, Primary::Root) && root
                        || matches!(primary, Primary::Variable(name) if name == "document" || entry && name == "entry");
                    if document {
                        match steps.first() {
                            Some(Step::Member(name)) => {
                                names.insert(name.clone());
                            }
                            _ => return None,
                        }
                    }
                    if let Primary::Nested(inner) = primary {
                        visit(inner, root, entry, names)?;
                    }
                    for step in steps {
                        if let Step::Filter(inner) = step {
                            visit(inner, root, entry, names)?;
                        }
                    }
                }
                Expr::Compare(left, _, right)
                | Expr::StartsWith(left, right)
                | Expr::And(left, right)
                | Expr::Or(left, right) => {
                    visit(left, root, entry, names)?;
                    visit(right, root, entry, names)?;
                }
                Expr::Not(inner) | Expr::Exists(inner) => visit(inner, root, entry, names)?,
            }
            Some(())
        }
        let mut names = BTreeSet::new();
        visit(&self.expr, root_is_document, entry_is_document, &mut names)?;
        Some(names)
    }

    pub(crate) fn parse(source: &str) -> Result<Self, String> {
        let mut parser = Parser {
            tokens: tokenize(source)?,
            index: 0,
        };
        let lax = !matches!(parser.peek(), Some(Token::Word(word)) if word == "strict");
        if matches!(parser.peek(), Some(Token::Word(word)) if word == "lax" || word == "strict") {
            parser.index += 1;
        }
        let expr = parser.expr()?;
        if parser.index != parser.tokens.len() {
            return Err(format!("unexpected input in JSON path {source:?}"));
        }
        Ok(Self { expr, lax })
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
            lax: self.lax,
        };
        match &self.expr {
            Expr::Path(..) => context
                .items(&self.expr, None)
                .unwrap_or_default()
                .into_iter()
                .map(Cow::into_owned)
                .collect(),
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
            lax: self.lax,
        };
        match &self.expr {
            Expr::Path(..) => match context.items(&self.expr, None).ok()?.as_slice() {
                [value] => value.as_bool(),
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
    lax: bool,
}

impl Context<'_> {
    fn unwrap<'a>(&self, items: Vec<Cow<'a, Value>>) -> Vec<Cow<'a, Value>> {
        if self.lax {
            unwrap(items)
        } else {
            items
        }
    }
    fn items<'a>(
        &'a self,
        expr: &'a Expr,
        current: Option<&'a Value>,
    ) -> Result<Vec<Cow<'a, Value>>, ()> {
        match expr {
            Expr::Path(primary, steps) => {
                let mut items = match primary {
                    Primary::Root => vec![Cow::Borrowed(self.root)],
                    Primary::Current => vec![Cow::Borrowed(current.ok_or(())?)],
                    Primary::Variable(name) => {
                        vec![Cow::Borrowed(
                            self.vars.and_then(|vars| vars.get(name)).ok_or(())?,
                        )]
                    }
                    Primary::Literal(value) => vec![Cow::Borrowed(value)],
                    Primary::Nested(inner) => self.items(inner, current)?,
                };
                for step in steps {
                    items = self.step(step, items)?;
                }
                Ok(items)
            }
            predicate => Ok(match self.truth(predicate, current) {
                Truth::True => vec![Cow::Owned(Value::Bool(true))],
                Truth::False => vec![Cow::Owned(Value::Bool(false))],
                Truth::Unknown => vec![Cow::Owned(Value::Null)],
            }),
        }
    }

    fn step<'a>(
        &'a self,
        step: &'a Step,
        items: Vec<Cow<'a, Value>>,
    ) -> Result<Vec<Cow<'a, Value>>, ()> {
        let mut output = Vec::new();
        match step {
            Step::Member(name) => {
                for item in self.unwrap(items) {
                    match item {
                        Cow::Borrowed(Value::Object(object)) => {
                            output.extend(object.get(name).map(Cow::Borrowed))
                        }
                        Cow::Owned(Value::Object(mut object)) => {
                            output.extend(object.remove(name).map(Cow::Owned))
                        }
                        _ => {}
                    }
                }
            }
            Step::Descendants => {
                for item in items {
                    // Borrow the tree and visit each node once. Cloning every
                    // ancestor subtree would turn a deep form into quadratic work.
                    match item {
                        Cow::Borrowed(value) => {
                            let mut pending = vec![value];
                            while let Some(value) = pending.pop() {
                                output.push(Cow::Borrowed(value));
                                match value {
                                    Value::Object(object) => pending.extend(object.values().rev()),
                                    Value::Array(array) => pending.extend(array.iter().rev()),
                                    _ => {}
                                }
                            }
                        }
                        Cow::Owned(value) => {
                            let mut pending = vec![&value];
                            while let Some(value) = pending.pop() {
                                output.push(Cow::Owned(value.clone()));
                                match value {
                                    Value::Object(object) => pending.extend(object.values().rev()),
                                    Value::Array(array) => pending.extend(array.iter().rev()),
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
            Step::AnyMember => {
                for item in self.unwrap(items) {
                    match item {
                        Cow::Borrowed(Value::Object(object)) => {
                            output.extend(object.values().map(Cow::Borrowed))
                        }
                        Cow::Owned(Value::Object(object)) => {
                            output.extend(object.into_values().map(Cow::Owned))
                        }
                        _ => {}
                    }
                }
            }
            Step::AnyElement => {
                for item in items {
                    match item {
                        Cow::Borrowed(Value::Array(elements)) => {
                            output.extend(elements.iter().map(Cow::Borrowed))
                        }
                        Cow::Owned(Value::Array(elements)) => {
                            output.extend(elements.into_iter().map(Cow::Owned))
                        }
                        other if self.lax => output.push(other),
                        _ => {}
                    }
                }
            }
            Step::Elements(ranges) => {
                for item in items {
                    if !self.lax && !item.is_array() {
                        continue;
                    }
                    let elements = unwrap(vec![item]);
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
                for item in self.unwrap(items) {
                    if self.truth(predicate, Some(&item)) == Truth::True {
                        output.push(item);
                    }
                }
            }
            Step::Type => {
                for item in items {
                    output.push(Cow::Owned(Value::String(type_name(&item).to_owned())));
                }
            }
            Step::Size => {
                for item in items {
                    let size = match item.as_ref() {
                        Value::Array(elements) => elements.len(),
                        _ => 1,
                    };
                    output.push(Cow::Owned(Value::from(size)));
                }
            }
            Step::KeyValue => {
                for item in self.unwrap(items) {
                    match item.into_owned() {
                        Value::Object(object) => {
                            for (key, value) in object {
                                output.push(Cow::Owned(
                                    serde_json::json!({"key": key, "value": value}),
                                ));
                            }
                        }
                        _ => return Err(()),
                    }
                }
            }
            Step::String => {
                for item in self.unwrap(items) {
                    output.push(Cow::Owned(match item.into_owned() {
                        Value::String(text) => Value::String(text),
                        Value::Number(number) => Value::String(number.to_string()),
                        Value::Bool(value) => Value::String(value.to_string()),
                        _ => return Err(()),
                    }));
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
                let (left, right) = (self.unwrap(left), self.unwrap(right));
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
                let [prefix] = right.as_slice() else {
                    return Truth::Unknown;
                };
                let Some(prefix) = prefix.as_str() else {
                    return Truth::Unknown;
                };
                let mut unknown = false;
                for item in self.unwrap(left) {
                    match item.as_ref() {
                        Value::String(text) if text.starts_with(prefix) => return Truth::True,
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
                    [value] => value.as_bool().map_or(Truth::Unknown, Truth::from_bool),
                    _ => Truth::Unknown,
                },
                Err(()) => Truth::Unknown,
            },
        }
    }
}

fn unwrap(items: Vec<Cow<'_, Value>>) -> Vec<Cow<'_, Value>> {
    let mut output = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Cow::Borrowed(Value::Array(elements)) => {
                output.extend(elements.iter().map(Cow::Borrowed))
            }
            Cow::Owned(Value::Array(elements)) => {
                output.extend(elements.into_iter().map(Cow::Owned))
            }
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
                        Some(Token::Star) => {
                            if self.peek() == Some(&Token::Star) {
                                self.index += 1;
                                steps.push(Step::Descendants);
                            } else {
                                steps.push(Step::AnyMember);
                            }
                        }
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
        assert!(query("strict $.items.id", json!({"items":[{"id":1}]})).is_empty());
        assert_eq!(
            query("strict $.items[*].id", json!({"items":[{"id":1}]})),
            [json!(1)]
        );
        assert!(query("strict $[*]", json!("scalar")).is_empty());
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
    fn recursive_fields_and_object_containers_keep_typed_values() {
        let document = json!({"fields":[
            {"properties":{"optionListId":"top"},"children":[{"properties":{"optionListId":"nested"}}]},
            {"properties":{"optionListId":null}}, {"properties":{"optionListId":27}}
        ],"irrelevant":{"optionListId":"outside"}});
        let selected = query(
            "strict $.fields.** ? (@.type() == \"object\").properties.optionListId",
            document,
        );
        assert_eq!(
            selected,
            [json!("top"), json!("nested"), Value::Null, json!(27)]
        );
        let document = json!({"output":{"group":{"evidence_segment_ids":["a","b"]},"more":[{"evidence_segment_ids":["c"]}]}});
        let path = JsonPath::parse("strict $.output.** ? (@.type() == \"object\")").unwrap();
        let ids = JsonPath::parse("$.evidence_segment_ids[*]").unwrap();
        let selected: Vec<_> = path
            .query(&document, None)
            .iter()
            .flat_map(|container| ids.query(container, None))
            .collect();
        assert_eq!(selected, [json!("a"), json!("b"), json!("c")]);
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
