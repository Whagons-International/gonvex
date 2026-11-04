//! Delivery v4: replica rows stay the JSON text SQLite produced
//! (`gonvex_store::rows::json_object_expr`), and their integrity hashes are
//! computed from those bytes without building a `serde_json::Value`.
//!
//! A row hash is SHA-256 of the row's canonical JSON (`replica::stable_json`:
//! object keys sorted, integers as written, other numbers in JavaScript's
//! shortest form, strings escaped as serde_json does plus U+2028/U+2029). Most
//! rows SQLite writes are already canonical except for the order of their
//! top-level columns and the spelling of non-integer numbers: no nested
//! objects with unsorted keys, no unusual escapes. [`check`] verifies that in
//! one pass over the bytes, recording each number token whose canonical form
//! differs (a REAL written by SQLite with 17 significant digits, `1.0`,
//! `1E5`, `-0`, an integer beyond 64 bits): it is parsed as serde_json does
//! (correctly rounded f64) and written as `stable_json` writes it (JavaScript
//! shortest form, `ryu_js`). Nested objects whose keys are out of order (a
//! JSON column such as `{"mode":…,"teamIds":…,"roles":…}`) are recorded with
//! their members and re-ordered the same way. [`canonical`] then returns the
//! bytes, borrowed when nothing differs, else re-assembled (members of every
//! recorded object in key order, numbers replaced). Anything else it cannot
//! prove canonical (an escape serde would not write, U+2028/U+2029, an escaped
//! or duplicate key, whitespace, deep nesting, a non-finite number) falls
//! back to parsing the row and writing `stable_json`, so the hash is always
//! byte-identical to the `Value` path.

use std::borrow::Cow;
use std::ops::Range;

use serde_json::Value;

/// Nesting deeper than this is not checked (falls back).
const MAX_DEPTH: usize = 64;

/// One top-level member of a row object: key text (no escapes) and the byte
/// range of its value.
#[derive(Clone, Debug)]
pub(crate) struct Member {
    pub key: Range<usize>,
    pub value: Range<usize>,
}

/// A nested object whose keys are not in canonical order: its byte range
/// and its members, sorted by key.
#[derive(Clone, Debug)]
pub(crate) struct Reorder {
    pub range: Range<usize>,
    pub members: Vec<Member>,
}

/// The result of checking a row.
pub(crate) struct Checked {
    pub members: Vec<Member>,
    /// Top-level keys already in canonical (byte) order.
    pub sorted: bool,
    /// Number tokens whose canonical spelling differs, in text order.
    pub numbers: Vec<(Range<usize>, String)>,
    /// Nested objects to re-order, by start offset.
    pub objects: Vec<Reorder>,
}

impl Checked {
    /// Whether the row text already is its canonical JSON.
    pub fn canonical(&self) -> bool {
        self.sorted && self.numbers.is_empty() && self.objects.is_empty()
    }
}

struct Checker<'a> {
    bytes: &'a [u8],
    at: usize,
    numbers: Vec<(Range<usize>, String)>,
    objects: Vec<Reorder>,
}

impl Checker<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn expect(&mut self, byte: u8) -> Option<()> {
        (self.peek()? == byte).then(|| self.at += 1)
    }

    fn literal(&mut self, text: &[u8]) -> Option<()> {
        self.bytes
            .get(self.at..self.at + text.len())
            .filter(|found| *found == text)
            .map(|_| self.at += text.len())
    }

    /// A string whose escapes are exactly serde_json's; returns whether it
    /// contained any escape (keys with escapes are not compared raw).
    fn string(&mut self) -> Option<bool> {
        self.expect(b'"')?;
        let mut escaped = false;
        loop {
            let byte = self.peek()?;
            match byte {
                b'"' => {
                    self.at += 1;
                    return Some(escaped);
                }
                0..=0x1f => return None,
                b'\\' => {
                    escaped = true;
                    let kind = *self.bytes.get(self.at + 1)?;
                    match kind {
                        b'"' | b'\\' | b'b' | b'f' | b'n' | b'r' | b't' => self.at += 2,
                        b'u' => {
                            // serde writes \u00XX (lowercase) only for the
                            // control characters without a short escape.
                            let hex = self.bytes.get(self.at + 2..self.at + 6)?;
                            if &hex[..2] != b"00" {
                                return None;
                            }
                            let digit = |byte: u8| match byte {
                                b'0'..=b'9' => Some(byte - b'0'),
                                b'a'..=b'f' => Some(byte - b'a' + 10),
                                _ => None,
                            };
                            let value = digit(hex[2])? * 16 + digit(hex[3])?;
                            if value >= 0x20 || matches!(value, 0x08 | 0x09 | 0x0a | 0x0c | 0x0d) {
                                return None;
                            }
                            self.at += 6;
                        }
                        _ => return None,
                    }
                }
                // U+2028 / U+2029 (E2 80 A8 / E2 80 A9) are escaped by the
                // canonical writer.
                0xE2 if self.bytes.get(self.at + 1) == Some(&0x80)
                    && matches!(self.bytes.get(self.at + 2), Some(0xA8 | 0xA9)) =>
                {
                    return None
                }
                _ => self.at += 1,
            }
        }
    }

    /// A number token. Recorded for replacement when serde_json would not
    /// parse it to an integer written back unchanged (`stable_json` writes
    /// i64 / u64 as digits and every other number as `ryu_js` of its f64).
    fn number(&mut self) -> Option<()> {
        let start = self.at;
        let negative = self.peek() == Some(b'-');
        if negative {
            self.at += 1;
        }
        let digits_start = self.at;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.at += 1;
        }
        let digits = self.at - digits_start;
        if digits == 0 || (digits > 1 && self.bytes[digits_start] == b'0') {
            return None;
        }
        let mut float = false;
        if self.peek() == Some(b'.') {
            float = true;
            self.at += 1;
            let fraction = self.at;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.at += 1;
            }
            if self.at == fraction {
                return None;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            float = true;
            self.at += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.at += 1;
            }
            let exponent = self.at;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.at += 1;
            }
            if self.at == exponent {
                return None;
            }
        }
        let token = std::str::from_utf8(&self.bytes[start..self.at]).ok()?;
        if !float {
            // serde_json: u64 when positive and in range, i64 when negative
            // and in range; `-0` and anything out of range become an f64.
            let integer = if negative {
                token
                    .parse::<i64>()
                    .ok()
                    .filter(|value| *value != 0)
                    .is_some()
            } else {
                token.parse::<u64>().is_ok()
            };
            if integer {
                return Some(());
            }
        }
        let value = token
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())?;
        let canonical = ryu_js::Buffer::new().format(value).to_owned();
        if canonical != token {
            self.numbers.push((start..self.at, canonical));
        }
        Some(())
    }

    fn value(&mut self, depth: usize) -> Option<()> {
        if depth > MAX_DEPTH {
            return None;
        }
        match self.peek()? {
            b'{' => {
                let object_start = self.at;
                self.at += 1;
                if self.peek()? == b'}' {
                    self.at += 1;
                    return Some(());
                }
                let mut previous: Option<Range<usize>> = None;
                let mut sorted = true;
                loop {
                    let start = self.at + 1;
                    if self.string()? {
                        return None;
                    }
                    let key = start..self.at - 1;
                    if let Some(previous) = &previous {
                        if self.bytes[previous.clone()] >= self.bytes[key.clone()] {
                            sorted = false;
                        }
                    }
                    previous = Some(key);
                    self.expect(b':')?;
                    self.value(depth + 1)?;
                    match self.peek()? {
                        b',' => self.at += 1,
                        b'}' => {
                            self.at += 1;
                            if !sorted {
                                let members = self.members_of(object_start, depth)?;
                                self.objects.push(Reorder {
                                    range: object_start..self.at,
                                    members,
                                });
                            }
                            return Some(());
                        }
                        _ => return None,
                    }
                }
            }
            b'[' => {
                self.at += 1;
                if self.peek()? == b']' {
                    self.at += 1;
                    return Some(());
                }
                loop {
                    self.value(depth + 1)?;
                    match self.peek()? {
                        b',' => self.at += 1,
                        b']' => {
                            self.at += 1;
                            return Some(());
                        }
                        _ => return None,
                    }
                }
            }
            b'"' => self.string().map(|_| ()),
            b't' => self.literal(b"true"),
            b'f' => self.literal(b"false"),
            b'n' => self.literal(b"null"),
            b'-' | b'0'..=b'9' => self.number(),
            _ => None,
        }
    }

    /// The members of the (already checked) object at `start`, sorted by
    /// key; None when two keys are equal (serde keeps the last one).
    fn members_of(&self, start: usize, depth: usize) -> Option<Vec<Member>> {
        let mut scan = Checker {
            bytes: self.bytes,
            at: start + 1,
            numbers: Vec::new(),
            objects: Vec::new(),
        };
        let mut members = Vec::new();
        loop {
            let key_start = scan.at + 1;
            scan.string()?;
            let key = key_start..scan.at - 1;
            scan.expect(b':')?;
            let value_start = scan.at;
            scan.value(depth + 1)?;
            members.push(Member {
                key,
                value: value_start..scan.at,
            });
            match scan.peek()? {
                b',' => scan.at += 1,
                _ => break,
            }
        }
        let bytes = self.bytes;
        members
            .sort_unstable_by(|left, right| bytes[left.key.clone()].cmp(&bytes[right.key.clone()]));
        if members
            .windows(2)
            .any(|pair| bytes[pair[0].key.clone()] == bytes[pair[1].key.clone()])
        {
            return None;
        }
        Some(members)
    }

    /// The top-level object: members are recorded, their order is checked
    /// by the caller.
    fn row(mut self, mut members: Vec<Member>) -> Option<Checked> {
        self.expect(b'{')?;
        members.clear();
        members.reserve(32);
        let mut sorted = true;
        if self.peek()? == b'}' {
            self.at += 1;
        } else {
            loop {
                let start = self.at + 1;
                if self.string()? {
                    return None;
                }
                let key = start..self.at - 1;
                self.expect(b':')?;
                let value_start = self.at;
                self.value(1)?;
                if let Some(last) = members.last() {
                    let last: &Member = last;
                    if self.bytes[last.key.clone()] >= self.bytes[key.clone()] {
                        sorted = false;
                    }
                }
                members.push(Member {
                    key,
                    value: value_start..self.at,
                });
                match self.peek()? {
                    b',' => self.at += 1,
                    b'}' => {
                        self.at += 1;
                        break;
                    }
                    _ => return None,
                }
            }
        }
        if self.at != self.bytes.len() {
            return None;
        }
        if !sorted {
            let mut keys: Vec<&[u8]> = members
                .iter()
                .map(|member| &self.bytes[member.key.clone()])
                .collect();
            keys.sort_unstable();
            if keys.windows(2).any(|pair| pair[0] == pair[1]) {
                return None;
            }
        }
        // Inner objects complete (and are recorded) before outer ones.
        self.objects.sort_by_key(|object| object.range.start);
        Some(Checked {
            members,
            sorted,
            numbers: self.numbers,
            objects: self.objects,
        })
    }
}

/// Checks that `row` is canonical JSON up to the order of its top-level
/// members. None: not provably canonical (use [`canonical`]'s fallback).
pub(crate) fn check(row: &str) -> Option<Checked> {
    check_reusing(row, &mut Vec::new())
}

/// [`check`] with the member list allocated from `spare` (give it back with
/// [`recycle`]): no allocation per row on the common path.
pub(crate) fn check_reusing(row: &str, spare: &mut Vec<Member>) -> Option<Checked> {
    Checker {
        bytes: row.as_bytes(),
        at: 0,
        numbers: Vec::new(),
        objects: Vec::new(),
    }
    .row(std::mem::take(spare))
}

/// Returns a checked row's member list to `spare` for the next row.
pub(crate) fn recycle(checked: Checked, spare: &mut Vec<Member>) {
    let mut members = checked.members;
    members.clear();
    *spare = members;
}

/// Top-level member `key` (the last one with that key, as serde keeps).
pub(crate) fn find<'c>(row: &str, members: &'c [Member], key: &str) -> Option<&'c Member> {
    members
        .iter()
        .rev()
        .find(|member| &row[member.key.clone()] == key)
}

/// A key column's value as the row key (`replica::row_key`): a string's
/// text (borrowed when it has no escapes), a number's or boolean's text.
/// None for any other value.
pub(crate) fn member_key(row: &str, member: &Member) -> Option<std::sync::Arc<str>> {
    let value = &row[member.value.clone()];
    if value.len() >= 2 && value.starts_with('"') && !value.contains('\\') {
        return Some(std::sync::Arc::from(&value[1..value.len() - 1]));
    }
    match serde_json::from_str::<Value>(value).ok()? {
        Value::String(text) => Some(text.into()),
        Value::Number(number) => Some(number.to_string().into()),
        Value::Bool(flag) => Some(flag.to_string().into()),
        _ => None,
    }
}

/// Hex SHA-256 of `bytes` as shared text (one allocation).
pub(crate) fn hex_sha256(bytes: &[u8]) -> std::sync::Arc<str> {
    use sha2::{Digest, Sha256};
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut out = [0u8; 64];
    for (index, byte) in digest.iter().enumerate() {
        out[index * 2] = HEX[usize::from(byte >> 4)];
        out[index * 2 + 1] = HEX[usize::from(byte & 15)];
    }
    std::sync::Arc::from(std::str::from_utf8(&out).expect("hex is ASCII"))
}

/// The parsed value of `member` (key and order columns only).
pub(crate) fn value_of(row: &str, member: Option<&Member>) -> Value {
    member
        .and_then(|member| serde_json::from_str(&row[member.value.clone()]).ok())
        .unwrap_or(Value::Null)
}

/// A checked row as a `JsonRow` (one allocation; the scanner validated it).
pub(crate) fn json_row(row: &str, _checked: &Checked) -> gonvex_protocol::JsonRow {
    // SAFETY: `check` accepted `row`: one JSON object, no surrounding
    // whitespace (`Checker::row` requires the object to end the text).
    unsafe { gonvex_protocol::JsonRow::from_str_unchecked(row) }
}

/// The canonical JSON text of `row` (a JSON object), as
/// `stable_json(serde_json::from_str(row))` writes it. Borrowed when the row
/// already is canonical; re-ordered into `scratch` when only its top-level
/// members are out of order; otherwise parsed and re-written. Errors when
/// the row is not valid JSON.
pub(crate) fn canonical<'a>(
    row: &'a str,
    checked: Option<&Checked>,
    scratch: &'a mut String,
) -> Result<Cow<'a, str>, serde_json::Error> {
    match checked {
        Some(checked) if checked.canonical() => Ok(Cow::Borrowed(row)),
        Some(checked) => {
            scratch.clear();
            scratch.reserve(row.len() + 16);
            if checked.sorted {
                emit_object(row, checked, checked.members.iter(), scratch);
            } else {
                let mut members: Vec<&Member> = checked.members.iter().collect();
                members.sort_unstable_by(|left, right| {
                    row.as_bytes()[left.key.clone()].cmp(&row.as_bytes()[right.key.clone()])
                });
                emit_object(row, checked, members.into_iter(), scratch);
            }
            Ok(Cow::Borrowed(scratch.as_str()))
        }
        None => {
            let value: Value = serde_json::from_str(row)?;
            Ok(Cow::Owned(super::replica::stable_json_text(&value)))
        }
    }
}

/// `members` (in the order given) as a canonical object.
fn emit_object<'m>(
    row: &str,
    checked: &Checked,
    members: impl Iterator<Item = &'m Member>,
    out: &mut String,
) {
    out.push('{');
    for (index, member) in members.enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push('"');
        out.push_str(&row[member.key.clone()]);
        out.push_str("\":");
        emit(row, checked, member.value.clone(), out);
    }
    out.push('}');
}

/// `row[range]` with its replaced numbers and re-ordered objects.
fn emit(row: &str, checked: &Checked, range: Range<usize>, out: &mut String) {
    let mut at = range.start;
    let mut number = checked.numbers.partition_point(|(span, _)| span.start < at);
    let mut object = checked
        .objects
        .partition_point(|reorder| reorder.range.start < at);
    loop {
        let next_number = checked
            .numbers
            .get(number)
            .filter(|(span, _)| span.end <= range.end);
        let next_object = checked
            .objects
            .get(object)
            .filter(|reorder| reorder.range.end <= range.end);
        match (next_number, next_object) {
            (None, None) => break,
            (Some((span, canonical)), next_object)
                if next_object.is_none_or(|reorder| span.start < reorder.range.start) =>
            {
                out.push_str(&row[at..span.start]);
                out.push_str(canonical);
                at = span.end;
                number += 1;
            }
            (_, Some(reorder)) => {
                out.push_str(&row[at..reorder.range.start]);
                emit_object(row, checked, reorder.members.iter(), out);
                at = reorder.range.end;
                number = checked.numbers.partition_point(|(span, _)| span.start < at);
                object = checked
                    .objects
                    .partition_point(|reorder| reorder.range.start < at);
            }
            (Some(_), None) => unreachable!("matched by the first number arm"),
        }
    }
    out.push_str(&row[at..range.end]);
}

/// The value of top-level member `key`, parsed (a small slice: key and order
/// columns only).
#[cfg(test)]
pub(crate) fn member_value(row: &str, checked: &Checked, key: &str) -> Option<Value> {
    checked
        .members
        .iter()
        .find(|member| &row[member.key.clone()] == key)
        .and_then(|member| serde_json::from_str(&row[member.value.clone()]).ok())
}

/// (raw-path hash, `Value`-path hash, whether the fast path applied) of a
/// row (tests and the perf fixture).
#[cfg(any(test, feature = "perf-fixture"))]
pub(crate) fn hashes(row: &str) -> Result<(String, String, bool), serde_json::Error> {
    use sha2::{Digest, Sha256};
    let hex = |text: &str| -> String {
        Sha256::digest(text.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    };
    let checked = check(row);
    let mut scratch = String::new();
    let raw = hex(&canonical(row, checked.as_ref(), &mut scratch)?);
    let value: Value = serde_json::from_str(row)?;
    Ok((
        raw,
        hex(&super::replica::stable_json_text(&value)),
        checked.is_some(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn hash(text: &str) -> String {
        Sha256::digest(text.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    /// The raw path's hash and the `Value` path's hash of `row`.
    fn both(row: &str) -> (String, String, bool) {
        let checked = check(row);
        let fast = checked.is_some();
        let mut scratch = String::new();
        let raw = canonical(row, checked.as_ref(), &mut scratch).unwrap();
        let value: Value = serde_json::from_str(row).unwrap();
        (
            hash(&raw),
            hash(&crate::replica::stable_json_text(&value)),
            fast,
        )
    }

    #[test]
    fn targeted_cases_hash_like_the_value_path() {
        let cases = [
            (r#"{"a":1,"b":"x"}"#, true),
            (r#"{"b":"x","a":1}"#, true),
            (r#"{"a":0.10000000000000001}"#, true),
            (r#"{"a":0.1}"#, true),
            (r#"{"b":[1.5,{"c":2.50}],"a":1e-7}"#, true),
            (r#"{"a":1e-400,"b":5e-324}"#, true),
            (r#"{"a":1.0}"#, true),
            (r#"{"a":1e21}"#, true),
            (r#"{"a":1E5}"#, true),
            (r#"{"a":-0}"#, true),
            (
                r#"{"a":-0.0,"b":-9223372036854775808,"c":-9223372036854775809}"#,
                true,
            ),
            (r#"{"a":-5,"b":0}"#, true),
            (r#"{"a":123456789012345678}"#, true),
            (r#"{"a":1234567890123456789}"#, true),
            (r#"{"a":18446744073709551615}"#, true),
            (r#"{"a":18446744073709551616}"#, true),
            (r#"{"a":99999999999999999999999}"#, true),
            (r#"{"a":"\u001f\n\t\"\\"}"#, true),
            (r#"{"a":"\u001F"}"#, false),
            (r#"{"a":"\u000a"}"#, false),
            (r#"{"a":"\/"}"#, false),
            (r#"{"a":"\u00e9"}"#, false),
            (r#"{"a":"\ud83d\ude00"}"#, false),
            ("{\"a\":\"é ü 日本 😀 \u{7f}\"}", true),
            ("{\"a\":\"line\u{2028}sep\u{2029}\"}", false),
            (r#"{"a":"😀"}"#, true),
            (r#"{"z":{"b":1,"a":2},"a":null}"#, true),
            (r#"{"z":{"a":1,"b":[1,{"y":true,"x":false}]}}"#, true),
            (
                r#"{"b":{"y":{"q":1.0,"p":[{"d":2,"c":1e2},{"b":{"z":0.10000000000000001,"a":[]}}]},"x":0.5E1},"a":-0}"#,
                true,
            ),
            (r#"{"mode":"teams","teamIds":["t"],"roles":[]}"#, true),
            (
                r#"{"a":{"mode":"teams","teamIds":["t"],"roles":[]},"b":{"d":{},"c":{"f":1,"e":[{"h":1,"g":2}]}}}"#,
                true,
            ),
            (r#"{"z":{"b":1,"a":2,"b":3}}"#, false),
            (r#"{"z":{"b":1,"a\"x":2}}"#, false),
            (r#"{"z":{"a":1,"b":[1,{"x":true,"y":false}]}}"#, true),
            (r#"{"z":{"a":1,"a":2}}"#, false),
            (r#"{"a":1,"a":2}"#, false),
            (r#"{"a\"b":1}"#, false),
            (r#"{ "a": 1 }"#, false),
            (r#"{}"#, true),
            (r#"{"a":[],"b":{},"c":[null,true,false]}"#, true),
            (r#"{"Z":1,"_id":"k","a":2,"é":3}"#, true),
            (r#"{"é":3,"a":2,"_id":"k","Z":1}"#, true),
        ];
        for (row, fast) in cases {
            let (raw, value, checked) = both(row);
            assert_eq!(raw, value, "hash of {row}");
            assert_eq!(checked, fast, "fast path for {row}");
        }
    }

    /// Rows as SQLite writes them for every column kind, including REALs
    /// written with 17 digits, JSON columns with unsorted nested keys,
    /// unicode, control characters, BLOBs and booleans.
    #[tokio::test]
    async fn sqlite_rows_hash_like_the_value_path() {
        use gonvex_store::rows::{json_object_expr, JsonKind};
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            r#"CREATE TABLE t ("_id" TEXT, "n" INTEGER, "r" REAL, "s" TEXT, "j" JSON, "b" BOOLEAN, "x" BLOB, "Zed" TEXT)"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let reals = [
            0.1,
            0.2,
            0.30000000000000004,
            1.0,
            -0.0,
            1e21,
            1e-7,
            123.456,
            5e-324,
            1.7976931348623157e308,
            2.5,
            100.0,
            -3.75,
            1.0 / 3.0,
        ];
        let texts = [
            "plain",
            "é ü 日本 😀",
            "quote \" backslash \\",
            "ctl \u{1} \u{1f} \n \t \r \u{8} \u{c}",
            "sep \u{2028} \u{2029}",
            "del \u{7f}",
            "",
            "slash /",
        ];
        let jsons = [
            r#"{"b":1,"a":[2,3]}"#,
            r#"{"a":1.5}"#,
            r#"[1,"x",{"z":null,"y":true}]"#,
            r#"{"a":{"d":1,"c":2}}"#,
            r#"{"a":1,"b":"x"}"#,
            "not json",
            r#"  {"spaced" : 1 }  "#,
            r#""str""#,
            "12",
            "1e3",
        ];
        let mut id = 0;
        for (index, real) in reals.iter().enumerate() {
            for (text_index, text) in texts.iter().enumerate() {
                id += 1;
                let json = jsons[(index + text_index) % jsons.len()];
                let integer: i64 = match id % 5 {
                    0 => i64::MAX,
                    1 => i64::MIN,
                    2 => 0,
                    3 => -42,
                    _ => 9_007_199_254_740_993,
                };
                sqlx::query(r#"INSERT INTO t VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#)
                    .bind(format!("row-{id}"))
                    .bind(integer)
                    .bind(*real)
                    .bind(*text)
                    .bind(json)
                    .bind(id % 2 == 0)
                    .bind(vec![0u8, 1, 0xff, id as u8])
                    .bind(if id % 3 == 0 { None } else { Some("z") })
                    .execute(&pool)
                    .await
                    .unwrap();
            }
        }
        // REAL stored in an INTEGER column and text in a REAL column.
        sqlx::query(r#"INSERT INTO t ("_id","n","r") VALUES ('mixed', 2.5, 'text in real')"#)
            .execute(&pool)
            .await
            .unwrap();
        let columns = [
            ("_id", JsonKind::Text),
            ("n", JsonKind::Integer),
            ("r", JsonKind::Real),
            ("s", JsonKind::Text),
            ("j", JsonKind::Json),
            ("b", JsonKind::Boolean),
            ("x", JsonKind::Text),
            ("Zed", JsonKind::Text),
        ]
        .map(|(name, kind)| (name.to_owned(), kind));
        let statement = format!("SELECT {} FROM t AS r", json_object_expr("r", &columns));
        let rows: Vec<String> = sqlx::query_scalar(&statement)
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(rows.len(), id + 1);
        let mut fast = 0;
        for row in &rows {
            let (raw, value, checked) = both(row);
            assert_eq!(raw, value, "hash of {row}");
            fast += usize::from(checked);
        }
        // REAL values take the fast path too (numbers are replaced in place).
        assert!(fast > 0);
        let integer_only = format!(
            "SELECT {} FROM t AS r",
            json_object_expr(
                "r",
                &[
                    ("_id".to_owned(), JsonKind::Text),
                    ("n".to_owned(), JsonKind::Integer),
                    ("s".to_owned(), JsonKind::Text),
                    ("Zed".to_owned(), JsonKind::Text),
                    ("b".to_owned(), JsonKind::Boolean)
                ]
            )
        );
        let rows: Vec<String> = sqlx::query_scalar(&integer_only)
            .fetch_all(&pool)
            .await
            .unwrap();
        let mut integer_fast = 0;
        for row in &rows {
            let (raw, value, checked) = both(row);
            assert_eq!(raw, value, "hash of {row}");
            integer_fast += usize::from(checked);
        }
        eprintln!(
            "fast path: {fast} of {} mixed rows, {integer_fast} of {} integer/text rows",
            id + 1,
            rows.len()
        );
        assert!(integer_fast * 4 > rows.len());
    }

    #[test]
    fn member_values_parse_only_their_slice() {
        let row = r#"{"_id":"a\"b","order":12,"x":{"y":1}}"#;
        let checked = check(row).unwrap();
        assert_eq!(
            member_value(row, &checked, "_id"),
            Some(Value::from("a\"b"))
        );
        assert_eq!(member_value(row, &checked, "order"), Some(Value::from(12)));
        assert_eq!(member_value(row, &checked, "missing"), None);
    }
}
