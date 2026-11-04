//! Change-log rows for snapshot opens and resumes (perf/snapshot-raw): the
//! row images stay the JSON text the capture triggers wrote, with the byte
//! ranges of their top-level members (`canonical_row`), instead of
//! `serde_json::Value` trees. Cache views read the key, filter and order
//! columns from those ranges and build cached rows by slicing the projection
//! out of the image; only the aggregate resume parses the few source-table
//! images it projects into a delta.
//!
//! An image the row scanner cannot check (an escape serde would not write,
//! U+2028/U+2029, a duplicate key, ...) is parsed instead and every helper
//! falls back to the `Value` functions it replaced, so results are the same.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::Value;
use sqlx::Row as _;

use crate::canonical_row::{self, Member};
use crate::replica::{self, ReplicaError};
use crate::modules::ReplicaCollectionDefinition;
use gonvex_store::TenantTransaction;

/// One row image of the change log.
pub(crate) enum RawImage {
    /// The trigger's text and its top-level members.
    Raw { text: Box<str>, members: Vec<Member> },
    /// Not provably canonical: parsed (the `Value` path).
    Parsed(Value),
}

impl RawImage {
    /// None for SQL NULL and JSON `null` (no image), as `LogChange` holds
    /// `Value::Null`.
    pub(crate) fn new(text: String) -> Result<Option<Self>, serde_json::Error> {
        match canonical_row::check(&text) {
            Some(checked) => Ok(Some(Self::Raw {
                text: text.into_boxed_str(),
                members: checked.members,
            })),
            None => {
                let value: Value = serde_json::from_str(&text)?;
                Ok((!value.is_null()).then_some(Self::Parsed(value)))
            }
        }
    }

    fn member(&self, column: &str) -> Option<(&str, &Member)> {
        match self {
            Self::Raw { text, members } => {
                canonical_row::find(text, members, column).map(|member| (&**text, member))
            }
            Self::Parsed(_) => None,
        }
    }

    /// The image as a `Value` (parsed for raw images).
    pub(crate) fn value(&self) -> Cow<'_, Value> {
        match self {
            Self::Raw { text, .. } => {
                Cow::Owned(serde_json::from_str(text).unwrap_or(Value::Null))
            }
            Self::Parsed(value) => Cow::Borrowed(value),
        }
    }

    /// `replica::row_key` of the image.
    pub(crate) fn key(&self, column: &str) -> Result<Arc<str>, ReplicaError> {
        match self {
            Self::Raw { .. } => self
                .member(column)
                .and_then(|(text, member)| canonical_row::member_key(text, member))
                .ok_or_else(|| ReplicaError::MissingKey(column.to_owned())),
            Self::Parsed(value) => replica::row_key(value, column).map(Arc::from),
        }
    }

    /// One column parsed (`Null` when absent): order and key values.
    pub(crate) fn column(&self, column: &str) -> Value {
        match self {
            Self::Raw { text, members } => {
                canonical_row::value_of(text, canonical_row::find(text, members, column))
            }
            Self::Parsed(value) => value.get(column).cloned().unwrap_or(Value::Null),
        }
    }

    /// `replica::collection_filters_admit` of the image.
    pub(crate) fn filters_admit(&self, args: &Value, definition: &ReplicaCollectionDefinition) -> bool {
        let Self::Raw { text, members } = self else {
            let Self::Parsed(value) = self else {
                unreachable!()
            };
            return replica::collection_filters_admit(value, args, definition);
        };
        let Some(args) = args.as_object() else {
            return false;
        };
        let find = |column: &str| canonical_row::find(text, members, column);
        if definition
            .exclude_when_set
            .iter()
            .any(|column| find(column).is_some_and(|member| &text[member.value.clone()] != "null"))
        {
            return false;
        }
        definition.equal_filters.iter().all(|(column, argument)| {
            let value = find(column).map(|member| {
                serde_json::from_str::<Value>(&text[member.value.clone()]).unwrap_or(Value::Null)
            });
            value.as_ref() == args.get(argument)
        })
    }

    /// `{"column":value,...}` of the image's members named by `columns` (in
    /// that order, absent ones skipped), then of `extra`; and whether the
    /// key column was among `columns`. Raw images only.
    pub(crate) fn object_text(
        &self,
        columns: &[String],
        extra: &[String],
        key: &str,
    ) -> Option<(String, Option<String>, bool)> {
        let Self::Raw { text, members } = self else {
            return None;
        };
        let mut out = String::with_capacity(text.len() + 2);
        let mut has_key = false;
        let mut first = true;
        let append = |out: &mut String, member: &Member, first: &mut bool| {
            if !*first {
                out.push(',');
            }
            *first = false;
            out.push('"');
            out.push_str(&text[member.key.clone()]);
            out.push_str("\":");
            out.push_str(&text[member.value.clone()]);
        };
        out.push('{');
        for column in columns {
            if let Some(member) = canonical_row::find(text, members, column) {
                has_key |= column == key;
                append(&mut out, member, &mut first);
            }
        }
        let eval = (!extra.is_empty()).then(|| {
            let mut eval = out.clone();
            let mut first = first;
            for column in extra {
                if let Some(member) = canonical_row::find(text, members, column) {
                    append(&mut eval, member, &mut first);
                }
            }
            eval.push('}');
            eval
        });
        out.push('}');
        Some((out, eval, has_key))
    }
}

/// One change-log row with raw images.
pub(crate) struct RawChange {
    pub revision: u64,
    pub table: String,
    pub row_id: String,
    pub operation: String,
    pub changed_columns: Vec<String>,
    pub old: Option<RawImage>,
    pub new: Option<RawImage>,
}

impl crate::replica::ChangeMeta for RawChange {
    fn table(&self) -> &str {
        &self.table
    }
    fn operation(&self) -> &str {
        &self.operation
    }
    fn changed_columns(&self) -> &[String] {
        &self.changed_columns
    }
}

/// One row's net change: its first before-image and last after-image.
pub(crate) struct RawCollapsed<'a> {
    pub old: Option<&'a RawImage>,
    pub new: Option<&'a RawImage>,
    pub changed_columns: BTreeSet<&'a str>,
}

/// `replica::collapse_changes` over raw changes (same order: by table, row).
pub(crate) fn collapse(changes: &[RawChange]) -> BTreeMap<(&str, &str), RawCollapsed<'_>> {
    let _site = crate::raw_sites::raw_site!(Collapse);
    crate::raw_sites::rows(crate::raw_sites::Site::Collapse, changes.len());
    let mut collapsed = BTreeMap::<(&str, &str), RawCollapsed<'_>>::new();
    for change in changes {
        let entry = collapsed
            .entry((change.table.as_str(), change.row_id.as_str()))
            .or_insert_with(|| RawCollapsed {
                old: change.old.as_ref(),
                new: change.new.as_ref(),
                changed_columns: BTreeSet::new(),
            });
        entry.new = change.new.as_ref();
        entry
            .changed_columns
            .extend(change.changed_columns.iter().map(String::as_str));
    }
    collapsed
}

/// The change log after `after` through `through` for `tables`, in log order
/// (`replica::resume_changes` with raw images).
pub(crate) async fn read_changes(
    transaction: &mut TenantTransaction,
    after: u64,
    through: u64,
    tables: BTreeSet<String>,
) -> Result<Vec<RawChange>, ReplicaError> {
    if tables.is_empty() {
        return Ok(Vec::new());
    }
    // One bound parameter per table (not `IN (SELECT value FROM json_each(..))`):
    // SQLite then plans the (table_name, revision, ordinal) index, which
    // skips other tables' rows in the range (a notification-heavy log made
    // per-table replays walk every row in the range).
    let placeholders = (0..tables.len())
        .map(|index| format!("${}", index + 3))
        .collect::<Vec<_>>()
        .join(", ");
    let statement = format!(
        "SELECT revision, table_name, row_id, operation, changed_columns, old_value, new_value
             FROM _gonvex_sync_changes
             WHERE revision > $1 AND revision <= $2 AND table_name IN ({placeholders})
             ORDER BY revision, ordinal"
    );
    let mut query = sqlx::query(&statement)
        .bind(after as i64)
        .bind(through as i64);
    for table in tables {
        query = query.bind(table);
    }
    let rows = query.fetch_all(&mut **transaction.transaction()).await?;
    rows.iter()
        .map(|row| {
            let _site = crate::raw_sites::raw_site!(LogRowDecode);
            let image = |column: &str| -> Result<Option<RawImage>, ReplicaError> {
                match row.try_get::<Option<String>, _>(column)? {
                    Some(text) => RawImage::new(text).map_err(|error| {
                        ReplicaError::InvalidIdentifier(format!("{column}: {error}"))
                    }),
                    None => Ok(None),
                }
            };
            let changed_columns = match row.try_get::<Option<String>, _>("changed_columns")? {
                Some(text) => match serde_json::from_str::<Value>(&text).map_err(|error| {
                    ReplicaError::InvalidIdentifier(format!("changed_columns: {error}"))
                })? {
                    Value::Array(values) => values
                        .into_iter()
                        .filter_map(|value| match value {
                            Value::String(column) => Some(column),
                            _ => None,
                        })
                        .collect(),
                    _ => Vec::new(),
                },
                None => Vec::new(),
            };
            Ok(RawChange {
                revision: row.try_get::<i64, _>("revision")?.max(0) as u64,
                table: row.try_get("table_name")?,
                row_id: row.try_get("row_id")?,
                operation: row.try_get("operation")?,
                changed_columns,
                old: image("old_value")?,
                new: image("new_value")?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition(value: Value) -> ReplicaCollectionDefinition {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn raw_images_answer_like_their_values() {
        let definition = definition(serde_json::json!({
            "table": "tasks", "key": "_id", "columns": ["_id", "name", "n"],
            "excludeWhenSet": ["deletedAt"], "equalFilters": {"workspaceId": "ws"},
            "orderBy": "n", "orderDirection": "desc"
        }));
        let images = [
            r#"{"_id":"a","name":"x","n":1.5,"workspaceId":"w","deletedAt":null}"#,
            r#"{"_id":"b","name":"x","n":2,"workspaceId":"v","deletedAt":null}"#,
            r#"{"_id":"c","name":"x","workspaceId":"w","deletedAt":5}"#,
            r#"{"_id":7,"workspaceId":"w"}"#,
            r#"{"name":"  sep","_id":"d","workspaceId":"w"}"#,
            r#"{"_id":"e\"q","name":{"b":1,"a":[0.10000000000000001]},"workspaceId":"w"}"#,
            r#"{"_id":"f","workspaceId":"w","_id":"g"}"#,
        ];
        for args in [serde_json::json!({"ws": "w"}), serde_json::json!({}), Value::Null] {
            for text in images {
                let value: Value = serde_json::from_str(text).unwrap();
                let image = RawImage::new(text.to_owned()).unwrap().unwrap();
                assert_eq!(
                    image.filters_admit(&args, &definition),
                    replica::collection_filters_admit(&value, &args, &definition),
                    "{text} {args}"
                );
                assert_eq!(
                    image.key("_id").ok(),
                    replica::row_key(&value, "_id").ok().map(Arc::from),
                    "{text}"
                );
                assert_eq!(image.column("n"), value.get("n").cloned().unwrap_or(Value::Null));
                assert_eq!(*image.value(), value);
                if let Some((projection, _, _)) =
                    image.object_text(&definition.columns, &[], &definition.key)
                {
                    let projected = replica::project_row(&value, &definition).unwrap();
                    assert_eq!(serde_json::from_str::<Value>(&projection).unwrap(), projected);
                }
            }
        }
        assert!(RawImage::new("null".to_owned()).unwrap().is_none());
    }
}
