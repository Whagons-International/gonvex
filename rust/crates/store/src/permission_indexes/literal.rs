//! Typed row literals. The new index is eligible only where the existing v2 SQL
//! comparison and declared-type JSON row comparison agree. Raw data is never
//! silently cast into a chosen interpretation.
use crate::{
    rows::{json_value_expr, JsonKind},
    DatabaseError,
};

pub(super) fn sql(column: &str, value: &Option<String>) -> Result<String, DatabaseError> {
    let column = super::program::quoted_column(column)?;
    Ok(match value {
        None => format!("{column} IS NULL"),
        Some(value) => format!("{column} = '{}'", value.replace('\'', "''")),
    })
}
pub(super) fn memory(
    column: &str,
    value: &Option<String>,
    kind: JsonKind,
) -> Result<String, DatabaseError> {
    let column = super::program::quoted_column(column)?;
    let encoded = json_value_expr(&column, kind);
    let array = format!("json_array({encoded})");
    Ok(match value {
        None=>format!("json_type({array},'$[0]')='null'"),
        Some(value)=>format!("(json_type({array},'$[0]')='text' AND json_extract({array},'$[0]') = '{}' COLLATE BINARY)",value.replace('\'',"''")),
    })
}
