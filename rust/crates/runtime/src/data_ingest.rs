//! Converts bounded CSV and spreadsheet uploads into read-only DuckDB files.

use std::fs;
use std::path::{Path, PathBuf};

use calamine::{open_workbook_auto, Data, Reader};
use duckdb::Connection;
use serde::Serialize;

const MAX_COLUMNS: usize = 200;
const MAX_SHEET_ROWS: usize = 2_000_000;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportedTable {
    pub table_name: String,
    pub row_count: i64,
    pub columns: Vec<String>,
}

pub fn ingest(bytes: &[u8], filename: &str, target: &Path) -> Result<Vec<ImportedTable>, String> {
    let extension = Path::new(filename)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let source = target.with_extension(format!("source.{extension}"));
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    fs::write(&source, bytes).map_err(|error| error.to_string())?;
    let building = target.with_extension("duckdb.building");
    let _ = fs::remove_file(&building);
    let result = match extension.as_str() {
        "csv" | "tsv" | "txt" => ingest_delimited(&source, &building),
        "xlsx" | "xlsm" | "xls" | "xlsb" | "ods" => ingest_workbook(&source, &building),
        _ => Err(format!(
            "unsupported data file type .{extension} (supported: CSV, TSV, XLSX, XLS, XLSB, ODS)"
        )),
    };
    let _ = fs::remove_file(&source);
    match result {
        Ok(tables) => {
            let _ = fs::remove_file(target);
            fs::rename(&building, target).map_err(|error| error.to_string())?;
            Ok(tables)
        }
        Err(error) => {
            let _ = fs::remove_file(&building);
            Err(error)
        }
    }
}

fn ingest_delimited(source: &Path, target: &Path) -> Result<Vec<ImportedTable>, String> {
    let connection = Connection::open(target).map_err(|error| error.to_string())?;
    let source = sql_literal(&source.to_string_lossy());
    connection
        .execute_batch(&format!(
            "CREATE TABLE data AS SELECT * FROM read_csv_auto('{source}', header=true, sample_size=100000);"
        ))
        .map_err(|error| format!("parse delimited file: {error}"))?;
    Ok(vec![table_meta(&connection, "data")?])
}

fn ingest_workbook(source: &Path, target: &Path) -> Result<Vec<ImportedTable>, String> {
    let mut workbook = open_workbook_auto(source).map_err(|error| error.to_string())?;
    let connection = Connection::open(target).map_err(|error| error.to_string())?;
    let mut tables = Vec::new();
    let names = workbook.sheet_names().to_vec();
    for (index, sheet_name) in names.into_iter().enumerate() {
        let range = workbook
            .worksheet_range(&sheet_name)
            .map_err(|error| format!("read sheet {sheet_name:?}: {error}"))?;
        if range.is_empty() {
            continue;
        }
        if range.height() > MAX_SHEET_ROWS + 1 {
            return Err(format!("sheet {sheet_name:?} exceeds the row limit"));
        }
        let width = range.width().min(MAX_COLUMNS);
        if range.width() > MAX_COLUMNS {
            return Err(format!("sheet {sheet_name:?} exceeds the column limit"));
        }
        let table = unique_table_name(&sheet_name, index);
        let csv_path = temporary_csv(target, index);
        let mut csv = String::new();
        for row in range.rows() {
            for column in 0..width {
                if column > 0 {
                    csv.push(',');
                }
                csv.push_str(&csv_cell(row.get(column).unwrap_or(&Data::Empty)));
            }
            csv.push('\n');
        }
        fs::write(&csv_path, csv).map_err(|error| error.to_string())?;
        let path = sql_literal(&csv_path.to_string_lossy());
        let statement = format!(
            "CREATE TABLE {} AS SELECT * FROM read_csv_auto('{path}', header=true, sample_size=100000);",
            quote_identifier(&table)
        );
        let result = connection
            .execute_batch(&statement)
            .map_err(|error| format!("parse sheet {sheet_name:?}: {error}"));
        let _ = fs::remove_file(&csv_path);
        result?;
        tables.push(table_meta(&connection, &table)?);
    }
    if tables.is_empty() {
        return Err("workbook has no non-empty sheets".to_owned());
    }
    Ok(tables)
}

fn table_meta(connection: &Connection, table: &str) -> Result<ImportedTable, String> {
    let table_identifier = quote_identifier(table);
    let row_count: i64 = connection
        .query_row(
            &format!("SELECT count(*) FROM {table_identifier}"),
            [],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    let mut statement = connection
        .prepare(&format!("DESCRIBE SELECT * FROM {table_identifier}"))
        .map_err(|error| error.to_string())?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    Ok(ImportedTable {
        table_name: table.to_owned(),
        row_count,
        columns,
    })
}

fn csv_cell(value: &Data) -> String {
    let value = match value {
        Data::Empty => String::new(),
        Data::String(value) => value.clone(),
        Data::Float(value) => value.to_string(),
        Data::Int(value) => value.to_string(),
        Data::Bool(value) => value.to_string(),
        Data::Error(value) => format!("{value:?}"),
        Data::DateTime(value) => value.to_string(),
        Data::DateTimeIso(value) | Data::DurationIso(value) => value.clone(),
    };
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn unique_table_name(value: &str, index: usize) -> String {
    let mut name = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_owned();
    if name.is_empty() || name.as_bytes().first().is_some_and(u8::is_ascii_digit) {
        name = format!("sheet_{}", index + 1);
    }
    format!("{name}_{}", index + 1)
}

fn temporary_csv(target: &Path, index: usize) -> PathBuf {
    target.with_extension(format!("sheet-{index}.csv"))
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_is_ingested_into_a_bounded_duckdb_artifact() {
        let root = std::env::temp_dir().join(format!("gonvex-ingest-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let target = root.join("file.duckdb");
        let tables = ingest(b"name,count\napples,2\npears,3\n", "stock.csv", &target).unwrap();
        assert_eq!(tables[0].table_name, "data");
        assert_eq!(tables[0].row_count, 2);
        assert_eq!(tables[0].columns, vec!["name", "count"]);
        assert!(target.exists());
        fs::remove_dir_all(root).unwrap();
    }
}
