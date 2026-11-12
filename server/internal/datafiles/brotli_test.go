package datafiles

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise DuckDB's bundled decoder, including the runtime image's security
// backport, without exposing Parquet imports through the application API.
func TestBrotliParquetRoundTrip(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	file := filepath.Join(t.TempDir(), "brotli.parquet")
	quoted := "'" + strings.ReplaceAll(file, "'", "''") + "'"
	_, err = db.Exec("COPY (SELECT range AS id, repeat('Whagons decoder control ', 20) AS label FROM range(1000)) TO " + quoted + " (FORMAT PARQUET, COMPRESSION BROTLI)")
	if err != nil {
		t.Fatal(err)
	}
	var count, sum int64
	var label string
	err = db.QueryRow("SELECT count(*), sum(id), min(label) FROM read_parquet(?)", file).Scan(&count, &sum, &label)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1000 || sum != 499500 || label != strings.Repeat("Whagons decoder control ", 20) {
		t.Fatalf("Brotli round trip changed data: count=%d sum=%d label=%q", count, sum, label)
	}
}
