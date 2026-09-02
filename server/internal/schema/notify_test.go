package schema

import (
	"strings"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
)

func TestNotifySchemaVersionReinstallsChangedTriggerDefinitions(t *testing.T) {
	if NotifySchemaVersion != "15" {
		t.Fatalf("notify schema version = %q, want 15 for task relationship update metadata", NotifySchemaVersion)
	}
	if notifySchemaVersionFunction != "gonvex_notify_schema_v"+NotifySchemaVersion {
		t.Fatalf("version function %q does not track notify schema version %q", notifySchemaVersionFunction, NotifySchemaVersion)
	}
	for _, want := range []string{`"gonvex_notify_schema_v15"`, "RETURN 15"} {
		if !strings.Contains(notifySchemaVersionSQL(), want) {
			t.Fatalf("expected notify schema marker SQL to contain %q: %s", want, notifySchemaVersionSQL())
		}
	}
}

func TestNotifySQLForTableUsesTableNameAndChannel(t *testing.T) {
	sql, err := NotifySQLForTable("messages", manifest.Table{Columns: map[string]manifest.Column{
		"id":   {Type: "id"},
		"body": {Type: "text"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"gonvex_notify_messages_insert",
		"gonvex_messages_notify_update",
		"AFTER DELETE ON \"messages\"",
		"pg_notify('gonvex_table_change'",
		"'table', 'messages'",
		"'operation', 'update'",
		"'mutationId', NULLIF(current_setting('gonvex.mutation_id', true), '')",
		"'changedColumns', CASE WHEN cardinality(changed_columns) <= 100",
		"FULL OUTER JOIN new_rows new_row USING (\"id\")",
		"jsonb_object_keys(",
		"LIMIT 101",
		"CASE WHEN row_count < 500 THEN ids ELSE ARRAY[]::text[] END",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected notify SQL to contain %q:\n%s", want, sql)
		}
	}
}

func TestNotifySQLForTableUsesConvexIDAndSuppressesEmptyStatements(t *testing.T) {
	sql, err := NotifySQLForTable("tasks", manifest.Table{Columns: map[string]manifest.Column{
		"_id":    {Type: "id"},
		"taskId": {Type: "id"},
		"name":   {Type: "text"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`SELECT "_id" FROM new_rows WHERE "_id" IS NOT NULL`,
		`FULL OUTER JOIN new_rows new_row USING ("_id")`,
		"IF row_count = 0 THEN",
		"'broad', row_count >= 500",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected notify SQL to contain %q:\n%s", want, sql)
		}
	}
}

func TestNotifySQLIncludesCommittedUserIDsWhenColumnExists(t *testing.T) {
	sql, err := NotifySQLForTable("notifications", manifest.Table{Columns: map[string]manifest.Column{
		"_id": {Type: "id"}, "userId": {Type: "id"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`SELECT "userId" FROM new_rows`, `'userIds', CASE WHEN row_count < 500 THEN user_ids`} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected notify SQL to contain %q:\n%s", want, sql)
		}
	}
}

func TestNotifySQLIncludesOldAndNewWorkspaceIDsForUpdates(t *testing.T) {
	sql, err := NotifySQLForTable("tasks", manifest.Table{Columns: map[string]manifest.Column{
		"_id": {Type: "id"}, "workspaceId": {Type: "id"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`SELECT "workspaceId" FROM old_rows UNION SELECT "workspaceId" FROM new_rows`,
		`'workspaceIds', CASE WHEN row_count < 500 THEN workspace_ids`,
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected notify SQL to contain %q:\n%s", want, sql)
		}
	}
}

func TestNotifySQLIncludesOldAndNewTaskIDsForUpdates(t *testing.T) {
	sql, err := NotifySQLForTable("taskAckReads", manifest.Table{Columns: map[string]manifest.Column{
		"_id": {Type: "id"}, "taskId": {Type: "id"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`SELECT "taskId" FROM old_rows UNION SELECT "taskId" FROM new_rows`,
		`'taskIds', CASE WHEN row_count < 500 THEN task_ids`,
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected notify SQL to contain %q:\n%s", want, sql)
		}
	}
}

func TestNotifySQLFallsBackToBroadInvalidationBeforePostgresPayloadLimit(t *testing.T) {
	sql, err := NotifySQLForTable("taskWorkspaceContexts", manifest.Table{Columns: map[string]manifest.Column{
		"_id": {Type: "id"}, "taskId": {Type: "id"}, "userId": {Type: "id"}, "workspaceId": {Type: "id"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"octet_length(notify_payload) >= 8000",
		"notify_payload := json_build_object(",
		"'broad', true",
		"'ids', ARRAY[]::text[]",
		"PERFORM pg_notify('gonvex_table_change', notify_payload)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected oversized NOTIFY fallback SQL to contain %q:\n%s", want, sql)
		}
	}
}

func TestNotifySQLForTableWithoutIDUsesBroadInvalidation(t *testing.T) {
	sql, err := NotifySQLForTable("events", manifest.Table{Columns: map[string]manifest.Column{
		"name": {Type: "text"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "SELECT id FROM") {
		t.Fatalf("table without id should not read ids:\n%s", sql)
	}
	if !strings.Contains(sql, "'broad', true") {
		t.Fatalf("table without id should use broad invalidation:\n%s", sql)
	}
}

func TestCreateTableSQLRejectsInvalidTableName(t *testing.T) {
	_, err := createTableSQL("bad-name", manifest.Table{Columns: map[string]manifest.Column{
		"id": {Type: "id", PrimaryKey: true},
	}})
	if err == nil {
		t.Fatal("expected invalid table name error")
	}
}

func TestColumnDefinitionCanDeferNotNullForExistingRows(t *testing.T) {
	column := manifest.Column{Type: "text"}

	enforced, err := columnDefinition("title", column, true)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(enforced, "NOT NULL") {
		t.Fatalf("expected enforced column to contain NOT NULL: %s", enforced)
	}

	deferred, err := columnDefinition("title", column, false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(deferred, "NOT NULL") {
		t.Fatalf("expected deferred column to omit NOT NULL: %s", deferred)
	}
}

func TestTrigramIndexSQLUsesGinTrigramOps(t *testing.T) {
	sql := trigramIndexSQL("tasks_search_text_trgm", "tasks", []string{"name", "title", "description"})

	for _, want := range []string{
		`CREATE INDEX IF NOT EXISTS "tasks_search_text_trgm" ON "tasks" USING gin`,
		`COALESCE("name"::text, '')`,
		`COALESCE("title"::text, '')`,
		`COALESCE("description"::text, '')`,
		`gin_trgm_ops`,
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected trigram SQL to contain %q:\n%s", want, sql)
		}
	}
}
