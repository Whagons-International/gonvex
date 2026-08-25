package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
)

func workspaceVisibilityPlan() manifest.VisibilityPlan {
	return manifest.VisibilityPlan{
		Table: "tasks",
		Key:   "id",
		Sets: map[string]manifest.VisibilitySet{
			"workspaces": {
				Table:  "workspaceMembers",
				Select: "workspaceId",
				Where: []manifest.VisibilityConstraint{{
					Table: "workspaceMembers", Column: "memberId", Context: "member.id",
				}},
			},
		},
		Where: &manifest.VisibilityExpression{
			Operator: "or",
			Children: []*manifest.VisibilityExpression{
				{Operator: "permission", Value: "tasks.viewAll"},
				{Operator: "inSet", Column: "workspaceId", Set: "workspaces"},
			},
		},
	}
}

func TestVisibilityOldAndNewRowsFailClosed(t *testing.T) {
	plan := workspaceVisibilityPlan()
	resolved := &resolvedVisibilityContext{
		Permissions: map[string]any{"tasks.viewAll": false},
		Sets: map[string]map[string]struct{}{
			"workspaces": {"workspace-a": {}},
		},
	}
	if !visibilityRawRowMatches(json.RawMessage(`{"id":"task-1","workspaceId":"workspace-a"}`), plan, resolved) {
		t.Fatal("a row in the member's workspace must be visible")
	}
	if visibilityRawRowMatches(json.RawMessage(`{"id":"task-1","workspaceId":"workspace-b"}`), plan, resolved) {
		t.Fatal("a row outside the member's workspace must fail closed")
	}
	if visibilityRawRowMatches(json.RawMessage(`{"id":"task-1"}`), plan, resolved) {
		t.Fatal("a row missing a visibility field must fail closed")
	}
	if visibilityRawRowMatches(json.RawMessage(`{"id":`), plan, resolved) {
		t.Fatal("a malformed row must fail closed")
	}
	if visibilityRawRowMatches(nil, plan, resolved) {
		t.Fatal("a missing row must fail closed")
	}
	resolved.Permissions["tasks.viewAll"] = true
	if !visibilityRawRowMatches(json.RawMessage(`{"id":"task-1","workspaceId":"workspace-b"}`), plan, resolved) {
		t.Fatal("the explicit view-all permission must admit the row")
	}
}

func TestVisibilityTransitionOperationCoversAllOldAndNewStates(t *testing.T) {
	tests := []struct {
		oldVisible bool
		newVisible bool
		operation  string
		emit       bool
	}{
		{oldVisible: true, newVisible: true, operation: "update", emit: true},
		{oldVisible: false, newVisible: true, operation: "insert", emit: true},
		{oldVisible: true, newVisible: false, operation: "delete", emit: true},
		{oldVisible: false, newVisible: false, operation: "", emit: false},
	}
	for _, test := range tests {
		operation, emit := visibilityTransitionOperation(test.oldVisible, test.newVisible)
		if operation != test.operation || emit != test.emit {
			t.Fatalf("transition old=%v new=%v = (%q, %v), want (%q, %v)",
				test.oldVisible, test.newVisible, operation, emit, test.operation, test.emit)
		}
	}
}

func TestMemberChangeIdentitiesIncludesOldAndNewAuthorityKeys(t *testing.T) {
	identities := memberChangeIdentities(replicaChangeBatch{changes: []replicaLogChange{
		{
			table:    "members",
			oldValue: json.RawMessage(`{"id":"member-old","account_id":"account-old"}`),
			newValue: json.RawMessage(`{"id":"member-new","account_id":"account-new"}`),
		},
		{table: "tasks", oldValue: json.RawMessage(`{"account_id":"must-not-match"}`)},
	}})
	for _, identity := range []string{"member-old", "account-old", "member-new", "account-new"} {
		if _, ok := identities[identity]; !ok {
			t.Fatalf("missing member identity %q in %#v", identity, identities)
		}
	}
	if _, ok := identities["must-not-match"]; ok {
		t.Fatal("a non-member row was treated as membership authority")
	}
}

func TestVisibilityFingerprintSharesOnlyEquivalentInputs(t *testing.T) {
	plan := workspaceVisibilityPlan()
	first := &resolvedVisibilityContext{
		Direct:      map[string]string{"member.id": "member-a", "account.id": "account-a"},
		Permissions: map[string]any{"tasks.viewAll": false, "unrelated": true},
		Sets:        map[string]map[string]struct{}{"workspaces": {"workspace-a": {}}},
	}
	second := &resolvedVisibilityContext{
		Direct:      map[string]string{"member.id": "member-b", "account.id": "account-b"},
		Permissions: map[string]any{"tasks.viewAll": false, "unrelated": false},
		Sets:        map[string]map[string]struct{}{"workspaces": {"workspace-a": {}}},
	}
	if visibilityFingerprint(plan, first) != visibilityFingerprint(plan, second) {
		t.Fatal("different identities with identical effective inputs should share one visibility fingerprint")
	}
	second.Sets["workspaces"] = map[string]struct{}{"workspace-b": {}}
	if visibilityFingerprint(plan, first) == visibilityFingerprint(plan, second) {
		t.Fatal("different effective workspace sets must not share execution")
	}

	directPlan := manifest.VisibilityPlan{
		Table: "notes", Key: "id", Sets: map[string]manifest.VisibilitySet{},
		Where: &manifest.VisibilityExpression{Operator: "eqContext", Column: "ownerId", Context: "member.id"},
	}
	if visibilityFingerprint(directPlan, first) == visibilityFingerprint(directPlan, second) {
		t.Fatal("direct member comparisons must retain member identity in the fingerprint")
	}
}

func TestVisibilitySQLUsesOneSafePlaceholderSequence(t *testing.T) {
	plan := workspaceVisibilityPlan()
	builder := &visibilitySQLBuilder{}
	for index := 0; index < 9; index++ {
		builder.argument(index)
	}
	predicate, err := compileVisibilitySQL(
		plan.Where, plan,
		map[string]string{"member.id": "member-a"},
		map[string]any{"tasks.viewAll": false}, "", builder, "r",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(predicate, `$10`) {
		t.Fatalf("visibility subquery did not continue the outer placeholder sequence: %s", predicate)
	}
	if strings.Contains(predicate, `$100`) {
		t.Fatalf("visibility placeholder replacement corrupted a double-digit parameter: %s", predicate)
	}
	if !strings.Contains(predicate, "FROM members AS _gonvex_member") ||
		!strings.Contains(predicate, "_gonvex_member.status = 'active'") ||
		!strings.Contains(predicate, "_gonvex_member.permissions ->>") {
		t.Fatalf("permission SQL did not recheck the active authoritative tenant member: %s", predicate)
	}
	foundMember := false
	for _, argument := range builder.args[9:] {
		if argument == "member-a" {
			foundMember = true
		}
	}
	if !foundMember {
		t.Fatalf("visibility SQL lost the member context argument: %#v", builder.args)
	}
}

func TestManifestUpdateRequiresVisibilityForEveryLiveDelivery(t *testing.T) {
	current := manifest.Manifest{
		Functions: map[string]manifest.FunctionEntry{
			"tasks.grid": {
				Kind: manifest.FunctionKindQuery, Delivery: manifest.DeliveryLive,
				Dependencies: manifest.FunctionDependencies{LiveQueryPlan: &manifest.LiveQueryPlan{Table: "tasks", Key: "id", Columns: []string{"id"}}},
			},
		},
	}
	if err := (&Server{}).requireVisibilityPlans(current); err == nil || !strings.Contains(err.Error(), "explicit visibility plan") {
		t.Fatalf("missing visibility plan error = %v", err)
	}
	current.Visibility = map[string]manifest.VisibilityPlan{
		"tasks": {Table: "tasks", Key: "id", Sets: map[string]manifest.VisibilitySet{}, Where: &manifest.VisibilityExpression{Operator: "public"}},
	}
	if err := (&Server{}).requireVisibilityPlans(current); err != nil {
		t.Fatalf("explicit public visibility plan was rejected: %v", err)
	}
}

func TestVisibilityDependenciesIncludeSetsJoinsAndIdentityAuthority(t *testing.T) {
	plan := workspaceVisibilityPlan()
	plan.Sets["workspaces"] = manifest.VisibilitySet{
		Table: "workspaceMembers", Select: "workspaceId",
		Joins: []manifest.VisibilityJoin{{Table: "teams", LeftColumn: "teamId", RightColumn: "id"}},
		Where: []manifest.VisibilityConstraint{{Table: "workspaceMembers", Column: "memberId", Context: "member.id"}},
	}
	dependencies := visibilityPlanDependencies(plan)
	for _, required := range []string{"members", "teams", "workspaceMembers"} {
		if !stringInSlice(required, dependencies) {
			t.Fatalf("visibility dependency %q missing from %#v", required, dependencies)
		}
	}
	public := manifest.VisibilityPlan{
		Table: "statuses", Key: "id", Sets: map[string]manifest.VisibilitySet{},
		Where: &manifest.VisibilityExpression{Operator: "public"},
	}
	if dependencies := visibilityPlanDependencies(public); len(dependencies) != 1 || dependencies[0] != "members" {
		t.Fatalf("tenant-public visibility did not retain the active Member gate: %#v", dependencies)
	}
}

func TestVisibilitySelfJoinUsesExplicitAliasesAndOnePhysicalDependency(t *testing.T) {
	set := manifest.VisibilitySet{
		Table: "memberTeams", Alias: "viewerTeams", Select: "memberId", SelectFrom: "peerTeams",
		Joins: []manifest.VisibilityJoin{{
			Table: "memberTeams", Alias: "peerTeams", LeftAlias: "viewerTeams",
			LeftColumn: "teamId", RightColumn: "teamId",
		}},
		Where: []manifest.VisibilityConstraint{{Table: "viewerTeams", Column: "memberId", Context: "member.id"}},
	}
	plan := manifest.VisibilityPlan{
		Table: "userLiveLocations", Key: "id", Sets: map[string]manifest.VisibilitySet{"teammates": set},
		Where: &manifest.VisibilityExpression{Operator: "inSet", Column: "memberId", Set: "teammates"},
	}
	if err := validateVisibilityPlan("userLiveLocations", plan); err != nil {
		t.Fatal(err)
	}
	query, args, err := compileVisibilitySet(set, map[string]string{"member.id": "member-viewer"})
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		`SELECT DISTINCT v1."memberId"`, `FROM "memberTeams" AS v0`,
		`JOIN "memberTeams" AS v1 ON v0."teamId" = v1."teamId"`, `v0."memberId" = $1`,
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("self-join SQL %q is missing %q", query, fragment)
		}
	}
	if len(args) != 1 || args[0] != "member-viewer" {
		t.Fatalf("self-join arguments = %#v", args)
	}
	dependencies := visibilityPlanDependencies(plan)
	count := 0
	for _, dependency := range dependencies {
		if dependency == "memberTeams" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("physical self-join dependency must occur once: %#v", dependencies)
	}

	ambiguous := set
	ambiguous.Alias = ""
	ambiguous.Joins[0].LeftAlias = ""
	if err := validateVisibilityPlan("userLiveLocations", manifest.VisibilityPlan{
		Table: plan.Table, Key: plan.Key, Sets: map[string]manifest.VisibilitySet{"teammates": ambiguous}, Where: plan.Where,
	}); err == nil || !strings.Contains(err.Error(), "alias") {
		t.Fatalf("ambiguous duplicate table error = %v", err)
	}
}

func TestVisibilityInvalidationIsScopedAndDependencyAware(t *testing.T) {
	server := New(config.Config{})
	t.Cleanup(server.Close)
	server.visibilityContexts["tasks-a"] = &resolvedVisibilityContext{
		ScopeKey: "project-a\x00tenant-a", Dependencies: map[string]struct{}{"workspace_members": {}},
	}
	server.visibilityContexts["notes-a"] = &resolvedVisibilityContext{
		ScopeKey: "project-a\x00tenant-a", Dependencies: map[string]struct{}{"teams": {}},
	}
	server.visibilityContexts["tasks-b"] = &resolvedVisibilityContext{
		ScopeKey: "project-a\x00tenant-b", Dependencies: map[string]struct{}{"workspace_members": {}},
	}

	server.invalidateVisibilityContexts("project-a", "tenant-a", []string{"workspace_members"})
	if _, ok := server.visibilityContexts["tasks-a"]; ok {
		t.Fatal("changed dependency retained a stale visibility context")
	}
	if _, ok := server.visibilityContexts["notes-a"]; !ok {
		t.Fatal("unrelated dependency evicted a valid visibility context")
	}
	if _, ok := server.visibilityContexts["tasks-b"]; !ok {
		t.Fatal("tenant-scoped invalidation evicted another tenant")
	}
	if got := server.visibilityEpochs["project-a\x00tenant-a"]; got != 1 {
		t.Fatalf("visibility epoch = %d, want 1", got)
	}

	server.invalidateProjectVisibilityContexts("project-a")
	if len(server.visibilityContexts) != 0 {
		t.Fatalf("project invalidation retained contexts: %#v", server.visibilityContexts)
	}
	if got := server.visibilityEpochs["project-a\x00*"]; got != 1 {
		t.Fatalf("project visibility epoch = %d, want 1", got)
	}
}

func TestStructuredVisibilityQueriesFailClosedWithoutPlan(t *testing.T) {
	server := New(config.Config{})
	t.Cleanup(server.Close)
	if _, err := server.executeStructuredReplicaQuery(context.Background(), "project", "tenant", callerContext{}, manifest.ReplicaCollectionDefinition{Table: "tasks"}, nil); err == nil || !strings.Contains(err.Error(), "visibility plan required") {
		t.Fatalf("Replica Collection missing-plan error = %v", err)
	}
	if _, err := server.executeStructuredLiveQuery(context.Background(), "project", "tenant", callerContext{}, manifest.LiveQueryPlan{Table: "tasks"}, nil); err == nil || !strings.Contains(err.Error(), "visibility plan required") {
		t.Fatalf("Live Query missing-plan error = %v", err)
	}
}

func TestRequiredVisibilityPlanRejectsMalformedPlan(t *testing.T) {
	plan := manifest.VisibilityPlan{Table: "tasks", Key: "id"}
	if err := validateVisibilityPlan("tasks", plan); err == nil || !strings.Contains(err.Error(), "must declare where") {
		t.Fatalf("malformed visibility plan error = %v", err)
	}
}

func TestVisibilitySQLAndFingerprintFollowLatestCommittedMembership(t *testing.T) {
	server := newTypeScriptTestServer(t, config.Config{})
	const project, tenant = "test", "tenant"
	tenantURL := server.config.TenantDatabases[project+":"+tenant]
	db, err := sql.Open("pgx", tenantURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE members (
			id TEXT PRIMARY KEY,
			account_id TEXT NOT NULL UNIQUE,
			status TEXT NOT NULL,
			role TEXT NOT NULL,
			permissions JSONB NOT NULL DEFAULT '{}'::jsonb
		);
		CREATE TABLE workspace_members (
			id TEXT PRIMARY KEY,
			member_id TEXT NOT NULL,
			workspace_id TEXT NOT NULL
		);
		CREATE TABLE tasks (
			id TEXT PRIMARY KEY,
			workspace_id TEXT NOT NULL,
			title TEXT NOT NULL
		);
		CREATE TABLE member_teams (
			id TEXT PRIMARY KEY,
			member_id TEXT NOT NULL,
			team_id TEXT NOT NULL
		);
		CREATE TABLE user_live_locations (
			id TEXT PRIMARY KEY,
			member_id TEXT NOT NULL,
			latitude DOUBLE PRECISION NOT NULL
		);
	`); err != nil {
		t.Fatal(err)
	}

	plan := manifest.VisibilityPlan{
		Table: "tasks", Key: "id",
		Sets: map[string]manifest.VisibilitySet{
			"workspaces": {
				Table: "workspace_members", Select: "workspace_id",
				Where: []manifest.VisibilityConstraint{{Table: "workspace_members", Column: "member_id", Context: "member.id"}},
			},
		},
		Where: &manifest.VisibilityExpression{Operator: "inSet", Column: "workspace_id", Set: "workspaces"},
	}
	replicaDefinition := manifest.ReplicaCollectionDefinition{
		Table: "tasks", Key: "id", Columns: []string{"id", "workspace_id", "title"},
	}
	livePlan := manifest.LiveQueryPlan{
		Table: "tasks", Key: "id", Columns: []string{"id", "workspace_id", "title"},
	}
	current := typeScriptTestManifest(project, map[string]manifest.FunctionEntry{
		"tasks.recent": {
			Kind: manifest.FunctionKindQuery, Delivery: manifest.DeliveryReplica, Replica: &replicaDefinition,
		},
		"tasks.grid": {
			Kind: manifest.FunctionKindQuery, Delivery: manifest.DeliveryLive,
			Dependencies: manifest.FunctionDependencies{LiveQueryPlan: &livePlan},
		},
	})
	current.Visibility = map[string]manifest.VisibilityPlan{"tasks": plan}
	current.Module.Visibility = current.Visibility
	current.Module.Hash, _ = current.Module.ComputedHash()
	payload, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/sync", bytes.NewReader(payload)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("sync visibility fixture: status %d: %s", recorder.Code, recorder.Body.String())
	}

	if _, err := db.Exec(`
		INSERT INTO members (id, account_id, status, role, permissions)
		VALUES
			('member-a', 'account-a', 'active', 'member', '{}'),
			('member-peer', 'account-peer', 'active', 'member', '{}');
		INSERT INTO workspace_members (id, member_id, workspace_id)
		VALUES ('membership-a', 'member-a', 'workspace-a');
		INSERT INTO member_teams (id, member_id, team_id) VALUES
			('team-viewer', 'member-a', 'team-one'),
			('team-peer', 'member-peer', 'team-one');
		INSERT INTO user_live_locations (id, member_id, latitude) VALUES
			('location-viewer', 'member-a', 1),
			('location-peer', 'member-peer', 2);
		INSERT INTO tasks (id, workspace_id, title) VALUES
			('task-a', 'workspace-a', 'Visible A'),
			('task-b', 'workspace-b', 'Visible B');
	`); err != nil {
		t.Fatal(err)
	}
	caller := callerContext{user: &gonvex.Account{ID: "account-a"}}
	assertVisibleTaskIDs(t, mustStructuredReplicaRows(t, server, project, tenant, caller, replicaDefinition), "task-a")
	assertVisibleTaskIDs(t, mustStructuredLiveRows(t, server, project, tenant, caller, livePlan), "task-a")

	selfJoinPlan := manifest.VisibilityPlan{
		Table: "user_live_locations", Key: "id",
		Sets: map[string]manifest.VisibilitySet{
			"teammates": {
				Table: "member_teams", Alias: "viewerTeams", Select: "member_id", SelectFrom: "peerTeams",
				Joins: []manifest.VisibilityJoin{{
					Table: "member_teams", Alias: "peerTeams", LeftAlias: "viewerTeams",
					LeftColumn: "team_id", RightColumn: "team_id",
				}},
				Where: []manifest.VisibilityConstraint{{Table: "viewerTeams", Column: "member_id", Context: "member.id"}},
			},
		},
		Where: &manifest.VisibilityExpression{Operator: "inSet", Column: "member_id", Set: "teammates"},
	}
	resolved, err := loadVisibilityContextFrom(context.Background(), db, project, tenant, caller, selfJoinPlan)
	if err != nil {
		t.Fatal(err)
	}
	if !visibilityRawRowMatches(json.RawMessage(`{"id":"location-peer","member_id":"member-peer"}`), selfJoinPlan, resolved) {
		t.Fatal("self-join visibility did not include a teammate")
	}
	if _, err := db.Exec(`DELETE FROM member_teams WHERE id='team-peer'`); err != nil {
		t.Fatal(err)
	}
	resolved, err = loadVisibilityContextFrom(context.Background(), db, project, tenant, caller, selfJoinPlan)
	if err != nil {
		t.Fatal(err)
	}
	if visibilityRawRowMatches(json.RawMessage(`{"id":"location-peer","member_id":"member-peer"}`), selfJoinPlan, resolved) {
		t.Fatal("self-join visibility retained a teammate after membership deletion")
	}
	countPlan := livePlan
	countPlan.ResultPath = []string{"rows"}
	countPlan.Window = &manifest.LiveWindow{OffsetArgument: "offset", LimitArgument: "limit", DefaultLimit: 100, MaxLimit: 100, Count: "exact"}
	countResult, err := server.executeStructuredLiveQuery(context.Background(), project, tenant, caller, countPlan, []byte(`{"offset":0,"limit":1}`))
	if err != nil {
		t.Fatal(err)
	}
	countEnvelope, ok := countResult.(map[string]any)
	if !ok {
		t.Fatalf("exact-count Live Query result type = %T", countResult)
	}
	if countEnvelope["total"] != int(1) && countEnvelope["total"] != int64(1) && countEnvelope["total"] != float64(1) {
		t.Fatalf("exact-count total = %#v, want 1", countEnvelope["total"])
	}
	if rows, ok := countEnvelope["rows"].([]any); !ok || len(rows) != 1 {
		t.Fatalf("exact-count rows = %#v, want one visible row", countEnvelope["rows"])
	}
	filteredPlan := livePlan
	filteredPlan.ResultPath = []string{"rows"}
	filteredPlan.Filters = &manifest.LiveFilters{
		Argument:         "filters",
		AllowedColumns:   []string{"title"},
		AllowedOperators: []manifest.FilterOperator{"contains"},
	}
	filteredPlan.Window = &manifest.LiveWindow{OffsetArgument: "offset", LimitArgument: "limit", DefaultLimit: 100, MaxLimit: 100, Count: "exact"}
	filteredResult, err := server.executeStructuredLiveQuery(context.Background(), project, tenant, caller, filteredPlan, []byte(`{"filters":[{"column":"title","operator":"contains","value":"visible"}],"offset":0,"limit":1}`))
	if err != nil {
		t.Fatal(err)
	}
	filteredEnvelope, ok := filteredResult.(map[string]any)
	if !ok || filteredEnvelope["total"] != int(1) && filteredEnvelope["total"] != int64(1) && filteredEnvelope["total"] != float64(1) {
		t.Fatalf("structured filter exact count = %#v, want 1", filteredResult)
	}
	if _, err := server.executeStructuredLiveQuery(context.Background(), project, tenant, caller, filteredPlan, []byte(`{"filters":[{"column":"workspace_id","operator":"contains","value":"workspace"}],"offset":0,"limit":1}`)); err == nil {
		t.Fatal("unallowlisted structured filter column was accepted")
	}
	if _, err := server.executeStructuredLiveQuery(context.Background(), project, tenant, caller, filteredPlan, []byte(`{"filters":[{"column":"title","operator":"server","value":"x"}],"offset":0,"limit":1}`)); err == nil {
		t.Fatal("unallowlisted structured filter operator was accepted")
	}

	subscription := querySubscription{
		ctx: context.Background(), project: project, tenant: tenant, path: "tasks.grid", caller: caller,
	}
	firstFingerprint := server.subscriptions.resolveAttachVisibilityKey(subscription)
	if firstFingerprint == "" || strings.HasPrefix(firstFingerprint, "denied:") {
		t.Fatalf("initial visibility fingerprint = %q", firstFingerprint)
	}
	if _, err := db.Exec(`
		DELETE FROM workspace_members WHERE id = 'membership-a';
		INSERT INTO workspace_members (id, member_id, workspace_id)
		VALUES ('membership-b', 'member-a', 'workspace-b');
	`); err != nil {
		t.Fatal(err)
	}
	// Do not call invalidateVisibilityContexts here. This deliberately models
	// the interval after COMMIT and before the LISTEN notification is processed.
	assertVisibleTaskIDs(t, mustStructuredReplicaRows(t, server, project, tenant, caller, replicaDefinition), "task-b")
	assertVisibleTaskIDs(t, mustStructuredLiveRows(t, server, project, tenant, caller, livePlan), "task-b")
	secondFingerprint := server.subscriptions.resolveAttachVisibilityKey(subscription)
	if secondFingerprint == firstFingerprint || strings.HasPrefix(secondFingerprint, "denied:") {
		t.Fatalf("attach reused stale visibility fingerprint across committed membership change: before=%q after=%q", firstFingerprint, secondFingerprint)
	}
	if _, err := db.Exec(`UPDATE members SET status = 'revoked' WHERE id = 'member-a'`); err != nil {
		t.Fatal(err)
	}
	if key := server.subscriptions.resolveAttachVisibilityKey(subscription); key != "denied:account-a" {
		t.Fatalf("revoked membership attach key = %q, want fail-closed denial", key)
	}
	if _, err := server.executeStructuredLiveQuery(context.Background(), project, tenant, caller, livePlan, nil); err == nil {
		t.Fatal("revoked membership retained Live Query access")
	}
}

func mustStructuredReplicaRows(t *testing.T, server *Server, project, tenant string, caller callerContext, definition manifest.ReplicaCollectionDefinition) []any {
	t.Helper()
	result, err := server.executeStructuredReplicaQuery(context.Background(), project, tenant, caller, definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, ok := result.([]any)
	if !ok {
		t.Fatalf("Replica Collection result type = %T", result)
	}
	return rows
}

func mustStructuredLiveRows(t *testing.T, server *Server, project, tenant string, caller callerContext, plan manifest.LiveQueryPlan) []any {
	t.Helper()
	result, err := server.executeStructuredLiveQuery(context.Background(), project, tenant, caller, plan, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, ok := result.([]any)
	if !ok {
		t.Fatalf("Live Query result type = %T", result)
	}
	return rows
}

func assertVisibleTaskIDs(t *testing.T, rows []any, want ...string) {
	t.Helper()
	got := map[string]bool{}
	for _, raw := range rows {
		row, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("visible row type = %T", raw)
		}
		got[strings.TrimSpace(row["id"].(string))] = true
	}
	if len(got) != len(want) {
		t.Fatalf("visible task ids = %#v, want %#v", got, want)
	}
	for _, id := range want {
		if !got[id] {
			t.Fatalf("visible task ids = %#v, missing %q", got, id)
		}
	}
}
