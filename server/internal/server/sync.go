package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/dbpool"
	schemasync "github.com/gonvex/gonvex/server/internal/schema"
	"github.com/google/uuid"
)

type syncCursor struct {
	Epoch    string `json:"epoch"`
	Revision uint64 `json:"revision"`
}

type syncClock struct {
	DatabaseEpoch    string
	Revision         uint64
	RetainedRevision uint64
}

type mutationIDContextKey struct{}

func withMutationID(ctx context.Context, mutationID string) context.Context {
	return context.WithValue(ctx, mutationIDContextKey{}, strings.TrimSpace(mutationID))
}

func mutationIDFromContext(ctx context.Context) string {
	value, _ := ctx.Value(mutationIDContextKey{}).(string)
	return value
}

type syncSubscription struct {
	conn             *wsConn
	id               string
	path             string
	project          string
	tenant           string
	args             json.RawMessage
	definition       manifest.SyncDefinition
	cursor           syncCursor
	listenerAcquired bool
	visibleKeys      map[string]bool
	visibleHashes    map[string]string
	clientDigest     string
	fullIntegrity    bool
	truncated        bool
	// verified means visibleKeys/visibleHashes were derived from an
	// authoritative handler result on this server connection, rather than
	// accepted from an untrusted persisted client cache.
	verified        bool
	mu              sync.Mutex
	closed          bool
	deliveryRunning bool
	deliveryPending bool
}

type syncLogChange struct {
	revision   uint64
	ordinal    int
	mutationID string
	table      string
	rowID      string
	operation  string
	oldValue   json.RawMessage
	newValue   json.RawMessage
}

const (
	defaultSyncRetention = 7 * 24 * time.Hour
	syncPruneInterval    = time.Hour
	syncDeliveryTimeout  = 30 * time.Second
	// Included in every cursor epoch. Bump whenever the meaning of a resumable
	// cursor becomes stricter so persisted clients are forced through a fresh
	// snapshot instead of inheriting an older, weaker freshness guarantee.
	syncCursorSemanticsVersion = 3
)

var errSyncCursorExpired = errors.New("sync cursor is older than the retained change log")

func manifestSyncDefinitions(current manifest.Manifest) map[string]manifest.SyncDefinition {
	definitions := map[string]manifest.SyncDefinition{}
	for _, entry := range current.Functions {
		if entry.Kind != manifest.FunctionKindSync || entry.Sync == nil {
			continue
		}
		definition := effectiveSyncDefinition(entry)
		table := strings.TrimSpace(definition.Table)
		if table == "" {
			continue
		}
		if existing, ok := definitions[table]; ok {
			existing.Columns = appendUniqueStrings(existing.Columns, definition.Columns...)
			for column, argument := range definition.EqualFilters {
				if existing.EqualFilters == nil {
					existing.EqualFilters = map[string]string{}
				}
				existing.EqualFilters[column] = argument
			}
			existing.VisibilityTables = appendUniqueStrings(existing.VisibilityTables, definition.VisibilityTables...)
			existing.ExcludeWhenSet = appendUniqueStrings(existing.ExcludeWhenSet, definition.ExcludeWhenSet...)
			if definition.OrderBy != "" {
				existing.OrderBy = definition.OrderBy
			}
			definitions[table] = existing
			continue
		}
		definition.Columns = appendUniqueStrings(nil, definition.Columns...)
		definitions[table] = definition
	}
	return definitions
}

func effectiveSyncDefinition(entry manifest.FunctionEntry) manifest.SyncDefinition {
	definition := *entry.Sync
	definition.VisibilityTables = appendUniqueStrings(nil, definition.VisibilityTables...)
	for _, read := range entry.Dependencies.Reads {
		table := strings.TrimSpace(read.Table)
		if table == "" || table == definition.Table {
			continue
		}
		definition.VisibilityTables = appendUniqueStrings(definition.VisibilityTables, table)
	}
	return definition
}

func syncDefinitionsForSchema(definitions map[string]manifest.SyncDefinition, current manifest.Schema) (map[string]manifest.SyncDefinition, error) {
	filtered := map[string]manifest.SyncDefinition{}
	for table, definition := range definitions {
		if _, ok := current.Tables[table]; ok {
			filtered[table] = definition
		}
	}
	sourceDefinitions := make([]manifest.SyncDefinition, 0, len(filtered))
	for _, definition := range filtered {
		sourceDefinitions = append(sourceDefinitions, definition)
	}
	// Visibility dependencies affect the authorized or computed result without
	// necessarily changing the source row. They therefore need durable log
	// triggers too; an in-memory NOTIFY can protect connected clients but cannot
	// prove freshness after an offline reconnect.
	for _, definition := range sourceDefinitions {
		for _, tableName := range definition.VisibilityTables {
			if _, exists := filtered[tableName]; exists {
				continue
			}
			table, exists := current.Tables[tableName]
			if !exists {
				continue
			}
			key := ""
			for columnName, column := range table.Columns {
				if column.PrimaryKey && (key == "" || columnName < key) {
					key = columnName
				}
			}
			if key == "" {
				return nil, fmt.Errorf("sync visibility dependency %q has no primary key for durable invalidation", tableName)
			}
			filtered[tableName] = manifest.SyncDefinition{
				Table:   tableName,
				Key:     key,
				Columns: []string{key},
			}
		}
	}
	return filtered, nil
}

func (s *Server) installTenantSyncLog(ctx context.Context, project, databaseURL string, current manifest.Schema) error {
	definitions, err := syncDefinitionsForSchema(
		manifestSyncDefinitions(s.runtime.ManifestForProject(project)),
		current,
	)
	if err != nil {
		return err
	}
	if len(definitions) == 0 {
		return nil
	}
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = schemasync.InstallSyncLog(ctx, db, current, definitions)
	return err
}

func (c *wsConn) openSync(ctx context.Context, message clientMessage) {
	started := time.Now()
	databaseURL := c.server.databaseURLForTenant(c.project, c.tenant)
	c.server.scheduleSyncLogPrune(c.project, c.tenant, databaseURL)
	clock, err := currentSyncClock(ctx, databaseURL)
	if err != nil {
		c.server.metrics.recordOperationalLog(c.syncProtocolLog(message, "open", 0, time.Since(started), err), time.Now().UTC())
		c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: err.Error()})
		return
	}
	c.openSyncWithClock(ctx, message, databaseURL, clock)
}

func (c *wsConn) openSyncMany(ctx context.Context, opens []syncOpenRequest) {
	if len(opens) == 0 {
		return
	}
	if len(opens) > 256 {
		c.write(serverMessage{Type: "sync.error", Error: "sync batch cannot contain more than 256 opens"})
		return
	}
	databaseURL := c.server.databaseURLForTenant(c.project, c.tenant)
	c.server.scheduleSyncLogPrune(c.project, c.tenant, databaseURL)
	start, err := currentSyncClock(ctx, databaseURL)
	if err != nil {
		for _, open := range opens {
			message := clientMessage{ID: open.ID, Path: open.Path, Args: open.Args}
			c.server.metrics.recordOperationalLog(c.syncProtocolLog(message, "open", 0, 0, err), time.Now().UTC())
			// A batched open still represents independent subscriptions. Route the
			// storage/bootstrap failure to every id so clients can mark each
			// collection failed and activate their query fallback. A single
			// id-less error is dispatched as a system frame and leaves all callers
			// waiting forever.
			c.write(serverMessage{Type: "sync.error", ID: open.ID, Path: open.Path, Error: err.Error()})
		}
		return
	}

	for _, open := range opens {
		c.openSyncWithClock(ctx, clientMessage{
			ID: open.ID, Path: open.Path, Args: open.Args, Cursor: open.Cursor, Keys: open.Keys, Hashes: open.Hashes,
			Digest: open.Digest, FullIntegrity: open.FullIntegrity,
		}, databaseURL, start)
	}
}

func (c *wsConn) openSyncWithClock(
	ctx context.Context,
	message clientMessage,
	databaseURL string,
	clock syncClock,
) {
	started := time.Now()
	phase := "open"
	resultCount := 0
	var protocolErr error
	defer func() {
		// One attributed entry feeds both call metrics and the error inbox. A
		// second recordFunction entry would duplicate the failure and replace
		// the latest occurrence with one lacking tenant and client context.
		c.server.metrics.recordRuntimeLog(
			c.syncProtocolLog(message, phase, resultCount, time.Since(started), protocolErr),
			time.Now().UTC(),
		)
	}()
	if strings.TrimSpace(message.ID) == "" || strings.TrimSpace(message.Path) == "" {
		protocolErr = errors.New("sync id and path are required")
		c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: protocolErr.Error()})
		return
	}
	current := c.server.runtime.ManifestForProject(c.project)
	entry, ok := current.Functions[message.Path]
	if !ok || entry.Kind != manifest.FunctionKindSync || entry.Sync == nil {
		protocolErr = errors.New("sync function is not registered")
		c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: protocolErr.Error()})
		return
	}
	definition := effectiveSyncDefinition(entry)
	base := syncCursorForClock(clock, definition, c.currentSyncScope())

	subscription := &syncSubscription{
		conn: c, id: message.ID, path: message.Path, project: c.project, tenant: c.tenant,
		args: append(json.RawMessage(nil), message.Args...), definition: definition, cursor: base,
		visibleKeys: syncKeySet(message.Keys), visibleHashes: cloneStringMap(message.Hashes),
		clientDigest: strings.TrimSpace(message.Digest), fullIntegrity: message.FullIntegrity,
	}
	c.closeSync(message.ID)

	if message.Cursor != nil && message.Cursor.Epoch == base.Epoch && message.Cursor.Revision <= base.Revision {
		resumable := message.Cursor.Revision >= clock.RetainedRevision
		if resumable {
			phase = "resume"
			subscription.cursor = *message.Cursor
			if err := c.attachSync(ctx, subscription); err != nil {
				protocolErr = err
				c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: err.Error()})
				return
			}
			if err := c.server.deliverSync(subscription); err != nil {
				protocolErr = err
				c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: err.Error()})
			}
			return
		}
	}

	phase = "snapshot"
	release, admitted := c.server.acquireQueryAdmission(ctx, admissionBootstrap, c.project, c.tenant)
	if !admitted {
		protocolErr = ctx.Err()
		c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: protocolErr.Error()})
		return
	}
	result, err := c.server.executeTenantQueryForCallerUncached(ctx, c.project, c.tenant, c.caller(), message.Path, message.Args)
	release()
	if err != nil {
		protocolErr = err
		c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: err.Error()})
		return
	}
	rows, truncated, err := syncSnapshotRows(result, definition)
	if err != nil {
		protocolErr = err
		c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: err.Error()})
		return
	}
	resultCount = len(rows)
	subscription.visibleKeys = syncRowsKeySet(rows, definition.Key)
	subscription.visibleHashes = syncRowsHashes(rows, definition.Key)
	subscription.truncated = truncated
	subscription.verified = true
	if err := c.attachSync(ctx, subscription); err != nil {
		protocolErr = err
		c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: err.Error()})
		return
	}
	c.write(serverMessage{
		Type: "sync.snapshot", ID: message.ID, Path: message.Path, Result: rows, Cursor: &base,
		Key: definition.Key, OrderBy: definition.OrderBy, OrderDirection: definition.OrderDirection,
		Mode: definition.Mode, MaxRows: definition.MaxRows, MaxBytes: definition.MaxBytes,
		Truncated: syncTruncatedField(truncated),
	})
	if err := c.server.deliverSync(subscription); err != nil {
		protocolErr = err
		c.write(serverMessage{Type: "sync.error", ID: message.ID, Path: message.Path, Error: err.Error()})
	}
	return
}

func (c *wsConn) syncProtocolLog(message clientMessage, phase string, resultCount int, duration time.Duration, protocolErr error) runtimeLogEntry {
	completed := time.Now().UTC()
	c.mu.Lock()
	project, tenant, connectionID, device := c.project, c.tenant, c.id, c.device
	userID, userEmail := "", ""
	if c.user != nil {
		userID, userEmail = c.user.ID, c.user.Email
	}
	c.mu.Unlock()
	outcome, errorMessage := "ok", ""
	if protocolErr != nil {
		outcome, errorMessage = "error", protocolErr.Error()
	}
	var resultCountValue *int
	if phase == "snapshot" && protocolErr == nil {
		resultCountValue = &resultCount
	}
	path := strings.TrimSpace(message.Path)
	if path == "" {
		path = "sync.open"
	}
	return runtimeLogEntry{
		Time: completed.Format(time.RFC3339Nano), CompletedAt: completed.Format(time.RFC3339Nano),
		StartedAt: completed.Add(-duration).Format(time.RFC3339Nano), ExecutionID: uuid.NewString(), OperationID: message.ID,
		Project: project, Tenant: tenant, UserID: userID, UserEmail: userEmail,
		ConnectionID: connectionID,
		Browser:      strings.TrimSpace(strings.Join([]string{device.BrowserName, device.BrowserVersion}, " ")),
		DeviceType:   device.DeviceType, Platform: device.Platform,
		Path: path, Kind: "sync", Outcome: outcome, DurationMS: float64(duration.Microseconds()) / 1000,
		Error: errorMessage, Source: "websocket", Reason: phase,
		Request: sanitizeRuntimeLogRequest(message.Args), RequestSizeBytes: len(message.Args), ResultCount: resultCountValue,
	}
}

func (s *Server) scheduleSyncLogPrune(project, tenant, databaseURL string) {
	key := project + "\x00" + tenant
	now := time.Now()
	s.syncPruneMu.Lock()
	if last := s.syncPrunedAt[key]; !last.IsZero() && now.Sub(last) < syncPruneInterval {
		s.syncPruneMu.Unlock()
		return
	}
	s.syncPrunedAt[key] = now
	s.syncPruneMu.Unlock()

	retention := syncRetentionForManifest(s.runtime.ManifestForProject(project))
	go func() {
		if err := pruneSyncLog(context.Background(), databaseURL, retention); err != nil {
			slog.Warn("could not prune Gonvex sync log", "project", project, "tenant", tenant, "error", err)
		}
	}()
}

func syncRetentionForManifest(current manifest.Manifest) time.Duration {
	retention := defaultSyncRetention
	for _, entry := range current.Functions {
		if entry.Kind != manifest.FunctionKindSync || entry.Sync == nil || entry.Sync.RetentionMilliseconds <= 0 {
			continue
		}
		candidate := time.Duration(entry.Sync.RetentionMilliseconds) * time.Millisecond
		if candidate > retention {
			retention = candidate
		}
	}
	return retention
}

func pruneSyncLog(ctx context.Context, databaseURL string, retention time.Duration) error {
	if strings.TrimSpace(databaseURL) == "" {
		return fmt.Errorf("tenant database is not configured")
	}
	if retention <= 0 {
		retention = defaultSyncRetention
	}
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var prunedThrough sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		WITH removed AS (
			DELETE FROM _gonvex_sync_changes
			WHERE created_at < clock_timestamp() - ($1::bigint * interval '1 millisecond')
			RETURNING revision
		)
		SELECT max(revision) FROM removed
	`, retention.Milliseconds()).Scan(&prunedThrough); err != nil {
		return err
	}
	if prunedThrough.Valid {
		if _, err := tx.ExecContext(ctx, `
			UPDATE _gonvex_sync_clock
			SET retained_revision = greatest(retained_revision, $1)
			WHERE singleton = true
		`, prunedThrough.Int64); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func syncSnapshotRows(result any, definition manifest.SyncDefinition) ([]json.RawMessage, bool, error) {
	payload, err := json.Marshal(explicitNull(result))
	if err != nil {
		return nil, false, err
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(payload, &rows); err != nil {
		return nil, false, fmt.Errorf("sync handler must return an array of keyed rows")
	}
	maxRows := definition.MaxRows
	maxBytes := definition.MaxBytes
	totalBytes := int64(0)
	kept := make([]json.RawMessage, 0, len(rows))
	seen := map[string]bool{}
	for _, row := range rows {
		key := syncRowKey(row, definition.Key)
		if key == "" {
			return nil, false, fmt.Errorf("sync row is missing key %q", definition.Key)
		}
		if seen[key] {
			return nil, false, fmt.Errorf("sync handler returned duplicate key %q", key)
		}
		seen[key] = true
		if maxRows > 0 && len(kept) >= maxRows {
			return kept, true, nil
		}
		canonical := canonicalSyncJSON(row)
		if maxBytes > 0 && totalBytes+int64(len(canonical)) > maxBytes {
			return kept, true, nil
		}
		totalBytes += int64(len(canonical))
		kept = append(kept, row)
	}
	return kept, false, nil
}

func syncTruncatedField(truncated bool) *bool {
	return &truncated
}

func syncRowKey(row json.RawMessage, key string) string {
	var object map[string]json.RawMessage
	if json.Unmarshal(row, &object) != nil {
		return ""
	}
	var value string
	if json.Unmarshal(object[key], &value) == nil {
		return value
	}
	var scalar any
	if json.Unmarshal(object[key], &scalar) == nil && scalar != nil {
		switch scalar.(type) {
		case float64, bool:
			// JSON number formatting intentionally matches JavaScript's
			// String(number) boundaries (not fmt.Sprint's scientific-notation
			// threshold), and normalizeSyncJSON makes -0 become "0".
			return string(canonicalSyncJSON(object[key]))
		}
	}
	return ""
}

func currentSyncClock(ctx context.Context, databaseURL string) (syncClock, error) {
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return syncClock{}, err
	}
	defer db.Close()
	var clock syncClock
	if err := db.QueryRowContext(
		ctx,
		`SELECT epoch, revision, retained_revision FROM _gonvex_sync_clock WHERE singleton = true`,
	).Scan(&clock.DatabaseEpoch, &clock.Revision, &clock.RetainedRevision); err != nil {
		return syncClock{}, fmt.Errorf("sync storage is not installed: %w", err)
	}
	return clock, nil
}

// syncCursorForClock derives the cursor epoch from the database epoch, the
// sync definition, and the caller's visibility scope (project/tenant/user/
// permissions). The code-bundle epoch is deliberately excluded: a resumed
// cursor is never trusted blindly — deliverAuthoritativeSync re-runs the query
// and repairs drift via deltas — so cursors may safely survive deploys.
func syncCursorForClock(clock syncClock, definition manifest.SyncDefinition, visibilityScope string) syncCursor {
	payload, _ := json.Marshal(struct {
		Semantics     int                     `json:"semantics"`
		DatabaseEpoch string                  `json:"databaseEpoch"`
		Definition    manifest.SyncDefinition `json:"definition"`
		Scope         string                  `json:"scope"`
	}{syncCursorSemanticsVersion, clock.DatabaseEpoch, definition, visibilityScope})
	hash := sha256.Sum256(payload)
	return syncCursor{Epoch: hex.EncodeToString(hash[:16]), Revision: clock.Revision}
}

func currentSyncCursor(ctx context.Context, databaseURL string, definition manifest.SyncDefinition, visibilityScope string) (syncCursor, error) {
	clock, err := currentSyncClock(ctx, databaseURL)
	if err != nil {
		return syncCursor{}, err
	}
	return syncCursorForClock(clock, definition, visibilityScope), nil
}

func (s *Server) deliverSync(subscription *syncSubscription) error {
	ctx, cancel := context.WithTimeout(context.Background(), syncDeliveryTimeout)
	defer cancel()

	// Authentication invalidation resets every sync on the connection and takes
	// each subscription lock. Revalidate before taking this subscription lock
	// to avoid self-deadlocking on an expired or revoked session.
	if err := subscription.conn.revalidateAppAuth(ctx); err != nil {
		subscription.conn.clearAuthentication()
		return nil
	}

	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed || !subscriptionCurrent(subscription) {
		return nil
	}
	databaseURL := s.databaseURLForTenant(subscription.project, subscription.tenant)
	latest, err := currentSyncCursor(ctx, databaseURL, subscription.definition, subscription.conn.currentSyncScope())
	if err != nil {
		return err
	}
	if latest.Epoch != subscription.cursor.Epoch {
		s.resetSyncWhileLocked(subscription, "definition-changed")
		return nil
	}
	if latest.Revision <= subscription.cursor.Revision {
		if !subscription.verified {
			settled, err := s.deliverAuthoritativeSync(ctx, subscription, nil, latest)
			if err != nil {
				return err
			}
			if !settled {
				return nil
			}
		}
		s.writeSyncReady(subscription, syncReadyServerMessage(subscription, latest))
		return nil
	}
	changes, err := readSyncChanges(
		ctx,
		databaseURL,
		subscription.cursor.Revision,
		latest.Revision,
		append([]string{subscription.definition.Table}, subscription.definition.VisibilityTables...)...,
	)
	if err != nil {
		if errors.Is(err, errSyncCursorExpired) {
			s.resetSyncWhileLocked(subscription, "cursor-expired")
			return nil
		}
		return err
	}
	args := map[string]json.RawMessage{}
	_ = json.Unmarshal(subscription.args, &args)
	if len(changes) == 0 && subscription.verified {
		subscription.cursor = latest
		s.writeSyncReady(subscription, syncReadyServerMessage(subscription, latest))
		return nil
	}
	authoritativeReconcile := syncNeedsAuthoritativeReconcile(subscription.definition, changes)
	if subscription.verified && authoritativeReconcile {
		subscription.conn.write(serverMessage{
			Type: "sync.syncing", ID: subscription.id, Path: subscription.path, Reason: "reconciling",
		})
	}
	if !subscription.verified {
		settled, err := s.deliverAuthoritativeSync(ctx, subscription, changes, latest)
		if err != nil {
			return err
		}
		if !settled {
			return nil
		}
		s.writeSyncReady(subscription, syncReadyServerMessage(subscription, subscription.cursor))
		return nil
	}
	if authoritativeReconcile {
		settled, err := s.deliverAuthoritativeSync(ctx, subscription, changes, latest)
		if err != nil {
			return err
		}
		if !settled {
			return nil
		}
		s.writeSyncReady(subscription, syncReadyServerMessage(subscription, subscription.cursor))
		return nil
	}
	for _, batch := range groupSyncChanges(changes) {
		upserts := map[string]json.RawMessage{}
		deleted := map[string]bool{}
		mutationIDs := map[string]bool{}
		for _, change := range batch.changes {
			newMatches := syncValueMatches(change.newValue, subscription.definition, args)
			switch {
			case newMatches:
				upserts[change.rowID] = change.newValue
				delete(deleted, change.rowID)
				subscription.visibleKeys[change.rowID] = true
				subscription.visibleHashes[change.rowID] = syncRowHash(change.newValue)
			case subscription.visibleKeys[change.rowID]:
				delete(upserts, change.rowID)
				deleted[change.rowID] = true
				delete(subscription.visibleKeys, change.rowID)
				delete(subscription.visibleHashes, change.rowID)
			}
			if change.mutationID != "" {
				mutationIDs[change.mutationID] = true
			}
		}
		cursor := syncCursor{Epoch: latest.Epoch, Revision: batch.revision}
		subscription.conn.write(serverMessage{
			Type: "sync.delta", ID: subscription.id, Path: subscription.path, Cursor: &cursor,
			Upserts: sortedSyncRows(upserts), Deleted: sortedSyncKeys(deleted), MutationIDs: sortedSyncKeys(mutationIDs),
			Digest: syncHashesDigest(subscription.visibleHashes),
		})
		subscription.cursor = cursor
	}
	if subscription.cursor.Revision < latest.Revision {
		subscription.cursor.Revision = latest.Revision
	}
	s.writeSyncReady(subscription, syncReadyServerMessage(subscription, subscription.cursor))
	return nil
}

func syncReadyServerMessage(subscription *syncSubscription, cursor syncCursor) serverMessage {
	return serverMessage{
		Type: "sync.ready", ID: subscription.id, Path: subscription.path, Cursor: &cursor,
		Mode: subscription.definition.Mode, Digest: syncHashesDigest(subscription.visibleHashes),
		Truncated: syncTruncatedField(subscription.truncated),
	}
}

func (s *Server) writeSyncReady(subscription *syncSubscription, message serverMessage) bool {
	_, ready := s.subscriptions.listeners.whileConnected(
		subscription.project,
		subscription.tenant,
		func() { subscription.conn.writeSyncReady(message) },
	)
	return ready
}

func (s *Server) resetSyncWhileLocked(subscription *syncSubscription, reason string) {
	subscription.conn.removeSync(subscription.id)
	subscription.closed = true
	acquired := subscription.listenerAcquired
	subscription.listenerAcquired = false
	if acquired {
		s.subscriptions.listeners.release(subscription.project, subscription.tenant)
	}
	subscription.conn.write(serverMessage{
		Type: "sync.reset", ID: subscription.id, Path: subscription.path, Reason: reason,
	})
}

func (s *Server) deliverAuthoritativeSync(
	ctx context.Context,
	subscription *syncSubscription,
	changes []syncLogChange,
	latest syncCursor,
) (bool, error) {
	release, admitted := s.acquireQueryAdmission(ctx, admissionReactive, subscription.project, subscription.tenant)
	if !admitted {
		return false, ctx.Err()
	}
	result, err := s.executeTenantQueryForCallerUncached(
		ctx,
		subscription.project,
		subscription.tenant,
		subscription.conn.caller(),
		subscription.path,
		subscription.args,
	)
	release()
	if err != nil {
		return false, err
	}
	rows, truncated, err := syncSnapshotRows(result, subscription.definition)
	if err != nil {
		return false, err
	}
	subscription.truncated = truncated
	currentRows := map[string]json.RawMessage{}
	currentKeys := map[string]bool{}
	for _, row := range rows {
		key := syncRowKey(row, subscription.definition.Key)
		if key == "" {
			continue
		}
		currentRows[key] = row
		currentKeys[key] = true
	}
	currentHashes := syncRowsHashes(rows, subscription.definition.Key)
	currentDigest := syncHashesDigest(currentHashes)
	if !subscription.verified && subscription.clientDigest != "" && !subscription.fullIntegrity {
		if subscription.clientDigest != currentDigest {
			s.pauseSyncForIntegrityWhileLocked(subscription)
			return false, nil
		}
		// The compact resume proof matched. No row identifiers or hashes need
		// to cross the wire on the overwhelmingly common unchanged path.
		subscription.visibleKeys = currentKeys
		subscription.visibleHashes = currentHashes
		subscription.verified = true
		subscription.cursor = latest
		return true, nil
	}

	upserts, deleted, mutationIDs := progressiveSyncDiff(
		currentRows,
		currentHashes,
		subscription.visibleKeys,
		subscription.visibleHashes,
		changes,
	)
	subscription.visibleKeys = currentKeys
	subscription.visibleHashes = currentHashes
	subscription.verified = true
	subscription.cursor = latest
	if len(upserts) > 0 || len(deleted) > 0 || len(mutationIDs) > 0 {
		subscription.conn.write(serverMessage{
			Type: "sync.delta", ID: subscription.id, Path: subscription.path, Cursor: &latest,
			Upserts: sortedSyncRows(upserts), Deleted: sortedSyncKeys(deleted), MutationIDs: sortedSyncKeys(mutationIDs),
			Digest: currentDigest,
		})
	}
	return true, nil
}

func (s *Server) pauseSyncForIntegrityWhileLocked(subscription *syncSubscription) {
	subscription.conn.removeSync(subscription.id)
	subscription.closed = true
	acquired := subscription.listenerAcquired
	subscription.listenerAcquired = false
	if acquired {
		s.subscriptions.listeners.release(subscription.project, subscription.tenant)
	}
	subscription.conn.write(serverMessage{
		Type: "sync.needHashes", ID: subscription.id, Path: subscription.path,
	})
}

func progressiveSyncDiff(
	currentRows map[string]json.RawMessage,
	currentHashes map[string]string,
	visibleKeys map[string]bool,
	visibleHashes map[string]string,
	changes []syncLogChange,
) (map[string]json.RawMessage, map[string]bool, map[string]bool) {
	upserts := map[string]json.RawMessage{}
	deleted := map[string]bool{}
	mutationIDs := map[string]bool{}
	for key, row := range currentRows {
		if !visibleKeys[key] || visibleHashes[key] != currentHashes[key] {
			upserts[key] = row
		}
	}
	for key := range visibleKeys {
		if _, exists := currentRows[key]; !exists {
			deleted[key] = true
		}
	}
	for _, change := range changes {
		if change.mutationID != "" {
			mutationIDs[change.mutationID] = true
		}
	}
	return upserts, deleted, mutationIDs
}

func syncRowsHashes(rows []json.RawMessage, keyField string) map[string]string {
	hashes := make(map[string]string, len(rows))
	for _, row := range rows {
		key := syncRowKey(row, keyField)
		if key == "" {
			continue
		}
		hashes[key] = syncRowHash(row)
	}
	return hashes
}

func syncRowHash(row json.RawMessage) string {
	sum := sha256.Sum256(canonicalSyncJSON(row))
	return hex.EncodeToString(sum[:])
}

// canonicalSyncJSON matches the client's stable JSON encoder: recursively
// sorted object keys, JavaScript-compatible JSON numbers, no HTML escaping,
// and escaped U+2028/U+2029. Decoding through float64 intentionally mirrors
// the JsonValue representation a browser obtains from JSON.parse.
func canonicalSyncJSON(value json.RawMessage) []byte {
	var decoded any
	if err := json.Unmarshal(value, &decoded); err != nil {
		return value
	}
	decoded = normalizeSyncJSON(decoded)
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(decoded); err != nil {
		return value
	}
	return bytes.TrimSpace(output.Bytes())
}

func normalizeSyncJSON(value any) any {
	switch current := value.(type) {
	case float64:
		// JSON.stringify(-0) is "0" while encoding/json preserves "-0".
		if current == 0 {
			return float64(0)
		}
		return current
	case []any:
		for index := range current {
			current[index] = normalizeSyncJSON(current[index])
		}
		return current
	case map[string]any:
		for key, nested := range current {
			current[key] = normalizeSyncJSON(nested)
		}
		return current
	default:
		return current
	}
}

func syncHashesDigest(hashes map[string]string) string {
	keys := make([]string, 0, len(hashes))
	for key := range hashes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([][2]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, [2]string{key, hashes[key]})
	}
	var payload bytes.Buffer
	encoder := json.NewEncoder(&payload)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(pairs)
	sum := sha256.Sum256(bytes.TrimSpace(payload.Bytes()))
	return hex.EncodeToString(sum[:])
}

func cloneStringMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		if strings.TrimSpace(key) != "" && strings.TrimSpace(value) != "" {
			result[key] = value
		}
	}
	return result
}

func syncKeySet(keys []string) map[string]bool {
	result := map[string]bool{}
	for _, key := range keys {
		if key = strings.TrimSpace(key); key != "" {
			result[key] = true
		}
	}
	return result
}

func syncRowsKeySet(rows []json.RawMessage, keyField string) map[string]bool {
	result := map[string]bool{}
	for _, row := range rows {
		if key := syncRowKey(row, keyField); key != "" {
			result[key] = true
		}
	}
	return result
}

type syncChangeBatch struct {
	revision uint64
	changes  []syncLogChange
}

func groupSyncChanges(changes []syncLogChange) []syncChangeBatch {
	batches := make([]syncChangeBatch, 0)
	for _, change := range changes {
		if len(batches) == 0 || batches[len(batches)-1].revision != change.revision {
			batches = append(batches, syncChangeBatch{revision: change.revision})
		}
		batches[len(batches)-1].changes = append(batches[len(batches)-1].changes, change)
	}
	return batches
}

func readSyncChanges(ctx context.Context, databaseURL string, after, through uint64, tables ...string) ([]syncLogChange, error) {
	db, err := dbpool.Open(databaseURL)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var retained uint64
	if err := tx.QueryRowContext(ctx, `SELECT retained_revision FROM _gonvex_sync_clock WHERE singleton = true`).Scan(&retained); err != nil {
		return nil, err
	}
	if after < retained {
		return nil, errSyncCursorExpired
	}
	tables = appendUniqueStrings(nil, tables...)
	if len(tables) == 0 {
		return nil, errors.New("sync replay requires at least one table")
	}
	queryArgs := []any{after, through}
	placeholders := make([]string, 0, len(tables))
	for _, table := range tables {
		queryArgs = append(queryArgs, table)
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(queryArgs)))
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT revision, ordinal, COALESCE(mutation_id, ''), table_name, row_id, operation,
		       COALESCE(old_value, 'null'::jsonb), COALESCE(new_value, 'null'::jsonb)
		FROM _gonvex_sync_changes
		WHERE revision > $1 AND revision <= $2 AND table_name IN (`+strings.Join(placeholders, ", ")+`)
		ORDER BY revision, ordinal
	`, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	changes := make([]syncLogChange, 0)
	for rows.Next() {
		var change syncLogChange
		if err := rows.Scan(
			&change.revision, &change.ordinal, &change.mutationID, &change.table,
			&change.rowID, &change.operation, &change.oldValue, &change.newValue,
		); err != nil {
			return nil, err
		}
		changes = append(changes, change)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return changes, nil
}

func syncVisibilityChanged(changes []syncLogChange, sourceTable string) bool {
	for _, change := range changes {
		if change.table != sourceTable {
			return true
		}
	}
	return false
}

func syncNeedsAuthoritativeReconcile(definition manifest.SyncDefinition, changes []syncLogChange) bool {
	return definition.Mode == "progressive" ||
		definition.MaxRows > 0 ||
		definition.MaxBytes > 0 ||
		syncVisibilityChanged(changes, definition.Table)
}

func syncValueMatches(value json.RawMessage, definition manifest.SyncDefinition, args map[string]json.RawMessage) bool {
	if len(value) == 0 || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return false
	}
	if len(definition.EqualFilters) == 0 && len(definition.ExcludeWhenSet) == 0 {
		return true
	}
	row := map[string]json.RawMessage{}
	if json.Unmarshal(value, &row) != nil {
		return false
	}
	for _, column := range definition.ExcludeWhenSet {
		raw, ok := row[column]
		if ok && len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return false
		}
	}
	for column, argument := range definition.EqualFilters {
		left, leftOK := row[column]
		right, rightOK := args[argument]
		if !leftOK || !rightOK || !jsonScalarEqual(left, right) {
			return false
		}
	}
	return true
}

func jsonScalarEqual(left, right json.RawMessage) bool {
	var leftValue any
	var rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	leftJSON, _ := json.Marshal(leftValue)
	rightJSON, _ := json.Marshal(rightValue)
	return bytes.Equal(leftJSON, rightJSON)
}

func sortedSyncRows(values map[string]json.RawMessage) []json.RawMessage {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]json.RawMessage, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, values[key])
	}
	return rows
}

func sortedSyncKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func subscriptionCurrent(subscription *syncSubscription) bool {
	subscription.conn.mu.Lock()
	defer subscription.conn.mu.Unlock()
	return subscription.conn.syncs[subscription.id] == subscription
}

func (c *wsConn) closeSync(id string) {
	c.mu.Lock()
	subscription := c.syncs[id]
	delete(c.syncs, id)
	c.resolvePendingWatermarksLocked(id, ^uint64(0))
	c.mu.Unlock()
	if subscription != nil {
		subscription.mu.Lock()
		subscription.closed = true
		subscription.mu.Unlock()
		c.releaseSyncListener(subscription)
	}
}

func (c *wsConn) removeSync(id string) {
	c.mu.Lock()
	delete(c.syncs, id)
	c.mu.Unlock()
}

func (c *wsConn) attachSync(ctx context.Context, subscription *syncSubscription) error {
	ready := c.server.subscriptions.listeners.acquire(subscription.project, subscription.tenant)
	if ready == nil {
		return errors.New("sync listener unavailable; freshness cannot be proven")
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ready:
		case <-ctx.Done():
			c.server.subscriptions.listeners.release(subscription.project, subscription.tenant)
			return ctx.Err()
		case <-timer.C:
			c.server.subscriptions.listeners.markNeedsRecovery(subscription.project, subscription.tenant)
			c.server.subscriptions.listeners.release(subscription.project, subscription.tenant)
			return errors.New("sync listener did not become ready; freshness cannot be proven")
		}
		next, attached := c.server.subscriptions.listeners.whileConnected(
			subscription.project,
			subscription.tenant,
			func() {
				// The subscription is not published yet, so these writes are
				// private to this goroutine and need no subscription lock.
				subscription.listenerAcquired = true
				c.mu.Lock()
				c.syncs[subscription.id] = subscription
				c.mu.Unlock()
			},
		)
		if attached {
			return nil
		}
		if next == nil {
			c.server.subscriptions.listeners.release(subscription.project, subscription.tenant)
			return errors.New("sync listener disappeared; freshness cannot be proven")
		}
		ready = next
	}
}

func (c *wsConn) closeAllSyncs() {
	c.mu.Lock()
	subscriptions := c.syncs
	c.syncs = map[string]*syncSubscription{}
	c.mu.Unlock()
	for _, subscription := range subscriptions {
		subscription.mu.Lock()
		subscription.closed = true
		subscription.mu.Unlock()
		c.releaseSyncListener(subscription)
	}
}

func (c *wsConn) resetSyncSubscriptions(reason string) {
	c.mu.Lock()
	subscriptions := c.syncs
	c.syncs = map[string]*syncSubscription{}
	c.mu.Unlock()
	for _, subscription := range subscriptions {
		subscription.mu.Lock()
		subscription.closed = true
		subscription.mu.Unlock()
		c.releaseSyncListener(subscription)
		c.write(serverMessage{Type: "sync.reset", ID: subscription.id, Path: subscription.path, Reason: reason})
	}
}

func (s *Server) notifySyncRevision(
	project string,
	tenant string,
	changedTables []string,
	notifiedDatabaseEpoch string,
	notifiedRevision uint64,
) {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0)
	for connection := range s.wsConns {
		if connection.project == project && connection.tenant == tenant {
			connections = append(connections, connection)
		}
	}
	s.wsMu.RUnlock()
	if len(changedTables) == 0 || strings.TrimSpace(notifiedDatabaseEpoch) == "" || notifiedRevision == 0 {
		s.scheduleAllSyncDeliveries(connections)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), syncDeliveryTimeout)
	clock, err := currentSyncClock(ctx, s.databaseURLForTenant(project, tenant))
	cancel()
	if err != nil || clock.DatabaseEpoch != notifiedDatabaseEpoch || clock.Revision < notifiedRevision {
		// Filtering is only an optimization. If the shared clock cannot prove the
		// notification belongs to the current database epoch, replay every sync.
		s.scheduleAllSyncDeliveries(connections)
		return
	}
	clock.Revision = notifiedRevision

	for _, connection := range connections {
		connection.mu.Lock()
		subscriptions := make([]*syncSubscription, 0, len(connection.syncs))
		for _, subscription := range connection.syncs {
			subscriptions = append(subscriptions, subscription)
		}
		useWatermark := connection.syncWatermark
		connection.mu.Unlock()
		waiting := make([]string, 0, len(subscriptions))
		deliveries := make([]*syncSubscription, 0, len(subscriptions))
		advanced := false
		for _, subscription := range subscriptions {
			if syncDefinitionIntersectsTables(subscription.definition, changedTables) {
				if useWatermark {
					waiting = append(waiting, subscription.id)
				}
				deliveries = append(deliveries, subscription)
				continue
			}
			handled, cursorAdvanced := s.advanceUnchangedSync(subscription, clock, !useWatermark)
			advanced = advanced || cursorAdvanced
			if !handled {
				if useWatermark {
					waiting = append(waiting, subscription.id)
				}
				deliveries = append(deliveries, subscription)
			}
		}
		if useWatermark && advanced {
			s.writeSyncWatermark(connection, clock.Revision, waiting)
		}
		for _, subscription := range deliveries {
			s.scheduleSyncDelivery(subscription)
		}
	}
}

func (s *Server) scheduleAllSyncDeliveries(connections []*wsConn) {
	for _, connection := range connections {
		connection.mu.Lock()
		subscriptions := make([]*syncSubscription, 0, len(connection.syncs))
		for _, subscription := range connection.syncs {
			subscriptions = append(subscriptions, subscription)
		}
		connection.mu.Unlock()
		for _, subscription := range subscriptions {
			s.scheduleSyncDelivery(subscription)
		}
	}
}

func syncDefinitionIntersectsTables(definition manifest.SyncDefinition, changedTables []string) bool {
	return intersectsStrings(
		append([]string{definition.Table}, definition.VisibilityTables...),
		changedTables,
	)
}

func (s *Server) advanceUnchangedSync(
	subscription *syncSubscription,
	clock syncClock,
	emitReady bool,
) (handled bool, advanced bool) {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed || !subscriptionCurrent(subscription) {
		return true, false
	}
	// A running delivery may cover an earlier relevant revision. Advancing past
	// it here would acknowledge unseen rows, so let the delivery coalescer replay
	// the full range instead.
	if !subscription.verified || subscription.deliveryRunning || subscription.deliveryPending {
		return false, false
	}
	latest := syncCursorForClock(clock, subscription.definition, subscription.conn.currentSyncScope())
	if latest.Epoch != subscription.cursor.Epoch {
		return false, false
	}
	if subscription.cursor.Revision >= latest.Revision {
		return true, false
	}
	// The table set describes exactly one transaction revision. A lagging cursor
	// has a wider, unknown table range and must use the durable change log.
	if subscription.cursor.Revision+1 != latest.Revision {
		return false, false
	}
	subscription.cursor = latest
	if emitReady {
		s.writeSyncReady(subscription, syncReadyServerMessage(subscription, latest))
	}
	return true, true
}

func (s *Server) writeSyncWatermark(connection *wsConn, revision uint64, waiting []string) bool {
	_, ready := s.subscriptions.listeners.whileConnected(
		connection.project,
		connection.tenant,
		func() { connection.writeSyncWatermark(revision, waiting) },
	)
	return ready
}

func (s *Server) markTenantSyncsOutOfDate(project, tenant, reason string) {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0)
	for connection := range s.wsConns {
		if connection.project == project && connection.tenant == tenant {
			connections = append(connections, connection)
		}
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.mu.Lock()
		subscriptions := make([]*syncSubscription, 0, len(connection.syncs))
		for _, subscription := range connection.syncs {
			subscriptions = append(subscriptions, subscription)
		}
		connection.mu.Unlock()
		for _, subscription := range subscriptions {
			connection.write(serverMessage{
				Type: "sync.syncing", ID: subscription.id, Path: subscription.path, Reason: reason,
			})
		}
	}
}

func beginSyncDelivery(subscription *syncSubscription) bool {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed {
		return false
	}
	if subscription.deliveryRunning {
		subscription.deliveryPending = true
		return false
	}
	subscription.deliveryRunning = true
	return true
}

func finishSyncDelivery(subscription *syncSubscription) bool {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed {
		subscription.deliveryRunning = false
		subscription.deliveryPending = false
		return false
	}
	if subscription.deliveryPending {
		subscription.deliveryPending = false
		return true
	}
	subscription.deliveryRunning = false
	return false
}

func (s *Server) scheduleSyncDelivery(subscription *syncSubscription) {
	if !beginSyncDelivery(subscription) {
		return
	}
	go func() {
		for {
			if err := s.deliverSync(subscription); err != nil && subscriptionCurrent(subscription) {
				subscription.conn.write(serverMessage{
					Type: "sync.error", ID: subscription.id, Path: subscription.path, Error: err.Error(),
				})
			}
			if !finishSyncDelivery(subscription) {
				return
			}
		}
	}()
}

func (s *Server) resetSyncsForVisibilityChange(change tableChange) {
	changedTables := tableChangeTables(change)
	s.wsMu.RLock()
	connections := make([]*wsConn, 0)
	for connection := range s.wsConns {
		if connection.project == change.project && connection.tenant == change.tenant {
			connections = append(connections, connection)
		}
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.mu.Lock()
		reset := make([]*syncSubscription, 0)
		reconcile := make([]*syncSubscription, 0)
		for id, subscription := range connection.syncs {
			if intersectsStrings(subscription.definition.VisibilityTables, changedTables) {
				if subscription.definition.Mode == "progressive" {
					reconcile = append(reconcile, subscription)
					continue
				}
				delete(connection.syncs, id)
				reset = append(reset, subscription)
			}
		}
		connection.mu.Unlock()
		for _, subscription := range reset {
			subscription.mu.Lock()
			subscription.closed = true
			subscription.mu.Unlock()
			connection.releaseSyncListener(subscription)
			connection.write(serverMessage{Type: "sync.reset", ID: subscription.id, Path: subscription.path, Reason: "visibility-changed"})
		}
		for _, subscription := range reconcile {
			s.scheduleSyncDelivery(subscription)
		}
	}
}

func (c *wsConn) releaseSyncListener(subscription *syncSubscription) {
	subscription.mu.Lock()
	acquired := subscription.listenerAcquired
	subscription.listenerAcquired = false
	subscription.mu.Unlock()
	if acquired {
		c.server.subscriptions.listeners.release(subscription.project, subscription.tenant)
	}
}
