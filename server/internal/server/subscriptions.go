package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
)

// Diffing tiny results costs more CPU than it can save on the wire. The
// encoded-size gate below still requires a 30% reduction, so this lower floor
// lets small, fast-growing collection envelopes become patchable without ever
// making their payload larger.
const minimumPatchResultBytes = 512

type subscriptionRevision struct {
	Epoch    string `json:"epoch"`
	Sequence uint64 `json:"sequence"`
}

type dependencyKey struct {
	project string
	tenant  string
	table   string
}

type subscriptionScope struct {
	project string
	tenant  string
}

type subscriptionManager struct {
	server *Server
	epoch  string

	mu      sync.Mutex
	groups  map[string]*sharedSubscription
	byTable map[dependencyKey]map[*sharedSubscription]struct{}
	broad   map[subscriptionScope]map[*sharedSubscription]struct{}
	// listenerCount is maintained while mu is held. Recomputing it by walking
	// every group makes distinct-user subscription startup O(n²).
	listenerCount    int
	listeners        *tenantListenerManager
	sequence         atomic.Uint64
	execute          func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error)
	visibilityMu     sync.Mutex
	visibilityRuns   map[string]*visibilityExecution
	visibilityAttach map[string]visibilityAttachEntry
}

type visibilityAttachEntry struct {
	key string
	at  time.Time
}

type visibilityExecution struct {
	done      chan struct{}
	result    any
	err       error
	prepareMu sync.Mutex
	prepared  map[string]visibilityPreparedResult
}

type visibilityPreparedResult struct {
	result any
	err    error
}

// visibilitySharedResult carries immutable work that is identical for every
// identity group admitted by the visibility proof. Delivery state (revision,
// cache scope, patch baseline, listeners) deliberately remains group-local.
type visibilitySharedResult struct {
	payload   json.RawMessage
	hash      [sha256.Size]byte
	queryPerf json.RawMessage
	rowIDs    map[string]bool
	patchMu   sync.Mutex
	patches   map[[sha256.Size]byte]visibilitySharedPatch
}

type visibilitySharedPatch struct {
	message     serverMessage
	encodedSize int
	ok          bool
}

type visibilityResultPartition struct {
	key       string
	listeners []querySubscription
	result    any
}

type visibilityPartitionedResult struct {
	partitions []visibilityResultPartition
}

type visibilityPartitionBaseline struct {
	payload  json.RawMessage
	hash     [sha256.Size]byte
	revision uint64
}

type sharedSubscription struct {
	manager             *subscriptionManager
	key                 string
	project             string
	tenant              string
	path                string
	args                json.RawMessage
	caller              callerContext
	cacheScope          string
	reads               []manifest.ReadDependency
	unknownDependencies bool
	retainSnapshot      bool

	ctx    context.Context
	cancel context.CancelFunc

	mu               sync.Mutex
	listeners        map[*subscriptionToken]querySubscription
	running          bool
	coalescing       bool
	awaitingListener bool
	dirty            bool
	// staleWhileIdle records an invalidation that arrived while the group had
	// no listeners (grace window). The retained snapshot predates that commit,
	// so a joining listener must trigger a fresh execution instead of being
	// served the stale replay — one-shot consumers (the legacy bridge, HTTP
	// queries) treat the first result as authoritative and would otherwise
	// read pre-mutation state forever.
	staleWhileIdle          bool
	requested               uint64
	completed               uint64
	revision                uint64
	pendingReason           string
	pendingChangedAtMS      float64
	pendingCommitIDs        map[string]struct{}
	activeCommitIDs         map[string]struct{}
	pendingRequestIDs       map[string]struct{}
	activeRequestIDs        map[string]struct{}
	pendingTables           map[string]struct{}
	activeTables            map[string]struct{}
	pendingTaskIDs          map[string]struct{}
	activeTaskIDs           map[string]struct{}
	completedCommitIDs      map[string]struct{}
	completedCommits        []string
	lastResult              json.RawMessage
	lastError               string
	lastHash                [sha256.Size]byte
	hasHash                 bool
	lastSingleListener      *subscriptionToken
	rowIDs                  map[string]bool
	partitionBaselines      map[string]visibilityPartitionBaseline
	listenerPartitions      map[*subscriptionToken]string
	visibilityUsers         []map[string]string
	visibilityUsersRevision uint64
	idleTimer               *time.Timer
}

func newSubscriptionManager(server *Server) *subscriptionManager {
	epochBytes := sha256.Sum256([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	manager := &subscriptionManager{
		server:           server,
		epoch:            hex.EncodeToString(epochBytes[:8]),
		groups:           map[string]*sharedSubscription{},
		byTable:          map[dependencyKey]map[*sharedSubscription]struct{}{},
		broad:            map[subscriptionScope]map[*sharedSubscription]struct{}{},
		visibilityRuns:   map[string]*visibilityExecution{},
		visibilityAttach: map[string]visibilityAttachEntry{},
	}
	manager.listeners = newTenantListenerManager(server)
	manager.execute = func(ctx context.Context, group *sharedSubscription, listener querySubscription, reason string, changedAtMS float64) (any, error) {
		return server.executeTenantQueryForCallerCached(ctx, group.project, group.tenant, listener.caller, group.path, group.args, group.cacheScope, reason)
	}
	return manager
}

func (m *subscriptionManager) attach(sub querySubscription) {
	sub.visibilityKey = m.resolveAttachVisibilityKey(sub)
	key, reads, unknown := m.groupKeyAndDependencies(sub)
	m.mu.Lock()
	baseKey := key
	group := m.groups[key]
	for shard := 1; group != nil && m.server.config.SharedSubscriptionMaxFanout > 0; shard++ {
		group.mu.Lock()
		full := len(group.listeners) >= m.server.config.SharedSubscriptionMaxFanout
		group.mu.Unlock()
		if !full {
			break
		}
		key = baseKey + ":" + strconv.Itoa(shard)
		group = m.groups[key]
	}
	created := false
	if group == nil {
		ctx, cancel := context.WithCancel(context.Background())
		retainSnapshot := false
		for _, read := range reads {
			retainSnapshot = retainSnapshot || read.Windowed
		}
		group = &sharedSubscription{
			manager: m, key: key, project: sub.project, tenant: sub.tenant,
			path: sub.path, args: append(json.RawMessage(nil), sub.args...), caller: sub.caller,
			cacheScope: m.executionCacheScope(sub), reads: reads, unknownDependencies: unknown, retainSnapshot: retainSnapshot,
			ctx: ctx, cancel: cancel, listeners: map[*subscriptionToken]querySubscription{}, awaitingListener: true,
		}
		m.groups[key] = group
		m.indexGroupLocked(group)
		created = true
	}
	group.mu.Lock()
	if group.idleTimer != nil {
		group.idleTimer.Stop()
		group.idleTimer = nil
	}
	if _, exists := group.listeners[sub.token]; !exists {
		m.listenerCount++
	}
	group.listeners[sub.token] = sub
	group.visibilityUsers = nil
	group.visibilityUsersRevision++
	hasSnapshot := len(group.lastResult) > 0 && !group.staleWhileIdle
	lastError := group.lastError
	running := group.running
	awaitingListener := group.awaitingListener
	revision := group.revision
	snapshot := append(json.RawMessage(nil), group.lastResult...)
	group.mu.Unlock()
	groups, listenerCount := m.countsLocked()
	m.mu.Unlock()
	m.server.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.SharedSubscriptions = groups
		metric.SubscriptionListeners = listenerCount
	})
	var listenerReady <-chan struct{}
	if created {
		listenerReady = m.listeners.acquire(sub.project, sub.tenant)
	}
	if hasSnapshot {
		// Preserve per-connection delivery order and avoid one goroutine per
		// late-listener snapshot. Each WebSocket already has its own reader
		// goroutine, so this only applies backpressure to that connection.
		group.sendFullTo(sub, snapshot, revision, "initial", 0)
		return
	}
	if lastError != "" {
		if listenerCurrent(sub) {
			sub.conn.write(serverMessage{Type: "query.error", ID: sub.id, Path: sub.path, Error: lastError})
		}
		return
	}
	if !created && (running || awaitingListener) {
		// The active execution broadcasts its first authoritative snapshot to
		// every listener, including this one. A newly created group may also be
		// waiting for its tenant LISTEN connection before that execution starts.
		return
	}
	if listenerReady == nil {
		group.markListenerReady()
		group.request("initial", 0)
		return
	}
	go func() {
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-listenerReady:
			group.markListenerReady()
			group.request("initial", 0)
		case <-timer.C:
			m.listeners.markNeedsRecovery(sub.project, sub.tenant)
			group.markListenerReady()
			group.request("initial", 0)
		case <-group.ctx.Done():
		}
	}()
}

func (group *sharedSubscription) markListenerReady() {
	group.mu.Lock()
	group.awaitingListener = false
	group.mu.Unlock()
}

func (m *subscriptionManager) detach(sub querySubscription) {
	if sub.token == nil {
		return
	}
	key, _, _ := m.groupKeyAndDependencies(sub)
	m.mu.Lock()
	group := m.groups[key]
	if group == nil {
		// A bundle/auth change can alter the computed key; find the listener by
		// its stable token instead of leaking the old group.
		for _, candidate := range m.groups {
			candidate.mu.Lock()
			_, found := candidate.listeners[sub.token]
			candidate.mu.Unlock()
			if found {
				group = candidate
				break
			}
		}
	}
	if group == nil {
		m.mu.Unlock()
		return
	}
	group.mu.Lock()
	if _, exists := group.listeners[sub.token]; exists {
		delete(group.listeners, sub.token)
		group.visibilityUsers = nil
		group.visibilityUsersRevision++
		m.listenerCount--
	}
	empty := len(group.listeners) == 0
	if empty && group.idleTimer == nil {
		grace := m.server.config.SharedSubscriptionGrace
		group.idleTimer = time.AfterFunc(grace, func() { m.expire(group) })
	}
	group.mu.Unlock()
	groups, listenerCount := m.countsLocked()
	m.mu.Unlock()
	m.server.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.SharedSubscriptions = groups
		metric.SubscriptionListeners = listenerCount
	})
}

func (m *subscriptionManager) expire(group *sharedSubscription) {
	m.mu.Lock()
	current := m.groups[group.key]
	if current != group {
		m.mu.Unlock()
		return
	}
	group.mu.Lock()
	if len(group.listeners) != 0 {
		group.idleTimer = nil
		group.mu.Unlock()
		m.mu.Unlock()
		return
	}
	delete(m.groups, group.key)
	m.unindexGroupLocked(group)
	group.idleTimer = nil
	group.cancel()
	group.mu.Unlock()
	groups, listenerCount := m.countsLocked()
	m.mu.Unlock()
	m.listeners.release(group.project, group.tenant)
	m.server.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.SharedSubscriptions = groups
		metric.SubscriptionListeners = listenerCount
	})
}

func (m *subscriptionManager) countsLocked() (int, int) {
	return len(m.groups), m.listenerCount
}

func (m *subscriptionManager) request(sub querySubscription, reason string, changedAtMS float64) {
	key, _, _ := m.groupKeyAndDependencies(sub)
	m.mu.Lock()
	group := m.groups[key]
	if group == nil {
		for _, candidate := range m.groups {
			candidate.mu.Lock()
			_, found := candidate.listeners[sub.token]
			candidate.mu.Unlock()
			if found {
				group = candidate
				break
			}
		}
	}
	m.mu.Unlock()
	if group != nil {
		group.request(reason, changedAtMS)
	}
}

func (m *subscriptionManager) requestChange(change tableChange) {
	m.mu.Lock()
	candidates := map[*sharedSubscription]struct{}{}
	scope := subscriptionScope{project: change.project, tenant: change.tenant}
	for group := range m.broad[scope] {
		candidates[group] = struct{}{}
	}
	// pendingChangeForDelivery already unions declared tables into change.tables
	// when the listener is unavailable. With an authoritative LISTEN batch,
	// declaredTables is accounting-only: selecting it here would rerun queries
	// for tables that provably had no committed row change.
	candidateTables := append(tableChangeTables(change), tableMapKeys(change.declaredTables)...)
	if change.broad && effectiveTableCount(change) == 0 {
		for _, group := range m.groups {
			if group.project == change.project && group.tenant == change.tenant {
				candidates[group] = struct{}{}
			}
		}
	}
	for _, table := range appendUniqueStrings(nil, candidateTables...) {
		for group := range m.byTable[dependencyKey{project: change.project, tenant: change.tenant, table: table}] {
			candidates[group] = struct{}{}
		}
	}
	inspected := len(candidates)
	selected := make([]*sharedSubscription, 0, inspected)
	skippedByTable := 0
	for group := range candidates {
		matches, tableMiss := group.matchResult(change)
		if matches {
			selected = append(selected, group)
		} else if tableMiss {
			skippedByTable++
		}
	}
	m.mu.Unlock()
	m.server.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.ChangeBatchesReceived++
		metric.SubscriptionsInspected += uint64(inspected)
		metric.CandidateSubscriptionsSelected += uint64(len(selected))
		metric.SubscriptionsSkippedByTable += uint64(skippedByTable)
	})
	// Every trigger notification for one mutation is observed after the whole
	// transaction committed. If a group was selected by an earlier table from
	// that commit, its query already sees the final state of all later table
	// notifications. Groups not selected by the earlier table have no matching
	// request and still run when their own dependency arrives.
	dedupID := strings.TrimSpace(change.commitID)
	for _, group := range selected {
		group.requestForCommitBatch("invalidate", change.changedAtMS, change.commitID, dedupID, change)
	}
}

func (m *subscriptionManager) refreshTenant(project, tenant string) {
	m.mu.Lock()
	groups := make([]*sharedSubscription, 0)
	for _, group := range m.groups {
		if group.project == project && group.tenant == tenant {
			groups = append(groups, group)
		}
	}
	m.mu.Unlock()
	changedAt := epochMillis(time.Now().UTC())
	for _, group := range groups {
		group.request("recover", changedAt)
	}
}

func (m *subscriptionManager) rebindProject(subs []querySubscription) {
	for _, sub := range subs {
		m.detach(sub)
		m.attach(sub)
	}
}

func (m *subscriptionManager) groupKeyAndDependencies(sub querySubscription) (string, []manifest.ReadDependency, bool) {
	current := m.server.runtime.ManifestForProject(sub.project)
	entry, exists := current.Functions[sub.path]
	reads := entry.Dependencies.Reads
	unknown := !exists || len(reads) == 0
	if unknown {
		for _, table := range subscriptionTables(sub.path) {
			reads = append(reads, manifest.ReadDependency{Table: table})
		}
		unknown = len(reads) == 0
	}
	bundleHash := ""
	if current.Bundle != nil {
		bundleHash = current.Bundle.Hash
	}
	userFingerprint := "anonymous"
	if sub.visibilityKey != "" {
		userFingerprint = "visibility:" + sub.visibilityKey
	} else if sub.caller.user != nil && sub.caller.user.ID != "" && !entry.Dependencies.ShareByPermissions {
		userFingerprint = sub.caller.user.ID
	}
	canonicalArgs := compactJSON(sub.args)
	executionCacheScope := m.executionCacheScopeForEntry(sub, entry)
	keyPayload, _ := json.Marshal(struct {
		Project     string          `json:"project"`
		Tenant      string          `json:"tenant"`
		Path        string          `json:"path"`
		Args        json.RawMessage `json:"args"`
		Permissions string          `json:"permissions"`
		User        string          `json:"user"`
		Bundle      string          `json:"bundle"`
		CacheScope  string          `json:"cacheScope"`
	}{
		sub.project,
		sub.tenant,
		sub.path,
		canonicalArgs,
		hashQueryCacheValue(sub.caller.permissions),
		userFingerprint,
		bundleHash,
		executionCacheScope,
	})
	sum := sha256.Sum256(keyPayload)
	return hex.EncodeToString(sum[:]), reads, unknown
}

func (m *subscriptionManager) resolveAttachVisibilityKey(sub querySubscription) string {
	entry := m.server.runtime.ManifestForProject(sub.project).Functions[sub.path]
	resolver := strings.TrimSpace(entry.Dependencies.ShareByVisibility)
	if resolver == "" || sub.caller.user == nil || strings.TrimSpace(sub.caller.user.ID) == "" {
		return ""
	}
	payload, _ := json.Marshal([]any{sub.project, sub.tenant, resolver, compactJSON(sub.args), sub.caller.user.ID, sub.caller.user.Email})
	sum := sha256.Sum256(payload)
	cacheKey := hex.EncodeToString(sum[:])
	m.visibilityMu.Lock()
	if cached, ok := m.visibilityAttach[cacheKey]; ok && time.Since(cached.at) < time.Second {
		m.visibilityMu.Unlock()
		return cached.key
	}
	m.visibilityMu.Unlock()
	values := map[string]any{}
	_ = json.Unmarshal(sub.args, &values)
	values["__gonvexVisibilityUsers"] = []map[string]string{{"id": sub.caller.user.ID, "email": sub.caller.user.Email}}
	resolverArgs, _ := json.Marshal(values)
	release, acquired := m.server.acquireQueryAdmission(sub.ctx, admissionBootstrap, sub.project, sub.tenant)
	if !acquired {
		return ""
	}
	result, err := m.server.executeTenantQueryForCallerUncached(sub.ctx, sub.project, sub.tenant, sub.caller, resolver, resolverArgs)
	release()
	if err != nil {
		return ""
	}
	key := visibilityKeyForUser(result, sub.caller.user.ID)
	if key == "" || key == "<nil>" {
		return ""
	}
	m.visibilityMu.Lock()
	m.visibilityAttach[cacheKey] = visibilityAttachEntry{key: key, at: time.Now()}
	m.visibilityMu.Unlock()
	return key
}

func (m *subscriptionManager) executionCacheScope(sub querySubscription) string {
	entry := m.server.runtime.ManifestForProject(sub.project).Functions[sub.path]
	return m.executionCacheScopeForEntry(sub, entry)
}

func (m *subscriptionManager) executionCacheScopeForEntry(sub querySubscription, entry manifest.FunctionEntry) string {
	if sub.visibilityKey != "" && strings.TrimSpace(entry.Dependencies.ShareByVisibility) != "" {
		return "visibility:" + sub.visibilityKey
	}
	if entry.Dependencies.ShareByPermissions {
		// The result-equivalence contract explicitly excludes identity. Use a
		// permission-derived server cache scope so per-user browser cache scopes
		// do not defeat shared execution. Delivery rewrites this to each
		// listener's own scope below.
		return "permissions:" + hashQueryCacheValue(sub.caller.permissions)
	}
	return sub.cacheScope
}

func compactJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("{}")
	}
	var buffer bytes.Buffer
	if json.Compact(&buffer, raw) == nil {
		return buffer.Bytes()
	}
	return append(json.RawMessage(nil), raw...)
}

func (m *subscriptionManager) indexGroupLocked(group *sharedSubscription) {
	if group.unknownDependencies {
		scope := subscriptionScope{project: group.project, tenant: group.tenant}
		if m.broad[scope] == nil {
			m.broad[scope] = map[*sharedSubscription]struct{}{}
		}
		m.broad[scope][group] = struct{}{}
		return
	}
	for _, read := range group.reads {
		key := dependencyKey{project: group.project, tenant: group.tenant, table: read.Table}
		if m.byTable[key] == nil {
			m.byTable[key] = map[*sharedSubscription]struct{}{}
		}
		m.byTable[key][group] = struct{}{}
	}
}

func (m *subscriptionManager) unindexGroupLocked(group *sharedSubscription) {
	for key, groups := range m.byTable {
		delete(groups, group)
		if len(groups) == 0 {
			delete(m.byTable, key)
		}
	}
	for key, groups := range m.broad {
		delete(groups, group)
		if len(groups) == 0 {
			delete(m.broad, key)
		}
	}
}

func (group *sharedSubscription) matches(change tableChange) bool {
	matches, _ := group.matchResult(change)
	return matches
}

func (group *sharedSubscription) matchResult(change tableChange) (bool, bool) {
	if group.unknownDependencies {
		return true, false
	}
	if change.broad && effectiveTableCount(change) == 0 {
		return true, false
	}
	group.mu.Lock()
	rowIDs := group.rowIDs
	group.mu.Unlock()
	intersected := false
	for _, read := range group.reads {
		if !changeContainsTable(change, read.Table) {
			continue
		}
		intersected = true
		detail := tableDetail(change, read.Table)
		if detail.precise && !readPredicateMatches(read.Predicate, group.args, group.caller, detail, rowIDs) {
			continue
		}
		if !detail.precise || detail.broad || detail.operation == "insert" || detail.operation == "delete" {
			return true, false
		}
		if detail.operation == "update" && len(detail.changedColumns) > 0 {
			columns := append(append(append([]string{}, read.Columns...), read.Filters...), read.OrdersBy...)
			if len(columns) > 0 && !intersectsStrings(columns, detail.changedColumns) {
				continue
			}
			// A changed filter can move a row into or out of the result, and a
			// changed ordering column can move an unseen row into a bounded window.
			// Neither case is safe to reject from the previous row-ID snapshot.
			if intersectsStrings(read.Filters, detail.changedColumns) ||
				(read.Windowed && intersectsStrings(read.OrdersBy, detail.changedColumns)) {
				return true, false
			}
		}
		if len(detail.rowIDs) == 0 || len(rowIDs) == 0 {
			return true, false
		}
		for id := range detail.rowIDs {
			if rowIDs[id] {
				return true, false
			}
		}
	}
	return false, !intersected
}

// readPredicateMatches implements predicates whose safety can be proven from
// the trigger metadata, subscription arguments, and the previous result rows.
// resultTaskIds scopes a task-linked table to tasks already in the result;
// resultTaskIdsOrColumnArg:workspaceId also admits changes that can move a task
// into the subscribed workspace. Unknown predicates remain conservative.
func readPredicateMatches(predicate string, args json.RawMessage, caller callerContext, detail tableChangeDetail, resultRowIDs map[string]bool) bool {
	predicate = strings.TrimSpace(predicate)
	if predicate == "" {
		return true
	}
	resultTaskIDsMatch := func() (bool, bool) {
		if len(detail.taskIDs) == 0 {
			return false, false
		}
		for taskID := range detail.taskIDs {
			if resultRowIDs[taskID] {
				return true, true
			}
		}
		return false, true
	}
	if predicate == "resultTaskIds" {
		matches, known := resultTaskIDsMatch()
		if !known {
			return true
		}
		return matches
	}
	if strings.HasPrefix(predicate, "resultTaskIdsOrColumnArg:") {
		column := strings.TrimSpace(strings.TrimPrefix(predicate, "resultTaskIdsOrColumnArg:"))
		if column != "workspaceId" {
			return true
		}
		matches, known := resultTaskIDsMatch()
		if matches {
			return true
		}
		if !known {
			return true
		}
		return readPredicateMatches("columnArg:"+column, args, caller, detail, resultRowIDs)
	}
	if strings.HasPrefix(predicate, "callerIdColumn:") {
		// The column suffix documents the manifest/trigger contract. userIds is
		// populated only when that physical column exists; missing metadata must
		// remain conservative for old triggers and broad recovery events.
		if strings.TrimSpace(strings.TrimPrefix(predicate, "callerIdColumn:")) != "userId" || len(detail.userIDs) == 0 {
			return true
		}
		if caller.user == nil || strings.TrimSpace(caller.user.ID) == "" {
			return true
		}
		return detail.userIDs[strings.TrimSpace(caller.user.ID)]
	}
	if strings.HasPrefix(predicate, "columnArg:") {
		column := strings.TrimSpace(strings.TrimPrefix(predicate, "columnArg:"))
		if column != "workspaceId" || len(detail.workspaceIDs) == 0 {
			return true
		}
		values := map[string]json.RawMessage{}
		if json.Unmarshal(args, &values) != nil {
			return true
		}
		raw, ok := values[column]
		if !ok {
			return true
		}
		var value any
		if json.Unmarshal(raw, &value) != nil {
			return true
		}
		id := strings.TrimSpace(fmt.Sprint(value))
		if id == "" || id == "<nil>" || id == "all" {
			return true
		}
		return detail.workspaceIDs[id]
	}
	prefix := "idArg:"
	changedIDs := detail.rowIDs
	requireArgument := false
	if strings.HasPrefix(predicate, "requiredIdArg:") {
		prefix = "requiredIdArg:"
		requireArgument = true
	} else if strings.HasPrefix(predicate, "requiredTaskIdArg:") {
		prefix = "requiredTaskIdArg:"
		changedIDs = detail.taskIDs
		requireArgument = true
	} else if strings.HasPrefix(predicate, "taskIdArg:") {
		prefix = "taskIdArg:"
		changedIDs = detail.taskIDs
	}
	if !strings.HasPrefix(predicate, prefix) {
		return true
	}
	if len(changedIDs) == 0 {
		return true
	}
	argument := strings.TrimSpace(strings.TrimPrefix(predicate, prefix))
	if argument == "" {
		return true
	}
	values := map[string]json.RawMessage{}
	if json.Unmarshal(args, &values) != nil {
		return true
	}
	raw, ok := values[argument]
	if !ok {
		if requireArgument {
			return false
		}
		return true
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return true
	}
	id := strings.TrimSpace(fmt.Sprint(value))
	if id == "" || id == "<nil>" {
		if requireArgument {
			return false
		}
		return true
	}
	return changedIDs[id]
}

func tableDetail(change tableChange, table string) tableChangeDetail {
	if detail, ok := change.details[table]; ok {
		return detail
	}
	return tableChangeDetail{
		operation: change.operation, changedColumns: change.changedColumns, rowIDs: change.rowIDs, taskIDs: change.taskIDs, userIDs: change.userIDs, workspaceIDs: change.workspaceIDs,
		precise: !change.broad, broad: change.broad,
	}
}

func changeContainsTable(change tableChange, table string) bool {
	if len(change.tables) > 0 {
		return change.tables[table]
	}
	return change.table == table
}

func intersectsStrings(left, right []string) bool {
	values := make(map[string]struct{}, len(left))
	for _, value := range left {
		values[value] = struct{}{}
	}
	for _, value := range right {
		if _, ok := values[value]; ok {
			return true
		}
	}
	return false
}

func sortedStringSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		if value != "" {
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

func (group *sharedSubscription) request(reason string, changedAtMS float64) {
	group.requestForCommit(reason, changedAtMS, "")
}

func (group *sharedSubscription) requestForCommit(reason string, changedAtMS float64, commitID string) {
	group.requestForCommitBatch(reason, changedAtMS, commitID, commitID)
}

func (group *sharedSubscription) requestForCommitBatch(reason string, changedAtMS float64, commitID, requestID string, changes ...tableChange) {
	commitID = strings.TrimSpace(commitID)
	requestID = strings.TrimSpace(requestID)
	group.mu.Lock()
	for _, change := range changes {
		if group.pendingTables == nil {
			group.pendingTables = map[string]struct{}{}
		}
		for _, table := range tableChangeTables(change) {
			if table != "" {
				group.pendingTables[table] = struct{}{}
			}
		}
		if group.pendingTaskIDs == nil {
			group.pendingTaskIDs = map[string]struct{}{}
		}
		for taskID := range change.taskIDs {
			if taskID != "" {
				group.pendingTaskIDs[taskID] = struct{}{}
			}
		}
		for table, detail := range change.details {
			for taskID := range detail.taskIDs {
				if taskID != "" {
					group.pendingTaskIDs[taskID] = struct{}{}
				}
			}
			if table == "tasks" {
				for taskID := range detail.rowIDs {
					if taskID != "" {
						group.pendingTaskIDs[taskID] = struct{}{}
					}
				}
			}
		}
	}
	if requestID != "" && group.commitAlreadyRequestedLocked(requestID) {
		if changedAtMS > group.pendingChangedAtMS {
			group.pendingChangedAtMS = changedAtMS
		}
		group.mu.Unlock()
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) { metric.RerunsCoalesced++ })
		return
	}
	group.requested++
	group.pendingReason = reason
	if commitID != "" {
		if group.pendingCommitIDs == nil {
			group.pendingCommitIDs = map[string]struct{}{}
		}
		group.pendingCommitIDs[commitID] = struct{}{}
	}
	if requestID != "" {
		if group.pendingRequestIDs == nil {
			group.pendingRequestIDs = map[string]struct{}{}
		}
		group.pendingRequestIDs[requestID] = struct{}{}
	}
	if changedAtMS > group.pendingChangedAtMS {
		group.pendingChangedAtMS = changedAtMS
	}
	if group.running {
		// Requests arriving during the pre-run window are part of the execution
		// that is already scheduled. Requests arriving during an active query
		// need one serialized trailing execution unless they name the same commit.
		if !group.coalescing {
			group.dirty = true
		}
		group.mu.Unlock()
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) { metric.RerunsCoalesced++ })
		return
	}
	group.running = true
	group.mu.Unlock()
	go group.run()
}

func (group *sharedSubscription) run() {
	for {
		group.mu.Lock()
		group.coalescing = false
		requested := group.requested
		reason := group.pendingReason
		changedAtMS := group.pendingChangedAtMS
		commitIDs := group.pendingCommitIDs
		group.pendingCommitIDs = nil
		group.activeCommitIDs = commitIDs
		requestIDs := group.pendingRequestIDs
		group.pendingRequestIDs = nil
		group.activeRequestIDs = requestIDs
		tables := group.pendingTables
		group.pendingTables = nil
		group.activeTables = tables
		taskIDs := group.pendingTaskIDs
		group.pendingTaskIDs = nil
		group.activeTaskIDs = taskIDs
		listeners := group.listenerSnapshotLocked()
		group.dirty = false
		group.mu.Unlock()
		if len(listeners) == 0 || group.ctx.Err() != nil {
			if len(listeners) == 0 && group.ctx.Err() == nil && reason == "invalidate" {
				// The commit that requested this run is now unrepresented in the
				// retained snapshot. Serve no replays until a fresh execution.
				group.mu.Lock()
				group.staleWhileIdle = true
				group.mu.Unlock()
			}
			group.finishRun(requested)
			return
		}

		representative, ok := group.firstAuthorizedListener(listeners)
		if !ok {
			group.finishRun(requested)
			return
		}
		startedAt := time.Now().UTC()
		group.manager.server.metrics.recordQueryCommitExecution(group.project, group.tenant, group.key, commitIDs)
		executionCtx := gonvex.WithQueryChangeDetails(group.ctx, reason, changedAtMS, sortedStringSet(tables), sortedStringSet(taskIDs))
		result, err := group.manager.executeVisibilityShared(executionCtx, group, representative, reason, changedAtMS)
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.QueriesRerun++
			metric.ReactiveExecutionPasses++
			metric.ReactiveExecutionDurationMS += float64(time.Since(startedAt).Microseconds()) / 1000
		})
		if group.ctx.Err() != nil {
			group.finishRun(requested)
			return
		}
		completionStarted := time.Now()
		if err != nil {
			group.completeError(err.Error())
		} else if partitioned, ok := result.(*visibilityPartitionedResult); ok {
			group.completePartitionedResult(partitioned, reason, changedAtMS, startedAt)
		} else {
			group.completeResult(result, reason, changedAtMS, startedAt)
		}
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.ResultCompletionPasses++
			metric.ResultCompletionDurationMS += float64(time.Since(completionStarted).Microseconds()) / 1000
		})
		group.mu.Lock()
		group.rememberCompletedCommitsLocked(requestIDs)
		group.activeCommitIDs = nil
		group.activeRequestIDs = nil
		group.activeTables = nil
		group.activeTaskIDs = nil
		group.completed = requested
		if group.dirty || group.requested > requested {
			group.coalescing = true
			group.mu.Unlock()
			if subscriptionRerunCooldown > 0 {
				time.AfterFunc(subscriptionRerunCooldown, group.run)
			} else {
				go group.run()
			}
			return
		}
		group.running = false
		group.mu.Unlock()
		return
	}
}

func (m *subscriptionManager) executeVisibilityShared(ctx context.Context, group *sharedSubscription, listener querySubscription, reason string, changedAtMS float64) (any, error) {
	entry := m.server.runtime.ManifestForProject(group.project).Functions[group.path]
	resolver := strings.TrimSpace(entry.Dependencies.ShareByVisibility)
	if resolver == "" {
		return m.executeWithRerunSlot(ctx, group, listener, reason, changedAtMS)
	}
	resolverSnapshot := any(changedAtMS)
	if commitIDs := sortedStringSet(group.activeCommitIDs); len(commitIDs) > 0 {
		resolverSnapshot = commitIDs
	} else {
		resolverSnapshot = group.key
	}
	// Resolve exactly the identities this group is about to partition. The
	// canonical user set is part of the singleflight key, so cross-path reuse is
	// allowed only when both groups have the same audience.
	users := m.visibilityUsers(group)
	resolverPayload, _ := json.Marshal([]any{group.project, group.tenant, json.RawMessage(group.args), resolver, resolverSnapshot, users})
	resolverSum := sha256.Sum256(resolverPayload)
	resolverKey := "resolver:" + hex.EncodeToString(resolverSum[:])
	m.visibilityMu.Lock()
	resolverRun := m.visibilityRuns[resolverKey]
	leader := resolverRun == nil
	if leader {
		resolverRun = &visibilityExecution{done: make(chan struct{})}
		m.visibilityRuns[resolverKey] = resolverRun
	}
	m.visibilityMu.Unlock()
	if leader {
		resolverStarted := time.Now()
		values := map[string]any{}
		_ = json.Unmarshal(group.args, &values)
		values["__gonvexVisibilityUsers"] = users
		resolverArgs, _ := json.Marshal(values)
		releaseSlot, acquired := m.acquireExecutionSlot(ctx, reason, group.project, group.tenant)
		if !acquired {
			resolverRun.err = ctx.Err()
		} else {
			resolverRun.result, resolverRun.err = m.server.executeTenantQueryForCallerUncached(ctx, group.project, group.tenant, listener.caller, resolver, resolverArgs)
			releaseSlot()
		}
		resolverDurationMS := float64(time.Since(resolverStarted).Microseconds()) / 1000
		m.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.VisibilityResolverExecutions++
			metric.VisibilityResolverDurationMS += resolverDurationMS
		})
		m.visibilityMu.Lock()
		close(resolverRun.done)
		m.visibilityMu.Unlock()
		time.AfterFunc(time.Second, func() {
			m.visibilityMu.Lock()
			if m.visibilityRuns[resolverKey] == resolverRun {
				delete(m.visibilityRuns, resolverKey)
			}
			m.visibilityMu.Unlock()
		})
	} else {
		select {
		case <-resolverRun.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if resolverRun.err != nil {
		return m.executeWithRerunSlot(ctx, group, listener, reason, changedAtMS)
	}
	group.mu.Lock()
	currentListeners := group.listenerSnapshotLocked()
	group.mu.Unlock()
	partitions := map[string][]querySubscription{}
	for _, candidate := range currentListeners {
		userID := ""
		if candidate.caller.user != nil {
			userID = strings.TrimSpace(candidate.caller.user.ID)
		}
		key := visibilityKeyForUser(resolverRun.result, userID)
		if key == "" || key == "<nil>" {
			key = "identity:" + userID
		}
		partitions[key] = append(partitions[key], candidate)
	}
	if len(partitions) > 1 {
		keys := make([]string, 0, len(partitions))
		for key := range partitions {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		result := &visibilityPartitionedResult{partitions: make([]visibilityResultPartition, len(keys))}
		errs := make([]error, len(keys))
		workerCount := min(32, len(keys))
		var workers sync.WaitGroup
		for worker := range workerCount {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for index := worker; index < len(keys); index += workerCount {
					key := keys[index]
					listeners := partitions[key]
					value, err := m.executeVisibilityPartition(ctx, group, listeners[0], reason, changedAtMS, key)
					result.partitions[index] = visibilityResultPartition{key: key, listeners: listeners, result: value}
					errs[index] = err
				}
			}()
		}
		workers.Wait()
		for _, err := range errs {
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	userID := "anonymous"
	if listener.caller.user != nil && strings.TrimSpace(listener.caller.user.ID) != "" {
		userID = strings.TrimSpace(listener.caller.user.ID)
	}
	visibilityKey := visibilityKeyForUser(resolverRun.result, userID)
	if visibilityKey == "" || visibilityKey == "<nil>" {
		return m.executeWithRerunSlot(ctx, group, listener, reason, changedAtMS)
	}
	return m.executeVisibilityPartition(ctx, group, listener, reason, changedAtMS, visibilityKey)
}

func (m *subscriptionManager) executeVisibilityPartition(ctx context.Context, group *sharedSubscription, listener querySubscription, reason string, changedAtMS float64, visibilityKey string) (any, error) {
	if strings.HasPrefix(visibilityKey, "identity:") {
		return m.executeWithRerunSlot(ctx, group, listener, reason, changedAtMS)
	}
	ctx = gonvex.WithQueryVisibilityKey(ctx, visibilityKey)
	entry := m.server.runtime.ManifestForProject(group.project).Functions[group.path]
	resultSource := strings.TrimSpace(entry.Dependencies.ShareResultFrom)
	resultField := strings.TrimSpace(entry.Dependencies.ShareResultField)
	executionPath := group.path
	if resultSource != "" && resultField != "" {
		executionPath = resultSource
	}
	snapshotKey := any(changedAtMS)
	if commitIDs := sortedStringSet(group.activeCommitIDs); len(commitIDs) > 0 {
		// Every trigger notification carrying one mutation ID is observed only
		// after that transaction committed. Subscriber groups selected by later
		// tables can therefore reuse the path-specific result computed for an
		// earlier table from the same committed mutation.
		snapshotKey = commitIDs
	}
	payload, _ := json.Marshal([]any{
		group.project, group.tenant, executionPath, json.RawMessage(group.args),
		visibilityKey, snapshotKey,
	})
	sum := sha256.Sum256(payload)
	key := hex.EncodeToString(sum[:])

	m.visibilityMu.Lock()
	if active := m.visibilityRuns[key]; active != nil {
		m.visibilityMu.Unlock()
		select {
		case <-active.done:
			return active.prepareResult(resultField)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	active := &visibilityExecution{done: make(chan struct{})}
	m.visibilityRuns[key] = active
	m.visibilityMu.Unlock()

	executionStarted := time.Now()
	releaseSlot, acquired := m.acquireExecutionSlot(ctx, reason, group.project, group.tenant)
	if !acquired {
		active.err = ctx.Err()
	} else {
		var value any
		var executeErr error
		if executionPath == group.path {
			value, executeErr = m.execute(ctx, group, listener, reason, changedAtMS)
		} else {
			value, executeErr = m.server.executeTenantQueryForCallerCached(ctx, group.project, group.tenant, listener.caller, executionPath, group.args, group.cacheScope, reason)
		}
		releaseSlot()
		active.err = executeErr
		active.result = value
	}
	executionDurationMS := float64(time.Since(executionStarted).Microseconds()) / 1000
	m.server.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.VisibilitySharedExecutions++
		metric.VisibilitySharedDurationMS += executionDurationMS
	})
	m.visibilityMu.Lock()
	close(active.done)
	m.visibilityMu.Unlock()
	time.AfterFunc(time.Second, func() {
		m.visibilityMu.Lock()
		if m.visibilityRuns[key] == active {
			delete(m.visibilityRuns, key)
		}
		m.visibilityMu.Unlock()
	})
	return active.prepareResult(resultField)
}

func (execution *visibilityExecution) prepareResult(field string) (any, error) {
	execution.prepareMu.Lock()
	defer execution.prepareMu.Unlock()
	if prepared, ok := execution.prepared[field]; ok {
		return prepared.result, prepared.err
	}
	result, err := prepareVisibilityResult(execution.result, field, execution.err)
	if execution.prepared == nil {
		execution.prepared = map[string]visibilityPreparedResult{}
	}
	execution.prepared[field] = visibilityPreparedResult{result: result, err: err}
	return result, err
}

func prepareVisibilityResult(value any, field string, executionErr error) (any, error) {
	if executionErr != nil {
		return nil, executionErr
	}
	if field != "" {
		object, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("shared result source returned %T, want object containing %q", value, field)
		}
		projected, exists := object[field]
		if !exists {
			return nil, fmt.Errorf("shared result source omitted projection %q", field)
		}
		value = projected
	}
	encoded, err := json.Marshal(explicitNull(value))
	if err != nil {
		return nil, err
	}
	hash, queryPerf := queryResultSemantics(encoded)
	return &visibilitySharedResult{payload: encoded, hash: hash, queryPerf: queryPerf, rowIDs: resultRowIDs(value)}, nil
}

func (m *subscriptionManager) visibilityUsers(group *sharedSubscription) []map[string]string {
	group.mu.Lock()
	if group.visibilityUsers != nil {
		result := group.visibilityUsers
		group.mu.Unlock()
		return result
	}
	revision := group.visibilityUsersRevision
	users := map[string]string{}
	for _, listener := range group.listeners {
		if listener.caller.user != nil && strings.TrimSpace(listener.caller.user.ID) != "" {
			users[strings.TrimSpace(listener.caller.user.ID)] = strings.TrimSpace(listener.caller.user.Email)
		}
	}
	group.mu.Unlock()
	ids := make([]string, 0, len(users))
	for user := range users {
		ids = append(ids, user)
	}
	sort.Strings(ids)
	result := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		result = append(result, map[string]string{"id": id, "email": users[id]})
	}
	group.mu.Lock()
	// An attach/detach may have invalidated the cache while this canonical list
	// was being sorted. Only publish it if no newer list already won the race;
	// callers still use this exact snapshot in their singleflight key.
	if group.visibilityUsers == nil && group.visibilityUsersRevision == revision {
		group.visibilityUsers = result
	}
	group.mu.Unlock()
	return result
}

func visibilityKeyForUser(result any, userID string) string {
	switch values := result.(type) {
	case map[string]string:
		return strings.TrimSpace(values[userID])
	case map[string]any:
		return strings.TrimSpace(fmt.Sprint(values[userID]))
	default:
		return strings.TrimSpace(fmt.Sprint(result))
	}
}

func (m *subscriptionManager) executeWithRerunSlot(ctx context.Context, group *sharedSubscription, listener querySubscription, reason string, changedAtMS float64) (any, error) {
	release, acquired := m.acquireExecutionSlot(ctx, reason, group.project, group.tenant)
	if !acquired {
		return nil, ctx.Err()
	}
	defer release()
	return m.execute(ctx, group, listener, reason, changedAtMS)
}

// acquireExecutionSlot admits one shared-subscription execution through the
// unified query admission controller. Invalidation and recovery reruns are
// reactive; initial executions are bootstrap hydration.
func (m *subscriptionManager) acquireExecutionSlot(ctx context.Context, reason, project, tenant string) (func(), bool) {
	return m.server.acquireQueryAdmission(ctx, admissionClassForReason(reason), project, tenant)
}

func (group *sharedSubscription) commitAlreadyRequestedLocked(commitID string) bool {
	return requestCoveredBy(group.pendingRequestIDs, commitID) ||
		requestCoveredBy(group.activeRequestIDs, commitID) ||
		requestCoveredBy(group.completedCommitIDs, commitID)
}

func requestCoveredBy(existing map[string]struct{}, requestID string) bool {
	if _, ok := existing[requestID]; ok {
		return true
	}
	commitID, tables, ok := splitCommitTableRequest(requestID)
	if !ok {
		return false
	}
	for candidate := range existing {
		candidateCommit, candidateTables, candidateOK := splitCommitTableRequest(candidate)
		if !candidateOK || candidateCommit != commitID {
			continue
		}
		covered := true
		for table := range tables {
			if _, exists := candidateTables[table]; !exists {
				covered = false
				break
			}
		}
		if covered {
			return true
		}
	}
	return false
}

func splitCommitTableRequest(requestID string) (string, map[string]struct{}, bool) {
	parts := strings.SplitN(requestID, "\x00", 2)
	if len(parts) != 2 || parts[0] == "" {
		return "", nil, false
	}
	tables := map[string]struct{}{}
	for _, table := range strings.Split(parts[1], "\x1f") {
		if table != "" {
			tables[table] = struct{}{}
		}
	}
	return parts[0], tables, true
}

func (group *sharedSubscription) rememberCompletedCommitsLocked(commitIDs map[string]struct{}) {
	const retainedCommitIDs = 256
	if len(commitIDs) == 0 {
		return
	}
	if group.completedCommitIDs == nil {
		group.completedCommitIDs = map[string]struct{}{}
	}
	for commitID := range commitIDs {
		if _, exists := group.completedCommitIDs[commitID]; exists {
			continue
		}
		group.completedCommitIDs[commitID] = struct{}{}
		group.completedCommits = append(group.completedCommits, commitID)
	}
	for len(group.completedCommits) > retainedCommitIDs {
		oldest := group.completedCommits[0]
		group.completedCommits = group.completedCommits[1:]
		delete(group.completedCommitIDs, oldest)
	}
}

func (group *sharedSubscription) finishRun(requested uint64) {
	group.mu.Lock()
	group.rememberCompletedCommitsLocked(group.activeRequestIDs)
	group.activeCommitIDs = nil
	group.activeRequestIDs = nil
	group.activeTables = nil
	group.activeTaskIDs = nil
	group.completed = requested
	group.running = false
	group.mu.Unlock()
}

func (group *sharedSubscription) listenerSnapshotLocked() []querySubscription {
	listeners := make([]querySubscription, 0, len(group.listeners))
	for _, listener := range group.listeners {
		listeners = append(listeners, listener)
	}
	return listeners
}

func (group *sharedSubscription) firstAuthorizedListener(listeners []querySubscription) (querySubscription, bool) {
	for _, listener := range listeners {
		if listener.ctx.Err() != nil {
			continue
		}
		if listener.conn == nil {
			return listener, true
		}
		if err := listener.conn.revalidateAppAuth(listener.ctx); err != nil {
			if listener.ctx.Err() == nil {
				listener.conn.write(serverMessage{Type: "query.error", ID: listener.id, Error: "authentication is required"})
			}
			continue
		}
		return listener, true
	}
	return querySubscription{}, false
}

func (group *sharedSubscription) completeResult(result any, reason string, changedAtMS float64, startedAt time.Time) {
	var sharedResult *visibilitySharedResult
	var payload json.RawMessage
	var hash [sha256.Size]byte
	var queryPerf json.RawMessage
	var rowIDs map[string]bool
	if shared, ok := result.(*visibilitySharedResult); ok {
		sharedResult = shared
		payload, hash, queryPerf, rowIDs = shared.payload, shared.hash, shared.queryPerf, shared.rowIDs
	} else {
		encoded, err := json.Marshal(explicitNull(result))
		if err != nil {
			group.broadcastError(err.Error())
			return
		}
		payload = encoded
		hash, queryPerf = queryResultSemantics(payload)
		rowIDs = resultRowIDs(result)
	}
	if sharedResult != nil && group.completeConvergedPartitionResult(sharedResult, reason, changedAtMS, startedAt) {
		return
	}
	group.mu.Lock()
	mutationIDs := make([]string, 0, len(group.activeCommitIDs))
	for commitID := range group.activeCommitIDs {
		if commitID = strings.TrimSpace(commitID); commitID != "" {
			mutationIDs = append(mutationIDs, commitID)
		}
	}
	sort.Strings(mutationIDs)
	// Snapshots are immutable after publication. Keep the previous slice by
	// reference and replace (never overwrite) group.lastResult below; this avoids
	// copying one full result per identity group before every keyed diff.
	previous := group.lastResult
	previousSingleListener := group.lastSingleListener
	unchanged := group.hasHash && hash == group.lastHash
	previousHash := group.lastHash
	previousRevision := group.revision
	listeners := group.listenerSnapshotLocked()
	sameSingleListener := len(listeners) == 1 && previousSingleListener != nil && previousSingleListener == listeners[0].token
	// An unchanged invalidation has no client-visible state transition. Do not
	// emit progress and, crucially, do not advance the server revision: the next
	// real keyed patch must still name the exact revision every client last
	// acknowledged. Initial/cache-revalidation paths retain progress semantics.
	if reason == "invalidate" && unchanged && (len(previous) > 0 || sameSingleListener) {
		group.lastError = ""
		group.rowIDs = rowIDs
		group.mu.Unlock()
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.UnchangedResultsSuppressed++
			metric.ResultBytesBefore += uint64(len(payload))
		})
		return
	}
	revision := group.manager.sequence.Add(1)
	group.revision = revision
	group.staleWhileIdle = false
	group.lastHash = hash
	group.hasHash = true
	group.lastError = ""
	group.rowIDs = rowIDs
	if len(listeners) == 1 {
		group.lastSingleListener = listeners[0].token
	} else {
		group.lastSingleListener = nil
	}
	// A one-listener group can rerun if a matching listener arrives later and
	// only needs the hash for unchanged-result suppression. Retaining the full
	// payload for every identity-scoped subscription multiplies memory by the
	// user count without improving correctness. Shared groups keep a snapshot
	// for immediate replay and keyed patches.
	if (len(listeners) > 1 || group.retainSnapshot) && len(payload) <= group.manager.server.config.SharedResultMaxBytes {
		if sharedResult != nil {
			// Every admitted group may retain the same immutable encoding. Listener
			// revisions and patch baselines remain group-local slice headers.
			group.lastResult = payload
		} else {
			group.lastResult = append(json.RawMessage(nil), payload...)
		}
	} else {
		group.lastResult = nil
	}
	group.mu.Unlock()

	revisionValue := &subscriptionRevision{Epoch: group.manager.epoch, Sequence: revision}
	if unchanged && (len(previous) > 0 || sameSingleListener) {
		message := serverMessage{Type: "query.progress", Path: group.path, Reason: reason, ThroughRevision: revisionValue, MutationIDs: mutationIDs, QueryPerf: queryPerf}
		group.broadcastTo(listeners, message, changedAtMS, startedAt)
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.UnchangedResultsSuppressed++
			metric.ProgressMessages++
			metric.ResultBytesBefore += uint64(len(payload))
		})
		return
	}

	cacheRevision := group.manager.server.nextQueryCacheRevision(hash)
	message := serverMessage{Type: "query.result", Path: group.path, Result: json.RawMessage(payload), Reason: reason, CacheScope: group.cacheScope, CacheRevision: cacheRevision, SubscriptionRevision: revisionValue, MutationIDs: mutationIDs, QueryPerf: queryPerf}
	encodedSize := len(payload)
	patched := false
	if len(previous) >= minimumPatchResultBytes {
		var patch serverMessage
		var patchOK bool
		patchEncodedSize := 0
		if sharedResult != nil {
			cached := sharedResult.keyedPatch(previousHash, previous)
			patch, patchOK = cached.message, cached.ok
			patchEncodedSize = cached.encodedSize
		} else {
			patch, patchOK = keyedResultPatch(previous, payload)
		}
		if patchOK {
			patch.SubscriptionRevision = revisionValue
			patch.BaseRevision = &subscriptionRevision{Epoch: group.manager.epoch, Sequence: previousRevision}
			patch.Path = group.path
			patch.Reason = reason
			patch.MutationIDs = mutationIDs
			patch.CacheScope = group.cacheScope
			patch.CacheRevision = cacheRevision
			patch.FullResult = payload
			if sharedResult != nil {
				message = patch
				// keyedPatch already proved this template is comfortably below the
				// full-result threshold before group-local metadata was attached.
				encodedSize = patchEncodedSize
				patched = true
			} else if encoded, encodeErr := json.Marshal(patch); encodeErr == nil && len(encoded) < len(payload)*7/10 {
				message = patch
				encodedSize = len(encoded)
				patched = true
			}
		}
	}
	group.broadcastTo(listeners, message, changedAtMS, startedAt)
	group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.ResultBytesBefore += uint64(len(payload))
		metric.ResultBytesAfter += uint64(encodedSize)
		if patched {
			metric.Patches++
		} else {
			metric.FullResults++
		}
	})
}

func (group *sharedSubscription) completePartitionedResult(result *visibilityPartitionedResult, reason string, changedAtMS float64, startedAt time.Time) {
	// Independently proven visibility scopes can still converge to the same
	// result. Collapse them only after comparing the actual committed payload;
	// this preserves identity isolation while avoiding duplicate delivery.
	if len(result.partitions) > 1 {
		var common *visibilitySharedResult
		converged := true
		for _, partition := range result.partitions {
			shared, ok := partition.result.(*visibilitySharedResult)
			if !ok {
				converged = false
				break
			}
			if common == nil {
				common = shared
				continue
			}
			if common.hash != shared.hash || !bytes.Equal(common.payload, shared.payload) {
				converged = false
				break
			}
		}
		if converged && common != nil {
			if group.completeConvergedPartitionResult(common, reason, changedAtMS, startedAt) {
				return
			}
			group.completeResult(common, reason, changedAtMS, startedAt)
			return
		}
	}
	group.mu.Lock()
	mutationIDs := sortedStringSet(group.activeCommitIDs)
	sharedPrevious := append(json.RawMessage(nil), group.lastResult...)
	sharedPreviousHash := group.lastHash
	sharedPreviousRevision := group.revision
	previousPartitions := group.partitionBaselines
	group.revision = group.manager.sequence.Add(1)
	group.staleWhileIdle = false
	revision := &subscriptionRevision{Epoch: group.manager.epoch, Sequence: group.revision}
	// A partitioned result proves that the old shared baseline is no longer
	// valid. Send full results now and force the next converged execution to
	// establish a fresh common baseline before keyed patches resume.
	group.lastResult = nil
	group.hasHash = false
	group.rowIDs = nil
	group.lastSingleListener = nil
	group.partitionBaselines = nil
	group.listenerPartitions = nil
	group.mu.Unlock()
	nextPartitions := map[string]visibilityPartitionBaseline{}
	nextListeners := map[*subscriptionToken]string{}
	retainedBytes := 0
	retainPartitions := len(result.partitions) <= 64
	for _, partition := range result.partitions {
		var sharedResult *visibilitySharedResult
		var payload json.RawMessage
		var hash [sha256.Size]byte
		var queryPerf json.RawMessage
		if shared, ok := partition.result.(*visibilitySharedResult); ok {
			sharedResult = shared
			payload, hash, queryPerf = shared.payload, shared.hash, shared.queryPerf
		} else {
			encoded, err := json.Marshal(explicitNull(partition.result))
			if err != nil {
				group.broadcastError(err.Error())
				return
			}
			payload = encoded
			hash, queryPerf = queryResultSemantics(payload)
		}
		message := serverMessage{
			Type: "query.result", Path: group.path, Result: json.RawMessage(payload), Reason: reason,
			CacheScope: group.cacheScope, CacheRevision: group.manager.server.nextQueryCacheRevision(hash),
			SubscriptionRevision: revision, MutationIDs: mutationIDs, QueryPerf: queryPerf,
		}
		encodedSize := len(payload)
		patched := false
		previous := sharedPrevious
		previousHash := sharedPreviousHash
		previousRevision := sharedPreviousRevision
		if baseline, ok := previousPartitions[partition.key]; ok {
			previous = baseline.payload
			previousHash = baseline.hash
			previousRevision = baseline.revision
		}
		// Every listener in the old group acknowledged the same committed
		// baseline. When a visibility change splits that group, each new result
		// can therefore use the ordinary keyed-patch protocol from that baseline.
		// The client validates BaseRevision and requests recovery on any mismatch,
		// so a lagging listener can never apply a patch to the wrong snapshot.
		if len(previous) >= minimumPatchResultBytes {
			var patch serverMessage
			var patchOK bool
			patchEncodedSize := 0
			if sharedResult != nil {
				cached := sharedResult.keyedPatch(previousHash, previous)
				patch, patchOK, patchEncodedSize = cached.message, cached.ok, cached.encodedSize
			} else {
				patch, patchOK = keyedResultPatch(previous, payload)
			}
			if patchOK {
				patch.SubscriptionRevision = revision
				patch.BaseRevision = &subscriptionRevision{Epoch: group.manager.epoch, Sequence: previousRevision}
				patch.Path = group.path
				patch.Reason = reason
				patch.MutationIDs = mutationIDs
				patch.CacheScope = group.cacheScope
				patch.CacheRevision = message.CacheRevision
				patch.FullResult = payload
				if sharedResult != nil {
					message = patch
					encodedSize = patchEncodedSize
					patched = true
				} else if encoded, encodeErr := json.Marshal(patch); encodeErr == nil && len(encoded) < len(payload)*7/10 {
					message = patch
					encodedSize = len(encoded)
					patched = true
				}
			}
		}
		group.broadcastTo(partition.listeners, message, changedAtMS, startedAt)
		if retainPartitions {
			retainedBytes += len(payload)
			if retainedBytes <= group.manager.server.config.SharedResultMaxBytes {
				nextPartitions[partition.key] = visibilityPartitionBaseline{payload: payload, hash: hash, revision: revision.Sequence}
				for _, listener := range partition.listeners {
					nextListeners[listener.token] = partition.key
				}
			} else {
				retainPartitions = false
				nextPartitions = nil
				nextListeners = nil
			}
		}
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			if patched {
				metric.Patches++
			} else {
				metric.FullResults++
			}
			metric.ResultBytesBefore += uint64(len(payload))
			metric.ResultBytesAfter += uint64(encodedSize)
		})
	}
	if retainPartitions {
		group.mu.Lock()
		group.partitionBaselines = nextPartitions
		group.listenerPartitions = nextListeners
		group.mu.Unlock()
	}
}

// completeConvergedPartitionResult patches each listener from the exact
// visibility partition it last acknowledged, then restores the compact shared
// baseline. It returns false when there is no retained partition state.
func (group *sharedSubscription) completeConvergedPartitionResult(shared *visibilitySharedResult, reason string, changedAtMS float64, startedAt time.Time) bool {
	group.mu.Lock()
	if len(group.partitionBaselines) == 0 {
		group.mu.Unlock()
		return false
	}
	baselines := group.partitionBaselines
	listenerPartitions := group.listenerPartitions
	listeners := group.listenerSnapshotLocked()
	mutationIDs := sortedStringSet(group.activeCommitIDs)
	group.revision = group.manager.sequence.Add(1)
	group.staleWhileIdle = false
	revision := &subscriptionRevision{Epoch: group.manager.epoch, Sequence: group.revision}
	group.lastHash = shared.hash
	group.hasHash = true
	group.lastError = ""
	group.rowIDs = shared.rowIDs
	if len(shared.payload) <= group.manager.server.config.SharedResultMaxBytes {
		group.lastResult = shared.payload
	} else {
		group.lastResult = nil
	}
	if len(listeners) == 1 {
		group.lastSingleListener = listeners[0].token
	} else {
		group.lastSingleListener = nil
	}
	group.partitionBaselines = nil
	group.listenerPartitions = nil
	group.mu.Unlock()

	partitionListeners := map[string][]querySubscription{}
	for _, listener := range listeners {
		partitionListeners[listenerPartitions[listener.token]] = append(partitionListeners[listenerPartitions[listener.token]], listener)
	}
	cacheRevision := group.manager.server.nextQueryCacheRevision(shared.hash)
	for key, selected := range partitionListeners {
		message := serverMessage{
			Type: "query.result", Path: group.path, Result: shared.payload, Reason: reason,
			CacheScope: group.cacheScope, CacheRevision: cacheRevision,
			SubscriptionRevision: revision, MutationIDs: mutationIDs, QueryPerf: shared.queryPerf,
		}
		encodedSize := len(shared.payload)
		patched := false
		if baseline, ok := baselines[key]; ok && len(baseline.payload) >= minimumPatchResultBytes {
			cached := shared.keyedPatch(baseline.hash, baseline.payload)
			if patch, ok := cached.message, cached.ok; ok {
				patch.SubscriptionRevision = revision
				patch.BaseRevision = &subscriptionRevision{Epoch: group.manager.epoch, Sequence: baseline.revision}
				patch.Path, patch.Reason = group.path, reason
				patch.MutationIDs, patch.CacheScope, patch.CacheRevision = mutationIDs, group.cacheScope, cacheRevision
				patch.FullResult = shared.payload
				message, encodedSize, patched = patch, cached.encodedSize, true
			}
		}
		group.broadcastTo(selected, message, changedAtMS, startedAt)
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			if patched {
				metric.Patches++
			} else {
				metric.FullResults++
			}
			metric.ResultBytesBefore += uint64(len(shared.payload))
			metric.ResultBytesAfter += uint64(encodedSize)
		})
	}
	return true
}

func (shared *visibilitySharedResult) keyedPatch(previousHash [sha256.Size]byte, previous json.RawMessage) visibilitySharedPatch {
	shared.patchMu.Lock()
	defer shared.patchMu.Unlock()
	if cached, ok := shared.patches[previousHash]; ok {
		return cached
	}
	patch, ok := keyedResultPatch(previous, shared.payload)
	encodedSize := len(shared.payload)
	if ok {
		if encoded, err := json.Marshal(patch); err != nil || len(encoded) >= len(shared.payload)*7/10 {
			ok = false
		} else {
			encodedSize = len(encoded)
		}
	}
	cached := visibilitySharedPatch{message: patch, encodedSize: encodedSize, ok: ok}
	if shared.patches == nil {
		shared.patches = map[[sha256.Size]byte]visibilitySharedPatch{}
	}
	shared.patches[previousHash] = cached
	return cached
}

func (group *sharedSubscription) completeError(message string) {
	group.mu.Lock()
	hasSuccessfulResult := group.hasHash
	if !hasSuccessfulResult {
		group.lastError = message
	}
	group.mu.Unlock()
	// A failed refresh never replaces a newer successful snapshot. Initial
	// failures still settle listeners and are replayed to late joiners.
	if !hasSuccessfulResult {
		group.broadcastError(message)
	}
}

func (group *sharedSubscription) broadcastTo(listeners []querySubscription, message serverMessage, changedAtMS float64, startedAt time.Time) {
	deliveryStarted := time.Now()
	type preparedDelivery struct {
		listener querySubscription
		message  serverMessage
	}
	type fanoutKey struct {
		conn        *wsConn
		messageType string
		cacheScope  string
	}
	type deliveryAccounting struct {
		delivery preparedDelivery
		sentAt   time.Time
		trace    *messageTrace
	}
	prepared := make([]preparedDelivery, 0, len(listeners))
	accounting := make([]deliveryAccounting, 0, len(listeners))
	for _, listener := range listeners {
		if listener.conn == nil {
			continue
		}
		if !listenerCurrent(listener) {
			continue
		}
		copy := message
		copy.ID = listener.id
		if copy.Type == "query.pagePatch" && !listener.conn.queryPagePatch {
			copy.Type = "query.result"
			copy.Result = copy.FullResult
			copy.Inserted, copy.Updated, copy.Deleted, copy.Order = nil, nil, nil, nil
		}
		if copy.Type == "query.objectPatch" && !listener.conn.queryObjectPatch {
			copy.Type = "query.result"
			copy.Result = copy.FullResult
			copy.Collections = nil
		}
		if !listener.conn.queryOrderDelta && messageUsesOrderDelta(copy) {
			copy.Type = "query.result"
			copy.Result = copy.FullResult
			copy.Inserted, copy.Updated, copy.Deleted, copy.Order = nil, nil, nil, nil
			copy.Prepend, copy.Append, copy.Collections = nil, nil, nil
		}
		// Compare against the listener's LIVE revision, not the subscribe-time
		// snapshot. The snapshot goes stale the moment the server pushes any
		// newer result: a subscribe-time revision that happens to equal a later
		// payload (seed → delete returns a list to its pre-seed contents, the
		// shape of every CRUD spec) converted the fresh result into a
		// "progress" while the client was rendering the intermediate state —
		// the grid then stayed stale forever.
		if copy.Type == "query.result" && copy.SubscriptionRevision != nil && queryCacheRevisionMatchesHash(currentListenerCacheRevision(listener), group.lastHash) {
			copy = serverMessage{
				Type:            "query.progress",
				ID:              listener.id,
				Path:            group.path,
				Reason:          message.Reason,
				ThroughRevision: copy.SubscriptionRevision,
				MutationIDs:     message.MutationIDs,
				QueryPerf:       message.QueryPerf,
			}
		}
		if copy.Type == "query.result" || copy.Type == "query.patch" || copy.Type == "query.pagePatch" || copy.Type == "query.objectPatch" {
			copy.CacheScope = listener.cacheScope
		}
		prepared = append(prepared, preparedDelivery{listener: listener, message: copy})
	}
	preparedAt := time.Now()
	queueDeliveryAccounting := func(delivery preparedDelivery, sentAt time.Time, trace *messageTrace) {
		listener, copy := delivery.listener, delivery.message
		if (copy.Type == "query.result" || copy.Type == "query.patch" || copy.Type == "query.pagePatch" || copy.Type == "query.objectPatch") && copy.CacheRevision != "" {
			storeListenerCacheRevision(listener, copy.CacheRevision)
		}
		accounting = append(accounting, deliveryAccounting{delivery: delivery, sentAt: sentAt, trace: trace})
	}

	makeTrace := func(copy serverMessage, sentAt time.Time) *messageTrace {
		return &messageTrace{
			ServerChangeCommittedAtMS:     changedAtMS,
			ServerSubscriptionStartedAtMS: epochMillis(startedAt),
			ServerSubscriptionSentAtMS:    epochMillis(sentAt),
			ServerDurationMS:              float64(sentAt.Sub(startedAt).Microseconds()) / 1000,
			QueryPerf:                     copy.QueryPerf,
		}
	}

	batches := map[fanoutKey][]preparedDelivery{}
	for _, delivery := range prepared {
		if delivery.listener.conn.queryFanout {
			key := fanoutKey{conn: delivery.listener.conn, messageType: delivery.message.Type, cacheScope: delivery.message.CacheScope}
			batches[key] = append(batches[key], delivery)
			continue
		}
		sentAt := time.Now().UTC()
		trace := makeTrace(delivery.message, sentAt)
		delivery.message.Trace = trace
		delivery.listener.conn.write(delivery.message)
		queueDeliveryAccounting(delivery, sentAt, trace)
	}
	type fanoutBatch struct {
		key   fanoutKey
		batch []preparedDelivery
	}
	orderedBatches := make([]fanoutBatch, 0, len(batches))
	for key, batch := range batches {
		if len(batch) > 0 {
			orderedBatches = append(orderedBatches, fanoutBatch{key: key, batch: batch})
		}
	}
	sort.Slice(orderedBatches, func(i, j int) bool { return orderedBatches[i].key.conn.id < orderedBatches[j].key.conn.id })
	batchAccounting := make([][]deliveryAccounting, len(orderedBatches))
	workerCount := min(32, len(orderedBatches))
	var batchWG sync.WaitGroup
	for worker := range workerCount {
		batchWG.Add(1)
		go func() {
			defer batchWG.Done()
			for index := worker; index < len(orderedBatches); index += workerCount {
				item := orderedBatches[index]
				key, batch := item.key, item.batch
				sentAt := time.Now().UTC()
				trace := makeTrace(batch[0].message, sentAt)
				if len(batch) == 1 {
					batch[0].message.Trace = trace
					key.conn.write(batch[0].message)
				} else {
					fanout := batch[0].message
					fanout.Type, fanout.QueryType, fanout.ID = "query.fanout", fanout.Type, ""
					fanout.IDs = make([]string, 0, len(batch))
					for _, delivery := range batch {
						fanout.IDs = append(fanout.IDs, delivery.listener.id)
					}
					fanout.Trace = trace
					key.conn.write(fanout)
				}
				local := make([]deliveryAccounting, 0, len(batch))
				for _, delivery := range batch {
					if (delivery.message.Type == "query.result" || delivery.message.Type == "query.patch" || delivery.message.Type == "query.pagePatch" || delivery.message.Type == "query.objectPatch") && delivery.message.CacheRevision != "" {
						storeListenerCacheRevision(delivery.listener, delivery.message.CacheRevision)
					}
					local = append(local, deliveryAccounting{delivery: delivery, sentAt: sentAt, trace: trace})
				}
				batchAccounting[index] = local
			}
		}()
	}
	batchWG.Wait()
	for _, local := range batchAccounting {
		accounting = append(accounting, local...)
	}
	writesAt := time.Now()
	// Telemetry is logical-subscription accounting, not delivery work. Keep it
	// behind every physical WebSocket write so a high duplicate-listener count
	// cannot delay the last client while preserving identical records/metrics.
	var changeLatencyTotal float64
	var changeLatencySamples uint64
	var serverDurationTotal float64
	for _, item := range accounting {
		if changedAtMS > 0 {
			changeLatencyTotal += epochMillis(item.sentAt) - changedAtMS
			changeLatencySamples++
		}
		serverDurationTotal += item.trace.ServerDurationMS
	}
	if changeLatencySamples > 0 {
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.ChangeToClientDurationMS += changeLatencyTotal
			metric.ChangeToClientSamples += changeLatencySamples
		})
	}
	if len(accounting) > 0 {
		first := accounting[0]
		trace := *first.trace
		trace.ServerDurationMS = serverDurationTotal / float64(len(accounting))
		trace.ServerSubscriptionSentAtMS = trace.ServerSubscriptionStartedAtMS + trace.ServerDurationMS
		entry := transactionEntryFromTrace(first.delivery.listener.project, first.delivery.listener.tenant, "", "query", first.delivery.listener.path, "server", message.Reason, "ok", "", &trace)
		entry.LogicalCount = int64(len(accounting))
		group.manager.server.enqueueSubscriptionTelemetry([]transactionTelemetryEntry{entry})
	}
	if message.Reason == "invalidate" {
		prepareDurationMS := float64(preparedAt.Sub(deliveryStarted).Microseconds()) / 1000
		writeDurationMS := float64(writesAt.Sub(preparedAt).Microseconds()) / 1000
		group.manager.server.metrics.recordReactive(func(metric *reactiveMetricState) {
			metric.DeliveryPasses++
			metric.DeliveryPrepareDurationMS += prepareDurationMS
			metric.DeliveryWriteDurationMS += writeDurationMS
		})
	}
}

func (group *sharedSubscription) broadcastError(message string) {
	group.mu.Lock()
	listeners := group.listenerSnapshotLocked()
	group.mu.Unlock()
	for _, listener := range listeners {
		if listener.conn == nil {
			continue
		}
		if listenerCurrent(listener) {
			listener.conn.write(serverMessage{Type: "query.error", ID: listener.id, Path: listener.path, Error: message})
		}
	}
}

func (group *sharedSubscription) sendFullTo(listener querySubscription, payload json.RawMessage, revision uint64, reason string, changedAtMS float64) {
	if !listenerCurrent(listener) {
		return
	}
	revisionValue := &subscriptionRevision{Epoch: group.manager.epoch, Sequence: revision}
	hash, queryPerf := queryResultSemantics(payload)
	var trace *messageTrace
	if len(queryPerf) > 0 {
		trace = &messageTrace{QueryPerf: queryPerf}
	}
	if queryCacheRevisionMatchesHash(currentListenerCacheRevision(listener), hash) {
		listener.conn.write(serverMessage{
			Type: "query.progress", ID: listener.id, Path: listener.path, Reason: reason,
			ThroughRevision: revisionValue, Trace: trace,
		})
		return
	}
	cacheRevision := group.manager.server.nextQueryCacheRevision(hash)
	listener.conn.write(serverMessage{
		Type: "query.result", ID: listener.id, Path: listener.path, Result: payload, Reason: reason,
		CacheScope: listener.cacheScope, CacheRevision: cacheRevision,
		SubscriptionRevision: revisionValue, Trace: trace,
	})
	storeListenerCacheRevision(listener, cacheRevision)
}

// currentListenerCacheRevision reads the listener's LIVE cache revision from
// the connection's subscription map. Group snapshots copy the revision at
// subscribe time; every delivered result advances the client's cache, so the
// snapshot value must never be used for "does the client already have this
// payload" decisions.
func currentListenerCacheRevision(listener querySubscription) string {
	if listener.token == nil {
		return listener.cacheRevision
	}
	revision, _ := listener.token.cacheRevision.Load().(string)
	return revision
}

// storeListenerCacheRevision records the revision most recently DELIVERED to
// this listener so later unchanged-payload checks compare against what the
// client actually holds.
func storeListenerCacheRevision(listener querySubscription, revision string) {
	if revision == "" || listener.token == nil || !listener.token.active.Load() {
		return
	}
	listener.token.cacheRevision.Store(revision)
}

func listenerCurrent(listener querySubscription) bool {
	if listener.conn == nil || listener.token == nil || listener.ctx.Err() != nil {
		return false
	}
	return listener.token.active.Load()
}

func keyedResultPatch(previous, next json.RawMessage) (serverMessage, bool) {
	patchType := "query.patch"
	var envelope map[string]json.RawMessage
	var patchResult any
	if json.Unmarshal(next, &envelope) == nil {
		if envelope["page"] != nil {
			patchType = "query.pagePatch"
			delete(envelope, "page")
			patchResult = envelope
		} else if patch, ok := keyedObjectResultPatch(previous, envelope); ok {
			return patch, true
		}
	}
	oldRows, oldOrder, ok := keyedRows(previous)
	if !ok {
		return serverMessage{}, false
	}
	newRows, newOrder, ok := keyedRows(next)
	if !ok {
		return serverMessage{}, false
	}
	inserted := []json.RawMessage{}
	updated := []json.RawMessage{}
	deleted := []string{}
	for id, row := range newRows {
		old, exists := oldRows[id]
		if !exists {
			inserted = append(inserted, row)
		} else if !bytes.Equal(old, row) {
			updated = append(updated, row)
		}
	}
	for id := range oldRows {
		if _, exists := newRows[id]; !exists {
			deleted = append(deleted, id)
		}
	}
	if len(inserted) == 0 && len(updated) == 0 && len(deleted) == 0 && equalStrings(oldOrder, newOrder) {
		return serverMessage{}, false
	}
	sort.Slice(inserted, func(i, j int) bool { return rowID(inserted[i]) < rowID(inserted[j]) })
	sort.Slice(updated, func(i, j int) bool { return rowID(updated[i]) < rowID(updated[j]) })
	sort.Strings(deleted)
	var order, prepend, append []string
	if !equalStrings(oldOrder, newOrder) {
		if prepend, append, ok = compactOrderDelta(oldOrder, newOrder, deleted, inserted); !ok {
			order, prepend, append = newOrder, nil, nil
		}
	}
	return serverMessage{Type: patchType, Result: patchResult, Inserted: inserted, Updated: updated, Deleted: deleted, Order: order, Prepend: prepend, Append: append}, true
}

// keyedObjectResultPatch handles object-shaped query results whose properties
// are independent keyed row collections (for example a pivot payload containing
// taskUsers, taskTags, and taskCustomFieldValues). It fails closed unless both
// objects have exactly the same property set and every changed property is a
// keyed array. This keeps patch application atomic and avoids inventing merge
// semantics for removed or mutable scalar fields.
func keyedObjectResultPatch(previous json.RawMessage, next map[string]json.RawMessage) (serverMessage, bool) {
	var old map[string]json.RawMessage
	if json.Unmarshal(previous, &old) != nil || len(old) != len(next) {
		return serverMessage{}, false
	}
	collections := make(map[string]keyedCollectionPatch)
	for key, newValue := range next {
		oldValue, exists := old[key]
		if !exists {
			return serverMessage{}, false
		}
		patch, changed, ok := keyedCollectionDiff(oldValue, newValue)
		if !ok {
			if !bytes.Equal(oldValue, newValue) {
				return serverMessage{}, false
			}
			continue
		}
		if changed {
			collections[key] = patch
		}
	}
	if len(collections) == 0 {
		return serverMessage{}, false
	}
	return serverMessage{Type: "query.objectPatch", Collections: collections}, true
}

func keyedCollectionDiff(previous, next json.RawMessage) (keyedCollectionPatch, bool, bool) {
	oldRows, oldOrder, ok := keyedRows(previous)
	if !ok {
		return keyedCollectionPatch{}, false, false
	}
	newRows, newOrder, ok := keyedRows(next)
	if !ok {
		return keyedCollectionPatch{}, false, false
	}
	patch := keyedCollectionPatch{}
	for id, row := range newRows {
		old, exists := oldRows[id]
		if !exists {
			patch.Inserted = append(patch.Inserted, row)
		} else if !bytes.Equal(old, row) {
			patch.Updated = append(patch.Updated, row)
		}
	}
	for id := range oldRows {
		if _, exists := newRows[id]; !exists {
			patch.Deleted = append(patch.Deleted, id)
		}
	}
	orderChanged := !equalStrings(oldOrder, newOrder)
	if len(patch.Inserted) == 0 && len(patch.Updated) == 0 && len(patch.Deleted) == 0 && !orderChanged {
		return keyedCollectionPatch{}, false, true
	}
	sort.Slice(patch.Inserted, func(i, j int) bool { return rowID(patch.Inserted[i]) < rowID(patch.Inserted[j]) })
	sort.Slice(patch.Updated, func(i, j int) bool { return rowID(patch.Updated[i]) < rowID(patch.Updated[j]) })
	sort.Strings(patch.Deleted)
	if orderChanged {
		if prepend, append, compact := compactOrderDelta(oldOrder, newOrder, patch.Deleted, patch.Inserted); compact {
			patch.Prepend, patch.Append = prepend, append
		} else {
			patch.Order = newOrder
		}
	}
	return patch, true, true
}

func compactOrderDelta(oldOrder, newOrder, deleted []string, inserted []json.RawMessage) ([]string, []string, bool) {
	deletedSet := make(map[string]bool, len(deleted))
	for _, id := range deleted {
		deletedSet[id] = true
	}
	base := make([]string, 0, len(oldOrder))
	for _, id := range oldOrder {
		if !deletedSet[id] {
			base = append(base, id)
		}
	}
	insertedSet := make(map[string]bool, len(inserted))
	for _, row := range inserted {
		insertedSet[rowID(row)] = true
	}
	if len(newOrder) < len(base) || len(newOrder)-len(base) != len(insertedSet) {
		return nil, nil, false
	}
	prefixLen := len(newOrder) - len(base)
	if equalStrings(newOrder[prefixLen:], base) {
		prefix := append([]string(nil), newOrder[:prefixLen]...)
		for _, id := range prefix {
			if !insertedSet[id] {
				return nil, nil, false
			}
		}
		return prefix, nil, true
	}
	if equalStrings(newOrder[:len(base)], base) {
		suffix := append([]string(nil), newOrder[len(base):]...)
		for _, id := range suffix {
			if !insertedSet[id] {
				return nil, nil, false
			}
		}
		return nil, suffix, true
	}
	return nil, nil, false
}

func messageUsesOrderDelta(message serverMessage) bool {
	if len(message.Prepend) > 0 || len(message.Append) > 0 {
		return true
	}
	for _, collection := range message.Collections {
		if len(collection.Prepend) > 0 || len(collection.Append) > 0 {
			return true
		}
	}
	return false
}

func keyedRows(payload json.RawMessage) (map[string]json.RawMessage, []string, bool) {
	var rows []json.RawMessage
	if json.Unmarshal(payload, &rows) != nil {
		var envelope map[string]json.RawMessage
		if json.Unmarshal(payload, &envelope) != nil || json.Unmarshal(envelope["page"], &rows) != nil {
			return nil, nil, false
		}
	}
	byID := make(map[string]json.RawMessage, len(rows))
	order := make([]string, 0, len(rows))
	for _, row := range rows {
		id := rowID(row)
		if id == "" {
			return nil, nil, false
		}
		if _, exists := byID[id]; exists {
			return nil, nil, false
		}
		var canonical bytes.Buffer
		if json.Compact(&canonical, row) != nil {
			return nil, nil, false
		}
		byID[id] = canonical.Bytes()
		order = append(order, id)
	}
	return byID, order, true
}

func rowID(row json.RawMessage) string {
	var object map[string]json.RawMessage
	if json.Unmarshal(row, &object) != nil {
		return ""
	}
	for _, key := range []string{"_id", "id"} {
		var value any
		if json.Unmarshal(object[key], &value) != nil || value == nil {
			continue
		}
		id := strings.TrimSpace(fmt.Sprint(value))
		if id != "" && id != "<nil>" {
			return id
		}
	}
	return ""
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func normalizedColumns(values []string) []string {
	clean := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			clean = append(clean, value)
		}
	}
	return clean
}
