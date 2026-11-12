package server

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/server/internal/schema"
	"github.com/jackc/pgx/v5"
)

type tenantListenerKey struct {
	project string
	tenant  string
}

type syncNotifyPayload struct {
	Epoch    string   `json:"epoch"`
	Revision uint64   `json:"revision"`
	Tables   []string `json:"tables"`
}

// tenantListener is one project/tenant's view of a LISTEN connection: its
// reference count, readiness barrier, and recovery state.
type tenantListener struct {
	key           tenantListenerKey
	databaseURL   string
	refs          int
	cancel        context.CancelFunc
	idle          *time.Timer
	ready         chan struct{}
	connected     bool
	needsRecovery bool
	connection    *listenerConnection
}

// listenerConnection is one physical PostgreSQL LISTEN session. Projects that
// route a tenant to the same database (the production mobile alias project
// serves the primary project's tenant databases) share it: trigger
// notifications are database-wide, so every member key receives every
// notification exactly as it did with one connection per key.
type listenerConnection struct {
	databaseURL string
	cancel      context.CancelFunc
	connected   bool
	members     map[tenantListenerKey]*tenantListener
}

type tenantListenerManager struct {
	server      *Server
	mu          sync.Mutex
	active      map[tenantListenerKey]*tenantListener
	connections map[string]*listenerConnection
}

func newTenantListenerManager(server *Server) *tenantListenerManager {
	return &tenantListenerManager{
		server:      server,
		active:      map[tenantListenerKey]*tenantListener{},
		connections: map[string]*listenerConnection{},
	}
}

func (m *tenantListenerManager) acquire(project, tenant string) <-chan struct{} {
	databaseURL := strings.TrimSpace(m.server.databaseURLForTenant(project, tenant))
	if databaseURL == "" || m.server.config.TenantListenerLimit == 0 {
		return nil
	}
	key := tenantListenerKey{project: project, tenant: tenant}
	m.mu.Lock()
	if listener := m.active[key]; listener != nil {
		if listener.databaseURL != databaseURL {
			// Tenant routing can be hydrated after the first subscription opens.
			// A listener connected before that hydration points at the project /
			// landlord database forever, so sync/query invalidations from the real
			// tenant database are never observed. Carry the existing references to
			// a replacement listener; their eventual release calls are key-based.
			if listener.idle != nil {
				listener.idle.Stop()
				listener.idle = nil
			}
			replacement := &tenantListener{
				key: key, databaseURL: databaseURL, refs: listener.refs + 1,
				ready: make(chan struct{}), needsRecovery: true,
			}
			m.detachLocked(listener)
			m.active[key] = replacement
			recoverNow := m.attachLocked(replacement)
			m.mu.Unlock()
			m.server.markTenantSyncsOutOfDate(project, tenant, "listener-route-changed")
			if recoverNow {
				go m.recover(key)
			}
			return replacement.ready
		}
		listener.refs++
		if listener.idle != nil {
			listener.idle.Stop()
			listener.idle = nil
		}
		m.mu.Unlock()
		return listener.ready
	}
	if len(m.active) >= m.server.config.TenantListenerLimit {
		m.mu.Unlock()
		m.server.metrics.recordReactive(func(metric *reactiveMetricState) { metric.ListenerLimitRefusals++ })
		return nil
	}
	listener := &tenantListener{
		key: key, databaseURL: databaseURL, refs: 1, ready: make(chan struct{}),
	}
	m.active[key] = listener
	m.attachLocked(listener)
	m.mu.Unlock()
	return listener.ready
}

// attachLocked joins listener to the LISTEN connection for its database,
// starting one if none exists. A listener joining an already connected
// session is ready at once; it reports whether that listener still needs a
// recovery pass (a route change can carry missed notifications over).
func (m *tenantListenerManager) attachLocked(listener *tenantListener) bool {
	connection := m.connections[listener.databaseURL]
	started := false
	if connection == nil {
		ctx, cancel := context.WithCancel(context.Background())
		connection = &listenerConnection{
			databaseURL: listener.databaseURL, cancel: cancel,
			members: map[tenantListenerKey]*tenantListener{},
		}
		m.connections[listener.databaseURL] = connection
		go m.run(ctx, connection)
		started = true
	}
	connection.members[listener.key] = listener
	listener.connection = connection
	listener.cancel = func() {
		m.mu.Lock()
		m.detachLocked(listener)
		m.mu.Unlock()
	}
	m.recordListenerCountsLocked()
	if started || !connection.connected || listener.connected {
		return false
	}
	listener.connected = true
	close(listener.ready)
	needsRecovery := listener.needsRecovery
	listener.needsRecovery = false
	return needsRecovery
}

// detachLocked removes listener from its LISTEN connection and closes the
// connection once no project/tenant uses it.
func (m *tenantListenerManager) detachLocked(listener *tenantListener) {
	connection := listener.connection
	if connection == nil {
		if listener.cancel != nil {
			listener.cancel()
		}
		return
	}
	listener.connection = nil
	// The cancel closure detaches through this function; once detached it must
	// not call back into it while the manager lock is held.
	listener.cancel = nil
	if connection.members[listener.key] == listener {
		delete(connection.members, listener.key)
	}
	if len(connection.members) == 0 {
		connection.cancel()
		if m.connections[connection.databaseURL] == connection {
			delete(m.connections, connection.databaseURL)
		}
	}
	m.recordListenerCountsLocked()
}

func (m *tenantListenerManager) recordListenerCountsLocked() {
	active, connections := len(m.active), len(m.connections)
	m.server.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.ActiveTenantListeners = active
		metric.ActiveListenerConnections = connections
	})
}

// recover replays what a listener may have missed while it was not
// listening. PostgreSQL notifications are edge-triggered, so rerun its live
// queries and force one coalesced sync delivery from the durable change log.
func (m *tenantListenerManager) recover(key tenantListenerKey) {
	m.server.subscriptions.refreshTenant(key.project, key.tenant)
	m.server.notifySyncRevision(key.project, key.tenant, nil, "", 0)
}

func (m *tenantListenerManager) markNeedsRecovery(project, tenant string) {
	m.mu.Lock()
	if listener := m.active[tenantListenerKey{project: project, tenant: tenant}]; listener != nil {
		listener.needsRecovery = true
	}
	m.mu.Unlock()
}

func (m *tenantListenerManager) markReady(listener *tenantListener) bool {
	m.mu.Lock()
	if current := m.active[listener.key]; current != listener {
		m.mu.Unlock()
		return false
	}
	needsRecovery := listener.needsRecovery
	listener.needsRecovery = false
	if !listener.connected {
		listener.connected = true
		close(listener.ready)
	}
	m.mu.Unlock()
	return needsRecovery
}

func (m *tenantListenerManager) markDisconnected(listener *tenantListener) {
	m.mu.Lock()
	if current := m.active[listener.key]; current != listener || !listener.connected {
		m.mu.Unlock()
		return
	}
	listener.connected = false
	listener.needsRecovery = true
	listener.ready = make(chan struct{})
	m.mu.Unlock()
	m.server.markTenantSyncsOutOfDate(listener.key.project, listener.key.tenant, "listener-reconnecting")
}

// whileConnected serializes a freshness-sensitive action with listener
// disconnect detection. In particular, a sync subscription must be published
// before a concurrent disconnect enumerates subscriptions, and sync.ready must
// be written before the corresponding sync.syncing frame. Returning the current
// readiness barrier lets callers retry safely when they observed an older,
// already-closed barrier just as the listener disconnected.
func (m *tenantListenerManager) whileConnected(
	project string,
	tenant string,
	action func(),
) (<-chan struct{}, bool) {
	key := tenantListenerKey{project: project, tenant: tenant}
	m.mu.Lock()
	defer m.mu.Unlock()
	listener := m.active[key]
	if listener == nil {
		return nil, false
	}
	if !listener.connected {
		return listener.ready, false
	}
	action()
	return listener.ready, true
}

func (m *tenantListenerManager) release(project, tenant string) {
	key := tenantListenerKey{project: project, tenant: tenant}
	m.mu.Lock()
	listener := m.active[key]
	if listener == nil {
		m.mu.Unlock()
		return
	}
	if listener.refs > 0 {
		listener.refs--
	}
	if listener.refs == 0 && listener.idle == nil {
		listener.idle = time.AfterFunc(m.server.config.TenantListenerIdleTimeout, func() { m.expire(key, listener) })
	}
	m.mu.Unlock()
}

func (m *tenantListenerManager) expire(key tenantListenerKey, expected *tenantListener) {
	m.mu.Lock()
	listener := m.active[key]
	if listener != expected || listener.refs != 0 {
		m.mu.Unlock()
		return
	}
	delete(m.active, key)
	m.detachLocked(listener)
	m.recordListenerCountsLocked()
	m.mu.Unlock()
}

func (m *tenantListenerManager) run(ctx context.Context, connection *listenerConnection) {
	backoff := 250 * time.Millisecond
	connectedBefore := false
	for ctx.Err() == nil {
		connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		pgConnection, err := pgx.Connect(connectCtx, connection.databaseURL)
		cancel()
		if err == nil {
			_, err = pgConnection.Exec(ctx, "LISTEN "+schema.NotifyChannel)
		}
		if err == nil {
			_, err = pgConnection.Exec(ctx, "LISTEN "+schema.SyncNotifyChannel)
		}
		if err == nil {
			recoveries := m.markConnectionReady(connection, connectedBefore)
			if connectedBefore {
				m.server.metrics.recordReactive(func(metric *reactiveMetricState) { metric.ListenerReconnects++ })
			}
			for _, key := range recoveries {
				m.recover(key)
			}
			connectedBefore = true
			backoff = 250 * time.Millisecond
			err = m.wait(ctx, pgConnection, connection)
			if err != nil && ctx.Err() == nil {
				m.markConnectionDisconnected(connection)
			}
		}
		if pgConnection != nil {
			_ = pgConnection.Close(context.Background())
		}
		if ctx.Err() != nil {
			return
		}
		m.server.metrics.recordReactive(func(metric *reactiveMetricState) { metric.ListenerFailures++ })
		jitterWindow := backoff / 4
		jitter := time.Duration(0)
		if jitterWindow > 0 {
			jitter = time.Duration(time.Now().UnixNano() % int64(jitterWindow))
		}
		timer := time.NewTimer(backoff + jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 10*time.Second {
			backoff *= 2
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
		}
	}
}

// markConnectionReady opens every member's readiness barrier once LISTEN
// succeeds and returns the members that need a recovery pass: all of them
// after a reconnect, otherwise those flagged by a timeout or route change.
func (m *tenantListenerManager) markConnectionReady(connection *listenerConnection, reconnected bool) []tenantListenerKey {
	m.mu.Lock()
	connection.connected = true
	members := make([]*tenantListener, 0, len(connection.members))
	for _, listener := range connection.members {
		members = append(members, listener)
	}
	m.mu.Unlock()
	recoveries := make([]tenantListenerKey, 0, len(members))
	for _, listener := range members {
		if needsRecovery := m.markReady(listener); reconnected || needsRecovery {
			recoveries = append(recoveries, listener.key)
		}
	}
	return recoveries
}

func (m *tenantListenerManager) markConnectionDisconnected(connection *listenerConnection) {
	m.mu.Lock()
	connection.connected = false
	members := make([]*tenantListener, 0, len(connection.members))
	for _, listener := range connection.members {
		members = append(members, listener)
	}
	m.mu.Unlock()
	for _, listener := range members {
		m.markDisconnected(listener)
	}
}

func (m *tenantListenerManager) connectionMembers(connection *listenerConnection) []tenantListenerKey {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]tenantListenerKey, 0, len(connection.members))
	for key := range connection.members {
		keys = append(keys, key)
	}
	return keys
}

func (m *tenantListenerManager) wait(ctx context.Context, pgConnection *pgx.Conn, connection *listenerConnection) error {
	for {
		notification, err := pgConnection.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		members := m.connectionMembers(connection)
		if notification.Channel == schema.SyncNotifyChannel {
			payload := parseSyncNotifyPayload(notification.Payload)
			for _, key := range members {
				m.server.notifySyncRevision(key.project, key.tenant, payload.Tables, payload.Epoch, payload.Revision)
			}
			continue
		}
		change, ok := parseTableNotification(notification.Payload)
		if !ok {
			// A malformed/empty table notification means this LISTEN stream can no
			// longer prove that its observed table set is complete. Keep declared
			// write invalidations authoritative until a reconnect recovery clears
			// this uncertainty.
			for _, key := range members {
				m.markNeedsRecovery(key.project, key.tenant)
			}
			continue
		}
		for _, key := range members {
			memberChange := change
			memberChange.project, memberChange.tenant = key.project, key.tenant
			m.server.scheduleTableChange(memberChange)
		}
	}
}

// parseTableNotification converts a trigger payload into a tableChange
// without a project or tenant; the caller addresses it to each listener.
func parseTableNotification(raw string) (tableChange, bool) {
	payload := tableNotifyPayload{}
	if raw != "" && raw[0] != '{' {
		payload.Table = raw
		payload.Broad = true
	} else if json.Unmarshal([]byte(raw), &payload) != nil || payload.Table == "" {
		return tableChange{}, false
	}
	rowIDs := map[string]bool{}
	for _, id := range payload.IDs {
		rowIDs[id] = true
	}
	taskIDs := map[string]bool{}
	for _, id := range payload.TaskIDs {
		taskIDs[id] = true
	}
	userIDs := map[string]bool{}
	for _, id := range payload.UserIDs {
		userIDs[id] = true
	}
	workspaceIDs := map[string]bool{}
	for _, id := range payload.WorkspaceIDs {
		workspaceIDs[id] = true
	}
	return tableChange{
		table: payload.Table, broad: payload.Broad, rowIDs: rowIDs, taskIDs: taskIDs, userIDs: userIDs,
		workspaceIDs: workspaceIDs, operation: payload.Operation,
		changedColumns: normalizedColumns(payload.ChangedColumns), changedAtMS: epochMillis(time.Now().UTC()),
		commitID: strings.TrimSpace(payload.MutationID), triggerObserved: true,
	}, true
}

func (m *tenantListenerManager) healthy(project, tenant string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	listener := m.active[tenantListenerKey{project: project, tenant: tenant}]
	return listener != nil && listener.connected && !listener.needsRecovery
}

func parseSyncNotifyPayload(raw string) syncNotifyPayload {
	payload := syncNotifyPayload{}
	if json.Unmarshal([]byte(raw), &payload) != nil || strings.TrimSpace(payload.Epoch) == "" || payload.Revision == 0 {
		return syncNotifyPayload{}
	}
	payload.Tables = appendUniqueStrings(nil, payload.Tables...)
	if len(payload.Tables) == 0 {
		// Legacy notifications and the trigger's oversized-payload fallback omit
		// tables. An explicit empty list is equally non-actionable, so all three
		// cases retain the full-delivery correctness backstop.
		payload.Tables = nil
	}
	return payload
}
