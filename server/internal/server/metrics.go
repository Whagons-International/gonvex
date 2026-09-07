package server

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/dbpool"
	"github.com/google/uuid"
)

const (
	metricsBucketWidth        = 30 * time.Second
	metricsBucketCount        = 24
	metricsLogLimit           = 1000
	metricsTelemetryLogLimit  = 1000
	metricsDatabasePointLimit = 360
	// metricsLoadPointLimit bounds the background load samples (connections,
	// users, CPU, memory). 360 points at loadSampleInterval (30s) ≈ 3 hours —
	// long enough to correlate server load with the daily connection ramp.
	metricsLoadPointLimit = 360
	// propagationTargetMS is the product goal for update propagation: a
	// committed change should be reflected in every subscribed browser within
	// this budget. Surfaced in the snapshot so dashboards chart against it.
	propagationTargetMS = 200
	// propagationBucketSampleLimit caps the per-bucket raw samples kept for
	// the p95 estimate; avg/max use every sample regardless.
	propagationBucketSampleLimit = 512
)

type runtimeMetrics struct {
	mu                   sync.Mutex
	functions            map[string]*functionMetrics
	transactions         map[string]*transactionMetrics
	cache                cacheMetrics
	runningByKind        map[string]int64
	runningBuckets       map[int64]map[string]int64
	database             map[string]*databaseMetricState
	reactive             reactiveMetricState
	commitExecutions     map[string]uint64
	commitExecutionOrder []string
	load                 []loadMetricPoint
	propagationBuckets   map[int64]*propagationMetricsBucket
	propagationTotals    propagationTotals
	logs                 []runtimeLogEntry
	telemetryLogs        []transactionTelemetryEntry
	telemetryPath        string
	logSubscribers       map[int]logSubscriber
	nextLogSubID         int
	mutationWrites       chan runtimeLogEntry
	// onFunctionError forwards failed log entries to the error store (see
	// runtime_errors.go). Set once at server construction; nil in tests.
	onFunctionError func(runtimeLogEntry)
	// admissionSource reads the query admission controller's counters for the
	// metrics API. Set once at server construction; nil in isolated tests.
	admissionSource func() queryAdmissionSnapshot
}

type logSubscriber struct {
	project string
	ch      chan runtimeLogEntry
}

type functionMetrics struct {
	Kind            string
	Calls           int64
	Errors          int64
	TotalDurationMS float64
	LastDurationMS  float64
	LastCalledAt    time.Time
	Buckets         map[int64]*functionMetricsBucket
}

type functionMetricsBucket struct {
	Calls           int64
	Errors          int64
	TotalDurationMS float64
}

type cacheMetrics struct {
	Hits     int64
	Misses   int64
	Bypasses int64
	Buckets  map[int64]*cacheMetricsBucket
}

type cacheMetricsBucket struct {
	Hits     int64
	Misses   int64
	Bypasses int64
}

type databaseMetricState struct {
	Current                databasePoolStats
	CurrentBudget          dbpool.BudgetStats
	PreviousWaits          int64
	PreviousWaitTime       time.Duration
	PreviousBudgetWaits    uint64
	PreviousBudgetWaitTime time.Duration
	Series                 []databaseMetricPoint
}

type runtimeLogEntry struct {
	Time             string          `json:"time"`
	ExecutionID      string          `json:"executionId,omitempty"`
	OperationID      string          `json:"operationId,omitempty"`
	StartedAt        string          `json:"startedAt,omitempty"`
	CompletedAt      string          `json:"completedAt,omitempty"`
	Project          string          `json:"project,omitempty"`
	Tenant           string          `json:"tenant,omitempty"`
	UserID           string          `json:"userId,omitempty"`
	UserEmail        string          `json:"userEmail,omitempty"`
	ConnectionID     string          `json:"connectionId,omitempty"`
	Browser          string          `json:"browser,omitempty"`
	DeviceType       string          `json:"deviceType,omitempty"`
	Platform         string          `json:"platform,omitempty"`
	Path             string          `json:"path"`
	Kind             string          `json:"kind"`
	Outcome          string          `json:"outcome"`
	DurationMS       float64         `json:"durationMs"`
	Error            string          `json:"error,omitempty"`
	Cache            string          `json:"cache,omitempty"`
	Source           string          `json:"source,omitempty"`
	Reason           string          `json:"reason,omitempty"`
	Request          json.RawMessage `json:"request,omitempty"`
	RequestSizeBytes int             `json:"requestSizeBytes,omitempty"`
	ResultCount      *int            `json:"resultCount,omitempty"`
}

type runtimeFunctionLog struct {
	entry   runtimeLogEntry
	started time.Time
}

type transactionTelemetryEntry struct {
	ServerSocketWriteStartedAtMS float64 `json:"serverSocketWriteStartedAtMs,omitempty"`
	ServerSocketQueueMS          float64 `json:"serverSocketQueueMs,omitempty"`
	Time                         string  `json:"time"`
	Project                      string  `json:"project,omitempty"`
	Tenant                       string  `json:"tenant,omitempty"`
	OperationID                  string  `json:"operationId,omitempty"`
	Kind                         string  `json:"kind"`
	Path                         string  `json:"path"`
	Phase                        string  `json:"phase"`
	Reason                       string  `json:"reason,omitempty"`
	Outcome                      string  `json:"outcome"`
	Error                        string  `json:"error,omitempty"`
	ClientSentAtMS               float64 `json:"clientSentAtMs,omitempty"`
	ClientReceivedAtMS           float64 `json:"clientReceivedAtMs,omitempty"`
	ClientDurationMS             float64 `json:"clientDurationMs,omitempty"`
	ServerReceivedAtMS           float64 `json:"serverReceivedAtMs,omitempty"`
	ServerCommittedAtMS          float64 `json:"serverCommittedAtMs,omitempty"`
	ServerCompletedAtMS          float64 `json:"serverCompletedAtMs,omitempty"`
	ServerSentAtMS               float64 `json:"serverSentAtMs,omitempty"`
	ChangeCommittedAtMS          float64 `json:"changeCommittedAtMs,omitempty"`
	ServerDurationMS             float64 `json:"serverDurationMs,omitempty"`
	ServerCommitMS               float64 `json:"serverCommitMs,omitempty"`
	ClientToCommitMS             float64 `json:"clientToCommitMs,omitempty"`
	ClientRoundTripMS            float64 `json:"clientRoundTripMs,omitempty"`
	ServerToBrowserMS            float64 `json:"serverToBrowserMs,omitempty"`
	ChangeToBrowserMS            float64 `json:"changeToBrowserMs,omitempty"`
	ChangeToAckMS                float64 `json:"changeToAckMs,omitempty"`
	SubscriptionDurationMS       float64 `json:"subscriptionDurationMs,omitempty"`
	BrowserName                  string  `json:"browserName,omitempty"`
	BrowserVersion               string  `json:"browserVersion,omitempty"`
	DeviceType                   string  `json:"deviceType,omitempty"`
	Platform                     string  `json:"platform,omitempty"`
	UserAgent                    string  `json:"userAgent,omitempty"`
	Language                     string  `json:"language,omitempty"`
	Timezone                     string  `json:"timezone,omitempty"`
	ViewportWidth                int     `json:"viewportWidth,omitempty"`
	ViewportHeight               int     `json:"viewportHeight,omitempty"`
	DeviceJSON                   string  `json:"device,omitempty"`
	LogicalCount                 int64   `json:"logicalCount,omitempty"`
}

type transactionMetrics struct {
	Kind                        string
	Path                        string
	ServerEvents                int64
	BrowserEvents               int64
	Errors                      int64
	TotalServerDurationMS       float64
	TotalServerCommitMS         float64
	ServerCommitSamples         int64
	TotalClientToCommitMS       float64
	ClientToCommitSamples       int64
	TotalClientRoundTripMS      float64
	ClientRoundTripSamples      int64
	TotalServerToBrowserMS      float64
	ServerToBrowserSamples      int64
	TotalChangeToBrowserMS      float64
	ChangeToBrowserSamples      int64
	TotalSubscriptionDurationMS float64
	SubscriptionDurationSamples int64
	LastEventAt                 time.Time
	Buckets                     map[int64]*transactionMetricsBucket
}

type transactionMetricsBucket struct {
	ServerEvents           int64
	BrowserEvents          int64
	Errors                 int64
	TotalServerDurationMS  float64
	TotalServerCommitMS    float64
	ServerCommitSamples    int64
	TotalClientRoundTripMS float64
	ClientRoundTripSamples int64
	TotalServerToBrowserMS float64
	ServerToBrowserSamples int64
	TotalChangeToBrowserMS float64
	ChangeToBrowserSamples int64
}

type runtimeMetricsSnapshot struct {
	GeneratedAt    string                               `json:"generatedAt"`
	Functions      map[string]functionMetricSnapshot    `json:"functions"`
	Transactions   map[string]transactionMetricSnapshot `json:"transactions"`
	Cache          cacheMetricSnapshot                  `json:"cache"`
	Running        runningMetricSnapshot                `json:"running"`
	WebSocket      websocketMetricSnapshot              `json:"websocket"`
	Database       databaseMetricSnapshot               `json:"database"`
	Resources      runtimeResourceSnapshot              `json:"resources"`
	Load           loadMetricSnapshot                   `json:"load"`
	Propagation    propagationMetricSnapshot            `json:"propagation"`
	Reactive       reactiveMetricSnapshot               `json:"reactive"`
	QueryAdmission queryAdmissionMetricSnapshot         `json:"queryAdmission"`
	Scheduler      *schedulerSnapshot                   `json:"scheduler,omitempty"`
	Logs           []runtimeLogEntry                    `json:"logs"`
	TelemetryLogs  []transactionTelemetryEntry          `json:"telemetryLogs"`
}

type runtimeResourceSnapshot struct {
	MemoryBytes          uint64  `json:"memoryBytes"`
	MemoryPerClientBytes uint64  `json:"memoryPerClientBytes"`
	CPUPercent           float64 `json:"cpuPercent"`
	CPUPerClientPercent  float64 `json:"cpuPerClientPercent"`
	BytesReceived        uint64  `json:"bytesReceived"`
	BytesSent            uint64  `json:"bytesSent"`
	BytesPerClient       uint64  `json:"bytesPerClient"`
}

type reactiveMetricState struct {
	ChangeBatchesReceived              uint64
	SubscriptionsInspected             uint64
	CandidateSubscriptionsSelected     uint64
	SubscriptionsSkippedByTable        uint64
	QueriesRerun                       uint64
	SubscriptionRerunQueueDepth        int
	SubscriptionRerunQueueWaitMS       float64
	SubscriptionCommitsObserved        uint64
	CommitQueryExecutions              uint64
	MaxExecutionsPerSubscriptionCommit uint64
	DuplicateCommitQueryExecutions     uint64
	ConcurrentExecutionViolations      uint64
	RerunsCoalesced                    uint64
	UnchangedResultsSuppressed         uint64
	FullResults                        uint64
	Patches                            uint64
	ProgressMessages                   uint64
	ResultBytesBefore                  uint64
	ResultBytesAfter                   uint64
	DatabaseQueryCount                 uint64
	DatabaseQueryDurationMS            float64
	VisibilityResolverExecutions       uint64
	VisibilityResolverDurationMS       float64
	VisibilitySharedExecutions         uint64
	VisibilitySharedDurationMS         float64
	ReactiveExecutionPasses            uint64
	ReactiveExecutionDurationMS        float64
	ResultCompletionPasses             uint64
	ResultCompletionDurationMS         float64
	DeliveryPasses                     uint64
	DeliveryPrepareDurationMS          float64
	DeliveryWriteDurationMS            float64
	ChangeToClientDurationMS           float64
	ChangeToClientSamples              uint64
	ActiveTenantListeners              int
	ListenerReconnects                 uint64
	ListenerFailures                   uint64
	ListenerLimitRefusals              uint64
	SharedSubscriptions                int
	SubscriptionListeners              int
}

type reactiveMetricSnapshot struct {
	ChangeBatchesReceived              uint64  `json:"changeBatchesReceived"`
	SubscriptionsInspected             uint64  `json:"subscriptionsInspected"`
	CandidateSubscriptionsSelected     uint64  `json:"candidateSubscriptionsSelected"`
	SubscriptionsSkippedByTable        uint64  `json:"subscriptionsSkippedByTable"`
	QueriesRerun                       uint64  `json:"queriesRerun"`
	SubscriptionRerunQueueDepth        int     `json:"subscriptionRerunQueueDepth"`
	SubscriptionRerunQueueWaitMS       float64 `json:"subscriptionRerunQueueWaitMs"`
	SubscriptionCommitsObserved        uint64  `json:"subscriptionCommitsObserved"`
	CommitQueryExecutions              uint64  `json:"commitQueryExecutions"`
	ExecutionsPerSubscriptionCommit    float64 `json:"executionsPerSubscriptionCommit"`
	MaxExecutionsPerSubscriptionCommit uint64  `json:"maxExecutionsPerSubscriptionCommit"`
	DuplicateCommitQueryExecutions     uint64  `json:"duplicateCommitQueryExecutions"`
	ConcurrentExecutionViolations      uint64  `json:"concurrentExecutionViolations"`
	RerunsCoalesced                    uint64  `json:"rerunsCoalesced"`
	UnchangedResultsSuppressed         uint64  `json:"unchangedResultsSuppressed"`
	FullResults                        uint64  `json:"fullResults"`
	Patches                            uint64  `json:"patches"`
	ProgressMessages                   uint64  `json:"progressMessages"`
	ResultBytesBefore                  uint64  `json:"resultBytesBefore"`
	ResultBytesAfter                   uint64  `json:"resultBytesAfter"`
	DatabaseQueryCount                 uint64  `json:"databaseQueryCount"`
	DatabaseQueryDurationMS            float64 `json:"databaseQueryDurationMs"`
	VisibilityResolverExecutions       uint64  `json:"visibilityResolverExecutions"`
	VisibilityResolverDurationMS       float64 `json:"visibilityResolverDurationMs"`
	VisibilitySharedExecutions         uint64  `json:"visibilitySharedExecutions"`
	VisibilitySharedDurationMS         float64 `json:"visibilitySharedDurationMs"`
	ReactiveExecutionPasses            uint64  `json:"reactiveExecutionPasses"`
	ReactiveExecutionDurationMS        float64 `json:"reactiveExecutionDurationMs"`
	ResultCompletionPasses             uint64  `json:"resultCompletionPasses"`
	ResultCompletionDurationMS         float64 `json:"resultCompletionDurationMs"`
	DeliveryPasses                     uint64  `json:"deliveryPasses"`
	DeliveryPrepareDurationMS          float64 `json:"deliveryPrepareDurationMs"`
	DeliveryWriteDurationMS            float64 `json:"deliveryWriteDurationMs"`
	AverageChangeToClientMS            float64 `json:"averageChangeToClientMs"`
	ActiveTenantListeners              int     `json:"activeTenantListeners"`
	ListenerReconnects                 uint64  `json:"listenerReconnects"`
	ListenerFailures                   uint64  `json:"listenerFailures"`
	ListenerLimitRefusals              uint64  `json:"listenerLimitRefusals"`
	SharedSubscriptions                int     `json:"sharedSubscriptions"`
	SubscriptionListeners              int     `json:"subscriptionListeners"`
}

type queryAdmissionClassSnapshot struct {
	Active             int     `json:"active"`
	QueueDepth         int     `json:"queueDepth"`
	Admitted           uint64  `json:"admitted"`
	Waited             uint64  `json:"waited"`
	Cancelled          uint64  `json:"cancelled"`
	WaitMS             float64 `json:"waitMs"`
	MaxWaitMS          float64 `json:"maxWaitMs"`
	TenantsQueued      int     `json:"tenantsQueued"`
	LargestTenantQueue int     `json:"largestTenantQueue"`
}

type queryAdmissionMetricSnapshot struct {
	Enabled                    bool                        `json:"enabled"`
	TotalPermits               int                         `json:"totalPermits"`
	BootstrapPermits           int                         `json:"bootstrapPermits"`
	Active                     int                         `json:"active"`
	BootstrapActive            int                         `json:"bootstrapActive"`
	Reactive                   queryAdmissionClassSnapshot `json:"reactive"`
	Foreground                 queryAdmissionClassSnapshot `json:"foreground"`
	Bootstrap                  queryAdmissionClassSnapshot `json:"bootstrap"`
	ReactiveDelayedByBootstrap uint64                      `json:"reactiveDelayedByBootstrap"`
}

func admissionClassSnapshot(metric admissionClassMetrics) queryAdmissionClassSnapshot {
	return queryAdmissionClassSnapshot{
		Active:             metric.Active,
		QueueDepth:         metric.QueueDepth,
		Admitted:           metric.Admitted,
		Waited:             metric.Waited,
		Cancelled:          metric.Cancelled,
		WaitMS:             metric.WaitMS,
		MaxWaitMS:          metric.MaxWaitMS,
		TenantsQueued:      metric.TenantsQueued,
		LargestTenantQueue: metric.LargestTenantQ,
	}
}

func (m *runtimeMetrics) recordReactive(update func(*reactiveMetricState)) {
	if m == nil || update == nil {
		return
	}
	m.mu.Lock()
	update(&m.reactive)
	m.mu.Unlock()
}

func (m *runtimeMetrics) recordQueryCommitExecution(project, tenant, groupKey string, commitIDs map[string]struct{}) {
	if m == nil || len(commitIDs) == 0 {
		return
	}
	const retainedSubscriptionCommits = 10_000
	m.mu.Lock()
	if m.commitExecutions == nil {
		m.commitExecutions = map[string]uint64{}
	}
	m.reactive.CommitQueryExecutions++
	for commitID := range commitIDs {
		if strings.TrimSpace(commitID) == "" {
			continue
		}
		key := strings.Join([]string{project, tenant, groupKey, commitID}, "\x00")
		if m.commitExecutions[key] == 0 {
			m.reactive.SubscriptionCommitsObserved++
			m.commitExecutionOrder = append(m.commitExecutionOrder, key)
		}
		m.commitExecutions[key]++
		executions := m.commitExecutions[key]
		if executions > m.reactive.MaxExecutionsPerSubscriptionCommit {
			m.reactive.MaxExecutionsPerSubscriptionCommit = executions
		}
		if executions > 1 {
			m.reactive.DuplicateCommitQueryExecutions++
		}
	}
	for len(m.commitExecutionOrder) > retainedSubscriptionCommits {
		oldest := m.commitExecutionOrder[0]
		m.commitExecutionOrder = m.commitExecutionOrder[1:]
		delete(m.commitExecutions, oldest)
	}
	m.mu.Unlock()
}

func (state reactiveMetricState) snapshot() reactiveMetricSnapshot {
	averageChangeToClient := float64(0)
	if state.ChangeToClientSamples > 0 {
		averageChangeToClient = state.ChangeToClientDurationMS / float64(state.ChangeToClientSamples)
	}
	executionsPerCommit := float64(0)
	if state.SubscriptionCommitsObserved > 0 {
		executionsPerCommit = float64(state.CommitQueryExecutions) / float64(state.SubscriptionCommitsObserved)
	}
	return reactiveMetricSnapshot{
		ChangeBatchesReceived:              state.ChangeBatchesReceived,
		SubscriptionsInspected:             state.SubscriptionsInspected,
		CandidateSubscriptionsSelected:     state.CandidateSubscriptionsSelected,
		SubscriptionsSkippedByTable:        state.SubscriptionsSkippedByTable,
		QueriesRerun:                       state.QueriesRerun,
		SubscriptionRerunQueueDepth:        state.SubscriptionRerunQueueDepth,
		SubscriptionRerunQueueWaitMS:       state.SubscriptionRerunQueueWaitMS,
		SubscriptionCommitsObserved:        state.SubscriptionCommitsObserved,
		CommitQueryExecutions:              state.CommitQueryExecutions,
		ExecutionsPerSubscriptionCommit:    executionsPerCommit,
		MaxExecutionsPerSubscriptionCommit: state.MaxExecutionsPerSubscriptionCommit,
		DuplicateCommitQueryExecutions:     state.DuplicateCommitQueryExecutions,
		ConcurrentExecutionViolations:      state.ConcurrentExecutionViolations,
		RerunsCoalesced:                    state.RerunsCoalesced,
		UnchangedResultsSuppressed:         state.UnchangedResultsSuppressed,
		FullResults:                        state.FullResults,
		Patches:                            state.Patches,
		ProgressMessages:                   state.ProgressMessages,
		ResultBytesBefore:                  state.ResultBytesBefore,
		ResultBytesAfter:                   state.ResultBytesAfter,
		DatabaseQueryCount:                 state.DatabaseQueryCount,
		DatabaseQueryDurationMS:            state.DatabaseQueryDurationMS,
		VisibilityResolverExecutions:       state.VisibilityResolverExecutions,
		VisibilityResolverDurationMS:       state.VisibilityResolverDurationMS,
		VisibilitySharedExecutions:         state.VisibilitySharedExecutions,
		VisibilitySharedDurationMS:         state.VisibilitySharedDurationMS,
		ReactiveExecutionPasses:            state.ReactiveExecutionPasses,
		ReactiveExecutionDurationMS:        state.ReactiveExecutionDurationMS,
		ResultCompletionPasses:             state.ResultCompletionPasses,
		ResultCompletionDurationMS:         state.ResultCompletionDurationMS,
		DeliveryPasses:                     state.DeliveryPasses,
		DeliveryPrepareDurationMS:          state.DeliveryPrepareDurationMS,
		DeliveryWriteDurationMS:            state.DeliveryWriteDurationMS,
		AverageChangeToClientMS:            averageChangeToClient,
		ActiveTenantListeners:              state.ActiveTenantListeners,
		ListenerReconnects:                 state.ListenerReconnects,
		ListenerFailures:                   state.ListenerFailures,
		ListenerLimitRefusals:              state.ListenerLimitRefusals,
		SharedSubscriptions:                state.SharedSubscriptions,
		SubscriptionListeners:              state.SubscriptionListeners,
	}
}

type databaseMetricSnapshot struct {
	Pools                      int                   `json:"pools"`
	OpenConnections            int                   `json:"openConnections"`
	InUse                      int                   `json:"inUse"`
	Idle                       int                   `json:"idle"`
	MaxOpenConnections         int                   `json:"maxOpenConnections"`
	WaitCount                  int64                 `json:"waitCount"`
	WaitDurationMS             float64               `json:"waitDurationMs"`
	GlobalBudgetLimit          int                   `json:"globalBudgetLimit"`
	GlobalBudgetActive         int                   `json:"globalBudgetActive"`
	GlobalBudgetWaiters        int                   `json:"globalBudgetWaiters"`
	GlobalBudgetWaitCount      uint64                `json:"globalBudgetWaitCount"`
	GlobalBudgetWaitDurationMS float64               `json:"globalBudgetWaitDurationMs"`
	Series                     []databaseMetricPoint `json:"series"`
}

type databaseMetricPoint struct {
	Time                       string  `json:"time"`
	OpenConnections            int     `json:"openConnections"`
	InUse                      int     `json:"inUse"`
	Idle                       int     `json:"idle"`
	WaitCount                  int64   `json:"waitCount"`
	WaitDurationMS             float64 `json:"waitDurationMs"`
	GlobalBudgetActive         int     `json:"globalBudgetActive"`
	GlobalBudgetWaiters        int     `json:"globalBudgetWaiters"`
	GlobalBudgetWaitCount      uint64  `json:"globalBudgetWaitCount"`
	GlobalBudgetWaitDurationMS float64 `json:"globalBudgetWaitDurationMs"`
}

type runningMetricSnapshot struct {
	Current map[string]int64     `json:"current"`
	Total   int64                `json:"total"`
	Series  []runningMetricPoint `json:"series"`
}

type runningMetricPoint struct {
	Time     string `json:"time"`
	Query    int64  `json:"query"`
	Mutation int64  `json:"mutation"`
	Action   int64  `json:"action"`
}

type functionMetricSnapshot struct {
	Kind              string                `json:"kind"`
	Calls             int64                 `json:"calls"`
	Errors            int64                 `json:"errors"`
	TotalDurationMS   float64               `json:"totalDurationMs"`
	AverageDurationMS float64               `json:"averageDurationMs"`
	LastDurationMS    float64               `json:"lastDurationMs"`
	LastCalledAt      string                `json:"lastCalledAt,omitempty"`
	Series            []functionMetricPoint `json:"series"`
}

type functionMetricPoint struct {
	Time              string  `json:"time"`
	Calls             int64   `json:"calls"`
	Errors            int64   `json:"errors"`
	AverageDurationMS float64 `json:"averageDurationMs"`
}

type cacheMetricSnapshot struct {
	Hits     int64              `json:"hits"`
	Misses   int64              `json:"misses"`
	Bypasses int64              `json:"bypasses"`
	Requests int64              `json:"requests"`
	HitRate  float64            `json:"hitRate"`
	Series   []cacheMetricPoint `json:"series"`
}

type cacheMetricPoint struct {
	Time     string  `json:"time"`
	Hits     int64   `json:"hits"`
	Misses   int64   `json:"misses"`
	Bypasses int64   `json:"bypasses"`
	HitRate  float64 `json:"hitRate"`
}

type transactionMetricSnapshot struct {
	Kind                          string                   `json:"kind"`
	Path                          string                   `json:"path"`
	ServerEvents                  int64                    `json:"serverEvents"`
	BrowserEvents                 int64                    `json:"browserEvents"`
	Errors                        int64                    `json:"errors"`
	AverageServerDurationMS       float64                  `json:"averageServerDurationMs"`
	AverageServerCommitMS         float64                  `json:"averageServerCommitMs"`
	AverageClientToCommitMS       float64                  `json:"averageClientToCommitMs"`
	AverageClientRoundTripMS      float64                  `json:"averageClientRoundTripMs"`
	AverageServerToBrowserMS      float64                  `json:"averageServerToBrowserMs"`
	AverageChangeToBrowserMS      float64                  `json:"averageChangeToBrowserMs"`
	AverageSubscriptionDurationMS float64                  `json:"averageSubscriptionDurationMs"`
	LastEventAt                   string                   `json:"lastEventAt,omitempty"`
	Series                        []transactionMetricPoint `json:"series"`
}

type transactionMetricPoint struct {
	Time                     string  `json:"time"`
	ServerEvents             int64   `json:"serverEvents"`
	BrowserEvents            int64   `json:"browserEvents"`
	Errors                   int64   `json:"errors"`
	AverageServerDurationMS  float64 `json:"averageServerDurationMs"`
	AverageServerCommitMS    float64 `json:"averageServerCommitMs"`
	AverageClientRoundTripMS float64 `json:"averageClientRoundTripMs"`
	AverageServerToBrowserMS float64 `json:"averageServerToBrowserMs"`
	AverageChangeToBrowserMS float64 `json:"averageChangeToBrowserMs"`
}

// Propagation measures the delay the product actually cares about: a user
// commits a change (e.g. clicks "change status") and every other subscribed
// user's GUI must reflect it. The end-to-end leg prefers ChangeToAckMS
// (commit → the receiving client's telemetry ack arriving back at the
// server), which uses ONLY the server clock: it is objective under client
// clock skew and a strict upper bound on the real user-visible delay (it adds
// one upstream network hop). Its per-bucket max is "when did the LAST user
// see it". ChangeToBrowserMS (browser-clock receive time) is the fallback for
// entries without an ack measurement. The server leg (commit → result sent)
// isolates backend fan-out delay from network/browser time. Only kind=query,
// reason=invalidate events count: initial loads and the mutator's own round
// trip are not propagation. Caveat: an ack delayed by reconnect buffering
// inflates its sample; p95 is robust to those outliers, max is not.
type propagationMetricsBucket struct {
	BrowserSamples int64
	BrowserTotalMS float64
	BrowserMaxMS   float64
	BrowserValues  []float64
	ServerSamples  int64
	ServerTotalMS  float64
	ServerMaxMS    float64
}

type propagationTotals struct {
	Samples int64
	TotalMS float64
	MaxMS   float64
}

type propagationMetricSnapshot struct {
	TargetMS  float64                  `json:"targetMs"`
	Samples   int64                    `json:"samples"`
	AverageMS float64                  `json:"averageMs"`
	MaxMS     float64                  `json:"maxMs"`
	Series    []propagationMetricPoint `json:"series"`
}

type propagationMetricPoint struct {
	Time            string  `json:"time"`
	Samples         int64   `json:"samples"`
	AverageMS       float64 `json:"averageMs"`
	P95MS           float64 `json:"p95Ms"`
	MaxMS           float64 `json:"maxMs"`
	ServerSamples   int64   `json:"serverSamples"`
	ServerAverageMS float64 `json:"serverAverageMs"`
	ServerMaxMS     float64 `json:"serverMaxMs"`
}

func (m *runtimeMetrics) recordPropagationLocked(entry transactionTelemetryEntry, now time.Time) {
	if entry.Kind != "query" || entry.Reason != "invalidate" {
		return
	}
	browserMS := float64(0)
	if entry.Phase == "browser" {
		if entry.ChangeToAckMS > 0 {
			browserMS = entry.ChangeToAckMS
		} else if entry.ChangeToBrowserMS > 0 {
			browserMS = entry.ChangeToBrowserMS
		}
	}
	serverMS := float64(0)
	if entry.Phase != "browser" && entry.ServerSentAtMS > 0 && entry.ChangeCommittedAtMS > 0 && entry.ServerSentAtMS > entry.ChangeCommittedAtMS {
		serverMS = entry.ServerSentAtMS - entry.ChangeCommittedAtMS
	}
	if browserMS == 0 && serverMS == 0 {
		return
	}
	count := entry.LogicalCount
	if count < 1 {
		count = 1
	}
	if m.propagationBuckets == nil {
		m.propagationBuckets = map[int64]*propagationMetricsBucket{}
	}
	key := bucketKey(now)
	bucket := m.propagationBuckets[key]
	if bucket == nil {
		bucket = &propagationMetricsBucket{}
		m.propagationBuckets[key] = bucket
	}
	if browserMS > 0 {
		bucket.BrowserSamples += count
		bucket.BrowserTotalMS += browserMS * float64(count)
		if browserMS > bucket.BrowserMaxMS {
			bucket.BrowserMaxMS = browserMS
		}
		if len(bucket.BrowserValues) < propagationBucketSampleLimit {
			bucket.BrowserValues = append(bucket.BrowserValues, browserMS)
		}
		m.propagationTotals.Samples += count
		m.propagationTotals.TotalMS += browserMS * float64(count)
		if browserMS > m.propagationTotals.MaxMS {
			m.propagationTotals.MaxMS = browserMS
		}
	}
	if serverMS > 0 {
		bucket.ServerSamples += count
		bucket.ServerTotalMS += serverMS * float64(count)
		if serverMS > bucket.ServerMaxMS {
			bucket.ServerMaxMS = serverMS
		}
	}
	oldest := bucketKey(now.Add(-metricsBucketWidth * metricsBucketCount))
	for existing := range m.propagationBuckets {
		if existing < oldest {
			delete(m.propagationBuckets, existing)
		}
	}
}

func (m *runtimeMetrics) propagationSnapshotLocked(now time.Time) propagationMetricSnapshot {
	points := make([]propagationMetricPoint, 0, metricsBucketCount)
	start := now.Truncate(metricsBucketWidth).Add(-metricsBucketWidth * (metricsBucketCount - 1))
	for index := 0; index < metricsBucketCount; index++ {
		bucketStart := start.Add(metricsBucketWidth * time.Duration(index))
		point := propagationMetricPoint{Time: bucketStart.Format(time.RFC3339Nano)}
		if bucket := m.propagationBuckets[bucketStart.UnixMilli()]; bucket != nil {
			point.Samples = bucket.BrowserSamples
			point.MaxMS = bucket.BrowserMaxMS
			if bucket.BrowserSamples > 0 {
				point.AverageMS = bucket.BrowserTotalMS / float64(bucket.BrowserSamples)
			}
			point.P95MS = percentile(bucket.BrowserValues, 0.95)
			point.ServerSamples = bucket.ServerSamples
			point.ServerMaxMS = bucket.ServerMaxMS
			if bucket.ServerSamples > 0 {
				point.ServerAverageMS = bucket.ServerTotalMS / float64(bucket.ServerSamples)
			}
		}
		points = append(points, point)
	}
	snapshot := propagationMetricSnapshot{
		TargetMS: propagationTargetMS,
		Samples:  m.propagationTotals.Samples,
		MaxMS:    m.propagationTotals.MaxMS,
		Series:   points,
	}
	if m.propagationTotals.Samples > 0 {
		snapshot.AverageMS = m.propagationTotals.TotalMS / float64(m.propagationTotals.Samples)
	}
	return snapshot
}

func percentile(values []float64, fraction float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	// Round the rank up: for latency percentiles the conservative (higher)
	// neighbor is the honest answer when the rank falls between samples.
	index := int(math.Ceil(float64(len(sorted)-1) * fraction))
	return sorted[index]
}

// loadMetricPoint is one background sample of connected-client load next to
// process resources, so the dashboard can chart "how does server load move
// with the number of connected users". Samples are process-global (all
// projects), like the running/cache series.
type loadMetricPoint struct {
	Time          string  `json:"time"`
	Connections   int     `json:"connections"`
	Users         int     `json:"users"`
	Subscriptions int     `json:"subscriptions"`
	CPUPercent    float64 `json:"cpuPercent"`
	MemoryBytes   uint64  `json:"memoryBytes"`
}

type loadMetricSnapshot struct {
	SampleIntervalSeconds int               `json:"sampleIntervalSeconds"`
	Series                []loadMetricPoint `json:"series"`
}

func (m *runtimeMetrics) recordLoad(point loadMetricPoint) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.load = append(m.load, point)
	if len(m.load) > metricsLoadPointLimit {
		m.load = m.load[len(m.load)-metricsLoadPointLimit:]
	}
}

func (m *runtimeMetrics) loadSnapshotLocked() loadMetricSnapshot {
	series := make([]loadMetricPoint, len(m.load))
	copy(series, m.load)
	return loadMetricSnapshot{
		SampleIntervalSeconds: int(loadSampleInterval / time.Second),
		Series:                series,
	}
}

type websocketMetricSnapshot struct {
	Connections      int                           `json:"connections"`
	Subscriptions    int                           `json:"subscriptions"`
	Users            int                           `json:"users"`
	BytesReceived    uint64                        `json:"bytesReceived"`
	BytesSent        uint64                        `json:"bytesSent"`
	Details          []websocketConnectionSnapshot `json:"details"`
	DetailsTruncated bool                          `json:"detailsTruncated,omitempty"`
}

func newRuntimeMetrics(telemetryPath ...string) *runtimeMetrics {
	path := ""
	if len(telemetryPath) > 0 {
		path = telemetryPath[0]
	}
	hardenExistingTelemetryPath(path)
	return &runtimeMetrics{
		functions:        map[string]*functionMetrics{},
		transactions:     map[string]*transactionMetrics{},
		cache:            cacheMetrics{Buckets: map[int64]*cacheMetricsBucket{}},
		runningByKind:    map[string]int64{},
		runningBuckets:   map[int64]map[string]int64{},
		database:         map[string]*databaseMetricState{},
		commitExecutions: map[string]uint64{},
		telemetryPath:    path,
		logSubscribers:   map[int]logSubscriber{},
	}
}

// normalizeRunningKind collapses the runtime's function kinds into the three
// concurrency lanes the dashboard charts (queries, mutations, actions).
func normalizeRunningKind(kind string) string {
	switch kind {
	case "mutation", "internalMutation":
		return "mutation"
	case "action":
		return "action"
	default:
		return "query"
	}
}

// recordFunctionStart marks a function as in-flight so the dashboard can chart
// live concurrency. Every call must be paired with recordFunctionEnd.
func (m *runtimeMetrics) recordFunctionStart(kind string) {
	if m == nil {
		return
	}
	lane := normalizeRunningKind(kind)
	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runningByKind[lane]++
	m.observeRunningLocked(now)
}

func (m *runtimeMetrics) recordFunctionEnd(kind string) {
	if m == nil {
		return
	}
	lane := normalizeRunningKind(kind)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.runningByKind[lane] > 0 {
		m.runningByKind[lane]--
	}
}

func (m *runtimeMetrics) observeRunningLocked(now time.Time) {
	key := bucketKey(now)
	bucket := m.runningBuckets[key]
	if bucket == nil {
		bucket = map[string]int64{}
		m.runningBuckets[key] = bucket
	}
	for lane, count := range m.runningByKind {
		if count > bucket[lane] {
			bucket[lane] = count
		}
	}
	oldest := bucketKey(now.Add(-metricsBucketWidth * metricsBucketCount))
	for existing := range m.runningBuckets {
		if existing < oldest {
			delete(m.runningBuckets, existing)
		}
	}
}

func sanitizeRuntimeLogRequest(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return json.RawMessage(`{"unavailable":"request was not valid JSON"}`)
	}
	redactRuntimeLogValue(value)
	encoded, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"unavailable":"request could not be captured"}`)
	}
	const maxRequestBytes = 16 * 1024
	if len(encoded) > maxRequestBytes {
		encoded, _ = json.Marshal(map[string]any{
			"sizeBytes": len(raw),
			"truncated": true,
		})
	}
	return encoded
}

func redactRuntimeLogValue(value any) {
	switch current := value.(type) {
	case map[string]any:
		for key, child := range current {
			if runtimeLogKeyIsSensitive(key) {
				current[key] = "[REDACTED]"
				continue
			}
			redactRuntimeLogValue(child)
		}
	case []any:
		for _, child := range current {
			redactRuntimeLogValue(child)
		}
	}
}

func runtimeLogKeyIsSensitive(key string) bool {
	normalized := strings.NewReplacer("_", "", "-", "", ".", "").Replace(strings.ToLower(key))
	for _, marker := range []string{"password", "passwd", "token", "secret", "authorization", "cookie", "credential", "apikey", "privatekey"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func newRuntimeFunctionLog(project string, tenant string, path string, kind string, caller callerContext, rawArgs json.RawMessage) runtimeFunctionLog {
	started := time.Now().UTC()
	entry := runtimeLogEntry{
		ExecutionID:      uuid.NewString(),
		StartedAt:        started.Format(time.RFC3339Nano),
		Project:          project,
		Tenant:           tenant,
		Path:             path,
		Kind:             kind,
		Request:          sanitizeRuntimeLogRequest(rawArgs),
		RequestSizeBytes: len(rawArgs),
	}
	if caller.user != nil {
		entry.UserID = caller.user.ID
		entry.UserEmail = caller.user.Email
	}
	return runtimeFunctionLog{entry: entry, started: started}
}

func (m *runtimeMetrics) recordFunctionExecution(execution runtimeFunctionLog, err error) {
	if m == nil || execution.entry.Path == "" {
		return
	}
	completed := time.Now().UTC()
	execution.entry.Time = completed.Format(time.RFC3339Nano)
	execution.entry.CompletedAt = execution.entry.Time
	execution.entry.DurationMS = float64(completed.Sub(execution.started).Microseconds()) / 1000
	if err != nil {
		execution.entry.Outcome = "error"
		execution.entry.Error = err.Error()
	} else {
		execution.entry.Outcome = "ok"
	}
	m.recordRuntimeLog(execution.entry, completed)
}

func runtimeLogSourceForCache(cache string) string {
	if cache == "hit" {
		return "redis"
	}
	if cache != "" {
		return "database"
	}
	return ""
}

func (m *runtimeMetrics) recordFunction(project string, path string, kind string, duration time.Duration, err error) {
	if m == nil || path == "" {
		return
	}
	m.recordRuntimeOperation(project, path, kind, duration, err, "")
}

func (m *runtimeMetrics) recordRuntimeOperation(project string, path string, kind string, duration time.Duration, err error, cache string) {
	if m == nil || path == "" {
		return
	}
	now := time.Now().UTC()
	durationMS := float64(duration.Microseconds()) / 1000
	outcome := "ok"
	errorMessage := ""
	if err != nil {
		outcome = "error"
		errorMessage = err.Error()
	}

	m.recordRuntimeLog(runtimeLogEntry{
		Time:        now.Format(time.RFC3339Nano),
		ExecutionID: uuid.NewString(),
		StartedAt:   now.Add(-duration).Format(time.RFC3339Nano),
		CompletedAt: now.Format(time.RFC3339Nano),
		Project:     project,
		Path:        path,
		Kind:        kind,
		Outcome:     outcome,
		DurationMS:  durationMS,
		Error:       errorMessage,
		Cache:       cache,
		Source:      runtimeLogSourceForCache(cache),
	}, now)
}

func (m *runtimeMetrics) recordRuntimeLog(log runtimeLogEntry, now time.Time) {
	m.mu.Lock()

	logKind := log.Kind
	if log.Kind != "runtime" {
		entry := m.functions[log.Path]
		if entry == nil {
			entry = &functionMetrics{Kind: log.Kind, Buckets: map[int64]*functionMetricsBucket{}}
			m.functions[log.Path] = entry
		}
		if log.Kind != "" {
			entry.Kind = log.Kind
		}
		logKind = entry.Kind
		entry.Calls++
		entry.TotalDurationMS += log.DurationMS
		entry.LastDurationMS = log.DurationMS
		entry.LastCalledAt = now
		if log.Outcome == "error" {
			entry.Errors++
		}

		bucket := entry.bucket(now)
		bucket.Calls++
		bucket.TotalDurationMS += log.DurationMS
		if log.Outcome == "error" {
			bucket.Errors++
		}
		entry.trimBuckets(now)
	}

	log.Kind = logKind
	m.appendLog(log)
	onFunctionError := m.onFunctionError
	m.mu.Unlock()

	m.persistMutationLog(log)
	if log.Outcome == "error" && onFunctionError != nil {
		onFunctionError(log)
	}
}

// recordOperationalLog publishes protocol and control-plane work without
// inflating function-call metrics. Failed entries still flow into durable logs
// and the grouped Errors inbox through the normal error callback.
func (m *runtimeMetrics) recordOperationalLog(log runtimeLogEntry, now time.Time) {
	m.mu.Lock()
	m.appendLog(log)
	onFunctionError := m.onFunctionError
	m.mu.Unlock()

	m.persistMutationLog(log)
	if log.Outcome == "error" && onFunctionError != nil {
		onFunctionError(log)
	}
}

func (m *runtimeMetrics) recordCache(project string, outcome string) {
	if m == nil {
		return
	}
	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()

	bucket := m.cache.bucket(now)
	switch outcome {
	case "hit":
		m.cache.Hits++
		bucket.Hits++
	case "miss":
		m.cache.Misses++
		bucket.Misses++
	default:
		m.cache.Bypasses++
		bucket.Bypasses++
		outcome = "bypass"
	}
	m.cache.trimBuckets(now)
}

func (m *runtimeMetrics) recordDatabase(project string, stats databasePoolStats) {
	if m == nil {
		return
	}
	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.database[project]
	if state == nil {
		state = &databaseMetricState{}
		m.database[project] = state
	}
	waitCount := stats.WaitCount - state.PreviousWaits
	if waitCount < 0 {
		waitCount = 0
	}
	waitDuration := stats.WaitDuration - state.PreviousWaitTime
	if waitDuration < 0 {
		waitDuration = 0
	}
	budget := dbpool.RuntimeBudgetStats()
	budgetWaitCount := budget.WaitCount - state.PreviousBudgetWaits
	budgetWaitDuration := budget.WaitDuration - state.PreviousBudgetWaitTime
	state.Current = stats
	state.CurrentBudget = budget
	state.PreviousWaits = stats.WaitCount
	state.PreviousWaitTime = stats.WaitDuration
	state.PreviousBudgetWaits = budget.WaitCount
	state.PreviousBudgetWaitTime = budget.WaitDuration
	state.Series = append(state.Series, databaseMetricPoint{
		Time:                       now.Format(time.RFC3339Nano),
		OpenConnections:            stats.OpenConnections,
		InUse:                      stats.InUse,
		Idle:                       stats.Idle,
		WaitCount:                  waitCount,
		WaitDurationMS:             float64(waitDuration.Microseconds()) / 1000,
		GlobalBudgetActive:         budget.Active,
		GlobalBudgetWaiters:        budget.Waiters,
		GlobalBudgetWaitCount:      budgetWaitCount,
		GlobalBudgetWaitDurationMS: float64(budgetWaitDuration.Microseconds()) / 1000,
	})
	if len(state.Series) > metricsDatabasePointLimit {
		state.Series = state.Series[len(state.Series)-metricsDatabasePointLimit:]
	}
}

func (m *runtimeMetrics) recordTransaction(entry transactionTelemetryEntry) {
	m.recordTransactions([]transactionTelemetryEntry{entry})
}

func (m *runtimeMetrics) recordTransactions(entries []transactionTelemetryEntry) {
	if m == nil || len(entries) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var encoder *json.Encoder
	var file *os.File
	if m.telemetryPath != "" {
		if opened, err := openTelemetryFile(m.telemetryPath); err == nil {
			file = opened
			encoder = json.NewEncoder(file)
			defer file.Close()
		}
	}
	for _, entry := range entries {
		if entry.Path == "" || entry.Kind == "" {
			continue
		}
		if entry.Time == "" {
			entry.Time = time.Now().UTC().Format(time.RFC3339Nano)
		}
		if entry.Outcome == "" {
			entry.Outcome = "ok"
		}
		now, err := time.Parse(time.RFC3339Nano, entry.Time)
		if err != nil {
			now = time.Now().UTC()
			entry.Time = now.Format(time.RFC3339Nano)
		}
		m.recordTransactionLocked(entry, now)
		if encoder != nil {
			_ = encoder.Encode(entry)
		}
	}
}

func (m *runtimeMetrics) recordTransactionLocked(entry transactionTelemetryEntry, now time.Time) {
	count := entry.LogicalCount
	if count < 1 {
		count = 1
	}
	key := entry.Kind + ":" + entry.Path
	metrics := m.transactions[key]
	if metrics == nil {
		metrics = &transactionMetrics{Kind: entry.Kind, Path: entry.Path, Buckets: map[int64]*transactionMetricsBucket{}}
		m.transactions[key] = metrics
	}
	metrics.Kind = entry.Kind
	metrics.Path = entry.Path
	if entry.Outcome == "error" {
		metrics.Errors += count
	}
	metrics.LastEventAt = now
	bucket := metrics.bucket(now)
	if entry.Outcome == "error" {
		bucket.Errors += count
	}
	if entry.Phase == "browser" {
		metrics.BrowserEvents += count
		bucket.BrowserEvents += count
	} else {
		metrics.ServerEvents += count
		bucket.ServerEvents += count
	}
	if entry.ServerDurationMS > 0 {
		metrics.TotalServerDurationMS += entry.ServerDurationMS * float64(count)
		bucket.TotalServerDurationMS += entry.ServerDurationMS * float64(count)
	}
	if entry.ServerCommitMS > 0 {
		metrics.TotalServerCommitMS += entry.ServerCommitMS
		metrics.ServerCommitSamples++
		bucket.TotalServerCommitMS += entry.ServerCommitMS
		bucket.ServerCommitSamples++
	}
	if entry.ClientToCommitMS > 0 {
		metrics.TotalClientToCommitMS += entry.ClientToCommitMS
		metrics.ClientToCommitSamples++
	}
	if entry.ClientRoundTripMS > 0 {
		metrics.TotalClientRoundTripMS += entry.ClientRoundTripMS
		metrics.ClientRoundTripSamples++
		bucket.TotalClientRoundTripMS += entry.ClientRoundTripMS
		bucket.ClientRoundTripSamples++
	}
	if entry.ServerToBrowserMS > 0 {
		metrics.TotalServerToBrowserMS += entry.ServerToBrowserMS
		metrics.ServerToBrowserSamples++
		bucket.TotalServerToBrowserMS += entry.ServerToBrowserMS
		bucket.ServerToBrowserSamples++
	}
	if entry.ChangeToBrowserMS > 0 {
		metrics.TotalChangeToBrowserMS += entry.ChangeToBrowserMS
		metrics.ChangeToBrowserSamples++
		bucket.TotalChangeToBrowserMS += entry.ChangeToBrowserMS
		bucket.ChangeToBrowserSamples++
	}
	if entry.SubscriptionDurationMS > 0 {
		metrics.TotalSubscriptionDurationMS += entry.SubscriptionDurationMS * float64(count)
		metrics.SubscriptionDurationSamples += count
	}
	metrics.trimBuckets(now)
	m.recordPropagationLocked(entry, now)
	m.appendTelemetryLog(entry)
}

func (m *runtimeMetrics) snapshot(current manifest.Manifest, connections int, subscriptions int, projectFilter string) runtimeMetricsSnapshot {
	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()

	functions := map[string]functionMetricSnapshot{}
	for path, entry := range current.Functions {
		metrics := m.functions[path]
		if metrics == nil {
			metrics = &functionMetrics{Kind: string(entry.Kind), Buckets: map[int64]*functionMetricsBucket{}}
		}
		functions[path] = metrics.snapshot(now)
	}
	for path, metrics := range m.functions {
		if _, ok := functions[path]; !ok {
			functions[path] = metrics.snapshot(now)
		}
	}

	logs := make([]runtimeLogEntry, 0, len(m.logs))
	for _, entry := range m.logs {
		if projectFilter != "" && entry.Project != projectFilter {
			continue
		}
		logs = append(logs, entry)
	}
	sort.Slice(logs, func(left, right int) bool {
		return logs[left].Time > logs[right].Time
	})

	transactions := map[string]transactionMetricSnapshot{}
	for key, metrics := range m.transactions {
		transactions[key] = metrics.snapshot(now)
	}
	telemetryLogs := make([]transactionTelemetryEntry, len(m.telemetryLogs))
	copy(telemetryLogs, m.telemetryLogs)
	sort.Slice(telemetryLogs, func(left, right int) bool {
		return telemetryLogs[left].Time > telemetryLogs[right].Time
	})

	result := runtimeMetricsSnapshot{
		GeneratedAt:  now.Format(time.RFC3339Nano),
		Functions:    functions,
		Transactions: transactions,
		Cache:        m.cache.snapshot(now),
		Running:      m.runningSnapshot(now),
		WebSocket: websocketMetricSnapshot{
			Connections:   connections,
			Subscriptions: subscriptions,
		},
		Database:      m.databaseSnapshot(projectFilter),
		Load:          m.loadSnapshotLocked(),
		Propagation:   m.propagationSnapshotLocked(now),
		Reactive:      m.reactive.snapshot(),
		Logs:          logs,
		TelemetryLogs: telemetryLogs,
	}
	if m.admissionSource != nil {
		admission := m.admissionSource()
		result.QueryAdmission = queryAdmissionMetricSnapshot{
			Enabled:                    admission.TotalPermits > 0,
			TotalPermits:               admission.TotalPermits,
			BootstrapPermits:           admission.BootstrapPermits,
			Active:                     admission.Active,
			BootstrapActive:            admission.BootstrapActive,
			Reactive:                   admissionClassSnapshot(admission.Classes[admissionReactive]),
			Foreground:                 admissionClassSnapshot(admission.Classes[admissionForeground]),
			Bootstrap:                  admissionClassSnapshot(admission.Classes[admissionBootstrap]),
			ReactiveDelayedByBootstrap: admission.Classes[admissionReactive].DelayedByBurst,
		}
		if admission.TotalPermits > 0 {
			// The pre-admission rerun limiter reported these two fields; keep
			// them alive for existing dashboards and load-test tooling.
			result.Reactive.SubscriptionRerunQueueDepth = admission.Classes[admissionReactive].QueueDepth
			result.Reactive.SubscriptionRerunQueueWaitMS = admission.Classes[admissionReactive].WaitMS
		}
	}
	return result
}

func (m *runtimeMetrics) databaseSnapshot(project string) databaseMetricSnapshot {
	state := m.database[project]
	if state == nil {
		return databaseMetricSnapshot{Series: []databaseMetricPoint{}}
	}
	series := make([]databaseMetricPoint, len(state.Series))
	copy(series, state.Series)
	return databaseMetricSnapshot{
		Pools:                      state.Current.Pools,
		OpenConnections:            state.Current.OpenConnections,
		InUse:                      state.Current.InUse,
		Idle:                       state.Current.Idle,
		MaxOpenConnections:         state.Current.MaxOpenConnections,
		WaitCount:                  state.Current.WaitCount,
		WaitDurationMS:             float64(state.Current.WaitDuration.Microseconds()) / 1000,
		GlobalBudgetLimit:          state.CurrentBudget.Limit,
		GlobalBudgetActive:         state.CurrentBudget.Active,
		GlobalBudgetWaiters:        state.CurrentBudget.Waiters,
		GlobalBudgetWaitCount:      state.CurrentBudget.WaitCount,
		GlobalBudgetWaitDurationMS: float64(state.CurrentBudget.WaitDuration.Microseconds()) / 1000,
		Series:                     series,
	}
}

func (m *runtimeMetrics) runningSnapshot(now time.Time) runningMetricSnapshot {
	current := make(map[string]int64, len(m.runningByKind))
	var total int64
	for lane, count := range m.runningByKind {
		current[lane] = count
		total += count
	}

	points := make([]runningMetricPoint, 0, metricsBucketCount)
	start := now.Truncate(metricsBucketWidth).Add(-metricsBucketWidth * (metricsBucketCount - 1))
	for index := 0; index < metricsBucketCount; index++ {
		bucketStart := start.Add(metricsBucketWidth * time.Duration(index))
		bucket := m.runningBuckets[bucketStart.UnixMilli()]
		point := runningMetricPoint{Time: bucketStart.Format(time.RFC3339Nano)}
		if bucket != nil {
			point.Query = bucket["query"]
			point.Mutation = bucket["mutation"]
			point.Action = bucket["action"]
		}
		points = append(points, point)
	}
	return runningMetricSnapshot{Current: current, Total: total, Series: points}
}

func (m *runtimeMetrics) appendLog(entry runtimeLogEntry) {
	m.logs = append(m.logs, entry)
	if len(m.logs) > metricsLogLimit {
		m.logs = m.logs[len(m.logs)-metricsLogLimit:]
	}
	for _, subscriber := range m.logSubscribers {
		if subscriber.project != "" && subscriber.project != entry.Project {
			continue
		}
		select {
		case subscriber.ch <- entry:
		default:
		}
	}
}

func (m *runtimeMetrics) subscribeLogs(project string) (int, <-chan runtimeLogEntry, []runtimeLogEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextLogSubID++
	id := m.nextLogSubID
	ch := make(chan runtimeLogEntry, 64)
	m.logSubscribers[id] = logSubscriber{project: project, ch: ch}
	recent := make([]runtimeLogEntry, 0, len(m.logs))
	for _, entry := range m.logs {
		if project == "" || entry.Project == project {
			recent = append(recent, entry)
		}
	}
	return id, ch, recent
}

func (m *runtimeMetrics) unsubscribeLogs(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.logSubscribers, id)
}

// clearLogs drops all retained runtime log entries and reports how many were
// removed. Other metrics (function counters, transactions, telemetry) are left
// untouched.
func (m *runtimeMetrics) clearLogs(projectFilter string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if projectFilter == "" {
		cleared := len(m.logs)
		m.logs = nil
		return cleared
	}
	kept := m.logs[:0]
	cleared := 0
	for _, entry := range m.logs {
		if entry.Project == projectFilter {
			cleared++
			continue
		}
		kept = append(kept, entry)
	}
	m.logs = kept
	return cleared
}

func (m *runtimeMetrics) appendTelemetryLog(entry transactionTelemetryEntry) {
	m.telemetryLogs = append(m.telemetryLogs, entry)
	if len(m.telemetryLogs) > metricsTelemetryLogLimit {
		m.telemetryLogs = m.telemetryLogs[len(m.telemetryLogs)-metricsTelemetryLogLimit:]
	}
}

func hardenExistingTelemetryPath(path string) {
	if path == "" {
		return
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if info, err := os.Stat(dir); err == nil && info.IsDir() {
			_ = os.Chmod(dir, 0o700)
		}
	}
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		_ = os.Chmod(path, 0o600)
	}
}

func openTelemetryFile(path string) (*os.File, error) {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			return nil, err
		}
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func (m *functionMetrics) bucket(now time.Time) *functionMetricsBucket {
	key := bucketKey(now)
	bucket := m.Buckets[key]
	if bucket == nil {
		bucket = &functionMetricsBucket{}
		m.Buckets[key] = bucket
	}
	return bucket
}

func (m *functionMetrics) trimBuckets(now time.Time) {
	oldest := bucketKey(now.Add(-metricsBucketWidth * metricsBucketCount))
	for key := range m.Buckets {
		if key < oldest {
			delete(m.Buckets, key)
		}
	}
}

func (m *functionMetrics) snapshot(now time.Time) functionMetricSnapshot {
	averageDurationMS := 0.0
	if m.Calls > 0 {
		averageDurationMS = m.TotalDurationMS / float64(m.Calls)
	}
	lastCalledAt := ""
	if !m.LastCalledAt.IsZero() {
		lastCalledAt = m.LastCalledAt.Format(time.RFC3339Nano)
	}
	return functionMetricSnapshot{
		Kind:              m.Kind,
		Calls:             m.Calls,
		Errors:            m.Errors,
		TotalDurationMS:   m.TotalDurationMS,
		AverageDurationMS: averageDurationMS,
		LastDurationMS:    m.LastDurationMS,
		LastCalledAt:      lastCalledAt,
		Series:            m.series(now),
	}
}

func (m *functionMetrics) series(now time.Time) []functionMetricPoint {
	points := make([]functionMetricPoint, 0, metricsBucketCount)
	start := now.Truncate(metricsBucketWidth).Add(-metricsBucketWidth * (metricsBucketCount - 1))
	for index := 0; index < metricsBucketCount; index++ {
		bucketStart := start.Add(metricsBucketWidth * time.Duration(index))
		bucket := m.Buckets[bucketStart.UnixMilli()]
		point := functionMetricPoint{Time: bucketStart.Format(time.RFC3339Nano)}
		if bucket != nil {
			point.Calls = bucket.Calls
			point.Errors = bucket.Errors
			if bucket.Calls > 0 {
				point.AverageDurationMS = bucket.TotalDurationMS / float64(bucket.Calls)
			}
		}
		points = append(points, point)
	}
	return points
}

func (m *transactionMetrics) bucket(now time.Time) *transactionMetricsBucket {
	key := bucketKey(now)
	bucket := m.Buckets[key]
	if bucket == nil {
		bucket = &transactionMetricsBucket{}
		m.Buckets[key] = bucket
	}
	return bucket
}

func (m *transactionMetrics) trimBuckets(now time.Time) {
	oldest := bucketKey(now.Add(-metricsBucketWidth * metricsBucketCount))
	for key := range m.Buckets {
		if key < oldest {
			delete(m.Buckets, key)
		}
	}
}

func (m *transactionMetrics) snapshot(now time.Time) transactionMetricSnapshot {
	lastEventAt := ""
	if !m.LastEventAt.IsZero() {
		lastEventAt = m.LastEventAt.Format(time.RFC3339Nano)
	}
	return transactionMetricSnapshot{
		Kind:                          m.Kind,
		Path:                          m.Path,
		ServerEvents:                  m.ServerEvents,
		BrowserEvents:                 m.BrowserEvents,
		Errors:                        m.Errors,
		AverageServerDurationMS:       divide(m.TotalServerDurationMS, m.ServerEvents),
		AverageServerCommitMS:         divide(m.TotalServerCommitMS, m.ServerCommitSamples),
		AverageClientToCommitMS:       divide(m.TotalClientToCommitMS, m.ClientToCommitSamples),
		AverageClientRoundTripMS:      divide(m.TotalClientRoundTripMS, m.ClientRoundTripSamples),
		AverageServerToBrowserMS:      divide(m.TotalServerToBrowserMS, m.ServerToBrowserSamples),
		AverageChangeToBrowserMS:      divide(m.TotalChangeToBrowserMS, m.ChangeToBrowserSamples),
		AverageSubscriptionDurationMS: divide(m.TotalSubscriptionDurationMS, m.SubscriptionDurationSamples),
		LastEventAt:                   lastEventAt,
		Series:                        m.series(now),
	}
}

func (m *transactionMetrics) series(now time.Time) []transactionMetricPoint {
	points := make([]transactionMetricPoint, 0, metricsBucketCount)
	start := now.Truncate(metricsBucketWidth).Add(-metricsBucketWidth * (metricsBucketCount - 1))
	for index := 0; index < metricsBucketCount; index++ {
		bucketStart := start.Add(metricsBucketWidth * time.Duration(index))
		bucket := m.Buckets[bucketStart.UnixMilli()]
		point := transactionMetricPoint{Time: bucketStart.Format(time.RFC3339Nano)}
		if bucket != nil {
			point.ServerEvents = bucket.ServerEvents
			point.BrowserEvents = bucket.BrowserEvents
			point.Errors = bucket.Errors
			point.AverageServerDurationMS = divide(bucket.TotalServerDurationMS, bucket.ServerEvents)
			point.AverageServerCommitMS = divide(bucket.TotalServerCommitMS, bucket.ServerCommitSamples)
			point.AverageClientRoundTripMS = divide(bucket.TotalClientRoundTripMS, bucket.ClientRoundTripSamples)
			point.AverageServerToBrowserMS = divide(bucket.TotalServerToBrowserMS, bucket.ServerToBrowserSamples)
			point.AverageChangeToBrowserMS = divide(bucket.TotalChangeToBrowserMS, bucket.ChangeToBrowserSamples)
		}
		points = append(points, point)
	}
	return points
}

func (m *cacheMetrics) bucket(now time.Time) *cacheMetricsBucket {
	key := bucketKey(now)
	bucket := m.Buckets[key]
	if bucket == nil {
		bucket = &cacheMetricsBucket{}
		m.Buckets[key] = bucket
	}
	return bucket
}

func (m *cacheMetrics) trimBuckets(now time.Time) {
	oldest := bucketKey(now.Add(-metricsBucketWidth * metricsBucketCount))
	for key := range m.Buckets {
		if key < oldest {
			delete(m.Buckets, key)
		}
	}
}

func (m *cacheMetrics) snapshot(now time.Time) cacheMetricSnapshot {
	requests := m.Hits + m.Misses
	return cacheMetricSnapshot{
		Hits:     m.Hits,
		Misses:   m.Misses,
		Bypasses: m.Bypasses,
		Requests: requests,
		HitRate:  cacheHitRate(m.Hits, m.Misses),
		Series:   m.series(now),
	}
}

func (m *cacheMetrics) series(now time.Time) []cacheMetricPoint {
	points := make([]cacheMetricPoint, 0, metricsBucketCount)
	start := now.Truncate(metricsBucketWidth).Add(-metricsBucketWidth * (metricsBucketCount - 1))
	for index := 0; index < metricsBucketCount; index++ {
		bucketStart := start.Add(metricsBucketWidth * time.Duration(index))
		bucket := m.Buckets[bucketStart.UnixMilli()]
		point := cacheMetricPoint{Time: bucketStart.Format(time.RFC3339Nano)}
		if bucket != nil {
			point.Hits = bucket.Hits
			point.Misses = bucket.Misses
			point.Bypasses = bucket.Bypasses
			point.HitRate = cacheHitRate(bucket.Hits, bucket.Misses)
		}
		points = append(points, point)
	}
	return points
}

func divide(total float64, count int64) float64 {
	if count == 0 {
		return 0
	}
	return total / float64(count)
}

func cacheHitRate(hits int64, misses int64) float64 {
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

func bucketKey(now time.Time) int64 {
	return now.UTC().Truncate(metricsBucketWidth).UnixMilli()
}
