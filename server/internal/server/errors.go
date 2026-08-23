package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

type capturedError struct {
	EventID     string            `json:"eventId"`
	Timestamp   string            `json:"timestamp"`
	Level       string            `json:"level"`
	Message     string            `json:"message"`
	Name        string            `json:"name,omitempty"`
	Stack       string            `json:"stack,omitempty"`
	Culprit     string            `json:"culprit,omitempty"`
	Project     string            `json:"project"`
	Tenant      string            `json:"tenant,omitempty"`
	Release     string            `json:"release,omitempty"`
	Environment string            `json:"environment,omitempty"`
	Account     map[string]any    `json:"account,omitempty"`
	DeviceID    string            `json:"deviceId,omitempty"`
	SessionID   string            `json:"sessionId,omitempty"`
	URL         string            `json:"url,omitempty"`
	UserAgent   string            `json:"userAgent,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Context     map[string]any    `json:"context,omitempty"`
	Breadcrumbs []map[string]any  `json:"breadcrumbs,omitempty"`
}

type errorGroup struct {
	Fingerprint  string         `json:"fingerprint"`
	Project      string         `json:"project"`
	Title        string         `json:"title"`
	Level        string         `json:"level"`
	Culprit      string         `json:"culprit,omitempty"`
	Status       string         `json:"status"`
	Priority     string         `json:"priority"`
	Assignee     string         `json:"assignee,omitempty"`
	FirstSeen    string         `json:"firstSeen"`
	LastSeen     string         `json:"lastSeen"`
	Count        int            `json:"count"`
	Tenants      map[string]int `json:"tenants"`
	Releases     map[string]int `json:"releases"`
	Environments map[string]int `json:"environments"`
	Accounts     map[string]int `json:"accounts"`
	Devices      map[string]int `json:"devices"`
	Regression   bool           `json:"regression"`
	Latest       capturedError  `json:"latest"`
}

type errorGroupUpdate struct {
	Status      string
	Priority    string
	Assignee    string
	AssigneeSet bool
}

type errorTracker struct {
	mu                sync.RWMutex
	groups            map[string]*errorGroup
	eventIDs          map[string]struct{}
	eventLog          []capturedError
	maxEvents, events int
	rateWindows       map[string]errorRateWindow
}

type errorRateWindow struct {
	started time.Time
	count   int
}

func newErrorTracker(max int) *errorTracker {
	return &errorTracker{groups: map[string]*errorGroup{}, eventIDs: map[string]struct{}{}, maxEvents: max, rateWindows: map[string]errorRateWindow{}}
}

func (t *errorTracker) capture(event capturedError) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.eventIDs[event.EventID]; event.EventID != "" && exists {
		return fingerprint(event), false
	}
	if t.events >= t.maxEvents {
		return "", false
	}
	if event.EventID != "" {
		t.eventIDs[event.EventID] = struct{}{}
	}
	fp := fingerprint(event)
	now := event.Timestamp
	if now == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
		event.Timestamp = now
	}
	group := t.groups[fp]
	when := eventTime(now)
	event.Timestamp = when.Format(time.RFC3339Nano)
	if group == nil {
		group = newErrorGroup(event, fp, when)
		t.groups[fp] = group
	} else {
		applyErrorToGroup(group, event, when)
	}
	t.eventLog = append(t.eventLog, event)
	t.events++
	return fp, true
}

func (t *errorTracker) listGroups(project, status, release, level string) ([]*errorGroup, []string) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	releases := orderedErrorReleases(t.eventLog, project)
	if release == "" {
		groups := make([]*errorGroup, 0, len(t.groups))
		for _, group := range t.groups {
			if project != "" && group.Project != project {
				continue
			}
			if status != "" && group.Status != status {
				continue
			}
			if level != "" && groupLevel(group) != level {
				continue
			}
			clone := *group
			groups = append(groups, &clone)
		}
		sort.Slice(groups, func(i, j int) bool { return groups[i].LastSeen > groups[j].LastSeen })
		return groups, releases
	}

	eventsByFingerprint := map[string][]capturedError{}
	for _, event := range t.eventLog {
		if event.Project == project && event.Release == release {
			fp := fingerprint(event)
			base := t.groups[fp]
			matches := base != nil &&
				(status == "" || base.Status == status) &&
				(level == "" || groupLevel(base) == level)
			if matches {
				eventsByFingerprint[fp] = append(eventsByFingerprint[fp], event)
			}
		}
	}
	groups := make([]*errorGroup, 0, len(eventsByFingerprint))
	for fp, events := range eventsByFingerprint {
		groups = append(groups, errorGroupForEvents(t.groups[fp], events))
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].LastSeen > groups[j].LastSeen })
	return groups, releases
}

func orderedErrorReleases(events []capturedError, project string) []string {
	latest := map[string]time.Time{}
	for _, event := range events {
		if event.Project != project || event.Release == "" {
			continue
		}
		when := eventTime(event.Timestamp)
		if previous, exists := latest[event.Release]; !exists || when.After(previous) {
			latest[event.Release] = when
		}
	}
	releases := make([]string, 0, len(latest))
	for release := range latest {
		releases = append(releases, release)
	}
	sort.Slice(releases, func(i, j int) bool {
		if latest[releases[i]].Equal(latest[releases[j]]) {
			return releases[i] > releases[j]
		}
		return latest[releases[i]].After(latest[releases[j]])
	})
	return releases
}

func errorGroupForEvents(base *errorGroup, events []capturedError) *errorGroup {
	sort.Slice(events, func(i, j int) bool {
		return eventTime(events[i].Timestamp).Before(eventTime(events[j].Timestamp))
	})
	group := newErrorGroup(events[0], base.Fingerprint, eventTime(events[0].Timestamp))
	for _, event := range events[1:] {
		applyErrorToGroup(group, event, eventTime(event.Timestamp))
	}
	group.Status = base.Status
	group.Priority = base.Priority
	group.Assignee = base.Assignee
	group.Regression = base.Regression
	// Rebuilt from stored payloads, which carry no level before this feature.
	// The persisted group knows better than the replayed events do.
	group.Level = base.Level
	return group
}

func newErrorGroup(event capturedError, fp string, when time.Time) *errorGroup {
	group := &errorGroup{Fingerprint: fp, Project: event.Project, Title: event.Message, Level: normalizeErrorLevel(event.Level), Culprit: event.Culprit, Status: "unresolved", Priority: "medium", FirstSeen: when.Format(time.RFC3339Nano), Tenants: map[string]int{}, Releases: map[string]int{}, Environments: map[string]int{}, Accounts: map[string]int{}, Devices: map[string]int{}}
	applyErrorToGroup(group, event, when)
	return group
}

func applyErrorToGroup(group *errorGroup, event capturedError, when time.Time) {
	previousRelease := group.Latest.Release
	if group.Status == "resolved" && event.Release != "" && previousRelease != "" && event.Release != previousRelease {
		group.Status = "unresolved"
		group.Regression = true
	}
	group.Count++
	group.LastSeen = when.Format(time.RFC3339Nano)
	group.Latest = event
	// Groups are fingerprinted per level, so this only fills in groups that
	// were persisted before levels existed.
	if group.Level == "" {
		group.Level = normalizeErrorLevel(event.Level)
	}
	if event.Tenant != "" {
		group.Tenants[event.Tenant]++
	}
	if event.Release != "" {
		group.Releases[event.Release]++
	}
	if event.Environment != "" {
		group.Environments[event.Environment]++
	}
	if event.DeviceID != "" {
		group.Devices[event.DeviceID]++
	}
	if id := errorAccountID(event); id != "" {
		group.Accounts[id]++
	}
	if group.Count >= 500 || len(group.Tenants) >= 25 {
		group.Priority = "critical"
	} else if group.Count >= 100 || len(group.Tenants) >= 10 {
		group.Priority = "high"
	}
}

// Levels are a closed set so the dashboard can rely on them for filtering and
// colour. Anything unrecognized — including events from clients that predate
// levels — counts as an error, which is the safer default to surface.
const (
	errorLevelError   = "error"
	errorLevelWarning = "warning"
)

// groupLevel reads a group's level tolerantly: rows persisted before levels
// existed have none, and every one of those was an error.
func groupLevel(group *errorGroup) string {
	if group == nil {
		return errorLevelError
	}
	return normalizeErrorLevel(group.Level)
}

// requestedErrorLevel maps a ?level= query value to a filter. Unlike
// normalizeErrorLevel, an empty or unrecognized value means "no filter" — an
// older dashboard build sends no level and must keep seeing everything.
func requestedErrorLevel(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case errorLevelWarning, "warn":
		return errorLevelWarning
	case errorLevelError:
		return errorLevelError
	default:
		return ""
	}
}

func normalizeErrorLevel(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case errorLevelWarning, "warn":
		return errorLevelWarning
	default:
		return errorLevelError
	}
}

func fingerprint(e capturedError) string {
	stack := e.Culprit
	if stack == "" {
		for _, line := range strings.Split(e.Stack, "\n") {
			if strings.Contains(line, "at ") && !strings.Contains(line, "node_modules") {
				stack = strings.TrimSpace(line)
				break
			}
		}
	}
	normalized := strings.ToLower(strings.TrimSpace(e.Name + "|" + normalizeErrorMessage(e.Message) + "|" + normalizeStackFrame(stack) + "|" + e.Project))
	// Level splits groups so a warning and an error sharing a message stay
	// separate signals. Only non-error levels extend the hash: every group
	// recorded before levels existed was an error, and rehashing those would
	// strand their triage state (status, assignee) on an unreachable
	// fingerprint and hide them from release-filtered views, which rebuild
	// fingerprints from stored payloads.
	if level := normalizeErrorLevel(e.Level); level != errorLevelError {
		normalized += "|" + level
	}
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:8])
}

func normalizeErrorMessage(value string) string {
	value = errorURLPattern.ReplaceAllString(value, "<url>")
	value = errorUUIDPattern.ReplaceAllString(value, "<id>")
	value = errorHexPattern.ReplaceAllString(value, "<id>")
	value = errorNumberPattern.ReplaceAllString(value, "<n>")
	return strings.Join(strings.Fields(value), " ")
}

var (
	errorURLPattern      = regexp.MustCompile(`https?://[^\s)]+`)
	errorUUIDPattern     = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f-]{27,}\b`)
	errorHexPattern      = regexp.MustCompile(`(?i)\b[0-9a-f]{12,}\b`)
	errorNumberPattern   = regexp.MustCompile(`\b\d{3,}\b`)
	stackLocationPattern = regexp.MustCompile(`:\d+(?::\d+)?\)?\s*$`)
	assetHashPattern     = regexp.MustCompile(`([._-])[A-Za-z0-9_-]{6,}(\.js\b)`)
)

func normalizeStackFrame(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Split(value, "?")[0]
	closing := ""
	if strings.HasSuffix(value, ")") {
		closing = ")"
	}
	value = stackLocationPattern.ReplaceAllString(value, closing)
	value = assetHashPattern.ReplaceAllString(value, `${1}<build>${2}`)
	if open := strings.Index(value, "("); open >= 0 {
		prefix, location := value[:open+1], value[open+1:]
		if parsed := strings.Index(location, "/assets/"); parsed >= 0 {
			location = location[parsed+1:]
		}
		value = prefix + location
	}
	return value
}

const filteredErrorValue = "[Filtered]"

var secretErrorKey = regexp.MustCompile(`(?i)password|passwd|secret|token|authorization|cookie|api[-_]?key`)
var sensitiveErrorTextPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+`),
	regexp.MustCompile(`(?i)((?:password|passwd|token|secret|api[-_]?key)\s*[=:]\s*)[^\s&,;]+`),
}
var jwtErrorTextPattern = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b`)

func sanitizeCapturedError(event capturedError) capturedError {
	event.EventID = truncateErrorString(event.EventID, 160)
	event.Timestamp = truncateErrorString(event.Timestamp, 64)
	event.Level = normalizeErrorLevel(event.Level)
	event.Message = scrubSensitiveErrorText(truncateErrorString(event.Message, 4000))
	event.Name = truncateErrorString(event.Name, 200)
	event.Stack = scrubSensitiveErrorText(truncateErrorString(event.Stack, 32000))
	event.Culprit = scrubSensitiveErrorText(truncateErrorString(event.Culprit, 2000))
	event.Project = truncateErrorString(event.Project, 200)
	event.Tenant = truncateErrorString(event.Tenant, 200)
	event.Release = truncateErrorString(event.Release, 200)
	event.Environment = truncateErrorString(event.Environment, 100)
	event.DeviceID = truncateErrorString(event.DeviceID, 200)
	event.SessionID = truncateErrorString(event.SessionID, 200)
	event.URL = truncateErrorString(strings.Split(strings.Split(event.URL, "?")[0], "#")[0], 2000)
	event.UserAgent = truncateErrorString(event.UserAgent, 2000)
	event.Account = sanitizeErrorMap(event.Account, 0)
	event.Context = sanitizeErrorMap(event.Context, 0)
	trimmedTags := map[string]string{}
	count := 0
	for key, value := range event.Tags {
		if count >= 50 {
			break
		}
		if secretErrorKey.MatchString(key) {
			value = filteredErrorValue
		}
		trimmedTags[truncateErrorString(key, 100)] = truncateErrorString(value, 500)
		count++
	}
	event.Tags = trimmedTags
	if len(event.Breadcrumbs) > 30 {
		event.Breadcrumbs = event.Breadcrumbs[len(event.Breadcrumbs)-30:]
	}
	for index := range event.Breadcrumbs {
		event.Breadcrumbs[index] = sanitizeErrorMap(event.Breadcrumbs[index], 0)
	}
	return event
}

func sanitizeErrorMap(value map[string]any, depth int) map[string]any {
	if value == nil {
		return nil
	}
	if depth > 5 {
		return map[string]any{"truncated": true}
	}
	result := map[string]any{}
	count := 0
	for key, raw := range value {
		if count >= 100 {
			break
		}
		key = truncateErrorString(key, 100)
		if secretErrorKey.MatchString(key) {
			result[key] = filteredErrorValue
		} else {
			result[key] = sanitizeErrorValue(raw, depth+1)
		}
		count++
	}
	return result
}

func sanitizeErrorValue(value any, depth int) any {
	switch typed := value.(type) {
	case string:
		return truncateErrorString(typed, 4000)
	case map[string]any:
		return sanitizeErrorMap(typed, depth)
	case []any:
		if len(typed) > 50 {
			typed = typed[:50]
		}
		result := make([]any, len(typed))
		for i := range typed {
			result[i] = sanitizeErrorValue(typed[i], depth+1)
		}
		return result
	case nil, bool, float64:
		return typed
	default:
		return truncateErrorString(fmt.Sprint(typed), 4000)
	}
}

func truncateErrorString(value string, max int) string {
	value = strings.TrimSpace(value)
	if len(value) <= max {
		return value
	}
	return value[:max]
}

func scrubSensitiveErrorText(value string) string {
	for _, pattern := range sensitiveErrorTextPatterns {
		value = pattern.ReplaceAllString(value, `${1}`+filteredErrorValue)
	}
	return jwtErrorTextPattern.ReplaceAllString(value, filteredErrorValue)
}
func generatedEventID(event capturedError) string {
	sum := sha256.Sum256([]byte(event.Timestamp + "|" + event.Message + "|" + event.Stack + "|" + event.DeviceID))
	return "generated-" + hex.EncodeToString(sum[:12])
}

func (t *errorTracker) allow(key string, now time.Time) bool {
	return t.allowLimit(key, now, 120)
}

func (t *errorTracker) allowLimit(key string, now time.Time, limit int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	window := t.rateWindows[key]
	if window.started.IsZero() || now.Sub(window.started) >= time.Minute {
		window = errorRateWindow{started: now}
	}
	if window.count >= limit {
		return false
	}
	window.count++
	t.rateWindows[key] = window
	return true
}

func (c *wsConn) handleNativeErrorTelemetry(ctx context.Context, message clientMessage) {
	project := strings.TrimSpace(c.project)
	if project == "" || !c.server.enableProjectErrorTracking(ctx, project) {
		c.write(serverMessage{Type: "error.ack", ID: message.ID, Error: "error telemetry is unavailable"})
		return
	}
	c.mu.Lock()
	accountID, tenantID, authenticated := "", "", c.auth && c.user != nil
	if c.user != nil {
		accountID = c.user.ID
	}
	if authenticated {
		tenantID = c.tenant
	}
	c.mu.Unlock()
	limit := 20
	if authenticated {
		limit = 120
	}
	rateSubject := accountID
	if !authenticated {
		rateSubject = c.remoteIP
		if strings.TrimSpace(rateSubject) == "" {
			rateSubject = c.id
		}
	}
	if !c.server.errorTracker.allowLimit("native:"+project+":"+rateSubject, time.Now(), limit) {
		c.write(serverMessage{Type: "error.ack", ID: message.ID, Error: "error telemetry rate limit exceeded"})
		return
	}
	if message.Type == "error.register" || message.Type == "error.heartbeat" {
		if authenticated {
			db, err := c.server.pooledProjectRegistry(ctx)
			if err == nil && db != nil {
				_, _ = db.ExecContext(ctx, `INSERT INTO gonvex_support_sessions(id,project_id,tenant_id,account_id,connection_id,release,environment)
					VALUES($1,$2,$3,$4,$1,$5,$6) ON CONFLICT(id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id,
					account_id=EXCLUDED.account_id,
					release=CASE WHEN EXCLUDED.release<>'' THEN EXCLUDED.release ELSE gonvex_support_sessions.release END,
					environment=CASE WHEN EXCLUDED.environment<>'' THEN EXCLUDED.environment ELSE gonvex_support_sessions.environment END,
					last_seen_at=now()`,
					c.id, project, tenantID, accountID, truncateErrorString(message.Release, 200), truncateErrorString(message.Environment, 100))
			}
		}
		c.write(serverMessage{Type: "error.ack", ID: message.ID})
		return
	}
	if len(message.Events) == 0 || len(message.Events) > 20 {
		c.write(serverMessage{Type: "error.ack", ID: message.ID, Error: "invalid error envelope"})
		return
	}
	total := 0
	for _, raw := range message.Events {
		total += len(raw)
	}
	if total > 256<<10 {
		c.write(serverMessage{Type: "error.ack", ID: message.ID, Error: "error envelope is too large"})
		return
	}
	accepted := 0
	fingerprints := []string{}
	for _, raw := range message.Events {
		var event capturedError
		if json.Unmarshal(raw, &event) != nil {
			continue
		}
		// Connection context is authoritative. Browser payload attribution is ignored.
		event.Project, event.Tenant, event.SessionID = project, tenantID, c.id
		if authenticated {
			event.Account = map[string]any{"id": accountID}
		} else {
			event.Account = nil
			event.Context = nil
			event.Breadcrumbs = nil
		}
		event = sanitizeCapturedError(event)
		if event.Message == "" {
			continue
		}
		if event.EventID == "" {
			event.EventID = generatedEventID(event)
		}
		persistCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		available, persisted, persistErr := c.server.persistError(persistCtx, event)
		cancel()
		if persistErr != nil {
			c.write(serverMessage{Type: "error.ack", ID: message.ID, Error: "error store unavailable"})
			return
		}
		fingerprintValue, ok := fingerprint(event), persisted
		if !available {
			fingerprintValue, ok = c.server.errorTracker.capture(event)
		} else if persisted {
			_, _ = c.server.errorTracker.capture(event)
		}
		if ok {
			accepted++
			fingerprints = append(fingerprints, fingerprintValue)
		}
	}
	c.write(serverMessage{Type: "error.ack", ID: message.ID, Accepted: accepted, Fingerprints: fingerprints})
}

func (s *Server) enableProjectErrorTracking(ctx context.Context, projectID string) bool {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return false
	}
	s.hydrateProjects()
	s.projectMu.Lock()
	project, ok := s.projects[projectID]
	changed := ok && !project.ErrorTrackingEnabled
	if changed {
		project.ErrorTrackingEnabled = true
		s.projects[projectID] = project
	}
	s.projectMu.Unlock()
	if changed {
		// The in-memory flag is enough for minimal local runtimes. Persist it when
		// a project registry is available so dashboard navigation survives restarts.
		_ = s.persistProjectErrorTrackingEnabled(ctx, projectID)
	}
	return ok
}

func (s *Server) projectErrorTrackingEnabled(projectID string) bool {
	s.hydrateProjects()
	s.projectMu.RLock()
	defer s.projectMu.RUnlock()
	return s.projects[strings.TrimSpace(projectID)].ErrorTrackingEnabled
}

func (s *Server) handleErrorRegistration(w http.ResponseWriter, r *http.Request) {
	project := projectID(r)
	if project == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "x-gonvex-project-id is required"})
		return
	}
	if !s.enableProjectErrorTracking(r.Context(), project) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "project not found"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"enabled": true, "project": project})
}

func (s *Server) handleErrorStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled": s.projectErrorTrackingEnabled(projectID(r)),
		"project": projectID(r),
	})
}

func (s *Server) handleErrorEnvelope(w http.ResponseWriter, r *http.Request) {
	project := projectID(r)
	if project == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "x-gonvex-project-id is required"})
		return
	}
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	if host == "" {
		host = r.RemoteAddr
	}
	if !s.errorTracker.allow(project+":"+host, time.Now()) {
		w.Header().Set("retry-after", "60")
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "error ingestion rate limit exceeded"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 512<<10)
	var envelope struct {
		Events []capturedError `json:"events"`
	}
	if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil || len(envelope.Events) == 0 || len(envelope.Events) > 20 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid error envelope"})
		return
	}
	s.enableProjectErrorTracking(r.Context(), project)
	accepted := 0
	fingerprints := []string{}
	for _, event := range envelope.Events {
		event.Project = project
		if scopedTenant := tenantID(r); scopedTenant != "" {
			event.Tenant = scopedTenant
		}
		event = sanitizeCapturedError(event)
		if event.Message == "" {
			continue
		}
		if event.EventID == "" {
			event.EventID = generatedEventID(event)
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		available, persisted, persistErr := s.persistError(ctx, event)
		cancel()
		if persistErr != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "error store unavailable"})
			return
		}
		fp, ok := fingerprint(event), persisted
		if !available {
			fp, ok = s.errorTracker.capture(event)
		} else if persisted {
			_, _ = s.errorTracker.capture(event)
		}
		if ok {
			accepted++
			fingerprints = append(fingerprints, fp)
		}
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": accepted, "fingerprints": fingerprints})
}

func (s *Server) handleErrorGroups(w http.ResponseWriter, r *http.Request) {
	project, status, release := projectID(r), r.URL.Query().Get("status"), r.URL.Query().Get("release")
	level := requestedErrorLevel(r.URL.Query().Get("level"))
	if groups, releases, available, err := s.persistentErrorGroups(r.Context(), project, status, release, level); err != nil {
		writeJSON(w, 503, map[string]any{"error": "error store unavailable"})
		return
	} else if available {
		writeJSON(w, 200, map[string]any{"groups": groups, "releases": releases})
		return
	}
	groups, releases := s.errorTracker.listGroups(project, status, release, level)
	writeJSON(w, http.StatusOK, map[string]any{"groups": groups, "releases": releases})
}

func (s *Server) handleErrorGroup(w http.ResponseWriter, r *http.Request) {
	if group, available, err := s.persistentErrorGroup(r.Context(), projectID(r), r.PathValue("fingerprint")); err != nil {
		writeJSON(w, 503, map[string]any{"error": "error store unavailable"})
		return
	} else if available {
		if group == nil {
			writeJSON(w, 404, map[string]any{"error": "error group not found"})
		} else {
			writeJSON(w, 200, group)
		}
		return
	}
	s.errorTracker.mu.RLock()
	defer s.errorTracker.mu.RUnlock()
	group := s.errorTracker.groups[r.PathValue("fingerprint")]
	if group == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "error group not found"})
		return
	}
	writeJSON(w, http.StatusOK, group)
}

func (s *Server) handleUpdateErrorGroup(w http.ResponseWriter, r *http.Request) {
	var update struct {
		Status   string  `json:"status"`
		Priority string  `json:"priority"`
		Assignee *string `json:"assignee"`
	}
	if json.NewDecoder(r.Body).Decode(&update) != nil {
		writeJSON(w, 400, map[string]any{"error": "invalid update"})
		return
	}
	groupUpdate := errorGroupUpdate{Status: update.Status, Priority: update.Priority, AssigneeSet: update.Assignee != nil}
	if update.Assignee != nil {
		groupUpdate.Assignee = *update.Assignee
	}
	if err := validateErrorGroupUpdate(groupUpdate); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if group, available, err := s.updatePersistentErrorGroup(r.Context(), projectID(r), r.PathValue("fingerprint"), groupUpdate); err != nil {
		writeJSON(w, 503, map[string]any{"error": "error store unavailable"})
		return
	} else if available {
		if group == nil {
			writeJSON(w, 404, map[string]any{"error": "error group not found"})
		} else {
			writeJSON(w, 200, group)
		}
		return
	}
	s.errorTracker.mu.Lock()
	defer s.errorTracker.mu.Unlock()
	group := s.errorTracker.groups[r.PathValue("fingerprint")]
	if group == nil {
		writeJSON(w, 404, map[string]any{"error": "error group not found"})
		return
	}
	if update.Status != "" {
		group.Status = update.Status
	}
	if update.Priority != "" {
		group.Priority = update.Priority
	}
	if update.Assignee != nil {
		group.Assignee = *update.Assignee
	}
	writeJSON(w, 200, group)
}

func (s *Server) handleErrorBugReport(w http.ResponseWriter, r *http.Request) {
	if group, available, err := s.persistentErrorGroup(r.Context(), projectID(r), r.PathValue("fingerprint")); err != nil {
		writeJSON(w, 503, map[string]any{"error": "error store unavailable"})
		return
	} else if available {
		if group == nil {
			writeJSON(w, 404, map[string]any{"error": "error group not found"})
		} else {
			writeErrorBugReport(w, group)
		}
		return
	}
	s.errorTracker.mu.RLock()
	defer s.errorTracker.mu.RUnlock()
	group := s.errorTracker.groups[r.PathValue("fingerprint")]
	if group == nil {
		writeJSON(w, 404, map[string]any{"error": "error group not found"})
		return
	}
	writeErrorBugReport(w, group)
}

func writeErrorBugReport(w http.ResponseWriter, group *errorGroup) {
	writeJSON(w, 200, map[string]any{"title": group.Title, "markdown": bugReport(group), "agentContext": map[string]any{"fingerprint": group.Fingerprint, "project": group.Project, "tenantImpact": group.Tenants, "accountImpact": group.Accounts, "deviceImpact": group.Devices, "release": group.Latest.Release, "culprit": group.Culprit, "stack": group.Latest.Stack, "breadcrumbs": group.Latest.Breadcrumbs, "context": group.Latest.Context}})
}

func bugReport(g *errorGroup) string {
	return fmt.Sprintf("## %s\n\n**Fingerprint:** `%s`\n**Impact:** %d events, %d tenants, %d accounts, %d devices\n**First/last seen:** %s / %s\n**Release:** %s\n**Likely source:** `%s`\n\n### Error\n```\n%s\n```\n\n### Acceptance criteria\n- Reproduce or verify the failing path.\n- Add a regression test that fails before the fix.\n- Fix the root cause without suppressing unrelated errors.\n- Verify the fix against the affected release and tenant context.\n", g.Title, g.Fingerprint, g.Count, len(g.Tenants), len(g.Accounts), len(g.Devices), g.FirstSeen, g.LastSeen, g.Latest.Release, g.Culprit, g.Latest.Stack)
}
func stringValue(v any) string { value, _ := v.(string); return value }
