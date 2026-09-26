package server

import (
	"net/http"
	"strings"
)

const dashboardWebSocketAuthProtocolPrefix = "gonvex-dashboard-auth."

func (s *Server) withDashboardProjectAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.needsDashboardProjectAuth(r) {
			next.ServeHTTP(w, r)
			return
		}
		// The dev CLI (`gonvex dev`) polls GET /dev/manifest after each sync to
		// confirm the runtime still holds its manifest, authenticating with the
		// project sync key — the same credential POST /dev/sync accepts — not a
		// dashboard session. Honor that key here for the read-only manifest
		// check. Without this the poll 401s, runtimeHasManifest reads any
		// non-200 as "state missing", and the watch loop resyncs every couple of
		// seconds forever.
		if r.Method == http.MethodGet && r.URL.Path == "/dev/manifest" {
			if project := projectID(r); project != "" && s.acceptsSyncKey(project, syncKey(r), r) {
				next.ServeHTTP(w, r)
				return
			}
		}
		r = requestWithDashboardWebSocketCredential(r)
		// The runtime admin key is the machine credential used by trusted
		// automation such as the release monitor. It is intentionally global,
		// so it must not be constrained by dashboard project membership.
		if s.acceptsAdminKey(syncKey(r)) {
			next.ServeHTTP(w, r)
			return
		}
		actor, ok := s.dashboardActorFromRequest(r)
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard sign-in is required"})
			return
		}
		project := projectID(r)
		if project == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
			return
		}
		if !s.canAccessProject(r.Context(), actor, project) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "project access is required"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) needsDashboardProjectAuth(r *http.Request) bool {
	if s.dashboardAuthOptional() {
		return false
	}
	path := r.URL.Path
	if !strings.HasPrefix(path, "/dev/") {
		return false
	}
	switch {
	case strings.HasPrefix(path, "/dev/auth/"):
		return false
	case path == "/dev/projects" || strings.HasPrefix(path, "/dev/projects/"):
		return false
	case path == "/dev/sync":
		return false
	case path == "/dev/logs/stream":
		return false
	default:
		return true
	}
}

// Browsers cannot set an Authorization header during the WebSocket handshake.
// Carry dashboard credentials in a dedicated Sec-WebSocket-Protocol value so
// they do not appear in URLs, reverse-proxy access logs, or browser history.
// The cloned request is only used by authentication middleware; the original
// WebSocket protocol list remains available to the upgrader.
func requestWithDashboardWebSocketCredential(r *http.Request) *http.Request {
	if r.URL.Path != "/dev/metrics/stream" || bearerToken(r) != "" {
		return r
	}
	for _, offered := range strings.Split(r.Header.Get("Sec-WebSocket-Protocol"), ",") {
		protocol := strings.TrimSpace(offered)
		if !strings.HasPrefix(protocol, dashboardWebSocketAuthProtocolPrefix) {
			continue
		}
		token := strings.TrimPrefix(protocol, dashboardWebSocketAuthProtocolPrefix)
		if token == "" {
			continue
		}
		clone := r.Clone(r.Context())
		clone.Header = r.Header.Clone()
		clone.Header.Set("Authorization", "Bearer "+token)
		return clone
	}
	return r
}
