package server

import (
	"net/http"
	"strings"
)

type runtimeLogStreamMessage struct {
	Type string          `json:"type"`
	Log  runtimeLogEntry `json:"log,omitempty"`
}

func (s *Server) handleLogStream(w http.ResponseWriter, r *http.Request) {
	project := projectID(r)
	if project == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	if !s.acceptsSyncKey(project, syncKeyFromRequest(r), r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid Gonvex sync key"})
		return
	}
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	id, logs, recent := s.metrics.subscribeLogs(project)
	defer s.metrics.unsubscribeLogs(id)
	_ = conn.WriteJSON(runtimeLogStreamMessage{Type: "ready"})
	if logStreamReplay(r) {
		for _, entry := range recent {
			if err := conn.WriteJSON(runtimeLogStreamMessage{Type: "log", Log: entry}); err != nil {
				return
			}
		}
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case entry := <-logs:
			if err := conn.WriteJSON(runtimeLogStreamMessage{Type: "log", Log: entry}); err != nil {
				return
			}
		}
	}
}

func logStreamReplay(r *http.Request) bool {
	value := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("replay")))
	switch value {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

func syncKeyFromRequest(r *http.Request) string {
	if value := syncKey(r); value != "" {
		return value
	}
	value := r.URL.Query().Get("key")
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return ""
}
