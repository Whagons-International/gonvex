package server

import (
	"net/http"
	"time"
)

type runtimeMetricsStreamMessage struct {
	Type    string                 `json:"type"`
	Metrics runtimeMetricsSnapshot `json:"metrics,omitempty"`
}

func (s *Server) handleMetricsStream(w http.ResponseWriter, r *http.Request) {
	project := projectID(r)
	if project == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return
	}
	// This stream carries UserID, UserEmail, Tenant, Path and the raw function
	// arguments JSON for live traffic. It is excluded from the dashboard project
	// auth middleware (see needsDashboardProjectAuth) because a websocket cannot
	// carry the dashboard session, which means it must authenticate itself --
	// exactly as /dev/logs/stream does. It previously did not, making it the one
	// /dev/* route with no enforcement anywhere.
	if !s.acceptsSyncKey(project, syncKeyFromRequest(r), r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid Gonvex sync key"})
		return
	}
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	send := func() error {
		return conn.WriteJSON(runtimeMetricsStreamMessage{
			Type:    "metrics",
			Metrics: s.metricsSnapshot(r.Context(), project),
		})
	}
	if err := send(); err != nil {
		return
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if err := send(); err != nil {
				return
			}
		}
	}
}
