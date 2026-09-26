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
