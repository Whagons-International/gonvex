package server

import (
	"context"
	"time"
)

// loadSampleInterval matches metricsBucketWidth so load points line up with
// the other dashboard series; retention is metricsLoadPointLimit samples.
const loadSampleInterval = 30 * time.Second

// startLoadSampler records connected-client load and process resources on a
// fixed cadence, independent of anyone polling /dev/metrics. Without it the
// websocket numbers are gauges that only exist at poll time, so load could
// never be correlated with connection count after the fact.
func (s *Server) startLoadSampler(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(loadSampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.sampleLoad(time.Now().UTC())
			}
		}
	}()
}

func (s *Server) sampleLoad(now time.Time) {
	connections, users, subscriptions := s.websocketCounts()
	cpuPercent := s.sampleCPUPercent(now)
	s.metrics.recordLoad(loadMetricPoint{
		Time:          now.Format(time.RFC3339Nano),
		Connections:   connections,
		Users:         users,
		Subscriptions: subscriptions,
		CPUPercent:    cpuPercent,
		MemoryBytes:   processResidentBytes(),
	})
	s.telegramAlerts.observeCPU(cpuPercent)
}
