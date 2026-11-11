package server

import (
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// defaultWSDrainWindow spreads restart closes across enough time that the
// resulting reconnects arrive as a trickle, while staying safely inside the
// supervisor's 20-second drain deadline.
const defaultWSDrainWindow = 12 * time.Second

func wsDrainWindowFromEnv() time.Duration {
	value := os.Getenv("GONVEX_WS_DRAIN_WINDOW")
	if value == "" {
		return defaultWSDrainWindow
	}
	window, err := time.ParseDuration(value)
	if err != nil || window <= 0 {
		return defaultWSDrainWindow
	}
	return window
}

// DrainWebSockets closes every live WebSocket connection spread across the
// window using close code 1012 (service restart), so clients reconnect to the
// replacement worker staggered instead of as one synchronized wave. Idle
// connections close first; a connection executing a mutation or action is
// given until the end of the window to finish before its close. The call
// blocks until every connection has been closed.
func (s *Server) DrainWebSockets(window time.Duration) {
	if window <= 0 {
		window = wsDrainWindowFromEnv()
	}
	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for connection := range s.wsConns {
		connections = append(connections, connection)
	}
	s.wsMu.RUnlock()
	if len(connections) == 0 {
		return
	}
	type drainCandidate struct {
		connection *wsConn
		busy       bool
		lastActive time.Time
	}
	candidates := make([]drainCandidate, 0, len(connections))
	for _, connection := range connections {
		connection.mu.Lock()
		lastActive := connection.lastActiveAt
		connection.mu.Unlock()
		candidates = append(candidates, drainCandidate{
			connection: connection,
			busy:       connection.writesInFlight.Load() > 0,
			lastActive: lastActive,
		})
	}
	sort.SliceStable(candidates, func(left, right int) bool {
		if candidates[left].busy != candidates[right].busy {
			return !candidates[left].busy
		}
		return candidates[left].lastActive.Before(candidates[right].lastActive)
	})
	slog.Info("draining WebSocket connections for worker restart", "connections", len(candidates), "window", window)
	step := window / time.Duration(len(candidates))
	deadline := time.Now().Add(window)
	var group sync.WaitGroup
	for index, candidate := range candidates {
		group.Add(1)
		go func(delay time.Duration, connection *wsConn) {
			defer group.Done()
			time.Sleep(delay)
			for connection.writesInFlight.Load() > 0 && time.Now().Before(deadline) {
				time.Sleep(250 * time.Millisecond)
			}
			connection.closeForRestart()
		}(step*time.Duration(index), candidate.connection)
	}
	group.Wait()
}

// closeForRestart sends a 1012 close frame before dropping the socket, so
// well-behaved clients treat the disconnect as a routine service restart and
// reconnect with their existing backoff behavior.
func (c *wsConn) closeForRestart() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return
	}
	deadline := time.Now().Add(websocketWriteTimeout)
	_ = c.conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseServiceRestart, "worker restarting"),
		deadline,
	)
	_ = c.conn.Close()
	c.conn = nil
}

// WebSocketDrainWindow is the window a supervised worker spreads its restart
// closes across (GONVEX_WS_DRAIN_WINDOW, default 12s).
func WebSocketDrainWindow() time.Duration {
	return wsDrainWindowFromEnv()
}
