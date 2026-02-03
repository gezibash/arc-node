package relay

import (
	"context"
	"log/slog"
	"time"
)

const (
	ReapInterval = 10 * time.Second
	StallTimeout = 30 * time.Second // disconnect if buffer full and no progress
	IdleTimeout  = 5 * time.Minute  // disconnect if no activity
)

// Reaper periodically checks for stalled or idle subscribers and disconnects them.
type Reaper struct {
	table      *Table
	interval   time.Duration
	stallTime  time.Duration
	idleTime   time.Duration
	disconnect func(id string) // callback to disconnect subscriber
}

// NewReaper creates a reaper with default timeouts.
func NewReaper(table *Table, disconnect func(id string)) *Reaper {
	return &Reaper{
		table:      table,
		interval:   ReapInterval,
		stallTime:  StallTimeout,
		idleTime:   IdleTimeout,
		disconnect: disconnect,
	}
}

// Run starts the reaper loop. Blocks until context is canceled.
func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reap()
		}
	}
}

func (r *Reaper) reap() {
	now := time.Now()

	for _, s := range r.table.All() {
		if s.IsClosed() {
			continue
		}

		// Check for stalled subscriber: buffer full AND no progress for stallTime
		if s.BufferFull() {
			lastSend := s.LastSend()
			if now.Sub(lastSend) > r.stallTime {
				slog.Warn("reaper: disconnecting stalled subscriber",
					"id", s.ID(),
					"name", s.Name(),
					"last_send", lastSend,
				)
				r.disconnect(s.ID())
				continue
			}
		}

		// Check for idle subscriber: no receive activity for idleTime
		lastRecv := s.LastRecv()
		if now.Sub(lastRecv) > r.idleTime {
			slog.Info("reaper: disconnecting idle subscriber",
				"id", s.ID(),
				"name", s.Name(),
				"last_recv", lastRecv,
			)
			r.disconnect(s.ID())
		}
	}
}
