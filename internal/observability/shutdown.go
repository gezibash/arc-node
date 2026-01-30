package observability

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// ShutdownCoordinator manages LIFO-ordered shutdown handlers.
type ShutdownCoordinator struct {
	mu       sync.Mutex
	handlers []namedHandler
}

type namedHandler struct {
	name string
	fn   func(context.Context) error
}

// Register adds a shutdown handler. Handlers run in LIFO order.
func (s *ShutdownCoordinator) Register(name string, fn func(context.Context) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers = append(s.handlers, namedHandler{name: name, fn: fn})
}

// Shutdown runs all registered handlers in reverse order.
func (s *ShutdownCoordinator) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	handlers := make([]namedHandler, len(s.handlers))
	copy(handlers, s.handlers)
	s.mu.Unlock()

	var errs []error
	for i := len(handlers) - 1; i >= 0; i-- {
		h := handlers[i]
		slog.Info("shutting down", "component", h.name)
		if err := h.fn(ctx); err != nil {
			slog.Error("shutdown error", "component", h.name, "error", err)
			errs = append(errs, fmt.Errorf("%s: %w", h.name, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("shutdown errors: %v", errs)
	}
	return nil
}
