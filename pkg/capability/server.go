package capability

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/gezibash/arc/v2/internal/names"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/logging"
	"github.com/gezibash/arc/v2/pkg/runtime"
	"github.com/gezibash/arc/v2/pkg/transport"
	"github.com/google/uuid"
)

// ServerConfig configures a capability server.
type ServerConfig struct {
	// Name is the capability name (e.g., "blob", "index").
	// Used for subscription label: {"capability": Name}
	Name string

	// RegisterName is the @name to register with the relay for addressed routing.
	// If empty, defaults to a petname derived from the runtime's public key.
	RegisterName string

	// Labels are additional subscription labels beyond {"capability": Name}.
	// Can be nil for capability-only subscription.
	Labels map[string]string

	// Handler processes incoming envelopes.
	Handler Handler

	// Runtime provides access to transport and other services.
	// The transport capability must be configured on this runtime.
	Runtime *runtime.Runtime

	// Log is the logger instance. If nil, uses Runtime.Log().
	Log *logging.Logger

	// Concurrency is the number of concurrent handlers (default 10).
	Concurrency int
}

// Server manages the lifecycle of a capability: connecting to relay,
// subscribing, and dispatching incoming envelopes to the handler.
type Server struct {
	name         string
	registerName string
	labels       map[string]string
	handler      Handler
	tr           transport.Transport
	log          *logging.Logger
	concurrency  int

	subID  string
	closed atomic.Bool
}

// NewServer creates a new capability server.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("capability name required")
	}
	if cfg.Handler == nil {
		return nil, fmt.Errorf("handler required")
	}
	if cfg.Runtime == nil {
		return nil, fmt.Errorf("runtime required")
	}

	// Get transport from runtime
	tr := transport.From(cfg.Runtime)
	if tr == nil {
		return nil, fmt.Errorf("transport not configured on runtime")
	}

	log := cfg.Log
	if log == nil {
		log = cfg.Runtime.Log()
	}

	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = 10
	}

	// Build subscription labels: capability name + any additional labels
	labels := make(map[string]string)
	labels["capability"] = cfg.Name
	for k, v := range cfg.Labels {
		labels[k] = v
	}

	regName := cfg.RegisterName
	if regName == "" {
		regName = names.Petname(cfg.Runtime.PublicKey().Bytes)
	}

	return &Server{
		name:         cfg.Name,
		registerName: regName,
		labels:       labels,
		handler:      cfg.Handler,
		tr:           tr,
		log:          log,
		concurrency:  concurrency,
		subID:        uuid.New().String(),
	}, nil
}

// Run starts the server and blocks until ctx is cancelled.
// It subscribes to the relay and dispatches incoming envelopes to the handler.
func (s *Server) Run(ctx context.Context) error {
	// Subscribe with our capability label
	if err := s.tr.Subscribe(s.subID, s.labels); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	// Register @name for addressed routing
	if err := s.tr.RegisterName(s.registerName); err != nil {
		return fmt.Errorf("register name %q: %w", s.registerName, err)
	}

	s.log.Info("capability server started",
		"capability", s.name,
		"name", "@"+s.registerName,
		"labels", s.labels,
		"concurrency", s.concurrency,
	)

	// Create semaphore for concurrency control
	sem := make(chan struct{}, s.concurrency)

	for {
		delivery, err := s.tr.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			return fmt.Errorf("receive: %w", err)
		}

		// Acquire semaphore slot
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return nil
		}

		// Handle in goroutine
		go func(d *transport.Delivery) {
			defer func() { <-sem }() // release slot
			s.handleDelivery(ctx, d)
		}(delivery)
	}
}

// handleDelivery processes a single delivery.
func (s *Server) handleDelivery(ctx context.Context, d *transport.Delivery) {
	env := &Envelope{
		Ref:            d.Ref,
		Sender:         d.Sender,
		Labels:         d.Labels,
		Action:         d.Labels["action"],
		Payload:        d.Payload,
		SubscriptionID: d.SubscriptionID,
	}

	log := s.log.WithPubkeyBytes("sender", d.Sender.Bytes)
	if env.Action != "" {
		log = log.With(slog.String("action", env.Action))
	}
	log.Debug("handling envelope")

	// Call handler
	resp, err := s.handler.Handle(ctx, env)
	if err != nil {
		log.WithError(err).Error("handler error")
		return
	}

	// No response requested
	if resp == nil || resp.NoReply {
		return
	}

	// Send response back to sender
	if err := s.sendResponse(ctx, d.Sender, resp); err != nil {
		log.WithError(err).Error("send response failed")
	}
}

// sendResponse sends a response envelope back to the sender.
func (s *Server) sendResponse(ctx context.Context, to identity.PublicKey, resp *Response) error {
	labels := make(map[string]string)
	labels["to"] = identity.EncodePublicKey(to)

	// Add response labels
	for k, v := range resp.Labels {
		labels[k] = v
	}

	// Add error label if present
	if resp.Error != "" {
		labels["error"] = resp.Error
	}

	env := &transport.Envelope{
		Labels:  labels,
		Payload: resp.Payload,
	}

	_, err := s.tr.Send(ctx, env)
	return err
}

// Close stops the server.
func (s *Server) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	if s.tr != nil {
		return s.tr.Unsubscribe(s.subID)
	}
	return nil
}
