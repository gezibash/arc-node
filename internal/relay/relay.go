package relay

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/logging"
)

var (
	ErrClosed  = errors.New("relay closed")
	ErrNoRoute = errors.New("no matching subscribers")
)

// Relay is the core message router.
// It is stateless - all state is in-memory and lost on restart.
type Relay struct {
	table  *Table
	router *Router
	reaper *Reaper
	signer identity.Signer

	closed     atomic.Bool
	wg         sync.WaitGroup
	cancelFunc context.CancelFunc
}

// Config holds relay configuration.
type Config struct {
	BufferSize int             // per-subscriber buffer size
	Signer     identity.Signer // relay's signing key
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		BufferSize: DefaultBufferSize,
	}
}

// New creates a new relay with the given config.
// Returns an error if no keypair is provided.
func New(cfg Config) (*Relay, error) {
	if cfg.Signer == nil {
		return nil, errors.New("signer is required")
	}

	table := NewTable()
	router := NewRouter(table)

	r := &Relay{
		table:  table,
		router: router,
		signer: cfg.Signer,
	}

	r.reaper = NewReaper(table, r.disconnect)
	return r, nil
}

// Signer returns the relay's signing key.
func (r *Relay) Signer() identity.Signer {
	return r.signer
}

// Start begins background tasks (reaper). Call once.
func (r *Relay) Start(ctx context.Context) {
	ctx, r.cancelFunc = context.WithCancel(ctx)

	pk := r.signer.PublicKey()
	slog.Info("relay started", "component", "relay", "pubkey", logging.FormatPubkey(pk))

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.reaper.Run(ctx)
	}()
}

// Close shuts down the relay and all subscribers.
func (r *Relay) Close() error {
	if r.closed.Swap(true) {
		return nil // already closed
	}

	slog.Info("relay closing",
		"component", "relay",
		"subscriber_count", r.table.Count(),
	)

	if r.cancelFunc != nil {
		r.cancelFunc()
	}

	// Close all subscribers
	for _, s := range r.table.All() {
		s.Close()
	}

	r.wg.Wait()
	return nil
}

// Route processes an envelope: route and deliver.
// Returns the number of subscribers that accepted the envelope.
func (r *Relay) Route(env *relayv1.Envelope) (int, error) {
	if r.closed.Load() {
		return 0, ErrClosed
	}

	// Compute ref if not set
	ref := env.GetRef()
	if len(ref) == 0 {
		ref = computeRef(env)
		env.Ref = ref
	}

	// Sign the envelope with relay's keypair
	SignEnvelope(env, r.signer)

	// Route
	subs, _ := r.router.Route(env)
	if len(subs) == 0 {
		return 0, ErrNoRoute
	}

	// Deliver (non-blocking)
	delivered := 0
	for _, s := range subs {
		frame := WrapDeliver(env, "")
		if s.Send(frame) {
			delivered++
		}
	}

	return delivered, nil
}

// Table returns the subscription table for registration.
func (r *Relay) Table() *Table {
	return r.table
}

// disconnect closes and removes a subscriber.
func (r *Relay) disconnect(id string) {
	if s, ok := r.table.Get(id); ok {
		s.Close()
		r.table.Remove(id)
	}
}

// computeRef computes SHA-256 of labels + payload.
func computeRef(env *relayv1.Envelope) []byte {
	h := sha256.New()
	// Hash labels in sorted order for determinism
	for k, v := range env.GetLabels() {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(v))
		h.Write([]byte{0})
	}
	h.Write(env.GetPayload())
	return h.Sum(nil)
}
