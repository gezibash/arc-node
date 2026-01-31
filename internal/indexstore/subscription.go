package indexstore

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/gezibash/arc/pkg/reference"

	celeval "github.com/gezibash/arc-node/internal/indexstore/cel"
	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

const (
	defaultBufferSize  = 100
	defaultIntakeSize  = 65536
	defaultWorkerCount = 4
	defaultMaxDrops    = 1000
)

// BackpressurePolicy controls behavior when a subscription's buffer is full.
type BackpressurePolicy int

const (
	// BackpressureDrop silently drops entries when the buffer is full.
	BackpressureDrop BackpressurePolicy = iota

	// BackpressureBlock blocks the dispatch goroutine for up to BlockTimeout.
	// If the timeout expires, the entry is dropped and counted.
	BackpressureBlock

	// BackpressureDisconnect disconnects the subscriber immediately on first drop.
	BackpressureDisconnect
)

// SubscriptionConfig configures the subscription manager.
type SubscriptionConfig struct {
	IntakeBufferSize    int // Size of the intake channel. Default 65536.
	WorkerCount         int // Number of fan-out goroutines. Default 4.
	MaxConsecutiveDrops int // Drops before disconnect. Default 1000. 0 = unlimited.
}

// SubscriptionOptions configures a single subscription.
type SubscriptionOptions struct {
	BufferSize         int                // Channel buffer size. Default 100.
	BackpressurePolicy BackpressurePolicy // Default BackpressureDrop.
	BlockTimeout       time.Duration      // For BackpressureBlock. Default 1s.
}

// SubscriptionHealth contains real-time health metrics for a subscription.
type SubscriptionHealth struct {
	ID               string
	TotalDelivered   int64
	TotalDropped     int64
	ConsecutiveDrops int64
	BufferUsed       int
	BufferCapacity   int
	Lagging          bool
}

// Subscription represents an active subscription to index entries.
type Subscription interface {
	ID() string
	Entries() <-chan *physical.Entry
	Cancel()
	Err() error
	Health() SubscriptionHealth
}

type subscription struct {
	id         string
	expression string
	entries    chan *physical.Entry
	cancel     context.CancelFunc
	err        error
	errMu      sync.RWMutex
	done       chan struct{}
	opts       SubscriptionOptions

	totalDelivered   atomic.Int64
	totalDropped     atomic.Int64
	consecutiveDrops atomic.Int64
}

func newSubscription(ctx context.Context, expression string, opts SubscriptionOptions) *subscription {
	if opts.BufferSize <= 0 {
		opts.BufferSize = defaultBufferSize
	}
	if opts.BlockTimeout <= 0 {
		opts.BlockTimeout = time.Second
	}

	ctx, cancel := context.WithCancel(ctx)
	s := &subscription{
		id:         uuid.NewString(),
		expression: expression,
		entries:    make(chan *physical.Entry, opts.BufferSize),
		cancel:     cancel,
		done:       make(chan struct{}),
		opts:       opts,
	}

	go func() {
		<-ctx.Done()
		close(s.entries)
		close(s.done)
	}()

	return s
}

func (s *subscription) ID() string                      { return s.id }
func (s *subscription) Entries() <-chan *physical.Entry { return s.entries }
func (s *subscription) Cancel()                         { s.cancel() }

func (s *subscription) Err() error {
	s.errMu.RLock()
	defer s.errMu.RUnlock()
	return s.err
}

func (s *subscription) Health() SubscriptionHealth {
	return SubscriptionHealth{
		ID:               s.id,
		TotalDelivered:   s.totalDelivered.Load(),
		TotalDropped:     s.totalDropped.Load(),
		ConsecutiveDrops: s.consecutiveDrops.Load(),
		BufferUsed:       len(s.entries),
		BufferCapacity:   cap(s.entries),
		Lagging:          len(s.entries) > cap(s.entries)*80/100,
	}
}

type subscriptionManager struct {
	mu     sync.RWMutex
	subs   map[string]*subscription
	eval   *celeval.Evaluator
	closed bool

	intake chan *physical.Entry
	wg     sync.WaitGroup
	stop   chan struct{}
	config SubscriptionConfig
}

func newSubscriptionManager(eval *celeval.Evaluator, cfg SubscriptionConfig) *subscriptionManager {
	if cfg.IntakeBufferSize <= 0 {
		cfg.IntakeBufferSize = defaultIntakeSize
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = defaultWorkerCount
	}
	if cfg.MaxConsecutiveDrops <= 0 {
		cfg.MaxConsecutiveDrops = defaultMaxDrops
	}

	m := &subscriptionManager{
		subs:   make(map[string]*subscription),
		eval:   eval,
		intake: make(chan *physical.Entry, cfg.IntakeBufferSize),
		stop:   make(chan struct{}),
		config: cfg,
	}

	for i := 0; i < cfg.WorkerCount; i++ {
		m.wg.Add(1)
		go m.fanoutWorker()
	}

	return m
}

func (m *subscriptionManager) fanoutWorker() {
	defer m.wg.Done()
	for {
		select {
		case entry, ok := <-m.intake:
			if !ok {
				return
			}
			m.dispatchEntry(entry)
		case <-m.stop:
			return
		}
	}
}

func (m *subscriptionManager) dispatchEntry(entry *physical.Entry) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, sub := range m.subs {
		match, err := m.eval.Match(context.Background(), sub.expression, entry)
		if err != nil {
			slog.Warn("subscription filter evaluation failed",
				"subscription_id", sub.id, "error", err)
			continue
		}
		if !match {
			continue
		}

		m.dispatchToSub(sub, entry)
	}
}

func (m *subscriptionManager) dispatchToSub(sub *subscription, entry *physical.Entry) {
	switch sub.opts.BackpressurePolicy {
	case BackpressureBlock:
		timer := time.NewTimer(sub.opts.BlockTimeout)
		select {
		case sub.entries <- entry:
			timer.Stop()
			sub.consecutiveDrops.Store(0)
			sub.totalDelivered.Add(1)
		case <-timer.C:
			sub.totalDropped.Add(1)
			drops := sub.consecutiveDrops.Add(1)
			slog.Warn("subscription buffer full after block timeout, entry dropped",
				"subscription_id", sub.id, "consecutive_drops", drops)
			if m.config.MaxConsecutiveDrops > 0 && drops >= int64(m.config.MaxConsecutiveDrops) {
				m.disconnectSub(sub, fmt.Sprintf("exceeded %d consecutive drops", m.config.MaxConsecutiveDrops))
			}
		}

	case BackpressureDisconnect:
		select {
		case sub.entries <- entry:
			sub.totalDelivered.Add(1)
		default:
			sub.totalDropped.Add(1)
			m.disconnectSub(sub, "buffer full")
		}

	default: // BackpressureDrop
		select {
		case sub.entries <- entry:
			sub.consecutiveDrops.Store(0)
			sub.totalDelivered.Add(1)
		default:
			sub.totalDropped.Add(1)
			drops := sub.consecutiveDrops.Add(1)
			if m.config.MaxConsecutiveDrops > 0 && drops >= int64(m.config.MaxConsecutiveDrops) {
				m.disconnectSub(sub, fmt.Sprintf("exceeded %d consecutive drops", m.config.MaxConsecutiveDrops))
			} else {
				slog.Warn("subscription buffer full, entry dropped",
					"subscription_id", sub.id,
					"ref", reference.Hex(entry.Ref),
					"consecutive_drops", drops)
			}
		}
	}
}

func (m *subscriptionManager) disconnectSub(sub *subscription, reason string) {
	slog.Warn("disconnecting slow subscriber",
		"subscription_id", sub.id, "reason", reason,
		"total_delivered", sub.totalDelivered.Load(),
		"total_dropped", sub.totalDropped.Load())
	sub.errMu.Lock()
	sub.err = fmt.Errorf("disconnected: %s", reason)
	sub.errMu.Unlock()
	sub.Cancel()
}

func (m *subscriptionManager) Subscribe(ctx context.Context, expression string, opts *SubscriptionOptions) (Subscription, error) {
	if err := m.eval.ValidateExpression(ctx, expression); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, ErrSubscriptionClosed
	}

	if opts == nil {
		opts = &SubscriptionOptions{}
	}

	sub := newSubscription(ctx, expression, *opts)
	m.subs[sub.id] = sub

	slog.Info("subscription registered",
		"subscription_id", sub.id,
		"active_subscriptions", len(m.subs),
	)

	go func() {
		<-sub.done
		m.mu.Lock()
		delete(m.subs, sub.id)
		remaining := len(m.subs)
		m.mu.Unlock()
		slog.Info("subscription removed",
			"subscription_id", sub.id,
			"remaining_subscriptions", remaining,
		)
	}()

	return sub, nil
}

// Notify enqueues an entry for async fan-out. Non-blocking: if the intake
// buffer is full the entry is dropped and a warning is logged.
func (m *subscriptionManager) Notify(_ context.Context, entry *physical.Entry) {
	select {
	case m.intake <- entry:
	default:
		slog.Warn("subscription intake buffer full, entry dropped",
			"ref", reference.Hex(entry.Ref))
	}
}

func (m *subscriptionManager) Close() {
	m.mu.Lock()
	m.closed = true
	for _, sub := range m.subs {
		sub.Cancel()
	}
	m.subs = nil
	m.mu.Unlock()

	close(m.stop)
	m.wg.Wait()
}

func (m *subscriptionManager) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.subs)
}

// Metrics returns aggregate subscription metrics.
func (m *subscriptionManager) Metrics() map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var totalDelivered, totalDropped, totalLagging int64
	for _, sub := range m.subs {
		totalDelivered += sub.totalDelivered.Load()
		totalDropped += sub.totalDropped.Load()
		if sub.Health().Lagging {
			totalLagging++
		}
	}

	return map[string]int64{
		"subscriptions_active":          int64(len(m.subs)),
		"subscriptions_total_delivered": totalDelivered,
		"subscriptions_total_dropped":   totalDropped,
		"subscriptions_lagging":         totalLagging,
	}
}
