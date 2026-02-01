package indexstore

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"sort"

	"github.com/google/uuid"

	"github.com/gezibash/arc/v2/pkg/reference"

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
	callerKey  [32]byte
	entries    chan *physical.Entry
	cancel     context.CancelFunc
	err        error
	errMu      sync.RWMutex
	done       chan struct{}
	opts       SubscriptionOptions

	totalDelivered   atomic.Int64
	totalDropped     atomic.Int64
	consecutiveDrops atomic.Int64

	// FIFO ordering: tracks last delivered sequence per sender.
	fifoMu      sync.Mutex
	fifoLastSeq map[string]int64          // sender → last delivered sequence
	fifoHeld    map[string][]*physical.Entry // sender → held-back entries sorted by sequence
}

func newSubscription(ctx context.Context, expression string, callerKey [32]byte, opts SubscriptionOptions) *subscription {
	if opts.BufferSize <= 0 {
		opts.BufferSize = defaultBufferSize
	}
	if opts.BlockTimeout <= 0 {
		opts.BlockTimeout = time.Second
	}

	ctx, cancel := context.WithCancel(ctx)
	s := &subscription{
		id:          uuid.NewString(),
		expression:  expression,
		callerKey:   callerKey,
		entries:     make(chan *physical.Entry, opts.BufferSize),
		cancel:      cancel,
		done:        make(chan struct{}),
		opts:        opts,
		fifoLastSeq: make(map[string]int64),
		fifoHeld:    make(map[string][]*physical.Entry),
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

// VisibilityFilter decides whether an entry should be visible to a subscriber
// identified by callerKey. Return true to allow delivery.
type VisibilityFilter func(entry *physical.Entry, callerKey [32]byte) bool

type subscriptionManager struct {
	mu     sync.RWMutex
	subs   map[string]*subscription
	eval   *celeval.Evaluator
	closed bool

	intake           chan *physical.Entry
	wg               sync.WaitGroup
	stop             chan struct{}
	config           SubscriptionConfig
	visibilityFilter VisibilityFilter
	rrCounter        atomic.Uint64
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

// SetVisibilityFilter sets the function used to check whether an entry
// should be visible to a given subscriber.
func (m *subscriptionManager) SetVisibilityFilter(f VisibilityFilter) {
	m.visibilityFilter = f
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
	matches := m.matchingSubs(entry)
	if len(matches) == 0 {
		return
	}

	switch entry.Pattern {
	case 2: // PATTERN_QUEUE — fanone
		target := m.selectOne(matches, entry)
		if target != nil {
			m.dispatchToSub(target, entry)
		}
	case 1: // PATTERN_REQ_REP — direct to "to" label subscriber
		toHex := entry.Labels["to"]
		for _, sub := range matches {
			if fmt.Sprintf("%x", sub.callerKey) == toHex {
				m.dispatchToSub(sub, entry)
				return
			}
		}
	default: // 0 (FIRE_AND_FORGET), 3 (PUB_SUB) — fanout
		for _, sub := range matches {
			m.dispatchToSub(sub, entry)
		}
	}
}

// matchingSubs returns subscriptions that match the entry's expression and pass
// the visibility filter.
func (m *subscriptionManager) matchingSubs(entry *physical.Entry) []*subscription {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var matches []*subscription
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
		if m.visibilityFilter != nil && !m.visibilityFilter(entry, sub.callerKey) {
			continue
		}
		matches = append(matches, sub)
	}
	// Sort by ID for deterministic ordering (needed for consistent affinity routing).
	sort.Slice(matches, func(i, j int) bool { return matches[i].id < matches[j].id })
	return matches
}

// selectOne picks a single subscriber for QUEUE pattern delivery.
func (m *subscriptionManager) selectOne(subs []*subscription, entry *physical.Entry) *subscription {
	if len(subs) == 0 {
		return nil
	}
	if len(subs) == 1 {
		return subs[0]
	}

	switch entry.Affinity {
	case 1: // AFFINITY_SENDER — hash from label
		key := entry.Labels["from"]
		idx := hashToIndex(key, len(subs))
		return subs[idx]
	case 2: // AFFINITY_KEY — hash AffinityKey
		idx := hashToIndex(entry.AffinityKey, len(subs))
		return subs[idx]
	default: // AFFINITY_NONE — round-robin
		n := m.rrCounter.Add(1)
		return subs[n%uint64(len(subs))]
	}
}

// hashToIndex returns a deterministic index for a string key.
func hashToIndex(key string, n int) int {
	var h uint64
	for i := 0; i < len(key); i++ {
		h = h*31 + uint64(key[i])
	}
	return int(h % uint64(n))
}

func (m *subscriptionManager) dispatchToSub(sub *subscription, entry *physical.Entry) {
	// FIFO ordering: hold back out-of-order entries.
	if entry.Ordering == 1 || entry.Ordering == 2 { // FIFO or CAUSAL (treated same for now)
		senderKey := entry.Labels["from"]
		sub.fifoMu.Lock()
		lastSeq := sub.fifoLastSeq[senderKey]
		if lastSeq > 0 && entry.Sequence != lastSeq+1 {
			// Out of order — hold back.
			sub.fifoHeld[senderKey] = append(sub.fifoHeld[senderKey], entry)
			// Sort held entries by sequence.
			held := sub.fifoHeld[senderKey]
			sort.Slice(held, func(i, j int) bool { return held[i].Sequence < held[j].Sequence })
			sub.fifoMu.Unlock()
			return
		}
		sub.fifoLastSeq[senderKey] = entry.Sequence
		sub.fifoMu.Unlock()

		// Deliver this entry, then drain any held entries that are now in order.
		m.deliverToSub(sub, entry)
		m.drainFIFOHeld(sub, senderKey)
		return
	}

	m.deliverToSub(sub, entry)
}

// drainFIFOHeld delivers held-back entries that are now consecutive.
func (m *subscriptionManager) drainFIFOHeld(sub *subscription, senderKey string) {
	sub.fifoMu.Lock()
	defer sub.fifoMu.Unlock()

	for {
		held := sub.fifoHeld[senderKey]
		if len(held) == 0 {
			break
		}
		lastSeq := sub.fifoLastSeq[senderKey]
		if held[0].Sequence != lastSeq+1 {
			break
		}
		// Deliver the next held entry.
		next := held[0]
		sub.fifoHeld[senderKey] = held[1:]
		sub.fifoLastSeq[senderKey] = next.Sequence
		// Unlock while delivering to avoid holding fifoMu during channel send.
		sub.fifoMu.Unlock()
		m.deliverToSub(sub, next)
		sub.fifoMu.Lock()
	}
}

// AckFIFO should be called when an entry is acked so held entries can be released.
// For now, FIFO delivery is sequence-based, not ack-based, so this is a no-op placeholder.

func (m *subscriptionManager) deliverToSub(sub *subscription, entry *physical.Entry) {
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

func (m *subscriptionManager) Subscribe(ctx context.Context, expression string, callerKey [32]byte, opts *SubscriptionOptions) (Subscription, error) {
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

	sub := newSubscription(ctx, expression, callerKey, *opts)
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
