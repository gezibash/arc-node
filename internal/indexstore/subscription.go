package indexstore

import (
	"context"
	"log/slog"
	"sync"

	"github.com/google/uuid"

	"github.com/gezibash/arc/pkg/reference"

	celeval "github.com/gezibash/arc-node/internal/indexstore/cel"
	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

const defaultBufferSize = 100

// Subscription represents an active subscription to index entries.
type Subscription interface {
	ID() string
	Entries() <-chan *physical.Entry
	Cancel()
	Err() error
}

type subscription struct {
	id         string
	expression string
	entries    chan *physical.Entry
	cancel     context.CancelFunc
	err        error
	errMu      sync.RWMutex
	done       chan struct{}
}

func newSubscription(ctx context.Context, expression string) *subscription {
	ctx, cancel := context.WithCancel(ctx)
	s := &subscription{
		id:         uuid.NewString(),
		expression: expression,
		entries:    make(chan *physical.Entry, defaultBufferSize),
		cancel:     cancel,
		done:       make(chan struct{}),
	}

	go func() {
		<-ctx.Done()
		close(s.entries)
		close(s.done)
	}()

	return s
}

func (s *subscription) ID() string                    { return s.id }
func (s *subscription) Entries() <-chan *physical.Entry { return s.entries }
func (s *subscription) Cancel()                        { s.cancel() }

func (s *subscription) Err() error {
	s.errMu.RLock()
	defer s.errMu.RUnlock()
	return s.err
}

type subscriptionManager struct {
	mu     sync.RWMutex
	subs   map[string]*subscription
	eval   *celeval.Evaluator
	closed bool
}

func newSubscriptionManager(eval *celeval.Evaluator) *subscriptionManager {
	return &subscriptionManager{
		subs: make(map[string]*subscription),
		eval: eval,
	}
}

func (m *subscriptionManager) Subscribe(ctx context.Context, expression string) (Subscription, error) {
	if err := m.eval.ValidateExpression(ctx, expression); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, ErrSubscriptionClosed
	}

	sub := newSubscription(ctx, expression)
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

func (m *subscriptionManager) Notify(ctx context.Context, entry *physical.Entry) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.subs) == 0 {
		return
	}

	for _, sub := range m.subs {
		match, err := m.eval.Match(ctx, sub.expression, entry)
		if err != nil {
			slog.Warn("subscription filter evaluation failed",
				"subscription_id", sub.id,
				"error", err,
			)
			continue
		}
		if !match {
			continue
		}

		select {
		case sub.entries <- entry:
		default:
			slog.Warn("subscription buffer full, entry dropped",
				"subscription_id", sub.id,
				"ref", reference.Hex(entry.Ref),
			)
		}
	}
}

func (m *subscriptionManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	for _, sub := range m.subs {
		sub.Cancel()
	}
	m.subs = nil
}

func (m *subscriptionManager) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.subs)
}
