package relay

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/internal/names"
	"github.com/gezibash/arc/v2/pkg/identity"
)

const (
	DefaultBufferSize = 100
	SendTimeout       = 5 * time.Second
)

// Subscriber represents a connected client with its buffer and subscriptions.
type Subscriber struct {
	id          string
	stream      relayv1.RelayService_ConnectServer
	buffer      chan *relayv1.ServerFrame
	sender      identity.PublicKey
	name        string // registered @name, if any
	connectedAt int64  // connection time (unix nanos)
	lastSend    atomic.Int64
	lastRecv    atomic.Int64
	latency     atomic.Int64 // RTT in nanoseconds (from ping/pong)
	closed      atomic.Bool
	closedMu    sync.Mutex
	closeOnce   sync.Once

	// Pending ping for latency measurement
	pendingPing      atomic.Int64 // timestamp when ping was sent (unix nanos)
	pendingPingNonce []byte

	// subscriptions maps subscription ID to subscription data (labels + state)
	subscriptions map[string]*SubscriptionData
	subMu         sync.RWMutex
}

// SubscriptionData holds typed labels and dynamic state for a subscription.
type SubscriptionData struct {
	Labels map[string]any // structural labels (gossip on change)
	State  map[string]any // dynamic metrics (gossip on timer)
}

// NewSubscriber creates a subscriber with a bounded buffer.
func NewSubscriber(id string, stream relayv1.RelayService_ConnectServer, sender identity.PublicKey, bufferSize int) *Subscriber {
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	now := time.Now().UnixNano()
	s := &Subscriber{
		id:            id,
		stream:        stream,
		buffer:        make(chan *relayv1.ServerFrame, bufferSize),
		sender:        sender,
		connectedAt:   now,
		subscriptions: make(map[string]*SubscriptionData),
	}
	s.lastSend.Store(now)
	s.lastRecv.Store(now)
	return s
}

// ID returns the subscriber's unique identifier.
func (s *Subscriber) ID() string { return s.id }

// Sender returns the subscriber's public key.
func (s *Subscriber) Sender() identity.PublicKey { return s.sender }

// Name returns the registered name, if any.
func (s *Subscriber) Name() string { return s.name }

// DisplayName returns the name if set, otherwise a docker-style petname.
func (s *Subscriber) DisplayName() string {
	if s.name != "" {
		return s.name
	}
	return names.Petname(s.sender.Bytes)
}

// SetName registers an addressed name.
func (s *Subscriber) SetName(name string) { s.name = name }

// Subscribe adds a label-match subscription with typed labels.
func (s *Subscriber) Subscribe(id string, labels map[string]any) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	s.subscriptions[id] = &SubscriptionData{
		Labels: labels,
		State:  make(map[string]any),
	}
}

// Unsubscribe removes a subscription.
func (s *Subscriber) Unsubscribe(id string) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	delete(s.subscriptions, id)
}

// UpdateLabels merges typed labels into an existing subscription and removes specified keys.
func (s *Subscriber) UpdateLabels(id string, merge map[string]any, remove []string) bool {
	s.subMu.Lock()
	defer s.subMu.Unlock()

	data, ok := s.subscriptions[id]
	if !ok {
		return false
	}

	for k, v := range merge {
		data.Labels[k] = v
	}
	for _, k := range remove {
		delete(data.Labels, k)
	}
	return true
}

// UpdateState merges state entries into an existing subscription and removes specified keys.
func (s *Subscriber) UpdateState(id string, state map[string]any, remove []string) bool {
	s.subMu.Lock()
	defer s.subMu.Unlock()

	data, ok := s.subscriptions[id]
	if !ok {
		return false
	}

	for k, v := range state {
		data.State[k] = v
	}
	for _, k := range remove {
		delete(data.State, k)
	}
	return true
}

// Subscriptions returns a snapshot of current subscriptions.
func (s *Subscriber) Subscriptions() map[string]*SubscriptionData {
	s.subMu.RLock()
	defer s.subMu.RUnlock()
	cp := make(map[string]*SubscriptionData, len(s.subscriptions))
	for id, data := range s.subscriptions {
		cp[id] = data
	}
	return cp
}

// Send attempts non-blocking send to the subscriber's buffer.
// Returns true if the frame was queued, false if buffer is full (drop).
func (s *Subscriber) Send(frame *relayv1.ServerFrame) bool {
	if s.closed.Load() {
		return false
	}
	select {
	case s.buffer <- frame:
		s.lastSend.Store(time.Now().UnixNano())
		return true
	default:
		slog.Warn("buffer full, dropping",
			"component", "subscriber",
			"subscriber_id", s.id,
			"subscriber", s.DisplayName(),
			"buffer_len", len(s.buffer),
		)
		return false // buffer full, drop
	}
}

// LastSend returns the time of the last successful send.
func (s *Subscriber) LastSend() time.Time {
	return time.Unix(0, s.lastSend.Load())
}

// LastRecv returns the time of the last received frame.
func (s *Subscriber) LastRecv() time.Time {
	return time.Unix(0, s.lastRecv.Load())
}

// TouchRecv updates the last receive time.
func (s *Subscriber) TouchRecv() {
	s.lastRecv.Store(time.Now().UnixNano())
}

// ConnectedAt returns the time when the subscriber connected.
func (s *Subscriber) ConnectedAt() time.Time {
	return time.Unix(0, s.connectedAt)
}

// ConnectedDuration returns how long the subscriber has been connected.
func (s *Subscriber) ConnectedDuration() time.Duration {
	return time.Duration(time.Now().UnixNano() - s.connectedAt)
}

// Latency returns the last measured RTT latency.
func (s *Subscriber) Latency() time.Duration {
	return time.Duration(s.latency.Load())
}

// SetLatency directly sets the subscriber's measured latency.
// Used when the client reports its own RTT measurement.
func (s *Subscriber) SetLatency(d time.Duration) {
	s.latency.Store(int64(d))
}

// SetPendingPing records that a ping was sent for latency measurement.
func (s *Subscriber) SetPendingPing(nonce []byte) {
	s.pendingPingNonce = nonce
	s.pendingPing.Store(time.Now().UnixNano())
}

// CompletePing calculates latency from a pong response.
// Returns true if this was a valid response to a pending ping.
func (s *Subscriber) CompletePing(nonce []byte) bool {
	sentAt := s.pendingPing.Swap(0)
	if sentAt == 0 {
		return false
	}

	// Verify nonce matches (simple check)
	if len(nonce) != len(s.pendingPingNonce) {
		return false
	}
	for i := range nonce {
		if nonce[i] != s.pendingPingNonce[i] {
			return false
		}
	}

	// Calculate and store latency
	latency := time.Now().UnixNano() - sentAt
	s.latency.Store(latency)
	s.pendingPingNonce = nil
	return true
}

// BufferLen returns current buffer occupancy.
func (s *Subscriber) BufferLen() int {
	return len(s.buffer)
}

// BufferFull returns true if the buffer is at capacity.
func (s *Subscriber) BufferFull() bool {
	return len(s.buffer) == cap(s.buffer)
}

// IsClosed returns true if the subscriber has been closed.
func (s *Subscriber) IsClosed() bool {
	return s.closed.Load()
}

// Close marks the subscriber as closed and closes the buffer.
func (s *Subscriber) Close() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.closedMu.Lock()
		close(s.buffer)
		s.closedMu.Unlock()
	})
}

// Run drains the buffer and sends frames to the stream.
// Blocks until the subscriber is closed or stream errors.
func (s *Subscriber) Run() error {
	for frame := range s.buffer {
		if err := s.stream.Send(frame); err != nil {
			return err
		}
	}
	return nil
}
