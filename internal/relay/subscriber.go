package relay

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	relayv1 "github.com/gezibash/arc-node/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
)

const (
	DefaultBufferSize = 100
	SendTimeout       = 5 * time.Second
)

// Subscriber represents a connected client with its buffer and subscriptions.
type Subscriber struct {
	id        string
	stream    relayv1.RelayService_ConnectServer
	buffer    chan *relayv1.ServerFrame
	sender    identity.PublicKey
	name      string // registered @name, if any
	lastSend  atomic.Int64
	lastRecv  atomic.Int64
	closed    atomic.Bool
	closedMu  sync.Mutex
	closeOnce sync.Once

	// subscriptions maps subscription ID to label matchers
	subscriptions map[string]map[string]string
	subMu         sync.RWMutex
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
		subscriptions: make(map[string]map[string]string),
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

// SetName registers an addressed name.
func (s *Subscriber) SetName(name string) { s.name = name }

// Subscribe adds a label-match subscription.
func (s *Subscriber) Subscribe(id string, labels map[string]string) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	s.subscriptions[id] = labels
}

// Unsubscribe removes a subscription.
func (s *Subscriber) Unsubscribe(id string) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	delete(s.subscriptions, id)
}

// Subscriptions returns a snapshot of current subscriptions.
func (s *Subscriber) Subscriptions() map[string]map[string]string {
	s.subMu.RLock()
	defer s.subMu.RUnlock()
	copy := make(map[string]map[string]string, len(s.subscriptions))
	for id, labels := range s.subscriptions {
		copy[id] = labels
	}
	return copy
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
			"subscriber_name", s.name,
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
