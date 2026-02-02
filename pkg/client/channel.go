package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// channelMux multiplexes operations over a single bidi Channel stream.
type channelMux struct {
	mu          sync.Mutex
	stream      nodev1.NodeService_ChannelClient
	nextID      atomic.Uint64
	pending     sync.Map // uint64 → chan *nodev1.ServerFrame
	subs        sync.Map // string → chan *nodev1.DeliveryFrame
	presenceSub chan *nodev1.PresenceEventFrame
	presenceMu  sync.Mutex
	closed      atomic.Bool
	done        chan struct{}
}

func newChannelMux(stream nodev1.NodeService_ChannelClient) *channelMux {
	m := &channelMux{
		stream: stream,
		done:   make(chan struct{}),
	}
	go m.recvLoop()
	return m
}

func (m *channelMux) recvLoop() {
	defer close(m.done)
	for {
		frame, err := m.stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				m.closed.Store(true)
				return
			}
			m.closed.Store(true)
			// Wake all pending waiters with nil (they'll see closed).
			m.pending.Range(func(_, value any) bool {
				if ch, ok := value.(chan *nodev1.ServerFrame); ok {
					select {
					case ch <- nil:
					default:
					}
				}
				return true
			})
			return
		}

		reqID := frame.GetRequestId()

		// Unsolicited push (delivery or presence event).
		if reqID == 0 {
			if df, ok := frame.Frame.(*nodev1.ServerFrame_Delivery); ok {
				if ch, loaded := m.subs.Load(df.Delivery.Channel); loaded {
					if dch, ok := ch.(chan *nodev1.DeliveryFrame); ok {
						select {
						case dch <- df.Delivery:
						default:
							// Drop if subscriber is slow.
						}
					}
				}
			}
			if pf, ok := frame.Frame.(*nodev1.ServerFrame_PresenceEvent); ok {
				m.presenceMu.Lock()
				ch := m.presenceSub
				m.presenceMu.Unlock()
				if ch != nil {
					select {
					case ch <- pf.PresenceEvent:
					default:
					}
				}
			}
			continue
		}

		// Correlated response.
		if ch, loaded := m.pending.LoadAndDelete(reqID); loaded {
			if respCh, ok := ch.(chan *nodev1.ServerFrame); ok {
				respCh <- frame
			}
		}
	}
}

func (m *channelMux) send(frame *nodev1.ClientFrame) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stream.Send(frame)
}

func (m *channelMux) roundTrip(ctx context.Context, frame *nodev1.ClientFrame) (*nodev1.ServerFrame, error) {
	if m.closed.Load() {
		return nil, fmt.Errorf("channel closed")
	}

	id := m.nextID.Add(1)
	frame.RequestId = id

	respCh := make(chan *nodev1.ServerFrame, 1)
	m.pending.Store(id, respCh)
	defer m.pending.Delete(id)

	if err := m.send(frame); err != nil {
		return nil, fmt.Errorf("send frame: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.done:
		return nil, fmt.Errorf("channel closed")
	case resp := <-respCh:
		if resp == nil {
			return nil, fmt.Errorf("channel closed")
		}
		// Check for error frame.
		if ef, ok := resp.Frame.(*nodev1.ServerFrame_Error); ok {
			msg := ef.Error.Message
			if ef.Error.Detail != "" {
				msg = ef.Error.Detail + ": " + msg
			}
			err := status.Error(codes.Code(ef.Error.Code), msg)
			if ef.Error.Retryable {
				err = &retryableError{cause: err}
			}
			return nil, err
		}
		return resp, nil
	}
}

func (m *channelMux) subscribe(channel string) <-chan *nodev1.DeliveryFrame {
	ch := make(chan *nodev1.DeliveryFrame, 64)
	m.subs.Store(channel, ch)
	return ch
}

func (m *channelMux) unsubscribe(channel string) {
	if v, loaded := m.subs.LoadAndDelete(channel); loaded {
		if ch, ok := v.(chan *nodev1.DeliveryFrame); ok {
			close(ch)
		}
	}
}

func (m *channelMux) subscribePresence() <-chan *nodev1.PresenceEventFrame {
	ch := make(chan *nodev1.PresenceEventFrame, 64)
	m.presenceMu.Lock()
	m.presenceSub = ch
	m.presenceMu.Unlock()
	return ch
}

func (m *channelMux) unsubscribePresence() {
	m.presenceMu.Lock()
	if m.presenceSub != nil {
		close(m.presenceSub)
		m.presenceSub = nil
	}
	m.presenceMu.Unlock()
}

func (m *channelMux) close() {
	if m.closed.Swap(true) {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_ = m.stream.CloseSend()
}

// retryableError wraps an error to indicate the operation may be retried.
type retryableError struct {
	cause error
}

func (e *retryableError) Error() string { return e.cause.Error() }
func (e *retryableError) Unwrap() error { return e.cause }

// GRPCStatus implements the gRPC status interface so status.FromError works.
func (e *retryableError) GRPCStatus() *status.Status {
	if s, ok := status.FromError(e.cause); ok {
		return s
	}
	return status.New(codes.Unknown, e.cause.Error())
}

// Retryable reports whether err was flagged as retryable by the server.
func Retryable(err error) bool {
	var re *retryableError
	return errors.As(err, &re)
}
