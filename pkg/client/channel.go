package client

import (
	"context"
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
	mu      sync.Mutex
	stream  nodev1.NodeService_ChannelClient
	nextID  atomic.Uint64
	pending sync.Map // uint64 → chan *nodev1.ServerFrame
	subs    sync.Map // string → chan *nodev1.DeliveryFrame
	closed  atomic.Bool
	done    chan struct{}
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
			if err == io.EOF {
				m.closed.Store(true)
				return
			}
			m.closed.Store(true)
			// Wake all pending waiters with nil (they'll see closed).
			m.pending.Range(func(key, value any) bool {
				ch := value.(chan *nodev1.ServerFrame)
				select {
				case ch <- nil:
				default:
				}
				return true
			})
			return
		}

		reqID := frame.GetRequestId()

		// Unsolicited push (delivery).
		if reqID == 0 {
			if df, ok := frame.Frame.(*nodev1.ServerFrame_Delivery); ok {
				if ch, loaded := m.subs.Load(df.Delivery.Channel); loaded {
					select {
					case ch.(chan *nodev1.DeliveryFrame) <- df.Delivery:
					default:
						// Drop if subscriber is slow.
					}
				}
			}
			continue
		}

		// Correlated response.
		if ch, loaded := m.pending.LoadAndDelete(reqID); loaded {
			ch.(chan *nodev1.ServerFrame) <- frame
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
			return nil, status.Error(codes.Code(ef.Error.Code), ef.Error.Message)
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
		close(v.(chan *nodev1.DeliveryFrame))
	}
}

func (m *channelMux) close() {
	if m.closed.Swap(true) {
		return
	}
	m.stream.CloseSend()
}
