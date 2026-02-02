package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc/v2/pkg/message"
	"google.golang.org/grpc/metadata"
)

// mockChannelStream implements nodev1.NodeService_ChannelClient for unit tests.
type mockChannelStream struct {
	sendCh chan *nodev1.ClientFrame
	recvCh chan *nodev1.ServerFrame
	closed bool
	mu     sync.Mutex
}

func newMockStream() *mockChannelStream {
	return &mockChannelStream{
		sendCh: make(chan *nodev1.ClientFrame, 100),
		recvCh: make(chan *nodev1.ServerFrame, 100),
	}
}

func (m *mockChannelStream) Send(f *nodev1.ClientFrame) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("stream closed")
	}
	m.sendCh <- f
	return nil
}

func (m *mockChannelStream) Recv() (*nodev1.ServerFrame, error) {
	f, ok := <-m.recvCh
	if !ok {
		return nil, io.EOF
	}
	return f, nil
}

func (m *mockChannelStream) Header() (metadata.MD, error) { return nil, nil }
func (m *mockChannelStream) Trailer() metadata.MD         { return nil }
func (m *mockChannelStream) CloseSend() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
func (m *mockChannelStream) Context() context.Context      { return context.Background() }
func (m *mockChannelStream) SendMsg(msg interface{}) error { return nil }
func (m *mockChannelStream) RecvMsg(msg interface{}) error { return nil }

func TestProtoToEntryDimensionsNil(t *testing.T) {
	result := protoToEntryDimensions(nil)
	if result != nil {
		t.Error("expected nil for nil input")
	}
}

func TestProtoToEntryDimensionsAllFields(t *testing.T) {
	d := &nodev1.Dimensions{
		Visibility:     nodev1.Visibility_VISIBILITY_LABEL_SCOPED,
		Persistence:    nodev1.Persistence_PERSISTENCE_DURABLE,
		Delivery:       nodev1.Delivery_DELIVERY_AT_LEAST_ONCE,
		Pattern:        nodev1.Pattern_PATTERN_PUB_SUB,
		Affinity:       nodev1.Affinity_AFFINITY_KEY,
		AffinityKey:    "test-key",
		TtlMs:          5000,
		Ordering:       nodev1.Ordering_ORDERING_FIFO,
		Dedup:          nodev1.Dedup_DEDUP_REF,
		IdempotencyKey: "idem-1",
		Complete:       nodev1.DeliveryComplete_DELIVERY_COMPLETE_ALL,
		CompleteN:      3,
		Priority:       10,
		MaxRedelivery:  5,
		AckTimeoutMs:   30000,
		Correlation:    "corr-test",
	}
	ed := protoToEntryDimensions(d)
	if ed == nil {
		t.Fatal("expected non-nil")
	}
	if ed.Visibility != int32(d.Visibility) {
		t.Error("visibility")
	}
	if ed.Persistence != int32(d.Persistence) {
		t.Error("persistence")
	}
	if ed.Delivery != int32(d.Delivery) {
		t.Error("delivery")
	}
	if ed.Pattern != int32(d.Pattern) {
		t.Error("pattern")
	}
	if ed.Affinity != int32(d.Affinity) {
		t.Error("affinity")
	}
	if ed.AffinityKey != "test-key" {
		t.Error("affinity_key")
	}
	if ed.TtlMs != 5000 {
		t.Error("ttl")
	}
	if ed.Ordering != int32(d.Ordering) {
		t.Error("ordering")
	}
	if ed.DedupMode != int32(d.Dedup) {
		t.Error("dedup")
	}
	if ed.IdempotencyKey != "idem-1" {
		t.Error("idempotency_key")
	}
	if ed.DeliveryComplete != int32(d.Complete) {
		t.Error("complete")
	}
	if ed.CompleteN != 3 {
		t.Error("complete_n")
	}
	if ed.Priority != 10 {
		t.Error("priority")
	}
	if ed.MaxRedelivery != 5 {
		t.Error("max_redelivery")
	}
	if ed.AckTimeoutMs != 30000 {
		t.Error("ack_timeout")
	}
	if ed.Correlation != "corr-test" {
		t.Error("correlation")
	}
}

func TestNodeInfoStateSetGet(t *testing.T) {
	s := &nodeInfoState{}
	_, ok := s.get()
	if ok {
		t.Error("expected ok=false before set")
	}
	s.set(NodeInfo{ServiceName: "test"})
	info, ok := s.get()
	if !ok {
		t.Error("expected ok=true after set")
	}
	if info.ServiceName != "test" {
		t.Error("service name mismatch")
	}
}

func TestNodeInfoStateConcurrency(t *testing.T) {
	s := &nodeInfoState{}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.set(NodeInfo{ServiceName: "w"})
		}()
		go func() {
			defer wg.Done()
			s.get()
		}()
	}
	wg.Wait()
}

func TestChannelMuxRoundTripErrorFrame(t *testing.T) {
	stream := newMockStream()
	mux := newChannelMux(stream)
	defer mux.close()

	// Respond with error frame
	go func() {
		f := <-stream.sendCh
		stream.recvCh <- &nodev1.ServerFrame{
			RequestId: f.RequestId,
			Frame: &nodev1.ServerFrame_Error{Error: &nodev1.ErrorFrame{
				Code:    5, // NOT_FOUND
				Message: "not found",
			}},
		}
	}()

	_, err := mux.roundTrip(context.Background(), &nodev1.ClientFrame{
		Frame: &nodev1.ClientFrame_Get{Get: &nodev1.GetFrame{Reference: make([]byte, 32)}},
	})
	if err == nil {
		t.Error("expected error")
	}
}

func TestChannelMuxRoundTripClosed(t *testing.T) {
	stream := newMockStream()
	mux := newChannelMux(stream)
	mux.close()
	time.Sleep(50 * time.Millisecond)

	_, err := mux.roundTrip(context.Background(), &nodev1.ClientFrame{
		Frame: &nodev1.ClientFrame_Get{Get: &nodev1.GetFrame{}},
	})
	if err == nil {
		t.Error("expected error on closed mux")
	}
}

func TestChannelMuxDeliveryToSubscriber(t *testing.T) {
	stream := newMockStream()
	mux := newChannelMux(stream)
	defer mux.close()

	ch := mux.subscribe("test-chan")

	// Push unsolicited delivery
	stream.recvCh <- &nodev1.ServerFrame{
		RequestId: 0,
		Frame: &nodev1.ServerFrame_Delivery{Delivery: &nodev1.DeliveryFrame{
			Channel: "test-chan",
			Entry:   &nodev1.IndexEntry{Reference: make([]byte, 32), Timestamp: 123},
		}},
	}

	select {
	case df := <-ch:
		if df.Entry.Timestamp != 123 {
			t.Errorf("timestamp = %d", df.Entry.Timestamp)
		}
	case <-time.After(time.Second):
		t.Fatal("no delivery received")
	}

	mux.unsubscribe("test-chan")
}

func TestChannelMuxDeliveryNoSubscriber(t *testing.T) {
	stream := newMockStream()
	mux := newChannelMux(stream)
	defer mux.close()

	// Push delivery with no subscriber - should not panic
	stream.recvCh <- &nodev1.ServerFrame{
		RequestId: 0,
		Frame: &nodev1.ServerFrame_Delivery{Delivery: &nodev1.DeliveryFrame{
			Channel: "nonexistent",
			Entry:   &nodev1.IndexEntry{Reference: make([]byte, 32)},
		}},
	}

	time.Sleep(50 * time.Millisecond)
}

func TestChannelMuxRecvLoopEOF(t *testing.T) {
	stream := newMockStream()
	mux := newChannelMux(stream)

	close(stream.recvCh) // triggers EOF
	<-mux.done           // wait for recvLoop to finish

	if !mux.closed.Load() {
		t.Error("expected closed after EOF")
	}
}

func TestChannelMuxRecvLoopWakesPending(t *testing.T) {
	stream := newMockStream()
	mux := newChannelMux(stream)

	// Start a roundTrip in background
	errCh := make(chan error, 1)
	go func() {
		_, err := mux.roundTrip(context.Background(), &nodev1.ClientFrame{
			Frame: &nodev1.ClientFrame_Get{Get: &nodev1.GetFrame{}},
		})
		errCh <- err
	}()

	// Wait for send then close stream to simulate error
	<-stream.sendCh
	close(stream.recvCh)

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("roundTrip not unblocked")
	}
}

func TestProtoToEntry(t *testing.T) {
	ref := make([]byte, 32)
	ref[0] = 0xAB
	e := protoToEntry(&nodev1.IndexEntry{
		Reference: ref,
		Labels:    map[string]string{"a": "b"},
		Timestamp: 999,
	})
	if e.Ref[0] != 0xAB {
		t.Error("ref mismatch")
	}
	if e.Labels["a"] != "b" {
		t.Error("labels mismatch")
	}
	if e.Timestamp != 999 {
		t.Error("timestamp mismatch")
	}
	if e.Dimensions != nil {
		t.Error("expected nil dimensions")
	}
}

func TestProtoToQueryResult(t *testing.T) {
	entries := []*nodev1.IndexEntry{
		{Reference: make([]byte, 32), Timestamp: 1},
		{Reference: make([]byte, 32), Timestamp: 2},
	}
	r := protoToQueryResult(entries, "cursor-abc", true)
	if len(r.Entries) != 2 {
		t.Errorf("entries = %d", len(r.Entries))
	}
	if r.NextCursor != "cursor-abc" {
		t.Error("cursor mismatch")
	}
	if !r.HasMore {
		t.Error("hasMore should be true")
	}
}

func TestChannelMuxRoundTripSendError(t *testing.T) {
	stream := newMockStream()
	mux := newChannelMux(stream)
	stream.mu.Lock()
	stream.closed = true
	stream.mu.Unlock()

	_, err := mux.roundTrip(context.Background(), &nodev1.ClientFrame{
		Frame: &nodev1.ClientFrame_Get{Get: &nodev1.GetFrame{}},
	})
	if err == nil {
		t.Error("expected send error")
	}
}

// Helper to create a channelMux with a mock that auto-responds with a given frame type.
func muxWithAutoResponse(respFactory func(uint64) *nodev1.ServerFrame) (*channelMux, *mockChannelStream) {
	stream := newMockStream()
	mux := newChannelMux(stream)
	go func() {
		for f := range stream.sendCh {
			stream.recvCh <- respFactory(f.RequestId)
		}
	}()
	return mux, stream
}

func TestPutContentWrongFrameType(t *testing.T) {
	mux, stream := muxWithAutoResponse(func(id uint64) *nodev1.ServerFrame {
		return &nodev1.ServerFrame{
			RequestId: id,
			Frame:     &nodev1.ServerFrame_Response{Response: &nodev1.ResponseFrame{}},
		}
	})
	defer func() { mux.close(); close(stream.sendCh) }()

	c := &Client{mux: mux, nodeInfo: &nodeInfoState{}}

	// PutContent expects Receipt, but gets Response
	_, err := c.PutContent(context.Background(), []byte("data"))
	if err == nil {
		t.Error("expected error for wrong frame type")
	}
}

func TestGetContentWrongFrameType(t *testing.T) {
	mux, stream := muxWithAutoResponse(func(id uint64) *nodev1.ServerFrame {
		return &nodev1.ServerFrame{
			RequestId: id,
			Frame:     &nodev1.ServerFrame_Receipt{Receipt: &nodev1.ReceiptFrame{}},
		}
	})
	defer func() { mux.close(); close(stream.sendCh) }()

	c := &Client{mux: mux, nodeInfo: &nodeInfoState{}}

	_, err := c.GetContent(context.Background(), [32]byte{})
	if err == nil {
		t.Error("expected error for wrong frame type")
	}
}

func TestSendMessageWrongFrameType(t *testing.T) {
	mux, stream := muxWithAutoResponse(func(id uint64) *nodev1.ServerFrame {
		return &nodev1.ServerFrame{
			RequestId: id,
			Frame:     &nodev1.ServerFrame_Response{Response: &nodev1.ResponseFrame{}},
		}
	})
	defer func() { mux.close(); close(stream.sendCh) }()

	c := &Client{mux: mux, nodeInfo: &nodeInfoState{}}

	msg := testMinimalMessage()
	_, err := c.SendMessage(context.Background(), msg, nil, nil)
	if err == nil {
		t.Error("expected error for wrong frame type")
	}
}

func TestResolveGetWrongFrameType(t *testing.T) {
	mux, stream := muxWithAutoResponse(func(id uint64) *nodev1.ServerFrame {
		return &nodev1.ServerFrame{
			RequestId: id,
			Frame:     &nodev1.ServerFrame_Receipt{Receipt: &nodev1.ReceiptFrame{}},
		}
	})
	defer func() { mux.close(); close(stream.sendCh) }()

	c := &Client{mux: mux, nodeInfo: &nodeInfoState{}}

	_, err := c.ResolveGet(context.Background(), "abc")
	if err == nil {
		t.Error("expected error for wrong frame type")
	}
}

func TestQueryMessagesWrongFrameType(t *testing.T) {
	mux, stream := muxWithAutoResponse(func(id uint64) *nodev1.ServerFrame {
		return &nodev1.ServerFrame{
			RequestId: id,
			Frame:     &nodev1.ServerFrame_Receipt{Receipt: &nodev1.ReceiptFrame{}},
		}
	})
	defer func() { mux.close(); close(stream.sendCh) }()

	c := &Client{mux: mux, nodeInfo: &nodeInfoState{}}

	_, err := c.QueryMessages(context.Background(), &QueryOptions{Expression: "true"})
	if err == nil {
		t.Error("expected error for wrong frame type")
	}
}

func TestListPeersWrongFrameType(t *testing.T) {
	mux, stream := muxWithAutoResponse(func(id uint64) *nodev1.ServerFrame {
		return &nodev1.ServerFrame{
			RequestId: id,
			Frame:     &nodev1.ServerFrame_Receipt{Receipt: &nodev1.ReceiptFrame{}},
		}
	})
	defer func() { mux.close(); close(stream.sendCh) }()

	c := &Client{mux: mux, nodeInfo: &nodeInfoState{}}

	_, err := c.ListPeers(context.Background())
	if err == nil {
		t.Error("expected error for wrong frame type")
	}
}

func TestFederateWrongFrameType(t *testing.T) {
	mux, stream := muxWithAutoResponse(func(id uint64) *nodev1.ServerFrame {
		return &nodev1.ServerFrame{
			RequestId: id,
			Frame:     &nodev1.ServerFrame_Receipt{Receipt: &nodev1.ReceiptFrame{}},
		}
	})
	defer func() { mux.close(); close(stream.sendCh) }()

	c := &Client{mux: mux, nodeInfo: &nodeInfoState{}}

	_, err := c.Federate(context.Background(), "localhost:1234", nil)
	if err == nil {
		t.Error("expected error for wrong frame type")
	}
}

func TestSubscribeChannelError(t *testing.T) {
	mux, stream := muxWithAutoResponse(func(id uint64) *nodev1.ServerFrame {
		return &nodev1.ServerFrame{
			RequestId: id,
			Frame: &nodev1.ServerFrame_Error{Error: &nodev1.ErrorFrame{
				Code:    13,
				Message: "internal",
			}},
		}
	})
	defer func() { mux.close(); close(stream.sendCh) }()

	c := &Client{mux: mux, nodeInfo: &nodeInfoState{}}

	_, _, err := c.SubscribeChannel(context.Background(), "true", nil)
	if err == nil {
		t.Error("expected error")
	}
}

// testMinimalMessage creates a minimal valid message for unit tests.
func testMinimalMessage() message.Message {
	return message.Message{
		ContentType: "test",
	}
}

func TestChannelUnavailable(t *testing.T) {
	// Create a client that connects to a non-existent server.
	// channel() will fail because the stream open fails.
	c, err := Dial("localhost:1")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err = c.PutContent(ctx, []byte("data"))
	if err == nil {
		t.Error("expected error")
	}

	_, err = c.GetContent(ctx, [32]byte{})
	if err == nil {
		t.Error("expected error")
	}

	_, err = c.ResolveGet(ctx, "abc")
	if err == nil {
		t.Error("expected error")
	}

	_, err = c.QueryMessages(ctx, &QueryOptions{Expression: "true"})
	if err == nil {
		t.Error("expected error")
	}

	_, err = c.ListPeers(ctx)
	if err == nil {
		t.Error("expected error")
	}

	_, err = c.Federate(ctx, "x", nil)
	if err == nil {
		t.Error("expected error")
	}

	err = c.Seek(ctx, "ch", 0)
	if err == nil {
		t.Error("expected error")
	}

	_, _, err = c.SubscribeMessages(ctx, "true", nil)
	if err == nil {
		t.Error("expected error")
	}

	_, _, err = c.SubscribeChannel(ctx, "true", nil)
	if err == nil {
		t.Error("expected error")
	}

	msg := testMinimalMessage()
	_, err = c.SendMessage(ctx, msg, nil, nil)
	if err == nil {
		t.Error("expected error")
	}
}
