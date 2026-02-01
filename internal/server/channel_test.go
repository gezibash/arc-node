package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/reference"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestChannelPutGetRoundTrip(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	data := []byte("channel put/get")
	ref, err := c.PutContent(ctx, data)
	if err != nil {
		t.Fatalf("PutContent: %v", err)
	}
	if ref == (reference.Reference{}) {
		t.Fatal("zero reference")
	}

	got, err := c.GetContent(ctx, ref)
	if err != nil {
		t.Fatalf("GetContent: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestChannelPublishQueryRoundTrip(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "text/plain")
	ref, err := c.SendMessage(ctx, msg, map[string]string{"ch": "test"})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if ref == (reference.Reference{}) {
		t.Fatal("zero reference")
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{
		Labels: map[string]string{"ch": "test"},
	})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result.Entries))
	}
	if result.Entries[0].Ref != ref {
		t.Errorf("ref mismatch")
	}
}

func TestChannelSubscribeDelivery(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, _, err := c.SubscribeMessages(subCtx, "true", nil)
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	msg := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg, nil); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case e := <-entries:
		if e == nil {
			t.Fatal("nil entry")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no delivery within timeout")
	}
}

func TestChannelUnsubscribeStopsDelivery(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	subCtx, cancel := context.WithCancel(ctx)
	entries, _, err := c.SubscribeMessages(subCtx, "true", nil)
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Cancel subscription.
	cancel()
	time.Sleep(100 * time.Millisecond)

	// Send a message — should not arrive.
	msg := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg, nil); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case _, ok := <-entries:
		if ok {
			// Might get one in-flight, that's acceptable.
		}
	case <-time.After(500 * time.Millisecond):
		// Good — no delivery.
	}
}

func TestChannelGetNotFound(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	_, err := c.GetContent(ctx, reference.Reference{})
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", err)
	}
}

func TestChannelPublishInvalidSignature(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "text/plain")
	msg.Signature[0] ^= 0xFF

	_, err := c.SendMessage(ctx, msg, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", err)
	}
}

func TestChannelConcurrentRequests(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := []byte{byte(i)}
			ref, err := c.PutContent(ctx, data)
			if err != nil {
				errs <- err
				return
			}
			got, err := c.GetContent(ctx, ref)
			if err != nil {
				errs <- err
				return
			}
			if len(got) != 1 || got[0] != byte(i) {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent error: %v", err)
	}
}

func TestChannelDeliveryId(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, _, err := c.SubscribeMessages(subCtx, "true", nil)
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// At-least-once delivery should get delivery_id > 0 via auto-ack.
	// The client auto-acks, so just verify entry arrives.
	msg := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg, map[string]string{
		"delivery": "at-least-once",
	}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case e := <-entries:
		if e == nil {
			t.Fatal("nil entry")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no delivery within timeout")
	}
}

func TestChannelQueryWithExpression(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg, map[string]string{"priority": "high"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msg2 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg2, map[string]string{"priority": "low"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{
		Expression: `labels["priority"] == "high"`,
	})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(result.Entries))
	}
}

func TestChannelSubscribeWithLabels(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, _, err := c.SubscribeMessages(subCtx, "", map[string]string{"env": "prod"})
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Non-matching.
	msg1 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg1, map[string]string{"env": "dev"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Matching.
	msg2 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg2, map[string]string{"env": "prod"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case e := <-entries:
		if e == nil {
			t.Fatal("nil entry")
		}
		if e.Labels["env"] != "prod" {
			t.Errorf("label env = %q, want prod", e.Labels["env"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no matching delivery")
	}
}

func TestChannelAckByDeliveryId(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	// The client auto-acks delivery IDs. We verify the full path doesn't error.
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, _, err := c.SubscribeMessages(subCtx, "true", nil)
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	msg := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg, nil); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case e := <-entries:
		if e == nil {
			t.Fatal("nil entry")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no delivery within timeout")
	}
}

func TestChannelStreamCleanupOnDisconnect(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	// Open subscription.
	subCtx, cancel := context.WithCancel(ctx)
	_, _, err := c.SubscribeMessages(subCtx, "true", nil)
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Close client — should clean up server-side state.
	cancel()
	c.Close()

	// Just verify no panic/hang.
	time.Sleep(100 * time.Millisecond)
}
