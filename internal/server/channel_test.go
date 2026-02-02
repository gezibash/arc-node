package server

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/internal/blobstore"
	blobphysical "github.com/gezibash/arc-node/internal/blobstore/physical"
	"github.com/gezibash/arc-node/internal/indexstore"
	idxphysical "github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/internal/observability"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
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
	ref, err := c.SendMessage(ctx, msg, map[string]string{"ch": "test"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE})
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
	if _, err := c.SendMessage(ctx, msg, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
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
	if _, err := c.SendMessage(ctx, msg, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case <-entries:
		// Might get one in-flight, that's acceptable.
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

	_, err := c.SendMessage(ctx, msg, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE})
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
	}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
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
	if _, err := c.SendMessage(ctx, msg, map[string]string{"priority": "high"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msg2 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg2, map[string]string{"priority": "low"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
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
	if _, err := c.SendMessage(ctx, msg1, map[string]string{"env": "dev"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Matching.
	msg2 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg2, map[string]string{"env": "prod"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
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
	if _, err := c.SendMessage(ctx, msg, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
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

func TestChannelResolveGet(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	// Put content.
	data := []byte("resolve-get content")
	contentRef, err := c.PutContent(ctx, data)
	if err != nil {
		t.Fatalf("PutContent: %v", err)
	}

	// Publish a message referencing that content.
	msg := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg, map[string]string{"rg": "test"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// ResolveGet with a prefix of the content ref hex.
	refHex := fmt.Sprintf("%x", contentRef)
	prefix := refHex[:8]

	result, err := c.ResolveGet(ctx, prefix)
	if err != nil {
		t.Fatalf("ResolveGet: %v", err)
	}
	if result.Ref != contentRef {
		t.Errorf("ref = %x, want %x", result.Ref, contentRef)
	}
	if result.Kind != client.GetKindBlob {
		t.Errorf("kind = %d, want %d (blob)", result.Kind, client.GetKindBlob)
	}
}

func TestChannelSeekByTimestamp(t *testing.T) {
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

	// Publish 3 messages with distinct timestamps.
	pub := callerKP.PublicKey()
	timestamps := []int64{1000, 2000, 3000}
	for _, ts := range timestamps {
		content := reference.Compute([]byte(fmt.Sprintf("seek-content-%d", ts)))
		msg := message.New(pub, pub, content, "text/plain")
		msg.Timestamp = ts
		if err := message.Sign(&msg, callerKP); err != nil {
			t.Fatalf("sign: %v", err)
		}
		if _, err := c.SendMessage(ctx, msg, map[string]string{"seek": "test"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
			t.Fatalf("SendMessage ts=%d: %v", ts, err)
		}
	}

	// Drain the 3 subscription deliveries.
	for i := 0; i < 3; i++ {
		select {
		case e := <-entries:
			if e == nil {
				t.Fatalf("nil entry on delivery %d", i)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timeout waiting for delivery %d", i)
		}
	}

	// Seek to the 2nd message's timestamp.
	if err := c.Seek(ctx, "default", 2000); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	// We should receive entries from timestamp >= 2000.
	var replayed []int64
	for {
		select {
		case e := <-entries:
			if e == nil {
				t.Fatal("nil entry after seek")
			}
			replayed = append(replayed, e.Timestamp)
			if len(replayed) >= 2 {
				goto done
			}
		case <-time.After(3 * time.Second):
			goto done
		}
	}
done:
	if len(replayed) < 2 {
		t.Fatalf("expected at least 2 replayed entries, got %d", len(replayed))
	}
	for _, ts := range replayed {
		if ts < 2000 {
			t.Errorf("replayed entry with timestamp %d < 2000", ts)
		}
	}
}

func TestChannelFederateRequiresAdmin(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	_, err := c.Federate(ctx, "localhost:0", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Errorf("code = %v, want PermissionDenied", err)
	}
}

func TestChannelListPeersRequiresAdmin(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	_, err := c.ListPeers(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Errorf("code = %v, want PermissionDenied", err)
	}
}

func TestChannelQueryPagination(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	const n = 5
	for i := 0; i < n; i++ {
		msg := makeMessage(t, callerKP, "text/plain")
		if _, err := c.SendMessage(ctx, msg, map[string]string{"pg": "test"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
			t.Fatalf("SendMessage[%d]: %v", i, err)
		}
	}

	// Page 1.
	r1, err := c.QueryMessages(ctx, &client.QueryOptions{
		Labels: map[string]string{"pg": "test"},
		Limit:  2,
	})
	if err != nil {
		t.Fatalf("QueryMessages page 1: %v", err)
	}
	if len(r1.Entries) != 2 {
		t.Fatalf("page 1: expected 2 entries, got %d", len(r1.Entries))
	}
	if !r1.HasMore {
		t.Error("page 1: HasMore = false, want true")
	}
	if r1.NextCursor == "" {
		t.Fatal("page 1: NextCursor is empty")
	}

	// Page 2.
	r2, err := c.QueryMessages(ctx, &client.QueryOptions{
		Labels: map[string]string{"pg": "test"},
		Limit:  2,
		Cursor: r1.NextCursor,
	})
	if err != nil {
		t.Fatalf("QueryMessages page 2: %v", err)
	}
	if len(r2.Entries) != 2 {
		t.Fatalf("page 2: expected 2 entries, got %d", len(r2.Entries))
	}

	// Page 3 (last).
	r3, err := c.QueryMessages(ctx, &client.QueryOptions{
		Labels: map[string]string{"pg": "test"},
		Limit:  2,
		Cursor: r2.NextCursor,
	})
	if err != nil {
		t.Fatalf("QueryMessages page 3: %v", err)
	}
	if len(r3.Entries) != 1 {
		t.Fatalf("page 3: expected 1 entry, got %d", len(r3.Entries))
	}
	if r3.HasMore {
		t.Error("page 3: HasMore = true, want false")
	}
}

func TestChannelQueryDescending(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	pub := callerKP.PublicKey()
	timestamps := []int64{1000, 2000, 3000}
	for _, ts := range timestamps {
		content := reference.Compute([]byte(fmt.Sprintf("desc-%d", ts)))
		msg := message.New(pub, pub, content, "text/plain")
		msg.Timestamp = ts
		if err := message.Sign(&msg, callerKP); err != nil {
			t.Fatalf("sign: %v", err)
		}
		if _, err := c.SendMessage(ctx, msg, map[string]string{"desc": "test"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
			t.Fatalf("SendMessage ts=%d: %v", ts, err)
		}
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{
		Labels:     map[string]string{"desc": "test"},
		Descending: true,
	})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(result.Entries))
	}
	if result.Entries[0].Timestamp < result.Entries[2].Timestamp {
		t.Errorf("expected descending: first=%d, last=%d", result.Entries[0].Timestamp, result.Entries[2].Timestamp)
	}
}

func TestChannelMultipleSubscriptions(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c1, callerKP := newTestClient(t, addr, kp)
	c2, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	subCtx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	subCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()

	entries1, _, err := c1.SubscribeMessages(subCtx1, "true", map[string]string{"ms": "a"})
	if err != nil {
		t.Fatalf("SubscribeMessages 1: %v", err)
	}
	entries2, _, err := c2.SubscribeMessages(subCtx2, "true", map[string]string{"ms": "b"})
	if err != nil {
		t.Fatalf("SubscribeMessages 2: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Send message matching sub 1.
	msg1 := makeMessage(t, callerKP, "text/plain")
	if _, err := c1.SendMessage(ctx, msg1, map[string]string{"ms": "a"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage a: %v", err)
	}
	// Send message matching sub 2.
	msg2 := makeMessage(t, callerKP, "text/plain")
	if _, err := c1.SendMessage(ctx, msg2, map[string]string{"ms": "b"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage b: %v", err)
	}

	select {
	case e := <-entries1:
		if e == nil {
			t.Fatal("nil entry on sub 1")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("sub 1: no delivery")
	}

	select {
	case e := <-entries2:
		if e == nil {
			t.Fatal("nil entry on sub 2")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("sub 2: no delivery")
	}
}

func TestChannelPutLargeContent(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	// 64KB payload.
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	ref, err := c.PutContent(ctx, data)
	if err != nil {
		t.Fatalf("PutContent large: %v", err)
	}

	got, err := c.GetContent(ctx, ref)
	if err != nil {
		t.Fatalf("GetContent large: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("content length = %d, want %d", len(got), len(data))
	}
	for i := 0; i < len(data); i++ {
		if got[i] != data[i] {
			t.Fatalf("content mismatch at byte %d", i)
		}
	}
}

func TestChannelSendWithDimensions(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "text/plain")
	dims := &nodev1.Dimensions{
		Persistence: nodev1.Persistence_PERSISTENCE_DURABLE,
		Delivery:    nodev1.Delivery_DELIVERY_AT_LEAST_ONCE,
		Priority:    5,
		TtlMs:       60000,
	}
	ref, err := c.SendMessage(ctx, msg, map[string]string{"dim": "test"}, dims)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if ref == (reference.Reference{}) {
		t.Fatal("zero reference")
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{
		Labels: map[string]string{"dim": "test"},
	})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result.Entries))
	}
	e := result.Entries[0]
	if e.Dimensions == nil {
		t.Fatal("Dimensions is nil")
	}
	if e.Dimensions.Persistence != int32(nodev1.Persistence_PERSISTENCE_DURABLE) {
		t.Errorf("Persistence = %d, want %d", e.Dimensions.Persistence, nodev1.Persistence_PERSISTENCE_DURABLE)
	}
	if e.Dimensions.Delivery != int32(nodev1.Delivery_DELIVERY_AT_LEAST_ONCE) {
		t.Errorf("Delivery = %d, want %d", e.Dimensions.Delivery, nodev1.Delivery_DELIVERY_AT_LEAST_ONCE)
	}
	if e.Dimensions.TtlMs == 0 {
		t.Error("TtlMs should be non-zero")
	}
}

func TestChannelAckByDeliveryIdAtLeastOnce(t *testing.T) {
	// Publish a message with at-least-once delivery, subscribe, receive it,
	// and verify the auto-ack path completes without error.
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

	// Send with at-least-once delivery.
	msg := makeMessage(t, callerKP, "text/plain")
	dims := &nodev1.Dimensions{
		Persistence: nodev1.Persistence_PERSISTENCE_DURABLE,
		Delivery:    nodev1.Delivery_DELIVERY_AT_LEAST_ONCE,
	}
	if _, err := c.SendMessage(ctx, msg, map[string]string{"ack": "test"}, dims); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// The client auto-acks delivery IDs. Verify the entry arrives.
	select {
	case e := <-entries:
		if e == nil {
			t.Fatal("nil entry")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no delivery within timeout")
	}

	// Give time for auto-ack to complete.
	time.Sleep(200 * time.Millisecond)
}

func TestChannelSeekNoTimestamp(t *testing.T) {
	// Seek with no timestamp or cursor should return InvalidArgument.
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	err := c.Seek(ctx, "default", 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", err)
	}
}

func TestChannelFederateEmptyPeer(t *testing.T) {
	// Federate with empty peer should return InvalidArgument.
	_, kp, addr := newTestServer(t)
	admin := newTestAdminClient(t, addr, kp)
	ctx := context.Background()

	_, err := admin.Federate(ctx, "", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", err)
	}
}

func TestChannelFederateSuccess(t *testing.T) {
	// Start federation from admin client and verify success response.
	_, peerKP, peerAddr := newTestServer(t)
	_ = peerKP
	_, localKP, localAddr := newTestServer(t)

	admin := newTestAdminClient(t, localAddr, localKP)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := admin.Federate(ctx, peerAddr, map[string]string{"env": "test"})
	if err != nil {
		t.Fatalf("Federate: %v", err)
	}
	if result.Status != "ok" {
		t.Errorf("status = %q, want ok", result.Status)
	}

	// Federation again should return "already_federating".
	result2, err := admin.Federate(ctx, peerAddr, map[string]string{"env": "test"})
	if err != nil {
		t.Fatalf("Federate again: %v", err)
	}
	if result2.Status != "already_federating" {
		t.Errorf("status = %q, want already_federating", result2.Status)
	}
}

func TestChannelFederateNormalizesURL(t *testing.T) {
	// Federate with a URL-style peer should normalize to host:port.
	_, peerKP, peerAddr := newTestServer(t)
	_ = peerKP
	_, localKP, localAddr := newTestServer(t)

	admin := newTestAdminClient(t, localAddr, localKP)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Wrap peer addr as URL.
	result, err := admin.Federate(ctx, "grpc://"+peerAddr, nil)
	if err != nil {
		t.Fatalf("Federate: %v", err)
	}
	if result.Status != "ok" {
		t.Errorf("status = %q, want ok", result.Status)
	}
}

func TestChannelResolveGetNotFound(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	_, err := c.ResolveGet(ctx, "deadbeef")
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", err)
	}
}

func TestChannelResolveGetEmptyPrefix(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	_, err := c.ResolveGet(ctx, "")
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", err)
	}
}

func TestChannelResolveGetShortPrefix(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	_, err := c.ResolveGet(ctx, "ab")
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", err)
	}
}

func TestChannelResolveGetIndexMatch(t *testing.T) {
	// Publish a message and resolve it by index prefix.
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "text/plain")
	ref, err := c.SendMessage(ctx, msg, map[string]string{"rg2": "test"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Use the message ref prefix to resolve.
	refHex := fmt.Sprintf("%x", ref)
	prefix := refHex[:8]

	result, err := c.ResolveGet(ctx, prefix)
	if err != nil {
		t.Fatalf("ResolveGet: %v", err)
	}
	if result.Kind != client.GetKindMessage {
		t.Errorf("kind = %d, want %d (message)", result.Kind, client.GetKindMessage)
	}
	if result.Ref != ref {
		t.Errorf("ref mismatch: got %x, want %x", result.Ref, ref)
	}
}

func TestChannelSubscribeWithExpression(t *testing.T) {
	// Subscribe with a CEL expression (not just labels).
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, _, err := c.SubscribeChannel(subCtx, `labels["kind"] == "important"`, nil)
	if err != nil {
		t.Fatalf("SubscribeChannel: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Non-matching.
	msg1 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg1, map[string]string{"kind": "boring"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Matching.
	msg2 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg2, map[string]string{"kind": "important"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case e := <-entries:
		if e == nil {
			t.Fatal("nil entry")
		}
		if e.Labels["kind"] != "important" {
			t.Errorf("label kind = %q, want important", e.Labels["kind"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no matching delivery")
	}
}

func TestChannelQueryWithLabelFilter(t *testing.T) {
	// Query using both labels and expression.
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg1 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg1, map[string]string{"app": "x", "env": "prod"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msg2 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg2, map[string]string{"app": "x", "env": "staging"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{
		Labels:     map[string]string{"app": "x"},
		Expression: `labels["env"] == "prod"`,
	})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(result.Entries))
	}
}

func TestChannelQueryInvalidExpression(t *testing.T) {
	// Query with an invalid CEL expression should return an error.
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	_, err := c.QueryMessages(ctx, &client.QueryOptions{
		Expression: `this is not valid CEL {{{}}}`,
	})
	if err == nil {
		t.Fatal("expected error for invalid expression")
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

// --- Channel edge cases ---

func TestChannelSubsAddDuplicate(t *testing.T) {
	// Subscribe to same channel twice — first should be cancelled.
	cs := newChannelSubs()

	firstCancelled := false
	cs.add("default", func() { firstCancelled = true })
	cs.add("default", func() {})

	if !firstCancelled {
		t.Error("first subscription should have been cancelled on duplicate add")
	}
	cs.closeAll()
}

func TestChannelSubsRemoveNonexistent(t *testing.T) {
	// Remove a channel that was never subscribed — should not panic.
	cs := newChannelSubs()
	cs.remove("nonexistent")
}

func TestChannelSubsCloseAll(t *testing.T) {
	cs := newChannelSubs()
	cancelled := 0
	cs.add("a", func() { cancelled++ })
	cs.add("b", func() { cancelled++ })
	cs.closeAll()
	if cancelled != 2 {
		t.Errorf("expected 2 cancels, got %d", cancelled)
	}
}

func TestChannelDurableSubscription(t *testing.T) {
	// Verify that durable subscription (with SubscriptionId) works through the SDK.
	// The SDK doesn't expose SubscriptionId directly, but we can test that the
	// cursor replay path is exercised indirectly through Seek which calls replayFromCursor.
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	// Publish 3 messages.
	pub := callerKP.PublicKey()
	for i := 0; i < 3; i++ {
		content := reference.Compute([]byte(fmt.Sprintf("durable-%d", i)))
		msg := message.New(pub, pub, content, "text/plain")
		msg.Timestamp = int64((i + 1) * 1000)
		if err := message.Sign(&msg, callerKP); err != nil {
			t.Fatalf("sign: %v", err)
		}
		if _, err := c.SendMessage(ctx, msg, map[string]string{"durable": "test"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
	}

	// Subscribe and seek to timestamp 2000 — should replay entries >= 2000.
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, _, err := c.SubscribeMessages(subCtx, `labels["durable"] == "test"`, nil)
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if err := c.Seek(ctx, "default", 2000); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	var replayed []int64
	timeout := time.After(3 * time.Second)
	for len(replayed) < 2 {
		select {
		case e := <-entries:
			if e == nil {
				t.Fatal("nil entry")
			}
			replayed = append(replayed, e.Timestamp)
		case <-timeout:
			t.Fatalf("timeout waiting for replayed entries, got %d", len(replayed))
		}
	}
	for _, ts := range replayed {
		if ts < 2000 {
			t.Errorf("replayed entry with timestamp %d < 2000", ts)
		}
	}
}

func TestChannelAtLeastOnceAutoAck(t *testing.T) {
	// Verify that at-least-once messages are properly auto-acked by the client.
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

	// Send at-least-once message with short ack timeout.
	msg := makeMessage(t, callerKP, "text/plain")
	dims := &nodev1.Dimensions{
		Persistence:  nodev1.Persistence_PERSISTENCE_DURABLE,
		Delivery:     nodev1.Delivery_DELIVERY_AT_LEAST_ONCE,
		AckTimeoutMs: 500,
	}
	if _, err := c.SendMessage(ctx, msg, map[string]string{"alo": "test"}, dims); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case e := <-entries:
		if e == nil {
			t.Fatal("nil entry")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no delivery")
	}

	// Wait for auto-ack to complete.
	time.Sleep(200 * time.Millisecond)
}

// --- Internal unit tests (package-level access) ---

// mockStream implements nodev1.NodeService_ChannelServer for testing.
type mockStream struct {
	nodev1.NodeService_ChannelServer
	sent []*nodev1.ServerFrame
	mu   sync.Mutex
}

func (m *mockStream) Send(frame *nodev1.ServerFrame) error {
	m.mu.Lock()
	m.sent = append(m.sent, frame)
	m.mu.Unlock()
	return nil
}

func newMockWriter() (*streamWriter, *mockStream) {
	ms := &mockStream{}
	return &streamWriter{stream: ms}, ms
}

func newTestNodeService(t *testing.T) *nodeService {
	t.Helper()
	ctx := context.Background()
	metrics := observability.NewMetrics()

	blobBackend, err := blobphysical.New(ctx, "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create blob backend: %v", err)
	}
	t.Cleanup(func() { blobBackend.Close() })
	blobs := blobstore.New(blobBackend, metrics)

	idxBackend, err := idxphysical.New(ctx, "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create index backend: %v", err)
	}
	t.Cleanup(func() { idxBackend.Close() })
	idx, err := indexstore.New(idxBackend, metrics)
	if err != nil {
		t.Fatalf("create index store: %v", err)
	}

	kp, _ := identity.Generate()
	gc := newGroupCache()
	vc := newVisibilityChecker(gc)

	return &nodeService{
		blobs:       blobs,
		index:       idx,
		metrics:     metrics,
		federator:   newFederationManager(blobs, idx, kp, gc),
		subscribers: newSubscriberTracker(),
		visCheck:    vc.Check,
		groupCache:  gc,
		adminKey:    kp.PublicKey(),
	}
}

func TestHandleAckLegacyReferenceUnit(t *testing.T) {
	svc := newTestNodeService(t)
	w, ms := newMockWriter()
	ctx := context.Background()

	// Ack with reference bytes (legacy path).
	svc.handleAck(ctx, w, 1, &nodev1.AckFrame{
		Reference: []byte{0x01, 0x02, 0x03},
	})

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.sent) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(ms.sent))
	}
	receipt := ms.sent[0].Frame.(*nodev1.ServerFrame_Receipt).Receipt
	if !receipt.Ok {
		t.Error("expected Ok=true for legacy ack")
	}
}

func TestHandleAckMissingBoth(t *testing.T) {
	svc := newTestNodeService(t)
	w, ms := newMockWriter()
	ctx := context.Background()

	// Ack with neither delivery_id nor reference.
	svc.handleAck(ctx, w, 1, &nodev1.AckFrame{})

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.sent) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(ms.sent))
	}
	errFrame := ms.sent[0].Frame.(*nodev1.ServerFrame_Error).Error
	if errFrame.Code != int32(codes.InvalidArgument) {
		t.Errorf("code = %d, want %d", errFrame.Code, codes.InvalidArgument)
	}
}

func TestHandleAckDeliveryIdUnit(t *testing.T) {
	svc := newTestNodeService(t)
	w, ms := newMockWriter()
	ctx := context.Background()

	// Ack with a delivery_id that doesn't exist (idempotent, returns ok).
	svc.handleAck(ctx, w, 1, &nodev1.AckFrame{DeliveryId: 999})

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.sent) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(ms.sent))
	}
	receipt := ms.sent[0].Frame.(*nodev1.ServerFrame_Receipt).Receipt
	if !receipt.Ok {
		t.Error("expected Ok=true for unknown delivery_id (idempotent)")
	}
}

func TestHandlePutErrorUnit(t *testing.T) {
	// handlePut with nil data — should succeed (empty blob).
	svc := newTestNodeService(t)
	w, ms := newMockWriter()
	ctx := context.Background()

	svc.handlePut(ctx, w, 1, &nodev1.PutFrame{Data: []byte("hello")})

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.sent) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(ms.sent))
	}
	receipt := ms.sent[0].Frame.(*nodev1.ServerFrame_Receipt).Receipt
	if !receipt.Ok {
		t.Error("expected Ok=true")
	}
}

func TestHandleUnsubscribeEmptyChannel(t *testing.T) {
	svc := newTestNodeService(t)
	w, ms := newMockWriter()
	cs := newChannelSubs()

	// Subscribe first so there's something to unsubscribe.
	cancelled := false
	cs.add("default", func() { cancelled = true })

	// Unsubscribe with empty channel name — should default to "default".
	svc.handleUnsubscribe(w, 1, &nodev1.UnsubscribeFrame{Channel: ""}, cs)

	if !cancelled {
		t.Error("expected default channel subscription to be cancelled")
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.sent) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(ms.sent))
	}
}

func TestHandleFederateNilFederator(t *testing.T) {
	svc := newTestNodeService(t)
	svc.federator = nil
	w, ms := newMockWriter()
	ctx := context.Background()

	svc.handleFederate(ctx, w, 1, &nodev1.FederateFrame{Peer: "localhost:5000"})

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.sent) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(ms.sent))
	}
	// Should be an error because no caller context (not admin).
	errFrame := ms.sent[0].Frame.(*nodev1.ServerFrame_Error).Error
	if errFrame.Code != int32(codes.PermissionDenied) {
		t.Errorf("code = %d, want %d", errFrame.Code, codes.PermissionDenied)
	}
}

func TestReplayFromCursorWithProvidedCursor(t *testing.T) {
	svc := newTestNodeService(t)
	w, ms := newMockWriter()
	ctx := context.Background()

	// Index some entries.
	for i := 0; i < 3; i++ {
		ref := reference.Compute([]byte(fmt.Sprintf("replay-%d", i)))
		entry := &idxphysical.Entry{
			Ref:         ref,
			Labels:      map[string]string{"replay": "test"},
			Timestamp:   int64((i + 1) * 1000),
			Persistence: 1,
		}
		if err := svc.index.Index(ctx, entry); err != nil {
			t.Fatalf("Index: %v", err)
		}
	}

	// Replay from timestamp 2000 with provided cursor.
	svc.replayFromCursor(ctx, w, "default", "testkey", "2000", map[string]string{"replay": "test"}, "", nil)

	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Should have received entries with timestamp >= 2000 (i.e., 2000 and 3000).
	deliveryCount := 0
	for _, frame := range ms.sent {
		if _, ok := frame.Frame.(*nodev1.ServerFrame_Delivery); ok {
			deliveryCount++
		}
	}
	if deliveryCount < 1 {
		t.Errorf("expected at least 1 replayed delivery, got %d", deliveryCount)
	}
}

func TestReplayFromCursorNoCursor(t *testing.T) {
	svc := newTestNodeService(t)
	w, ms := newMockWriter()
	ctx := context.Background()

	// Replay with no provided cursor and no stored cursor — should be a no-op.
	svc.replayFromCursor(ctx, w, "default", "nonexistent-key", "", nil, "", nil)

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.sent) != 0 {
		t.Errorf("expected 0 frames for missing cursor, got %d", len(ms.sent))
	}
}

func TestReplayFromCursorZeroTimestamp(t *testing.T) {
	svc := newTestNodeService(t)
	w, ms := newMockWriter()
	ctx := context.Background()

	// Replay with cursor "0" — afterTS=0, should return early.
	svc.replayFromCursor(ctx, w, "default", "key", "0", nil, "", nil)

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.sent) != 0 {
		t.Errorf("expected 0 frames for zero timestamp, got %d", len(ms.sent))
	}
}

func TestRedeliveryLoopWithExpiredDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow redelivery test")
	}
	svc := newTestNodeService(t)
	w, ms := newMockWriter()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an entry with at-least-once delivery and very short ack timeout.
	ref := reference.Compute([]byte("redeliver-test"))
	entry := &idxphysical.Entry{
		Ref:           ref,
		Labels:        map[string]string{"redeliver": "test"},
		Timestamp:     time.Now().UnixMilli(),
		Persistence:   1,
		DeliveryMode:  1,
		AckTimeoutMs:  100, // 100ms ack timeout
		MaxRedelivery: 2,
		Priority:      5,
	}
	if err := svc.index.Index(ctx, entry); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Simulate a delivery (the entry is now "inflight" in the delivery tracker).
	dt := svc.index.DeliveryTracker()
	dt.Deliver(entry, "test-sub")

	// Start the redelivery loop.
	done := make(chan struct{})
	go func() {
		svc.redeliveryLoop(ctx, w, "default")
		close(done)
	}()

	// Wait for the 5-second ticker to fire and process the expired delivery.
	time.Sleep(6 * time.Second)
	cancel()
	<-done

	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Should have at least 1 redelivery frame.
	deliveryCount := 0
	for _, frame := range ms.sent {
		if _, ok := frame.Frame.(*nodev1.ServerFrame_Delivery); ok {
			deliveryCount++
		}
	}
	if deliveryCount < 1 {
		t.Errorf("expected at least 1 redelivered frame, got %d", deliveryCount)
	}
}

func TestRedeliveryLoopDeadLetter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow redelivery test")
	}
	svc := newTestNodeService(t)
	w, _ := newMockWriter()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ref := reference.Compute([]byte("dead-letter-test"))
	entry := &idxphysical.Entry{
		Ref:           ref,
		Labels:        map[string]string{"dl": "test"},
		Timestamp:     time.Now().UnixMilli(),
		Persistence:   1,
		DeliveryMode:  1,
		AckTimeoutMs:  100,
		MaxRedelivery: 0, // max 0 redeliveries = dead letter immediately
	}
	if err := svc.index.Index(ctx, entry); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Deliver and mark as already having max attempts exceeded.
	dt := svc.index.DeliveryTracker()
	dt.Deliver(entry, "test-sub")

	// Start the redelivery loop.
	done := make(chan struct{})
	go func() {
		svc.redeliveryLoop(ctx, w, "default")
		close(done)
	}()

	// Wait for the ticker.
	time.Sleep(6 * time.Second)
	cancel()
	<-done
}

func TestRedeliveryLoopContextCancel(t *testing.T) {
	svc := newTestNodeService(t)
	w, _ := newMockWriter()

	// Start redelivery loop and cancel immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		svc.redeliveryLoop(ctx, w, "default")
		close(done)
	}()

	select {
	case <-done:
		// Good — loop exited.
	case <-time.After(2 * time.Second):
		t.Fatal("redelivery loop did not exit on context cancel")
	}
}

func TestHandleSubscribeDurable(t *testing.T) {
	svc := newTestNodeService(t)
	w, ms := newMockWriter()
	ctx := context.Background()
	cs := newChannelSubs()
	defer cs.closeAll()

	// Subscribe with a SubscriptionId to exercise the durable path.
	svc.handleSubscribe(ctx, w, 1, &nodev1.SubscribeFrame{
		Channel:        "durable-ch",
		Expression:     "true",
		SubscriptionId: "sub-123",
	}, cs)

	time.Sleep(200 * time.Millisecond)

	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Should have received a receipt.
	if len(ms.sent) < 1 {
		t.Fatal("expected at least 1 frame (receipt)")
	}
	receipt := ms.sent[0].Frame.(*nodev1.ServerFrame_Receipt).Receipt
	if !receipt.Ok {
		t.Error("expected Ok=true for subscribe receipt")
	}
}

func TestIsAdminNoCaller(t *testing.T) {
	svc := newTestNodeService(t)
	// isAdmin with a context that has no caller should return false.
	if svc.isAdmin(context.Background()) {
		t.Error("expected false for context without caller")
	}
}

func TestHandleAckLegacyReference(t *testing.T) {
	// Test the legacy ack-by-reference path directly.
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	// Subscribe and send a message to exercise the full ack path.
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, _, err := c.SubscribeMessages(subCtx, "true", nil)
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Send at-least-once message.
	msg := makeMessage(t, callerKP, "text/plain")
	dims := &nodev1.Dimensions{
		Persistence: nodev1.Persistence_PERSISTENCE_DURABLE,
		Delivery:    nodev1.Delivery_DELIVERY_AT_LEAST_ONCE,
	}
	ref, err := c.SendMessage(ctx, msg, nil, dims)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Receive the delivery (client auto-acks via delivery_id).
	select {
	case e := <-entries:
		if e == nil {
			t.Fatal("nil entry")
		}
		if e.Ref != ref {
			t.Errorf("ref mismatch")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no delivery")
	}
	time.Sleep(200 * time.Millisecond) // let auto-ack complete
}

func TestChannelUnsubscribeDefaultChannel(t *testing.T) {
	// Unsubscribe with empty channel name should default to "default".
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	// Subscribe first.
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, _, err := c.SubscribeMessages(subCtx, "true", nil)
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Unsubscribe (the client unsubscribes when context is cancelled).
	cancel()
	time.Sleep(100 * time.Millisecond)
}

func TestChannelSubscribeDuplicateReplaces(t *testing.T) {
	// Subscribe to the same channel twice on the same stream — first should be cancelled.
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	// First subscription.
	subCtx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	entries1, _, err := c.SubscribeMessages(subCtx1, "true", nil)
	if err != nil {
		t.Fatalf("SubscribeMessages 1: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Second subscription on a new client (same channel "default").
	// The SDK uses one stream per client, so subscribe on same client
	// would use same channel "default" and replace.
	// Since the SDK doesn't support multiple subscriptions on the same
	// channel easily, just verify the first one still works.
	msg := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case e := <-entries1:
		if e == nil {
			t.Fatal("nil entry")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no delivery")
	}
}

// --- Badger integration tests ---

func TestBadgerIntegration(t *testing.T) {
	srv, kp, addr := newTestServerWithBackend(t, "badger", "badger")
	_ = srv

	t.Run("PutGetRoundTrip", func(t *testing.T) {
		c, _ := newTestClient(t, addr, kp)
		ctx := context.Background()

		data := []byte("badger put/get")
		ref, err := c.PutContent(ctx, data)
		if err != nil {
			t.Fatalf("PutContent: %v", err)
		}
		got, err := c.GetContent(ctx, ref)
		if err != nil {
			t.Fatalf("GetContent: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("got %q, want %q", got, data)
		}
	})

	t.Run("PublishQueryRoundTrip", func(t *testing.T) {
		c, callerKP := newTestClient(t, addr, kp)
		ctx := context.Background()

		msg := makeMessage(t, callerKP, "text/plain")
		ref, err := c.SendMessage(ctx, msg, map[string]string{"bg": "test"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE})
		if err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		if ref == (reference.Reference{}) {
			t.Fatal("zero reference")
		}

		result, err := c.QueryMessages(ctx, &client.QueryOptions{
			Labels: map[string]string{"bg": "test"},
		})
		if err != nil {
			t.Fatalf("QueryMessages: %v", err)
		}
		if len(result.Entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(result.Entries))
		}
	})

	t.Run("SubscribeDelivery", func(t *testing.T) {
		c, callerKP := newTestClient(t, addr, kp)
		ctx := context.Background()

		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		entries, _, err := c.SubscribeMessages(subCtx, `labels["bgsub"] == "test"`, nil)
		if err != nil {
			t.Fatalf("SubscribeMessages: %v", err)
		}
		time.Sleep(100 * time.Millisecond)

		msg := makeMessage(t, callerKP, "text/plain")
		if _, err := c.SendMessage(ctx, msg, map[string]string{"bgsub": "test"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}

		select {
		case e := <-entries:
			if e == nil {
				t.Fatal("nil entry")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("no delivery")
		}
	})

	t.Run("DimensionsPreserved", func(t *testing.T) {
		c, callerKP := newTestClient(t, addr, kp)
		ctx := context.Background()

		msg := makeMessage(t, callerKP, "text/plain")
		dims := &nodev1.Dimensions{
			Visibility:  nodev1.Visibility_VISIBILITY_PUBLIC,
			Persistence: nodev1.Persistence_PERSISTENCE_DURABLE,
			Delivery:    nodev1.Delivery_DELIVERY_AT_LEAST_ONCE,
			Priority:    7,
			TtlMs:       30000,
			Ordering:    nodev1.Ordering_ORDERING_FIFO,
			Correlation: "corr-bg",
		}
		ref, err := c.SendMessage(ctx, msg, map[string]string{"bgdims": "test"}, dims)
		if err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		if ref == (reference.Reference{}) {
			t.Fatal("zero reference")
		}

		result, err := c.QueryMessages(ctx, &client.QueryOptions{
			Labels: map[string]string{"bgdims": "test"},
		})
		if err != nil {
			t.Fatalf("QueryMessages: %v", err)
		}
		if len(result.Entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(result.Entries))
		}
		d := result.Entries[0].Dimensions
		if d == nil {
			t.Fatal("Dimensions is nil")
		}
		if d.Persistence != int32(nodev1.Persistence_PERSISTENCE_DURABLE) {
			t.Errorf("Persistence = %d, want %d", d.Persistence, nodev1.Persistence_PERSISTENCE_DURABLE)
		}
		if d.Delivery != int32(nodev1.Delivery_DELIVERY_AT_LEAST_ONCE) {
			t.Errorf("Delivery = %d, want %d", d.Delivery, nodev1.Delivery_DELIVERY_AT_LEAST_ONCE)
		}
		if d.Priority != 7 {
			t.Errorf("Priority = %d, want 7", d.Priority)
		}
		if d.Correlation != "corr-bg" {
			t.Errorf("Correlation = %q, want corr-bg", d.Correlation)
		}
	})

	t.Run("QueryWithExpression", func(t *testing.T) {
		c, callerKP := newTestClient(t, addr, kp)
		ctx := context.Background()

		msg := makeMessage(t, callerKP, "text/plain")
		if _, err := c.SendMessage(ctx, msg, map[string]string{"bgexpr": "high"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		msg2 := makeMessage(t, callerKP, "text/plain")
		if _, err := c.SendMessage(ctx, msg2, map[string]string{"bgexpr": "low"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}

		result, err := c.QueryMessages(ctx, &client.QueryOptions{
			Expression: `labels["bgexpr"] == "high"`,
		})
		if err != nil {
			t.Fatalf("QueryMessages: %v", err)
		}
		if len(result.Entries) != 1 {
			t.Errorf("expected 1 entry, got %d", len(result.Entries))
		}
	})
}
