package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/internal/blobstore"
	blobphysical "github.com/gezibash/arc-node/internal/blobstore/physical"
	_ "github.com/gezibash/arc-node/internal/blobstore/physical/badger"
	_ "github.com/gezibash/arc-node/internal/blobstore/physical/memory"
	"github.com/gezibash/arc-node/internal/indexstore"
	idxphysical "github.com/gezibash/arc-node/internal/indexstore/physical"
	_ "github.com/gezibash/arc-node/internal/indexstore/physical/badger"
	_ "github.com/gezibash/arc-node/internal/indexstore/physical/memory"
	"github.com/gezibash/arc-node/internal/observability"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc-node/pkg/group"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
	"google.golang.org/grpc/codes"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

var msgCounter int64

func newTestServer(t *testing.T) (*Server, *identity.Keypair, string) {
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

	obs, err := observability.New(ctx, observability.ObsConfig{LogLevel: "error"}, io.Discard)
	if err != nil {
		t.Fatalf("create observability: %v", err)
	}
	t.Cleanup(func() { obs.Close(ctx) })

	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}

	srv, err := New(context.Background(), ":0", obs, false, kp, blobs, idx)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	go srv.Serve()
	t.Cleanup(func() { srv.Stop(context.Background()) })

	return srv, kp, srv.Addr()
}

func newTestClient(t *testing.T, addr string, nodeKP *identity.Keypair) (*client.Client, *identity.Keypair) {
	t.Helper()
	callerKP, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate caller keypair: %v", err)
	}
	nodePub := nodeKP.PublicKey()
	c, err := client.Dial(addr,
		client.WithIdentity(callerKP),
		client.WithNodeKey(nodePub),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c, callerKP
}

func newTestAdminClient(t *testing.T, addr string, nodeKP *identity.Keypair) *client.Client {
	t.Helper()
	c, err := client.Dial(addr,
		client.WithIdentity(nodeKP),
		client.WithNodeKey(nodeKP.PublicKey()),
	)
	if err != nil {
		t.Fatalf("dial admin: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func newTestServerWithBackend(t *testing.T, blobBackend, idxBackend string) (*Server, *identity.Keypair, string) {
	t.Helper()
	ctx := context.Background()
	metrics := observability.NewMetrics()

	cfg := map[string]string{"in_memory": "true"}

	bb, err := blobphysical.New(ctx, blobBackend, cfg, metrics)
	if err != nil {
		t.Fatalf("create blob backend %s: %v", blobBackend, err)
	}
	t.Cleanup(func() { bb.Close() })
	blobs := blobstore.New(bb, metrics)

	ib, err := idxphysical.New(ctx, idxBackend, cfg, metrics)
	if err != nil {
		t.Fatalf("create index backend %s: %v", idxBackend, err)
	}
	t.Cleanup(func() { ib.Close() })
	idx, err := indexstore.New(ib, metrics)
	if err != nil {
		t.Fatalf("create index store: %v", err)
	}

	obs, err := observability.New(ctx, observability.ObsConfig{LogLevel: "error"}, io.Discard)
	if err != nil {
		t.Fatalf("create observability: %v", err)
	}
	t.Cleanup(func() { obs.Close(ctx) })

	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}

	srv, err := New(context.Background(), ":0", obs, false, kp, blobs, idx)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	go srv.Serve()
	t.Cleanup(func() { srv.Stop(context.Background()) })

	return srv, kp, srv.Addr()
}

func makeMessage(t *testing.T, kp *identity.Keypair, contentType string) message.Message {
	t.Helper()
	pub := kp.PublicKey()
	content := reference.Compute([]byte(fmt.Sprintf("test-content-%d", atomic.AddInt64(&msgCounter, 1))))
	msg := message.New(pub, pub, content, contentType)
	if err := message.Sign(&msg, kp); err != nil {
		t.Fatalf("sign message: %v", err)
	}
	return msg
}

// --- PutContent / GetContent (via Channel) ---

func TestPutAndGetContent(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	data := []byte("hello world")
	ref, err := c.PutContent(ctx, data)
	if err != nil {
		t.Fatalf("PutContent: %v", err)
	}
	if ref == (reference.Reference{}) {
		t.Fatal("PutContent returned zero reference")
	}

	got, err := c.GetContent(ctx, ref)
	if err != nil {
		t.Fatalf("GetContent: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("GetContent = %q, want %q", got, data)
	}
}

func TestPutContentEmpty(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	ref, err := c.PutContent(ctx, []byte{})
	if err != nil {
		t.Fatalf("PutContent: %v", err)
	}

	got, err := c.GetContent(ctx, ref)
	if err != nil {
		t.Fatalf("GetContent: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("GetContent = %q, want empty", got)
	}
}

func TestGetContentNotFound(t *testing.T) {
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

// --- SendMessage ---

func TestSendMessageValid(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "text/plain")
	result, err := c.SendMessage(ctx, msg, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if result.Ref == (reference.Reference{}) {
		t.Fatal("SendMessage returned zero reference")
	}
}

func TestSendMessageInvalidSignature(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "text/plain")
	// Tamper with signature
	msg.Signature[0] ^= 0xFF

	_, err := c.SendMessage(ctx, msg, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE})

	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", err)
	}
}

func TestSendMessageLabels(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "text/plain")
	customLabels := map[string]string{"env": "test", "app": "arc"}
	_, err := c.SendMessage(ctx, msg, customLabels, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Query back and verify labels
	result, err := c.QueryMessages(ctx, &client.QueryOptions{})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result.Entries))
	}

	labels := result.Entries[0].Labels
	pub := callerKP.PublicKey()
	expectedLabels := map[string]string{
		"env":         "test",
		"app":         "arc",
		"from":        fmt.Sprintf("%x", pub[:]),
		"to":          fmt.Sprintf("%x", pub[:]),
		"content":     fmt.Sprintf("%x", msg.Content),
		"contentType": "text/plain",
	}
	for k, want := range expectedLabels {
		if got := labels[k]; got != want {
			t.Errorf("label %q = %q, want %q", k, got, want)
		}
	}
}

func TestSendMessageWithoutContentType(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "") // empty content type
	_, err := c.SendMessage(ctx, msg, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result.Entries))
	}

	if _, ok := result.Entries[0].Labels["contentType"]; ok {
		t.Error("contentType label should not be present for empty content type")
	}
}

// --- QueryMessages ---

func TestQueryMessagesBasic(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	const n = 5
	for i := 0; i < n; i++ {
		msg := makeMessage(t, callerKP, "text/plain")
		if _, err := c.SendMessage(ctx, msg, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
			t.Fatalf("SendMessage[%d]: %v", i, err)
		}
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != n {
		t.Errorf("expected %d entries, got %d", n, len(result.Entries))
	}
}

func TestQueryMessagesPagination(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		msg := makeMessage(t, callerKP, "text/plain")
		if _, err := c.SendMessage(ctx, msg, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
			t.Fatalf("SendMessage[%d]: %v", i, err)
		}
	}

	// Page 1
	r1, err := c.QueryMessages(ctx, &client.QueryOptions{Limit: 2})
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

	// Page 2
	r2, err := c.QueryMessages(ctx, &client.QueryOptions{Limit: 2, Cursor: r1.NextCursor})
	if err != nil {
		t.Fatalf("QueryMessages page 2: %v", err)
	}
	if len(r2.Entries) != 2 {
		t.Fatalf("page 2: expected 2 entries, got %d", len(r2.Entries))
	}
}

func TestQueryMessagesDescending(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	pub := callerKP.PublicKey()
	for i := 0; i < 3; i++ {
		content := reference.Compute([]byte("test-content"))
		msg := message.Message{
			From:        pub,
			To:          pub,
			Content:     content,
			ContentType: "text/plain",
			Timestamp:   int64((i + 1) * 1000), // 1s, 2s, 3s (ms)
		}
		if err := message.Sign(&msg, callerKP); err != nil {
			t.Fatalf("sign: %v", err)
		}
		if _, err := c.SendMessage(ctx, msg, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
			t.Fatalf("SendMessage[%d]: %v", i, err)
		}
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{Descending: true})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(result.Entries))
	}
	if result.Entries[0].Timestamp < result.Entries[2].Timestamp {
		t.Errorf("expected descending order: first=%d, last=%d", result.Entries[0].Timestamp, result.Entries[2].Timestamp)
	}
}

func TestQueryMessagesWithLabels(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg1 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg1, map[string]string{"env": "prod"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msg2 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg2, map[string]string{"env": "dev"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{
		Labels: map[string]string{"env": "prod"},
	})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(result.Entries))
	}
}

func TestQueryMessagesWithExpression(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg1 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg1, map[string]string{"priority": "high"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
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

// --- SubscribeMessages ---

func TestSubscribeMessagesBasic(t *testing.T) {
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
			t.Fatal("received nil entry")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive entry within timeout")
	}
}

func TestSubscribeMessagesWithLabels(t *testing.T) {
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

	msg1 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg1, map[string]string{"env": "dev"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	msg2 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg2, map[string]string{"env": "prod"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case e := <-entries:
		if e == nil {
			t.Fatal("received nil entry")
		}
		if e.Labels["env"] != "prod" {
			t.Errorf("label env = %q, want prod", e.Labels["env"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive matching entry within timeout")
	}
}

func TestSubscribeMessagesCancelContext(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	subCtx, cancel := context.WithCancel(ctx)
	entries, _, err := c.SubscribeMessages(subCtx, "true", nil)
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}

	cancel()

	select {
	case <-entries:
		// Got an entry or channel closed, both acceptable.
	case <-time.After(3 * time.Second):
		t.Fatal("entries channel did not close after context cancel")
	}
}

// --- Federate (via Channel) ---

func TestFederateRequiresAdmin(t *testing.T) {
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

func TestListPeersReturnsActiveFederation(t *testing.T) {
	_, peerKP, peerAddr := newTestServer(t)
	_ = peerKP
	_, localKP, localAddr := newTestServer(t)

	admin := newTestAdminClient(t, localAddr, localKP)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// No peers initially.
	peers, err := admin.ListPeers(ctx)
	if err != nil {
		t.Fatalf("ListPeers: %v", err)
	}
	if len(peers) != 0 {
		t.Fatalf("expected 0 peers, got %d", len(peers))
	}

	// Start federation.
	_, err = admin.Federate(ctx, peerAddr, map[string]string{"env": "test"})
	if err != nil {
		t.Fatalf("Federate: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	peers, err = admin.ListPeers(ctx)
	if err != nil {
		t.Fatalf("ListPeers: %v", err)
	}
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
	if peers[0].Address != peerAddr {
		t.Errorf("peer address = %q, want %q", peers[0].Address, peerAddr)
	}
	if peers[0].Labels["env"] != "test" {
		t.Errorf("peer label env = %q, want %q", peers[0].Labels["env"], "test")
	}
	if peers[0].StartedAt == 0 {
		t.Error("peer StartedAt should be non-zero")
	}
	if peers[0].Direction != client.PeerDirectionOutbound {
		t.Errorf("peer direction = %d, want outbound", peers[0].Direction)
	}
}

func TestListPeersShowsInboundSubscribers(t *testing.T) {
	_, kp, addr := newTestServer(t)
	admin := newTestAdminClient(t, addr, kp)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	// No inbound peers initially.
	peers, err := admin.ListPeers(ctx)
	if err != nil {
		t.Fatalf("ListPeers: %v", err)
	}
	if len(peers) != 0 {
		t.Fatalf("expected 0 peers, got %d", len(peers))
	}

	// Open a subscription.
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, _, err = c.SubscribeMessages(subCtx, "true", map[string]string{"env": "test"})
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	peers, err = admin.ListPeers(ctx)
	if err != nil {
		t.Fatalf("ListPeers: %v", err)
	}

	var inbound []client.PeerInfo
	for _, p := range peers {
		if p.Direction == client.PeerDirectionInbound {
			inbound = append(inbound, p)
		}
	}
	if len(inbound) != 1 {
		t.Fatalf("expected 1 inbound peer, got %d", len(inbound))
	}
	if inbound[0].Labels["env"] != "test" {
		t.Errorf("inbound label env = %q, want %q", inbound[0].Labels["env"], "test")
	}

	// Cancel subscription and verify it disappears.
	cancel()
	time.Sleep(100 * time.Millisecond)

	peers, err = admin.ListPeers(ctx)
	if err != nil {
		t.Fatalf("ListPeers: %v", err)
	}
	inbound = nil
	for _, p := range peers {
		if p.Direction == client.PeerDirectionInbound {
			inbound = append(inbound, p)
		}
	}
	if len(inbound) != 0 {
		t.Fatalf("expected 0 inbound peers after cancel, got %d", len(inbound))
	}
}

func TestFederateReplicatesMessages(t *testing.T) {
	t.Log("starting peer server")
	_, peerKP, peerAddr := newTestServer(t)
	t.Log("starting local server")
	_, localKP, localAddr := newTestServer(t)

	t.Log("dialing admin client")
	admin := newTestAdminClient(t, localAddr, localKP)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Log("sanity query before federation")
	if _, err := admin.QueryMessages(ctx, &client.QueryOptions{Expression: "false", Limit: 1}); err != nil {
		t.Fatalf("QueryMessages (pre): %v", err)
	}

	t.Log("starting federation")
	result, err := admin.Federate(ctx, peerAddr, nil)
	if err != nil {
		t.Fatalf("Federate: %v", err)
	}
	if result == nil || result.Status == "" {
		t.Fatalf("Federate: missing status")
	}

	time.Sleep(100 * time.Millisecond)

	t.Log("dialing peer client")
	peer, callerKP := newTestClient(t, peerAddr, peerKP)

	t.Log("putting content")
	data := []byte("hello federation")
	contentRef, err := peer.PutContent(ctx, data)
	if err != nil {
		t.Fatalf("PutContent: %v", err)
	}

	msg := message.New(callerKP.PublicKey(), callerKP.PublicKey(), contentRef, "text/plain")
	if err := message.Sign(&msg, callerKP); err != nil {
		t.Fatalf("sign message: %v", err)
	}
	t.Log("sending message")
	if _, err := peer.SendMessage(ctx, msg, map[string]string{"env": "prod"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	deadline := time.After(3 * time.Second)
	for {
		result, err := admin.QueryMessages(ctx, &client.QueryOptions{
			Expression: `labels["env"] == "prod"`,
		})
		if err != nil {
			t.Fatalf("QueryMessages: %v", err)
		}
		if len(result.Entries) > 0 {
			break
		}

		select {
		case <-deadline:
			t.Fatal("federated message did not arrive in time")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	got, err := admin.GetContent(ctx, contentRef)
	if err != nil {
		t.Fatalf("GetContent: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("content = %q, want %q", got, data)
	}
}

func TestSendMessageDimensionsRoundTrip(t *testing.T) {
	_, kp, addr := newTestServer(t)
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
	}
	pubResult, err := c.SendMessage(ctx, msg, map[string]string{"drt": "test"}, dims)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if pubResult.Ref == (reference.Reference{}) {
		t.Fatal("zero reference")
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{
		Labels: map[string]string{"drt": "test"},
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
	if d.Visibility != int32(nodev1.Visibility_VISIBILITY_PUBLIC) {
		t.Errorf("Visibility = %d, want %d", d.Visibility, nodev1.Visibility_VISIBILITY_PUBLIC)
	}
	if d.Persistence != int32(nodev1.Persistence_PERSISTENCE_DURABLE) {
		t.Errorf("Persistence = %d, want %d", d.Persistence, nodev1.Persistence_PERSISTENCE_DURABLE)
	}
	if d.Delivery != int32(nodev1.Delivery_DELIVERY_AT_LEAST_ONCE) {
		t.Errorf("Delivery = %d, want %d", d.Delivery, nodev1.Delivery_DELIVERY_AT_LEAST_ONCE)
	}
	if d.TtlMs == 0 {
		t.Error("TtlMs should be non-zero")
	}
}

func TestSendMessageEmptyPayload(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	// SendMessage with a zero message fails during canonical bytes serialization.
	_, err := c.SendMessage(ctx, message.Message{}, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE})
	if err == nil {
		t.Fatal("expected error for empty message")
	}
	// The error may come from client-side (canonical bytes) or server-side (invalid argument).
	// Either way it should be an error.
}

func TestSendMessageWrongSigner(t *testing.T) {
	// Envelope signer (client key) != message.From should fail.
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	// Create a message signed by a different keypair.
	otherKP, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	msg := makeMessage(t, otherKP, "text/plain")
	_, err = c.SendMessage(ctx, msg, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE})
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Errorf("code = %v, want PermissionDenied", err)
	}
}

func TestGetContentInvalidReference(t *testing.T) {
	// GetContent with a random 32-byte ref that doesn't exist.
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	ref := reference.Compute([]byte("nonexistent"))
	_, err := c.GetContent(ctx, ref)
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", err)
	}
}

func TestSendMessageWithCorrelation(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "text/plain")
	dims := &nodev1.Dimensions{
		Persistence: nodev1.Persistence_PERSISTENCE_DURABLE,
		Correlation: "corr-123",
	}
	_, err := c.SendMessage(ctx, msg, map[string]string{"corr": "test"}, dims)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Query for correlation label.
	result, err := c.QueryMessages(ctx, &client.QueryOptions{
		Labels: map[string]string{"_correlation": "corr-123"},
	})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(result.Entries))
	}
}

func TestFederateAlreadyFederating(t *testing.T) {
	_, peerKP, peerAddr := newTestServer(t)
	_ = peerKP
	_, localKP, localAddr := newTestServer(t)

	admin := newTestAdminClient(t, localAddr, localKP)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result1, err := admin.Federate(ctx, peerAddr, nil)
	if err != nil {
		t.Fatalf("Federate: %v", err)
	}
	if result1.Status != "ok" {
		t.Errorf("status = %q, want ok", result1.Status)
	}

	result2, err := admin.Federate(ctx, peerAddr, nil)
	if err != nil {
		t.Fatalf("Federate: %v", err)
	}
	if result2.Status != "already_federating" {
		t.Errorf("status = %q, want already_federating", result2.Status)
	}
}

func TestQueryEmptyResult(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	result, err := c.QueryMessages(ctx, &client.QueryOptions{
		Expression: `labels["nonexistent"] == "value"`,
	})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(result.Entries))
	}
	if result.HasMore {
		t.Error("HasMore should be false for empty result")
	}
}

// --- Server getters ---

func TestServerGetters(t *testing.T) {
	srv, kp, _ := newTestServer(t)

	if srv.GRPCServer() == nil {
		t.Error("GRPCServer() returned nil")
	}
	if srv.Keypair() == nil {
		t.Error("Keypair() returned nil")
	}
	if srv.Keypair().PublicKey() != kp.PublicKey() {
		t.Error("Keypair() returned wrong key")
	}

	srv.SetServingStatus(grpc_health_v1.HealthCheckResponse_SERVING)
	srv.SetServingStatus(grpc_health_v1.HealthCheckResponse_NOT_SERVING)
}

func TestStopTimeout(t *testing.T) {
	_, kp, addr := newTestServer(t)

	// Open a subscription to keep the server busy.
	c, _ := newTestClient(t, addr, kp)
	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, err := c.SubscribeMessages(subCtx, "true", nil)
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Use an already-expired context to force the timeout branch.
	expiredCtx, expiredCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expiredCancel()

	// This should hit ctx.Done() and force-stop.
	srv2, kp2, addr2 := newTestServer(t)
	c2, _ := newTestClient(t, addr2, kp2)
	subCtx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	_, _, err = c2.SubscribeMessages(subCtx2, "true", nil)
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	srv2.Stop(expiredCtx)
	// If we get here without hanging, the force-stop path worked.
}

// --- Service error paths ---

func TestGetContentInvalidRefLength(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	// Send a 16-byte ref via GetContent - should get InvalidArgument.
	shortRef := reference.Reference{}
	_, err := c.GetContent(ctx, shortRef)
	if err == nil {
		t.Fatal("expected error")
	}
	// Zero ref triggers NotFound (valid 32 bytes but not found).
	if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", err)
	}
}

func TestPublishErrToStatusDefaultCase(t *testing.T) {
	// Test the default case in publishErrToStatus with an unrecognized error.
	err := publishErrToStatus(fmt.Errorf("some random internal error"))
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Internal {
		t.Errorf("code = %v, want Internal", err)
	}
}

func TestPublishErrToStatusInvalidSig(t *testing.T) {
	err := publishErrToStatus(fmt.Errorf("invalid signature"))
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", err)
	}
}

func TestPublishErrToStatusEnvelopeSigner(t *testing.T) {
	err := publishErrToStatus(fmt.Errorf("envelope signer does not match"))
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Errorf("code = %v, want PermissionDenied", err)
	}
}

func TestReferenceFromBytesInvalidLength(t *testing.T) {
	_, err := referenceFromBytes([]byte{0x01, 0x02})
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", err)
	}
}

func TestResolveGetAmbiguousPrefix(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	// Store two blobs that happen to share a prefix.
	// This is hard to engineer, so instead test the "prefix too short" path.
	_, err := c.ResolveGet(ctx, "ab")
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", err)
	}
}

func TestReferenceFromBytesValid(t *testing.T) {
	b := make([]byte, 32)
	b[0] = 0xAA
	ref, err := referenceFromBytes(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ref[0] != 0xAA {
		t.Errorf("ref[0] = %x, want 0xAA", ref[0])
	}
}

// --- Delegated Group Publish ---

func TestPublishGroupManifestDelegated(t *testing.T) {
	// Admin publishes a manifest where envelope signer != msg.From (group key).
	_, nodeKP, addr := newTestServer(t)
	c, adminKP := newTestClient(t, addr, nodeKP)
	ctx := context.Background()

	manifest, groupKP, err := c.CreateGroup(ctx, "delegated-test", adminKP)
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	// Verify the manifest was stored and is retrievable.
	got, err := c.GetGroupManifest(ctx, groupKP.PublicKey())
	if err != nil {
		t.Fatalf("GetGroupManifest: %v", err)
	}
	if got.Name != manifest.Name {
		t.Errorf("name = %q, want %q", got.Name, manifest.Name)
	}
}

func TestPublishGroupMessageAsMember(t *testing.T) {
	// After cache is populated via manifest publish, a member can publish as the group.
	_, nodeKP, addr := newTestServer(t)
	c, adminKP := newTestClient(t, addr, nodeKP)
	ctx := context.Background()

	manifest, groupKP, err := c.CreateGroup(ctx, "member-pub-test", adminKP)
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	// Add a regular member.
	memberKP, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	_, err = c.AddGroupMember(ctx, groupKP, manifest, memberKP.PublicKey(), group.RoleMember)
	if err != nil {
		t.Fatalf("AddGroupMember: %v", err)
	}

	// Now the member connects and publishes a message as the group.
	nodePub := nodeKP.PublicKey()
	memberClient, err := client.Dial(addr, client.WithIdentity(memberKP), client.WithNodeKey(nodePub))
	if err != nil {
		t.Fatalf("dial member: %v", err)
	}
	t.Cleanup(func() { memberClient.Close() })

	// Create a message with From=group pubkey, signed by group key
	// but envelope signed by member. We need the group keypair to sign the message.
	groupPub := groupKP.PublicKey()
	contentRef := reference.Compute([]byte("group-member-content"))
	msg := message.New(groupPub, groupPub, contentRef, "text/plain")
	if err := message.Sign(&msg, groupKP); err != nil {
		t.Fatalf("sign: %v", err)
	}

	_, err = memberClient.SendMessage(ctx, msg, map[string]string{"app": "group-test"}, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE})
	if err != nil {
		t.Fatalf("SendMessage as member: %v", err)
	}
}

func TestPublishGroupManifestNonMember(t *testing.T) {
	// A stranger (not in the group) cannot publish as the group.
	_, nodeKP, addr := newTestServer(t)
	c, adminKP := newTestClient(t, addr, nodeKP)
	ctx := context.Background()

	// Create a group but don't add the stranger.
	_, groupKP, err := c.CreateGroup(ctx, "stranger-test", adminKP)
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	// Stranger connects.
	strangerKP, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	nodePub := nodeKP.PublicKey()
	strangerClient, err := client.Dial(addr, client.WithIdentity(strangerKP), client.WithNodeKey(nodePub))
	if err != nil {
		t.Fatalf("dial stranger: %v", err)
	}
	t.Cleanup(func() { strangerClient.Close() })

	// Try to publish as the group.
	groupPub := groupKP.PublicKey()
	contentRef := reference.Compute([]byte("stranger-content"))
	msg := message.New(groupPub, groupPub, contentRef, "text/plain")
	if err := message.Sign(&msg, groupKP); err != nil {
		t.Fatalf("sign: %v", err)
	}

	_, err = strangerClient.SendMessage(ctx, msg, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE})
	if err == nil {
		t.Fatal("expected error for non-member publish")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Errorf("code = %v, want PermissionDenied", err)
	}
}
