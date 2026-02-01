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
	_ "github.com/gezibash/arc-node/internal/blobstore/physical/memory"
	"github.com/gezibash/arc-node/internal/indexstore"
	idxphysical "github.com/gezibash/arc-node/internal/indexstore/physical"
	_ "github.com/gezibash/arc-node/internal/indexstore/physical/memory"
	"github.com/gezibash/arc-node/internal/observability"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/pkg/identity"
	"github.com/gezibash/arc/pkg/message"
	"github.com/gezibash/arc/pkg/reference"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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

	srv, err := New(":0", obs, false, kp, blobs, idx)
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

// --- PutContent / GetContent ---

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

func TestGetContentInvalidReference(t *testing.T) {
	_, kp, addr := newTestServer(t)
	ctx := context.Background()

	// Dial directly without envelope — should be rejected as Unauthenticated
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stub := nodev1.NewNodeServiceClient(conn)
	_, err = stub.GetContent(ctx, &nodev1.GetContentRequest{Reference: []byte{1, 2, 3, 4, 5}})
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Unauthenticated {
		t.Errorf("code = %v, want Unauthenticated", err)
	}

	// Also verify the correct error through the envelope client
	c, _ := newTestClient(t, addr, kp)
	_, err = c.GetContent(ctx, reference.Reference{})
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
	ref, err := c.SendMessage(ctx, msg, nil)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if ref == (reference.Reference{}) {
		t.Fatal("SendMessage returned zero reference")
	}
}

func TestSendMessageEmptyBytes(t *testing.T) {
	_, kp, addr := newTestServer(t)
	ctx := context.Background()

	// Dial directly without envelope — should be rejected as Unauthenticated
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stub := nodev1.NewNodeServiceClient(conn)
	_, err = stub.SendMessage(ctx, &nodev1.SendMessageRequest{})
	if err == nil {
		t.Fatal("expected error")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Unauthenticated {
		t.Errorf("code = %v, want Unauthenticated", err)
	}

	_ = kp // suppress unused
}

func TestSendMessageInvalidSignature(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "text/plain")
	// Tamper with signature
	msg.Signature[0] ^= 0xFF

	_, err := c.SendMessage(ctx, msg, nil)

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
	_, err := c.SendMessage(ctx, msg, customLabels)
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
	_, err := c.SendMessage(ctx, msg, nil)
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
		if _, err := c.SendMessage(ctx, msg, nil); err != nil {
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
		if _, err := c.SendMessage(ctx, msg, nil); err != nil {
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
		if _, err := c.SendMessage(ctx, msg, nil); err != nil {
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
	if _, err := c.SendMessage(ctx, msg1, map[string]string{"env": "prod"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msg2 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg2, map[string]string{"env": "dev"}); err != nil {
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
	if _, err := c.SendMessage(ctx, msg1, map[string]string{"priority": "high"}); err != nil {
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

	// Allow the subscription stream to be fully established
	time.Sleep(100 * time.Millisecond)

	// Send a message after subscribing
	msg := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg, nil); err != nil {
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

	// Allow the subscription stream to be fully established
	time.Sleep(100 * time.Millisecond)

	// Send non-matching message
	msg1 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg1, map[string]string{"env": "dev"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Send matching message
	msg2 := makeMessage(t, callerKP, "text/plain")
	if _, err := c.SendMessage(ctx, msg2, map[string]string{"env": "prod"}); err != nil {
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

	// Channel should close
	select {
	case _, ok := <-entries:
		if ok {
			// Got an entry before close, that's fine, drain
		}
	case <-time.After(3 * time.Second):
		t.Fatal("entries channel did not close after context cancel")
	}
}

// --- Federate ---

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
	if _, err := peer.SendMessage(ctx, msg, map[string]string{"env": "prod"}); err != nil {
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
