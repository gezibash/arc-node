package client_test

import (
	"context"
	"fmt"
	"io"
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
	"github.com/gezibash/arc-node/internal/server"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
)

func newTestServer(t *testing.T) (string, *identity.Keypair) {
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

	srv, err := server.New(":0", obs, false, kp, blobs, idx)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Stop(context.Background()) })

	return srv.Addr(), kp
}

func newTestClient(t *testing.T, addr string, nodeKP *identity.Keypair) (*client.Client, *identity.Keypair) {
	t.Helper()
	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	nodePub := nodeKP.PublicKey()
	c, err := client.Dial(addr, client.WithIdentity(kp), client.WithNodeKey(nodePub))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c, kp
}

// newAdminClient creates a client using the node's own keypair as identity,
// which grants admin privileges for operations like ListPeers and Federate.
func newAdminClient(t *testing.T, addr string, nodeKP *identity.Keypair) *client.Client {
	t.Helper()
	nodePub := nodeKP.PublicKey()
	c, err := client.Dial(addr, client.WithIdentity(nodeKP), client.WithNodeKey(nodePub))
	if err != nil {
		t.Fatalf("dial admin: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func makeMessage(t *testing.T, kp *identity.Keypair, contentRef reference.Reference, ct string) message.Message {
	t.Helper()
	pub := kp.PublicKey()
	nodePub := pub // self-addressed for tests
	msg := message.New(pub, nodePub, contentRef, ct)
	if err := message.Sign(&msg, kp); err != nil {
		t.Fatalf("sign message: %v", err)
	}
	return msg
}

func TestDialAndClose(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, _ := newTestClient(t, addr, nodeKP)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPutGetContent(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, _ := newTestClient(t, addr, nodeKP)
	ctx := context.Background()

	data := []byte("hello arc world")
	ref, err := c.PutContent(ctx, data)
	if err != nil {
		t.Fatalf("PutContent: %v", err)
	}
	if ref == (reference.Reference{}) {
		t.Fatal("PutContent returned zero ref")
	}

	got, err := c.GetContent(ctx, ref)
	if err != nil {
		t.Fatalf("GetContent: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("GetContent = %q, want %q", got, data)
	}
}

func TestSendAndQueryMessages(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, kp := newTestClient(t, addr, nodeKP)
	ctx := context.Background()

	// Use unique content per message to avoid content-addressed dedup.
	for i := 0; i < 3; i++ {
		ref, err := c.PutContent(ctx, []byte(fmt.Sprintf("content-%d", i)))
		if err != nil {
			t.Fatalf("PutContent: %v", err)
		}
		msg := makeMessage(t, kp, ref, "test/plain")
		labels := map[string]string{"app": "test", "type": "msg"}
		if _, err := c.SendMessage(ctx, msg, labels); err != nil {
			t.Fatalf("SendMessage[%d]: %v", i, err)
		}
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{
		Labels: map[string]string{"app": "test", "type": "msg"},
	})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(result.Entries))
	}

	// Pagination: descending, limit 2.
	r1, err := c.QueryMessages(ctx, &client.QueryOptions{
		Labels:     map[string]string{"app": "test", "type": "msg"},
		Limit:      2,
		Descending: true,
	})
	if err != nil {
		t.Fatalf("QueryMessages page1: %v", err)
	}
	if len(r1.Entries) != 2 {
		t.Fatalf("page1: expected 2, got %d", len(r1.Entries))
	}
	if !r1.HasMore {
		t.Error("page1: HasMore should be true")
	}
	if r1.NextCursor == "" {
		t.Fatal("page1: NextCursor is empty")
	}

	r2, err := c.QueryMessages(ctx, &client.QueryOptions{
		Labels:     map[string]string{"app": "test", "type": "msg"},
		Limit:      2,
		Cursor:     r1.NextCursor,
		Descending: true,
	})
	if err != nil {
		t.Fatalf("QueryMessages page2: %v", err)
	}
	if len(r2.Entries) != 1 {
		t.Fatalf("page2: expected 1, got %d", len(r2.Entries))
	}
	if r2.HasMore {
		t.Error("page2: HasMore should be false")
	}
}

func TestSendMessageWithDimensions(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, kp := newTestClient(t, addr, nodeKP)
	ctx := context.Background()

	ref, err := c.PutContent(ctx, []byte("dim-content"))
	if err != nil {
		t.Fatalf("PutContent: %v", err)
	}
	msg := makeMessage(t, kp, ref, "test/dims")
	dims := &nodev1.Dimensions{
		Persistence: nodev1.Persistence_PERSISTENCE_DURABLE,
		Priority:    5,
		Correlation: "corr-123",
	}
	labels := map[string]string{"app": "test", "type": "dims"}
	if _, err := c.SendMessageWithDimensions(ctx, msg, labels, dims); err != nil {
		t.Fatalf("SendMessageWithDimensions: %v", err)
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{
		Labels: map[string]string{"app": "test", "type": "dims"},
		Limit:  1,
	})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result.Entries))
	}
	e := result.Entries[0]
	if e.Dimensions == nil {
		t.Fatal("expected dimensions on entry")
	}
	if e.Dimensions.Persistence != int32(nodev1.Persistence_PERSISTENCE_DURABLE) {
		t.Errorf("persistence = %d, want %d", e.Dimensions.Persistence, nodev1.Persistence_PERSISTENCE_DURABLE)
	}
	if e.Dimensions.Priority != 5 {
		t.Errorf("priority = %d, want 5", e.Dimensions.Priority)
	}
	if e.Dimensions.Correlation != "corr-123" {
		t.Errorf("correlation = %q, want %q", e.Dimensions.Correlation, "corr-123")
	}
}

func TestSubscribeMessages(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, kp := newTestClient(t, addr, nodeKP)
	ctx := context.Background()

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, _, err := c.SubscribeMessages(subCtx, "true", map[string]string{"app": "sub-test"})
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	ref, _ := c.PutContent(ctx, []byte("sub-data"))
	msg := makeMessage(t, kp, ref, "test/sub")
	if _, err := c.SendMessage(ctx, msg, map[string]string{"app": "sub-test"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case e := <-entries:
		if e == nil {
			t.Fatal("received nil entry")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive subscribed entry within timeout")
	}
}

func TestResolveGet(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, kp := newTestClient(t, addr, nodeKP)
	ctx := context.Background()

	data := []byte("resolve-me")
	contentRef, err := c.PutContent(ctx, data)
	if err != nil {
		t.Fatalf("PutContent: %v", err)
	}

	msg := makeMessage(t, kp, contentRef, "test/resolve")
	labels := map[string]string{
		"app":     "test",
		"type":    "resolve",
		"content": reference.Hex(contentRef),
	}
	msgRef, err := c.SendMessage(ctx, msg, labels)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Resolve by message ref prefix.
	msgPrefix := reference.Hex(msgRef)[:12]
	result, err := c.ResolveGet(ctx, msgPrefix)
	if err != nil {
		t.Fatalf("ResolveGet(msg prefix): %v", err)
	}
	if result.Kind != client.GetKindMessage {
		t.Errorf("expected GetKindMessage, got %d", result.Kind)
	}

	// Resolve by blob ref prefix.
	blobPrefix := reference.Hex(contentRef)[:12]
	result, err = c.ResolveGet(ctx, blobPrefix)
	if err != nil {
		t.Fatalf("ResolveGet(blob prefix): %v", err)
	}
	if result.Kind != client.GetKindBlob {
		t.Errorf("expected GetKindBlob, got %d", result.Kind)
	}
	if string(result.Data) != string(data) {
		t.Errorf("blob data = %q, want %q", result.Data, data)
	}
}

func TestNodeInfoAndNodeKey(t *testing.T) {
	addr, nodeKP := newTestServer(t)

	nodePub := nodeKP.PublicKey()
	c, _ := newTestClient(t, addr, nodeKP)
	info, ok := c.NodeInfo()
	if !ok {
		t.Fatal("NodeInfo should return ok when WithNodeKey used")
	}
	if info.PublicKey != nodePub {
		t.Error("NodeInfo public key mismatch")
	}
	key, ok := c.NodeKey()
	if !ok {
		t.Fatal("NodeKey should return ok")
	}
	if key != nodePub {
		t.Error("NodeKey mismatch")
	}
}

func TestPing(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, _ := newTestClient(t, addr, nodeKP)
	ctx := context.Background()

	d, err := c.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if d <= 0 {
		t.Errorf("Ping duration = %v, want > 0", d)
	}
}

func TestListPeers(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c := newAdminClient(t, addr, nodeKP)
	ctx := context.Background()

	peers, err := c.ListPeers(ctx)
	if err != nil {
		t.Fatalf("ListPeers: %v", err)
	}
	if len(peers) != 0 {
		t.Errorf("expected 0 peers, got %d", len(peers))
	}
}

func TestSeek(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, kp := newTestClient(t, addr, nodeKP)
	ctx := context.Background()

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	_, _, err := c.SubscribeMessages(subCtx, "true", map[string]string{"app": "seek-test"})
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	ref, _ := c.PutContent(ctx, []byte("seek"))
	msg := makeMessage(t, kp, ref, "test/seek")
	c.SendMessage(ctx, msg, map[string]string{"app": "seek-test"})

	err = c.Seek(ctx, "default", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("Seek: %v", err)
	}
}

func TestChannelMuxClosedStream(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, _ := newTestClient(t, addr, nodeKP)
	ctx := context.Background()

	_, err := c.PutContent(ctx, []byte("init"))
	if err != nil {
		t.Fatalf("PutContent: %v", err)
	}

	c.Close()

	_, err = c.PutContent(ctx, []byte("after-close"))
	if err == nil {
		t.Error("expected error after close")
	}
}

func TestWithIdentityOption(t *testing.T) {
	addr, nodeKP := newTestServer(t)

	nodePub := nodeKP.PublicKey()
	c, err := client.Dial(addr, client.WithNodeKey(nodePub))
	if err != nil {
		t.Fatalf("Dial without identity: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	ctx := context.Background()
	_, err = c.PutContent(ctx, []byte("no-identity"))
	_ = err
}

func TestCreateAndGetGroupManifest(t *testing.T) {
	addr, nodeKP := newTestServer(t)

	// Use node's keypair as identity so the envelope signer matches
	// when sending group messages from the client's identity.
	nodePub := nodeKP.PublicKey()
	adminKP, _ := identity.Generate()
	c, err := client.Dial(addr, client.WithIdentity(adminKP), client.WithNodeKey(nodePub))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	manifest, groupKP, err := c.CreateGroup(ctx, "test-group", adminKP)
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	got, err := c.GetGroupManifest(ctx, groupKP.PublicKey())
	if err != nil {
		t.Fatalf("GetGroupManifest: %v", err)
	}
	if got.Name != manifest.Name {
		t.Errorf("name mismatch: %q vs %q", got.Name, manifest.Name)
	}
}

var ctx = context.Background()

func TestAddAndRemoveGroupMember(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	nodePub := nodeKP.PublicKey()
	adminKP, _ := identity.Generate()
	c, err := client.Dial(addr, client.WithIdentity(adminKP), client.WithNodeKey(nodePub))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	manifest, groupKP, err := c.CreateGroup(ctx, "member-test", adminKP)
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	newMember, _ := identity.Generate()
	updated, err := c.AddGroupMember(ctx, groupKP, manifest, newMember.PublicKey(), 0)
	if err != nil {
		t.Fatalf("AddGroupMember: %v", err)
	}
	if len(updated.Members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(updated.Members))
	}

	removed, err := c.RemoveGroupMember(ctx, groupKP, updated, newMember.PublicKey())
	if err != nil {
		t.Fatalf("RemoveGroupMember: %v", err)
	}
	if len(removed.Members) != 1 {
		t.Fatalf("expected 1 member after remove, got %d", len(removed.Members))
	}
}

func TestFederate(t *testing.T) {
	addr1, nodeKP1 := newTestServer(t)
	addr2, _ := newTestServer(t)
	c := newAdminClient(t, addr1, nodeKP1)

	result, err := c.Federate(ctx, addr2, map[string]string{"app": "test"})
	if err != nil {
		t.Fatalf("Federate: %v", err)
	}
	if result == nil {
		t.Fatal("Federate returned nil result")
	}
}
