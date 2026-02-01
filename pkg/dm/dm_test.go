package dm

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"strings"
	"testing"
	"time"

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
)

// newTestServer creates an in-memory arc-node server and returns its address and keypair.
func newTestServer(t *testing.T) (string, *identity.Keypair) {
	t.Helper()
	ctx := context.Background()
	metrics := observability.NewMetrics()

	blobBackend, err := blobphysical.New(ctx, "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create blob backend: %v", err)
	}
	t.Cleanup(func() { _ = blobBackend.Close() })
	blobs := blobstore.New(blobBackend, metrics)

	idxBackend, err := idxphysical.New(ctx, "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create index backend: %v", err)
	}
	t.Cleanup(func() { _ = idxBackend.Close() })
	idx, err := indexstore.New(idxBackend, metrics)
	if err != nil {
		t.Fatalf("create index store: %v", err)
	}

	obs, err := observability.New(ctx, observability.ObsConfig{LogLevel: "error"}, io.Discard)
	if err != nil {
		t.Fatalf("create observability: %v", err)
	}
	t.Cleanup(func() { _ = obs.Close(ctx) })

	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}

	srv, err := server.New(context.Background(), ":0", obs, false, kp, blobs, idx)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Stop(context.Background()) })

	return srv.Addr(), kp
}

// newTestClient creates a client with a fresh identity connected to the test server.
func newTestClient(t *testing.T, addr string, nodeKP *identity.Keypair) (*client.Client, *identity.Keypair) {
	t.Helper()
	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	nodePub := nodeKP.PublicKey()
	c, err := client.Dial(addr,
		client.WithIdentity(kp),
		client.WithNodeKey(nodePub),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, kp
}

// newTestDM creates a DM session between two keypairs on the given server.
func newTestDM(t *testing.T, addr string, nodeKP *identity.Keypair) (*DM, *client.Client, *identity.Keypair, *identity.Keypair) {
	t.Helper()
	c, senderKP := newTestClient(t, addr, nodeKP)
	peerKP, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer keypair: %v", err)
	}
	nodePub := nodeKP.PublicKey()
	dm, err := New(c, senderKP, peerKP.PublicKey(), WithNodeKey(nodePub))
	if err != nil {
		t.Fatalf("create DM: %v", err)
	}
	return dm, c, senderKP, peerKP
}

func TestDMSendAndRead(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	dm, _, _, _ := newTestDM(t, addr, nodeKP)
	ctx := context.Background()

	plaintext := []byte("hello, this is a secret message")
	result, err := dm.Send(ctx, plaintext, nil)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	// List to get the content ref from labels.
	lr, err := dm.List(ctx, ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(lr.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(lr.Messages))
	}
	_ = result

	// Read using the content ref from the send result's stored blob.
	// The content label contains the encrypted blob's ref.
	contentHex := lr.Messages[0].Labels["content"]
	if contentHex == "" {
		t.Fatal("no content label on listed message")
	}

	// Use Read with the original content ref from PutContent (stored in the message).
	// We need to get the ref from labels since Read expects the blob ref.
	msg, err := dm.Read(ctx, lr.Messages[0].Ref)
	if err != nil {
		// The Ref from List is the index ref, not the blob ref. Try content label.
		ref, err2 := parseContentRef(contentHex)
		if err2 != nil {
			t.Fatalf("Read failed: %v, parse content ref: %v", err, err2)
		}
		msg, err = dm.Read(ctx, ref)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}

	if !bytes.Equal(msg.Content, plaintext) {
		t.Errorf("Read content = %q, want %q", msg.Content, plaintext)
	}
}

func parseContentRef(hexStr string) ([32]byte, error) {
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		return [32]byte{}, err
	}
	var ref [32]byte
	copy(ref[:], b)
	return ref, nil
}

func TestDMSendAndList(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	dm, _, _, _ := newTestDM(t, addr, nodeKP)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := dm.Send(ctx, []byte("msg"), nil); err != nil {
			t.Fatalf("Send[%d]: %v", i, err)
		}
	}

	lr, err := dm.List(ctx, ListOptions{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(lr.Messages) != 3 {
		t.Errorf("expected 3 messages, got %d", len(lr.Messages))
	}
}

func TestDMListPagination(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	dm, _, _, _ := newTestDM(t, addr, nodeKP)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := dm.Send(ctx, []byte("msg"), nil); err != nil {
			t.Fatalf("Send[%d]: %v", i, err)
		}
	}

	// Page 1.
	r1, err := dm.List(ctx, ListOptions{Limit: 2})
	if err != nil {
		t.Fatalf("List page 1: %v", err)
	}
	if len(r1.Messages) != 2 {
		t.Fatalf("page 1: expected 2, got %d", len(r1.Messages))
	}
	if !r1.HasMore {
		t.Error("page 1: HasMore should be true")
	}
	if r1.NextCursor == "" {
		t.Fatal("page 1: NextCursor is empty")
	}

	// Page 2.
	r2, err := dm.List(ctx, ListOptions{Limit: 2, Cursor: r1.NextCursor})
	if err != nil {
		t.Fatalf("List page 2: %v", err)
	}
	if len(r2.Messages) != 2 {
		t.Fatalf("page 2: expected 2, got %d", len(r2.Messages))
	}

	// Page 3.
	r3, err := dm.List(ctx, ListOptions{Limit: 2, Cursor: r2.NextCursor})
	if err != nil {
		t.Fatalf("List page 3: %v", err)
	}
	if len(r3.Messages) != 1 {
		t.Fatalf("page 3: expected 1, got %d", len(r3.Messages))
	}
	if r3.HasMore {
		t.Error("page 3: HasMore should be false")
	}
}

func TestDMPreview(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	dm, _, _, _ := newTestDM(t, addr, nodeKP)
	ctx := context.Background()

	plaintext := []byte("preview test message")
	if _, err := dm.Send(ctx, plaintext, nil); err != nil {
		t.Fatalf("Send: %v", err)
	}

	lr, err := dm.List(ctx, ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(lr.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(lr.Messages))
	}

	preview, err := dm.Preview(ctx, lr.Messages[0])
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if preview != "preview test message" {
		t.Errorf("Preview = %q, want %q", preview, "preview test message")
	}
}

func TestTruncatePreview(t *testing.T) {
	// Test byte truncation.
	long := bytes.Repeat([]byte("a"), 300)
	got := truncatePreview(long)
	if len(got) > previewMaxBytes+3 { // +3 for "..."
		t.Errorf("truncatePreview byte limit: len = %d", len(got))
	}
	if !strings.HasSuffix(got, "...") {
		t.Error("truncatePreview byte limit: should end with ...")
	}

	// Test line truncation.
	lines := "line1\nline2\nline3\nline4\nline5\nline6"
	got = truncatePreview([]byte(lines))
	if strings.Count(got, "\n") >= previewMaxLines {
		t.Errorf("truncatePreview line limit: too many newlines in %q", got)
	}
	if !strings.HasSuffix(got, "...") {
		t.Error("truncatePreview line limit: should end with ...")
	}

	// Test short content (no truncation).
	short := []byte("hi")
	got = truncatePreview(short)
	if got != "hi" {
		t.Errorf("truncatePreview short = %q, want %q", got, "hi")
	}
}

func TestDMSubscribe(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	dm, _, _, _ := newTestDM(t, addr, nodeKP)
	ctx := context.Background()

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	msgs, _, err := dm.Subscribe(subCtx, ListOptions{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Allow subscription to establish.
	time.Sleep(100 * time.Millisecond)

	if _, err := dm.Send(ctx, []byte("realtime"), nil); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case m := <-msgs:
		if m == nil {
			t.Fatal("received nil message")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive subscribed message within timeout")
	}
}

func TestThreadsListThreads(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	ctx := context.Background()
	nodePub := nodeKP.PublicKey()

	// Create DMs with two different peers.
	peer1KP, _ := identity.Generate()
	peer2KP, _ := identity.Generate()

	dm1, err := New(c, senderKP, peer1KP.PublicKey(), WithNodeKey(nodePub))
	if err != nil {
		t.Fatalf("New dm1: %v", err)
	}
	dm2, err := New(c, senderKP, peer2KP.PublicKey(), WithNodeKey(nodePub))
	if err != nil {
		t.Fatalf("New dm2: %v", err)
	}

	if _, err := dm1.Send(ctx, []byte("hello peer1"), nil); err != nil {
		t.Fatalf("Send dm1: %v", err)
	}
	if _, err := dm2.Send(ctx, []byte("hello peer2"), nil); err != nil {
		t.Fatalf("Send dm2: %v", err)
	}

	threads := NewThreads(c, senderKP, WithNodeKey(nodePub))
	list, err := threads.ListThreads(ctx)
	if err != nil {
		t.Fatalf("ListThreads: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("expected 2 threads, got %d", len(list))
	}
}

func TestParseFrom(t *testing.T) {
	// Valid case.
	kp, _ := identity.Generate()
	pub := kp.PublicKey()
	m := Message{
		Labels: map[string]string{
			"dm_from": hex.EncodeToString(pub[:]),
		},
	}
	got, err := ParseFrom(m)
	if err != nil {
		t.Fatalf("ParseFrom: %v", err)
	}
	if got != pub {
		t.Errorf("ParseFrom = %x, want %x", got, pub)
	}

	// Missing label.
	_, err = ParseFrom(Message{Labels: map[string]string{}})
	if err == nil {
		t.Error("ParseFrom with no label: expected error")
	}

	// Invalid hex.
	_, err = ParseFrom(Message{Labels: map[string]string{"dm_from": "zzzz"}})
	if err == nil {
		t.Error("ParseFrom with invalid hex: expected error")
	}
}

func TestConversationLabels(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	dm, _, senderKP, peerKP := newTestDM(t, addr, nodeKP)
	ctx := context.Background()

	if _, err := dm.Send(ctx, []byte("test"), nil); err != nil {
		t.Fatalf("Send: %v", err)
	}

	lr, err := dm.List(ctx, ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(lr.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(lr.Messages))
	}

	labels := lr.Messages[0].Labels
	senderPub := senderKP.PublicKey()
	peerPub := peerKP.PublicKey()

	expected := map[string]string{
		"app":          "dm",
		"type":         "message",
		"conversation": ConversationID(senderPub, peerPub),
		"dm_from":      hex.EncodeToString(senderPub[:]),
		"dm_to":        hex.EncodeToString(peerPub[:]),
	}
	for k, want := range expected {
		if got := labels[k]; got != want {
			t.Errorf("label %q = %q, want %q", k, got, want)
		}
	}
}

func TestDMOptions(t *testing.T) {
	kp, _ := identity.Generate()
	peerKP, _ := identity.Generate()
	nodeKP, _ := identity.Generate()
	nodePub := nodeKP.PublicKey()

	dm, err := New(nil, kp, peerKP.PublicKey(), WithNodeKey(nodePub))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got := dm.recipientKey()
	if got != nodePub {
		t.Errorf("recipientKey = %x, want %x", got, nodePub)
	}
}

func TestThreadsSubscribeAll(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	ctx := context.Background()
	nodePub := nodeKP.PublicKey()

	peerKP, _ := identity.Generate()
	dm, err := New(c, senderKP, peerKP.PublicKey(), WithNodeKey(nodePub))
	if err != nil {
		t.Fatalf("New DM: %v", err)
	}

	threads := NewThreads(c, senderKP, WithNodeKey(nodePub))
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	msgs, _, err := threads.SubscribeAll(subCtx)
	if err != nil {
		t.Fatalf("SubscribeAll: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if _, err := dm.Send(ctx, []byte("sub all msg"), nil); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case m := <-msgs:
		if m == nil {
			t.Fatal("received nil message")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive SubscribeAll message within timeout")
	}
}

func TestThreadsPreviewThread(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	ctx := context.Background()
	nodePub := nodeKP.PublicKey()

	peerKP, _ := identity.Generate()
	dm, err := New(c, senderKP, peerKP.PublicKey(), WithNodeKey(nodePub))
	if err != nil {
		t.Fatalf("New DM: %v", err)
	}

	if _, err := dm.Send(ctx, []byte("preview thread test"), nil); err != nil {
		t.Fatalf("Send: %v", err)
	}

	threads := NewThreads(c, senderKP, WithNodeKey(nodePub))
	list, err := threads.ListThreads(ctx)
	if err != nil {
		t.Fatalf("ListThreads: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 thread, got %d", len(list))
	}

	preview, err := threads.PreviewThread(ctx, list[0])
	if err != nil {
		t.Fatalf("PreviewThread: %v", err)
	}
	if preview != "preview thread test" {
		t.Errorf("preview = %q, want %q", preview, "preview thread test")
	}
}

func TestThreadsOpenConversation(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	ctx := context.Background()
	nodePub := nodeKP.PublicKey()

	peerKP, _ := identity.Generate()
	threads := NewThreads(c, senderKP, WithNodeKey(nodePub))

	dm, err := threads.OpenConversation(peerKP.PublicKey())
	if err != nil {
		t.Fatalf("OpenConversation: %v", err)
	}

	if _, err := dm.Send(ctx, []byte("from opened conv"), nil); err != nil {
		t.Fatalf("Send: %v", err)
	}

	lr, err := dm.List(ctx, ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(lr.Messages) != 1 {
		t.Errorf("expected 1 message, got %d", len(lr.Messages))
	}
}

func TestDMSubscribeCancel(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	dm, _, _, _ := newTestDM(t, addr, nodeKP)
	ctx := context.Background()

	subCtx, cancel := context.WithCancel(ctx)
	msgs, _, err := dm.Subscribe(subCtx, ListOptions{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	cancel()

	// Channel should eventually close.
	select {
	case <-msgs:
		// Got a message or channel closed, both acceptable.
	case <-time.After(2 * time.Second):
		t.Fatal("message channel not closed after cancel")
	}
}

func TestDMSendWithLabels(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	dm, _, _, _ := newTestDM(t, addr, nodeKP)
	ctx := context.Background()

	custom := map[string]string{"mood": "happy", "priority": "high"}
	if _, err := dm.Send(ctx, []byte("labeled"), custom); err != nil {
		t.Fatalf("Send: %v", err)
	}

	lr, err := dm.List(ctx, ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(lr.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(lr.Messages))
	}
	if lr.Messages[0].Labels["mood"] != "happy" {
		t.Errorf("mood = %q, want happy", lr.Messages[0].Labels["mood"])
	}
	if lr.Messages[0].Labels["priority"] != "high" {
		t.Errorf("priority = %q, want high", lr.Messages[0].Labels["priority"])
	}
}

func TestDMRecipientKeyFallback(t *testing.T) {
	kp, _ := identity.Generate()
	peerKP, _ := identity.Generate()

	// No WithNodeKey, no client — should fall back to self pubkey.
	dm, err := New(nil, kp, peerKP.PublicKey())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got := dm.recipientKey()
	if got != kp.PublicKey() {
		t.Errorf("recipientKey = %x, want self pubkey %x", got, kp.PublicKey())
	}
}
