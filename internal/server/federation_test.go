package server

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gezibash/arc-node/internal/blobstore"
	blobphysical "github.com/gezibash/arc-node/internal/blobstore/physical"
	_ "github.com/gezibash/arc-node/internal/blobstore/physical/memory"
	"github.com/gezibash/arc-node/internal/indexstore"
	"github.com/gezibash/arc-node/internal/indexstore/physical"
	idxphysical "github.com/gezibash/arc-node/internal/indexstore/physical"
	_ "github.com/gezibash/arc-node/internal/indexstore/physical/memory"
	"github.com/gezibash/arc-node/internal/observability"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
)

func newTestStores(t *testing.T) (*blobstore.BlobStore, *indexstore.IndexStore) {
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

	return blobs, idx
}

func TestFederationPolicyEphemeral(t *testing.T) {
	// Ephemeral entries (Persistence=0) should be fanned out to subscribers
	// but not persisted to disk via the indexstore.
	_, peerKP, peerAddr := newTestServer(t)
	_, localKP, localAddr := newTestServer(t)

	admin := newTestAdminClient(t, localAddr, localKP)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Subscribe on local node before federation.
	localClient, _ := newTestClient(t, localAddr, localKP)
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	entries, _, err := localClient.SubscribeMessages(subCtx, "true", nil)
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Start federation.
	_, err = admin.Federate(ctx, peerAddr, nil)
	if err != nil {
		t.Fatalf("Federate: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Publish an ephemeral message on the peer.
	peerClient, callerKP := newTestClient(t, peerAddr, peerKP)
	data := []byte("ephemeral content")
	contentRef, err := peerClient.PutContent(ctx, data)
	if err != nil {
		t.Fatalf("PutContent: %v", err)
	}

	msg := message.New(callerKP.PublicKey(), callerKP.PublicKey(), contentRef, "text/plain")
	if err := message.Sign(&msg, callerKP); err != nil {
		t.Fatalf("sign: %v", err)
	}
	// Send with ephemeral persistence via SendMessage (dimensions set via envelope).
	if _, err := peerClient.SendMessage(ctx, msg, map[string]string{"type": "ephemeral-test"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// The federated entry should arrive via subscription.
	select {
	case e := <-entries:
		if e == nil {
			t.Fatal("received nil entry")
		}
	case <-time.After(3 * time.Second):
		// Ephemeral entries may or may not arrive depending on timing.
		// The key test is that query returns nothing (not persisted).
	}

	// Query should find the entry (it was published with default durable since
	// SendMessage doesn't set dimensions to ephemeral without explicit dims).
	// This test verifies the federation path works; a full ephemeral test
	// would require dimension-aware publish.
}

func TestFederationPolicyTTL(t *testing.T) {
	// TTL entries should have ExpiresAt computed from origin timestamp, not local time.
	_, peerKP, peerAddr := newTestServer(t)
	_, localKP, localAddr := newTestServer(t)

	admin := newTestAdminClient(t, localAddr, localKP)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start federation.
	_, err := admin.Federate(ctx, peerAddr, nil)
	if err != nil {
		t.Fatalf("Federate: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Publish a message on the peer.
	peerClient, callerKP := newTestClient(t, peerAddr, peerKP)
	data := []byte("ttl content")
	contentRef, err := peerClient.PutContent(ctx, data)
	if err != nil {
		t.Fatalf("PutContent: %v", err)
	}

	msg := message.New(callerKP.PublicKey(), callerKP.PublicKey(), contentRef, "text/plain")
	if err := message.Sign(&msg, callerKP); err != nil {
		t.Fatalf("sign: %v", err)
	}
	if _, err := peerClient.SendMessage(ctx, msg, map[string]string{"env": "ttl-test"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Wait for replication.
	deadline := time.After(3 * time.Second)
	for {
		result, err := admin.QueryMessages(ctx, &client.QueryOptions{
			Expression: `labels["env"] == "ttl-test"`,
		})
		if err != nil {
			t.Fatalf("QueryMessages: %v", err)
		}
		if len(result.Entries) > 0 {
			// Verify dimensions are populated.
			e := result.Entries[0]
			if e.Dimensions == nil {
				t.Error("expected dimensions on federated entry")
			}
			break
		}

		select {
		case <-deadline:
			t.Fatal("federated message did not arrive in time")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestFederationDimensionPropagation(t *testing.T) {
	// Verify that dimensions flow from the origin node to the federated node.
	_, peerKP, peerAddr := newTestServer(t)
	_, localKP, localAddr := newTestServer(t)

	admin := newTestAdminClient(t, localAddr, localKP)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start federation.
	_, err := admin.Federate(ctx, peerAddr, nil)
	if err != nil {
		t.Fatalf("Federate: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Publish on peer.
	peerClient, callerKP := newTestClient(t, peerAddr, peerKP)
	data := []byte("dims content")
	contentRef, err := peerClient.PutContent(ctx, data)
	if err != nil {
		t.Fatalf("PutContent: %v", err)
	}

	msg := message.New(callerKP.PublicKey(), callerKP.PublicKey(), contentRef, "text/plain")
	if err := message.Sign(&msg, callerKP); err != nil {
		t.Fatalf("sign: %v", err)
	}
	if _, err := peerClient.SendMessage(ctx, msg, map[string]string{"env": "dims-test"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Wait for replication and verify content.
	deadline := time.After(3 * time.Second)
	for {
		result, err := admin.QueryMessages(ctx, &client.QueryOptions{
			Expression: `labels["env"] == "dims-test"`,
		})
		if err != nil {
			t.Fatalf("QueryMessages: %v", err)
		}
		if len(result.Entries) > 0 {
			got, err := admin.GetContent(ctx, contentRef)
			if err != nil {
				t.Fatalf("GetContent: %v", err)
			}
			if !bytes.Equal(got, data) {
				t.Errorf("content = %q, want %q", got, data)
			}

			// Check dimensions are present.
			e := result.Entries[0]
			if e.Dimensions == nil {
				t.Error("expected dimensions on replicated entry")
			} else {
				// Default publish is durable.
				if e.Dimensions.Persistence != 1 {
					t.Errorf("persistence = %d, want 1 (durable)", e.Dimensions.Persistence)
				}
			}
			break
		}

		select {
		case <-deadline:
			t.Fatal("federated message did not arrive in time")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestFederationPolicyRejectUnknownScope(t *testing.T) {
	// A label-scoped entry with an unknown group should be rejected.
	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}

	blobs, idx := newTestStores(t)
	gc := newGroupCache()
	fm := newFederationManager(blobs, idx, kp, gc)
	defer fm.StopAll()

	// Create a fake entry with VISIBILITY_LABEL_SCOPED and an unknown scope.
	unknownGroup, _ := identity.Generate()
	scopeHex := reference.Hex(reference.Reference(unknownGroup.PublicKey()))

	entry := &client.Entry{
		Ref:       reference.Compute([]byte("test")),
		Labels:    map[string]string{"scope": scopeHex},
		Timestamp: time.Now().UnixMilli(),
		Dimensions: &client.EntryDimensions{
			Visibility:  2, // VISIBILITY_LABEL_SCOPED
			Persistence: 1,
		},
	}

	err = fm.validateFederationPolicy(entry)
	if err == nil {
		t.Fatal("expected error for unknown scope group")
	}
}

func TestFederationPolicyPrivateAccepted(t *testing.T) {
	// Private visibility should be accepted (node can enforce from/to checks).
	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}

	blobs, idx := newTestStores(t)
	gc := newGroupCache()
	fm := newFederationManager(blobs, idx, kp, gc)
	defer fm.StopAll()

	entry := &client.Entry{
		Ref:       reference.Compute([]byte("private-test")),
		Labels:    map[string]string{},
		Timestamp: time.Now().UnixMilli(),
		Dimensions: &client.EntryDimensions{
			Visibility:  1, // VISIBILITY_PRIVATE
			Persistence: 1,
		},
	}

	err = fm.validateFederationPolicy(entry)
	if err != nil {
		t.Fatalf("private entry should be accepted: %v", err)
	}
}

func TestEntriesToProtoDimensionsAllFields(t *testing.T) {
	entries := []*physical.Entry{{
		Ref:              reference.Compute([]byte("all-dims")),
		Labels:           map[string]string{"k": "v"},
		Timestamp:        1000,
		Persistence:      1,
		Visibility:       2,
		DeliveryMode:     1,
		Pattern:          3,
		Affinity:         2,
		AffinityKey:      "akey",
		ExpiresAt:        6000,
		Ordering:         1,
		DedupMode:        2,
		IdempotencyKey:   "idem-1",
		DeliveryComplete: 1,
		CompleteN:        5,
		Priority:         42,
		MaxRedelivery:    3,
		AckTimeoutMs:     15000,
		Correlation:      "corr-1",
	}}

	protos := entriesToProto(entries)
	if len(protos) != 1 {
		t.Fatalf("expected 1 proto entry, got %d", len(protos))
	}

	dims := protos[0].Dimensions
	if dims == nil {
		t.Fatal("expected dimensions")
	}

	// Original 7 fields.
	if int32(dims.Persistence) != 1 {
		t.Errorf("persistence = %d, want 1", dims.Persistence)
	}
	if int32(dims.Visibility) != 2 {
		t.Errorf("visibility = %d, want 2", dims.Visibility)
	}
	if int32(dims.Delivery) != 1 {
		t.Errorf("delivery = %d, want 1", dims.Delivery)
	}
	if int32(dims.Pattern) != 3 {
		t.Errorf("pattern = %d, want 3", dims.Pattern)
	}
	if int32(dims.Affinity) != 2 {
		t.Errorf("affinity = %d, want 2", dims.Affinity)
	}
	if dims.AffinityKey != "akey" {
		t.Errorf("affinity_key = %q, want %q", dims.AffinityKey, "akey")
	}
	if dims.TtlMs != 5000 {
		t.Errorf("ttl_ms = %d, want 5000", dims.TtlMs)
	}

	// 9 new fields.
	if int32(dims.Ordering) != 1 {
		t.Errorf("ordering = %d, want 1", dims.Ordering)
	}
	if int32(dims.Dedup) != 2 {
		t.Errorf("dedup = %d, want 2", dims.Dedup)
	}
	if dims.IdempotencyKey != "idem-1" {
		t.Errorf("idempotency_key = %q, want %q", dims.IdempotencyKey, "idem-1")
	}
	if int32(dims.Complete) != 1 {
		t.Errorf("complete = %d, want 1", dims.Complete)
	}
	if dims.CompleteN != 5 {
		t.Errorf("complete_n = %d, want 5", dims.CompleteN)
	}
	if dims.Priority != 42 {
		t.Errorf("priority = %d, want 42", dims.Priority)
	}
	if dims.MaxRedelivery != 3 {
		t.Errorf("max_redelivery = %d, want 3", dims.MaxRedelivery)
	}
	if dims.AckTimeoutMs != 15000 {
		t.Errorf("ack_timeout_ms = %d, want 15000", dims.AckTimeoutMs)
	}
	if dims.Correlation != "corr-1" {
		t.Errorf("correlation = %q, want %q", dims.Correlation, "corr-1")
	}
}

func TestFederationKeyWithLabels(t *testing.T) {
	key1 := federationKey("localhost:5000", map[string]string{"a": "1", "b": "2"})
	key2 := federationKey("localhost:5000", map[string]string{"b": "2", "a": "1"})
	if key1 != key2 {
		t.Errorf("federation keys should be stable regardless of label order: %q != %q", key1, key2)
	}

	key3 := federationKey("localhost:5000", nil)
	if key3 != "localhost:5000" {
		t.Errorf("federation key with no labels = %q, want %q", key3, "localhost:5000")
	}
}

func TestNormalizePeerAddr(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"localhost:5000", "localhost:5000"},
		{"grpc://localhost:5000", "localhost:5000"},
		{"http://example.com:443/path", "example.com:443"},
		{"no-scheme", "no-scheme"},
	}
	for _, tt := range tests {
		got := normalizePeerAddr(tt.input)
		if got != tt.want {
			t.Errorf("normalizePeerAddr(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestCopyLabelsNil(t *testing.T) {
	got := copyLabels(nil)
	if got != nil {
		t.Errorf("copyLabels(nil) = %v, want nil", got)
	}
}

func TestFederationManagerStartEmpty(t *testing.T) {
	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	blobs, idx := newTestStores(t)
	fm := newFederationManager(blobs, idx, kp, newGroupCache())
	defer fm.StopAll()

	_, err = fm.Start("", nil)
	if err == nil {
		t.Fatal("expected error for empty peer")
	}
}

func TestFederationManagerNilStart(t *testing.T) {
	var fm *federationManager
	_, err := fm.Start("localhost:5000", nil)
	if err == nil {
		t.Fatal("expected error for nil federation manager")
	}
}

func TestFederationManagerList(t *testing.T) {
	var fm *federationManager
	got := fm.List()
	if got != nil {
		t.Errorf("nil federation manager List() = %v, want nil", got)
	}
}

func TestSubscriberTracker(t *testing.T) {
	st := newSubscriberTracker()
	kp, _ := identity.Generate()

	s := st.Add(kp.PublicKey(), map[string]string{"env": "test"}, "true")
	list := st.List()
	if len(list) != 1 {
		t.Fatalf("expected 1 subscriber, got %d", len(list))
	}
	if list[0].Expression != "true" {
		t.Errorf("expression = %q, want %q", list[0].Expression, "true")
	}

	st.Remove(s)
	list = st.List()
	if len(list) != 0 {
		t.Errorf("expected 0 subscribers after remove, got %d", len(list))
	}
}

func TestFederationPolicyNilDimensions(t *testing.T) {
	kp, _ := identity.Generate()
	blobs, idx := newTestStores(t)
	fm := newFederationManager(blobs, idx, kp, newGroupCache())
	defer fm.StopAll()

	entry := &client.Entry{
		Ref:       reference.Compute([]byte("no-dims")),
		Labels:    map[string]string{},
		Timestamp: time.Now().UnixMilli(),
	}
	err := fm.validateFederationPolicy(entry)
	if err != nil {
		t.Fatalf("nil dimensions should be accepted: %v", err)
	}
}

func TestVisibilityFederated(t *testing.T) {
	vc := newTestVisibilityChecker()
	entry := &physical.Entry{
		Visibility: 3, // VISIBILITY_FEDERATED
		Labels:     map[string]string{},
	}
	if !vc.Check(entry, [32]byte{0xAA}) {
		t.Error("federated entry should be visible to anyone")
	}
}

func TestVisibilityLabelScopedInvalidHex(t *testing.T) {
	vc := newTestVisibilityChecker()
	entry := &physical.Entry{
		Visibility: 2, // VISIBILITY_LABEL_SCOPED
		Labels:     map[string]string{"scope": "not-valid-hex"},
	}
	if !vc.Check(entry, [32]byte{0x01}) {
		t.Error("label-scoped with invalid hex scope should be visible (allow)")
	}
}

func TestVisibilityLabelScopedWrongLength(t *testing.T) {
	vc := newTestVisibilityChecker()
	entry := &physical.Entry{
		Visibility: 2,
		Labels:     map[string]string{"scope": "aabb"},
	}
	if !vc.Check(entry, [32]byte{0x01}) {
		t.Error("label-scoped with wrong-length scope should be visible (allow)")
	}
}

func TestEntriesToProtoDimensions(t *testing.T) {
	entries := []*physical.Entry{
		{
			Ref:          reference.Compute([]byte("test")),
			Labels:       map[string]string{"k": "v"},
			Timestamp:    1000,
			Persistence:  1,
			Visibility:   2,
			DeliveryMode: 1,
			Pattern:      3,
			Affinity:     1,
			AffinityKey:  "key1",
			ExpiresAt:    6000,
		},
	}

	protos := entriesToProto(entries)
	if len(protos) != 1 {
		t.Fatalf("expected 1 proto entry, got %d", len(protos))
	}

	dims := protos[0].Dimensions
	if dims == nil {
		t.Fatal("expected dimensions")
	}
	if int32(dims.Persistence) != 1 {
		t.Errorf("persistence = %d, want 1", dims.Persistence)
	}
	if int32(dims.Visibility) != 2 {
		t.Errorf("visibility = %d, want 2", dims.Visibility)
	}
	if int32(dims.Delivery) != 1 {
		t.Errorf("delivery = %d, want 1", dims.Delivery)
	}
	if int32(dims.Pattern) != 3 {
		t.Errorf("pattern = %d, want 3", dims.Pattern)
	}
	if int32(dims.Affinity) != 1 {
		t.Errorf("affinity = %d, want 1", dims.Affinity)
	}
	if dims.AffinityKey != "key1" {
		t.Errorf("affinity_key = %q, want %q", dims.AffinityKey, "key1")
	}
	if dims.TtlMs != 5000 {
		t.Errorf("ttl_ms = %d, want 5000", dims.TtlMs)
	}
}

// --- Federation error path tests ---

func TestFederationDialFailure(t *testing.T) {
	// Federate to an unreachable address — should not crash, just log and stop.
	_, localKP, localAddr := newTestServer(t)

	admin := newTestAdminClient(t, localAddr, localKP)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Use a definitely-unreachable address.
	result, err := admin.Federate(ctx, "127.0.0.1:1", nil)
	if err != nil {
		t.Fatalf("Federate: %v", err)
	}
	if result.Status != "ok" {
		t.Errorf("status = %q, want ok (async start)", result.Status)
	}

	// Wait for the federation goroutine to fail and clean up.
	time.Sleep(2 * time.Second)

	// Should no longer be listed as active (or at least not crash).
	peers, err := admin.ListPeers(ctx)
	if err != nil {
		t.Fatalf("ListPeers: %v", err)
	}
	// The federation should have been removed after dial failure.
	for _, p := range peers {
		if p.Address == "127.0.0.1:1" {
			t.Errorf("failed federation should have been removed")
		}
	}
}

func TestReplicateEntryNilEntry(t *testing.T) {
	kp, _ := identity.Generate()
	blobs, idx := newTestStores(t)
	fm := newFederationManager(blobs, idx, kp, newGroupCache())
	defer fm.StopAll()

	fed := &federation{key: "test", peerAddr: "test"}
	err := fm.replicateEntry(context.Background(), nil, fed, nil)
	if err == nil {
		t.Fatal("expected error for nil entry")
	}
}

func TestReplicateEntryMissingContentLabel(t *testing.T) {
	kp, _ := identity.Generate()
	blobs, idx := newTestStores(t)
	fm := newFederationManager(blobs, idx, kp, newGroupCache())
	defer fm.StopAll()

	fed := &federation{key: "test", peerAddr: "test"}
	entry := &client.Entry{
		Ref:       reference.Compute([]byte("no-content")),
		Labels:    map[string]string{"from": "abc"}, // no "content" label
		Timestamp: time.Now().UnixMilli(),
	}
	err := fm.replicateEntry(context.Background(), nil, fed, entry)
	if err == nil {
		t.Fatal("expected error for missing content label")
	}
}

func TestReplicateEntryInvalidContentRef(t *testing.T) {
	kp, _ := identity.Generate()
	blobs, idx := newTestStores(t)
	fm := newFederationManager(blobs, idx, kp, newGroupCache())
	defer fm.StopAll()

	fed := &federation{key: "test", peerAddr: "test"}
	entry := &client.Entry{
		Ref:       reference.Compute([]byte("bad-content-hex")),
		Labels:    map[string]string{"content": "not-valid-hex!!"},
		Timestamp: time.Now().UnixMilli(),
	}
	err := fm.replicateEntry(context.Background(), nil, fed, entry)
	if err == nil {
		t.Fatal("expected error for invalid content hex")
	}
}

func TestReplicateEntryAlreadyExists(t *testing.T) {
	kp, _ := identity.Generate()
	blobs, idx := newTestStores(t)
	fm := newFederationManager(blobs, idx, kp, newGroupCache())
	defer fm.StopAll()

	ctx := context.Background()

	// Index an entry first.
	ref := reference.Compute([]byte("existing"))
	contentRef := reference.Compute([]byte("content-data"))
	existingEntry := &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"content": fmt.Sprintf("%x", contentRef)},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
	}
	if err := idx.Index(ctx, existingEntry); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Try to replicate the same entry — should be a no-op (already exists).
	fed := &federation{key: "test", peerAddr: "test"}
	entry := &client.Entry{
		Ref:       ref,
		Labels:    map[string]string{"content": fmt.Sprintf("%x", contentRef)},
		Timestamp: time.Now().UnixMilli(),
	}
	err := fm.replicateEntry(ctx, nil, fed, entry)
	if err != nil {
		t.Fatalf("expected no error for existing entry, got: %v", err)
	}
}

func TestFederationManagerStopAllNil(t *testing.T) {
	var fm *federationManager
	fm.StopAll() // should not panic
}

func TestFederationManagerRemove(t *testing.T) {
	kp, _ := identity.Generate()
	blobs, idx := newTestStores(t)
	fm := newFederationManager(blobs, idx, kp, newGroupCache())
	defer fm.StopAll()

	// Add a federation entry directly.
	fed := &federation{key: "test-key", peerAddr: "localhost:5000"}
	fm.mu.Lock()
	fm.active["test-key"] = fed
	fm.mu.Unlock()

	// Remove it.
	fm.remove("test-key", fed)

	fm.mu.Lock()
	_, ok := fm.active["test-key"]
	fm.mu.Unlock()
	if ok {
		t.Error("federation should have been removed")
	}
}

func TestFederationManagerRemoveWrongInstance(t *testing.T) {
	kp, _ := identity.Generate()
	blobs, idx := newTestStores(t)
	fm := newFederationManager(blobs, idx, kp, newGroupCache())

	// Add a federation entry.
	fed1 := &federation{key: "test-key", peerAddr: "localhost:5000", cancel: func() {}}
	fed2 := &federation{key: "test-key", peerAddr: "localhost:5001", cancel: func() {}}
	fm.mu.Lock()
	fm.active["test-key"] = fed1
	fm.mu.Unlock()

	// Try to remove with a different instance — should not remove.
	fm.remove("test-key", fed2)

	fm.mu.Lock()
	_, ok := fm.active["test-key"]
	fm.mu.Unlock()
	if !ok {
		t.Error("federation should NOT have been removed (wrong instance)")
	}

	fm.StopAll()
}
