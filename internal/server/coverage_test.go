package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	idxphysical "github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc-node/pkg/group"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
)

// TestPublishWithCorrelationAndTTL covers the correlation label injection
// and TTL->ExpiresAt computation in doPublish with explicit dimensions.
func TestPublishWithCorrelationAndTTL(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "text/plain")
	dims := &nodev1.Dimensions{
		Persistence: nodev1.Persistence_PERSISTENCE_DURABLE,
		Correlation: "corr-cov",
		TtlMs:       60000,
		Priority:    10,
	}
	_, err := c.SendMessage(ctx, msg, map[string]string{"env": "corr-cov"}, dims)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{
		Expression: `labels["_correlation"] == "corr-cov"`,
	})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result.Entries))
	}
	if result.Entries[0].Dimensions.Correlation != "corr-cov" {
		t.Errorf("correlation mismatch")
	}
	if result.Entries[0].Dimensions.TtlMs <= 0 {
		t.Error("expected positive TTL")
	}
}

// TestCanPublishForNonManifestContentType covers the canPublishFor
// path where contentType != "arc/group.manifest" returns false.
func TestCanPublishForNonManifestContentType(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	groupKP, _ := identity.Generate()

	data := []byte("not a manifest")
	contentRef, _ := c.PutContent(ctx, data)

	msg := message.New(groupKP.PublicKey(), callerKP.PublicKey(), contentRef, "text/plain")
	if err := message.Sign(&msg, groupKP); err != nil {
		t.Fatalf("sign: %v", err)
	}

	_, err := c.SendMessage(ctx, msg, nil, nil)
	if err == nil {
		t.Fatal("expected error for publish-for with non-manifest content")
	}
}

// TestCanPublishForBadManifestData covers the canPublishFor bootstrap
// path where the content is "arc/group.manifest" but data is invalid.
func TestCanPublishForBadManifestData(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	groupKP, _ := identity.Generate()

	data := []byte("invalid manifest data")
	contentRef, _ := c.PutContent(ctx, data)

	msg := message.New(groupKP.PublicKey(), callerKP.PublicKey(), contentRef, "arc/group.manifest")
	if err := message.Sign(&msg, groupKP); err != nil {
		t.Fatalf("sign: %v", err)
	}

	_, err := c.SendMessage(ctx, msg, nil, nil)
	if err == nil {
		t.Fatal("expected error for publish-for with bad manifest")
	}
}

// TestResolveGetBlobHitViaPrefix covers the doResolveGet blob hit path.
func TestResolveGetBlobHitViaPrefix(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	data := []byte("resolve-get-blob-cov")
	ref, _ := c.PutContent(ctx, data)

	prefix := hex.EncodeToString(ref[:])[:8]
	result, err := c.ResolveGet(ctx, prefix)
	if err != nil {
		t.Fatalf("ResolveGet: %v", err)
	}
	if result.Kind != client.GetKindBlob {
		t.Errorf("kind = %v, want blob", result.Kind)
	}
}

// TestResolveGetMessageHitViaPrefix covers the doResolveGet index fallback path.
func TestResolveGetMessageHitViaPrefix(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "text/plain")
	pubResult, _ := c.SendMessage(ctx, msg, map[string]string{"rg": "cov"}, nil)

	prefix := hex.EncodeToString(pubResult.Ref[:])[:8]
	result, err := c.ResolveGet(ctx, prefix)
	if err != nil {
		t.Fatalf("ResolveGet: %v", err)
	}
	if result.Kind != client.GetKindMessage {
		t.Errorf("kind = %v, want message", result.Kind)
	}
}

// TestPublishAllDimensionFields covers doPublish dimension mapping for all fields.
func TestPublishAllDimensionFields(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "text/plain")
	dims := &nodev1.Dimensions{
		Persistence:    nodev1.Persistence_PERSISTENCE_DURABLE,
		Delivery:       nodev1.Delivery_DELIVERY_AT_LEAST_ONCE,
		Visibility:     nodev1.Visibility_VISIBILITY_PUBLIC,
		Pattern:        nodev1.Pattern_PATTERN_PUB_SUB,
		Affinity:       nodev1.Affinity_AFFINITY_NONE,
		AffinityKey:    "akey",
		Ordering:       nodev1.Ordering_ORDERING_FIFO,
		Dedup:          nodev1.Dedup_DEDUP_IDEMPOTENCY_KEY,
		IdempotencyKey: "idem-cov",
		Complete:       nodev1.DeliveryComplete_DELIVERY_COMPLETE_ALL,
		CompleteN:      5,
		Priority:       42,
		MaxRedelivery:  3,
		AckTimeoutMs:   15000,
		Correlation:    "corr-cov2",
		TtlMs:          60000,
	}
	_, err := c.SendMessage(ctx, msg, map[string]string{"env": "all-dims-cov"}, dims)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{
		Expression: `labels["env"] == "all-dims-cov"`,
	})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result.Entries))
	}
	d := result.Entries[0].Dimensions
	if d == nil {
		t.Fatal("expected dimensions")
	}
	if d.Priority != 42 {
		t.Errorf("priority = %d, want 42", d.Priority)
	}
	if d.AckTimeoutMs != 15000 {
		t.Errorf("ack_timeout_ms = %d, want 15000", d.AckTimeoutMs)
	}
}

// TestReplicateEntryWithAllDimensions covers the federation replicateEntry
// dimension mapping for all extended fields.
func TestReplicateEntryWithAllDimensions(t *testing.T) {
	kp, _ := identity.Generate()
	blobs, idx := newTestStores(t)
	fm := newFederationManager(blobs, idx, kp, newGroupCache())
	defer fm.StopAll()

	ctx := context.Background()

	data := []byte("replicate-all-dims")
	contentRef := reference.Compute(data)
	if _, err := blobs.Store(ctx, data); err != nil {
		t.Fatalf("Store: %v", err)
	}

	fed := &federation{key: "test", peerAddr: "test"}
	entry := &client.Entry{
		Ref:       reference.Compute([]byte("unique-all-dims")),
		Labels:    map[string]string{"content": fmt.Sprintf("%x", contentRef)},
		Timestamp: time.Now().UnixMilli(),
		Dimensions: &client.EntryDimensions{
			Persistence:      1,
			Visibility:       2,
			Delivery:         1,
			Pattern:          3,
			Affinity:         2,
			AffinityKey:      "akey",
			Ordering:         1,
			DedupMode:        2,
			IdempotencyKey:   "idem-1",
			DeliveryComplete: 1,
			CompleteN:        5,
			Priority:         42,
			MaxRedelivery:    3,
			AckTimeoutMs:     15000,
			Correlation:      "corr-1",
			TtlMs:            60000,
		},
	}

	err := fm.replicateEntry(ctx, nil, fed, entry)
	if err != nil {
		t.Fatalf("replicateEntry: %v", err)
	}

	got, err := idx.Get(ctx, entry.Ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Priority != 42 {
		t.Errorf("priority = %d, want 42", got.Priority)
	}
	if got.ExpiresAt == 0 {
		t.Error("expected non-zero ExpiresAt from TTL")
	}
}

// TestReplicateEntryNoDimensions covers the default durable path
// when dimensions are nil.
func TestReplicateEntryNoDimensions(t *testing.T) {
	kp, _ := identity.Generate()
	blobs, idx := newTestStores(t)
	fm := newFederationManager(blobs, idx, kp, newGroupCache())
	defer fm.StopAll()

	ctx := context.Background()

	data := []byte("replicate-no-dims")
	contentRef := reference.Compute(data)
	if _, err := blobs.Store(ctx, data); err != nil {
		t.Fatalf("Store: %v", err)
	}

	fed := &federation{key: "test", peerAddr: "test"}
	entry := &client.Entry{
		Ref:       reference.Compute([]byte("unique-no-dims")),
		Labels:    map[string]string{"content": fmt.Sprintf("%x", contentRef)},
		Timestamp: time.Now().UnixMilli(),
		// No Dimensions
	}

	err := fm.replicateEntry(ctx, nil, fed, entry)
	if err != nil {
		t.Fatalf("replicateEntry: %v", err)
	}

	got, err := idx.Get(ctx, entry.Ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Persistence != 1 {
		t.Errorf("persistence = %d, want 1 (default durable)", got.Persistence)
	}
}

// TestSeekWithCursorReplay covers the handleSeek -> replayFromCursor path
// with a valid timestamp that finds entries.
func TestSeekWithCursorReplay(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	beforeTS := time.Now().UnixMilli()

	for i := 0; i < 3; i++ {
		msg := makeMessage(t, callerKP, "text/plain")
		if _, err := c.SendMessage(ctx, msg, map[string]string{"seek": "replay"}, nil); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
	}

	// Subscribe first so we have a channel open.
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	_, _, err := c.SubscribeMessages(subCtx, "true", map[string]string{"seek": "replay"})
	if err != nil {
		t.Fatalf("SubscribeMessages: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Seek back before the messages.
	err = c.Seek(ctx, "default", beforeTS-1000)
	if err != nil {
		t.Fatalf("Seek: %v", err)
	}
}

// TestPublishNilDimsDefaultDurable covers doPublish with nil dims
// falling through to the default Persistence=1.
func TestPublishNilDimsDefaultDurable(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	msg := makeMessage(t, callerKP, "text/plain")
	_, err := c.SendMessage(ctx, msg, map[string]string{"env": "nil-dims-cov"}, nil)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	result, err := c.QueryMessages(ctx, &client.QueryOptions{
		Expression: `labels["env"] == "nil-dims-cov"`,
	})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result.Entries))
	}
	if result.Entries[0].Dimensions != nil && result.Entries[0].Dimensions.Persistence != 1 {
		t.Errorf("persistence = %d, want 1", result.Entries[0].Dimensions.Persistence)
	}
}

// TestCanPublishForManifestWrongGroupID covers the canPublishFor bootstrap
// path where the manifest group ID does not match the author public key.
func TestCanPublishForManifestWrongGroupID(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	groupKP, _ := identity.Generate()
	otherGroupKP, _ := identity.Generate()

	// Store a valid manifest but for a different group.
	m := &group.Manifest{
		ID:   otherGroupKP.PublicKey(),
		Name: "wrong-group",
		Members: []group.Member{
			{PublicKey: callerKP.PublicKey(), Role: group.RoleAdmin},
		},
	}
	manifestData, _ := group.MarshalManifest(m)
	contentRef, _ := c.PutContent(ctx, manifestData)

	msg := message.New(groupKP.PublicKey(), callerKP.PublicKey(), contentRef, "arc/group.manifest")
	if err := message.Sign(&msg, groupKP); err != nil {
		t.Fatalf("sign: %v", err)
	}

	_, err := c.SendMessage(ctx, msg, nil, nil)
	if err == nil {
		t.Fatal("expected error: manifest group ID does not match author")
	}
}

// TestPublishGroupManifestAutoUpdate covers the doPublish path where
// a valid group manifest is published and auto-updates the group cache.
func TestPublishGroupManifestAutoUpdate(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, callerKP := newTestClient(t, addr, kp)
	ctx := context.Background()

	// Create a valid group manifest.
	manifest := &group.Manifest{
		ID:   callerKP.PublicKey(),
		Name: "test-group",
		Members: []group.Member{
			{PublicKey: callerKP.PublicKey(), Role: group.RoleAdmin},
		},
	}
	manifestData, err := group.MarshalManifest(manifest)
	if err != nil {
		t.Fatalf("MarshalManifest: %v", err)
	}

	contentRef, _ := c.PutContent(ctx, manifestData)

	msg := message.New(callerKP.PublicKey(), callerKP.PublicKey(), contentRef, "arc/group.manifest")
	if err := message.Sign(&msg, callerKP); err != nil {
		t.Fatalf("sign: %v", err)
	}

	// Should succeed and auto-update group cache.
	_, err = c.SendMessage(ctx, msg, nil, nil)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
}

// TestResolveGetIndexNotFound covers the final NotFound path in doResolveGet
// where neither blob nor index match.
func TestResolveGetIndexNotFoundCov(t *testing.T) {
	_, kp, addr := newTestServer(t)
	c, _ := newTestClient(t, addr, kp)
	ctx := context.Background()

	// Use a prefix that won't match anything.
	_, err := c.ResolveGet(ctx, "aabbccdd")
	if err == nil {
		t.Fatal("expected not-found error")
	}
}

// TestReplayFromCursorWithPriority covers the replayFromCursor path with
// entries that have different priorities to exercise the sort.
func TestReplayFromCursorWithPriority(t *testing.T) {
	svc := newTestNodeService(t)
	ctx := context.Background()

	// Index entries with different priorities.
	ts := time.Now().UnixMilli()
	for i, prio := range []int32{1, 5, 3} {
		entry := &idxphysical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("prio-%d", i))),
			Labels:      map[string]string{"ch": "prio"},
			Timestamp:   ts + int64(i),
			Persistence: 1,
			Priority:    prio,
		}
		if err := svc.index.Index(ctx, entry); err != nil {
			t.Fatalf("Index: %v", err)
		}
	}

	w, ms := newMockWriter()
	svc.replayFromCursor(ctx, w, "default", "testkey", fmt.Sprintf("%d", ts-1), map[string]string{"ch": "prio"}, "", nil)

	ms.mu.Lock()
	defer ms.mu.Unlock()
	deliveryCount := 0
	for _, frame := range ms.sent {
		if _, ok := frame.Frame.(*nodev1.ServerFrame_Delivery); ok {
			deliveryCount++
		}
	}
	if deliveryCount != 3 {
		t.Errorf("expected 3 deliveries, got %d", deliveryCount)
	}
}

// TestReplayFromCursorWithDeliveryMode covers replayFromCursor with
// at-least-once entries that trigger Deliver().
func TestReplayFromCursorAtLeastOnce(t *testing.T) {
	svc := newTestNodeService(t)
	ctx := context.Background()

	ts := time.Now().UnixMilli()
	entry := &idxphysical.Entry{
		Ref:          reference.Compute([]byte("replay-alo")),
		Labels:       map[string]string{"ch": "alo"},
		Timestamp:    ts,
		Persistence:  1,
		DeliveryMode: 1, // at-least-once
	}
	if err := svc.index.Index(ctx, entry); err != nil {
		t.Fatalf("Index: %v", err)
	}

	w, ms := newMockWriter()
	svc.replayFromCursor(ctx, w, "default", "testkey", fmt.Sprintf("%d", ts-1), map[string]string{"ch": "alo"}, "", nil)

	ms.mu.Lock()
	defer ms.mu.Unlock()
	for _, frame := range ms.sent {
		if df, ok := frame.Frame.(*nodev1.ServerFrame_Delivery); ok {
			if df.Delivery.DeliveryId == 0 {
				t.Error("expected non-zero delivery ID for at-least-once")
			}
		}
	}
}

// TestReplayFromCursorWithTracker covers replayFromCursor with a non-nil
// subscriber tracker to exercise the entriesSent increment path.
func TestReplayFromCursorWithTracker(t *testing.T) {
	svc := newTestNodeService(t)
	ctx := context.Background()

	ts := time.Now().UnixMilli()
	entry := &idxphysical.Entry{
		Ref:         reference.Compute([]byte("replay-tracker")),
		Labels:      map[string]string{"ch": "tracker"},
		Timestamp:   ts,
		Persistence: 1,
	}
	if err := svc.index.Index(ctx, entry); err != nil {
		t.Fatalf("Index: %v", err)
	}

	tracker := &subscriber{}
	w, _ := newMockWriter()
	svc.replayFromCursor(ctx, w, "default", "testkey", fmt.Sprintf("%d", ts-1), map[string]string{"ch": "tracker"}, "", tracker)

	if tracker.entriesSent.Load() != 1 {
		t.Errorf("entriesSent = %d, want 1", tracker.entriesSent.Load())
	}
}
