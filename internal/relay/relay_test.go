package relay

import (
	"encoding/hex"
	"errors"
	"testing"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
)

func TestTable(t *testing.T) {
	table := NewTable()

	// Create a mock subscriber
	sub := &Subscriber{
		id:            "sub1",
		subscriptions: make(map[string]*SubscriptionData),
	}

	table.Add(sub)

	if got, ok := table.Get("sub1"); !ok || got != sub {
		t.Error("subscriber should be retrievable")
	}

	// Register name
	if err := table.RegisterName("sub1", "alice"); err != nil {
		t.Errorf("RegisterName failed: %v", err)
	}

	if got, ok := table.LookupName("alice"); !ok || got != sub {
		t.Error("name lookup should work")
	}

	// Duplicate name should fail for different subscriber
	sub2 := &Subscriber{
		id:            "sub2",
		subscriptions: make(map[string]*SubscriptionData),
	}
	table.Add(sub2)

	if err := table.RegisterName("sub2", "alice"); !errors.Is(err, ErrNameTaken) {
		t.Errorf("expected ErrNameTaken, got: %v", err)
	}

	// Remove subscriber
	table.Remove("sub1")

	if _, ok := table.Get("sub1"); ok {
		t.Error("subscriber should be removed")
	}
	if _, ok := table.LookupName("alice"); ok {
		t.Error("name should be removed")
	}
}

func TestRouter(t *testing.T) {
	table := NewTable()
	router := NewRouter(table)

	// Create subscribers
	alice := &Subscriber{
		id:            "alice-id",
		name:          "alice",
		subscriptions: make(map[string]*SubscriptionData),
	}
	storage := &Subscriber{
		id:            "storage-id",
		subscriptions: make(map[string]*SubscriptionData),
	}
	storage.Subscribe("cap-sub", map[string]any{"capability": "storage"})

	topicSub := &Subscriber{
		id:            "topic-id",
		subscriptions: make(map[string]*SubscriptionData),
	}
	topicSub.Subscribe("s1", map[string]any{"topic": "news"})

	table.Add(alice)
	table.Add(storage)
	table.Add(topicSub)
	table.RegisterName("alice-id", "alice")

	// Test addressed routing
	env := &relayv1.Envelope{
		Labels: map[string]string{"to": "alice"},
	}
	subs, mode := router.Route(env)
	if mode != RouteModeAddressed {
		t.Errorf("expected addressed mode, got: %v", mode)
	}
	if len(subs) != 1 || subs[0] != alice {
		t.Error("addressed routing should find alice")
	}

	// Test capability routing (now via label-match)
	env = &relayv1.Envelope{
		Labels: map[string]string{"capability": "storage"},
	}
	subs, mode = router.Route(env)
	if mode != RouteModeLabelMatch {
		t.Errorf("expected label-match mode for capability, got: %v", mode)
	}
	if len(subs) != 1 || subs[0] != storage {
		t.Error("capability routing should find storage via label-match")
	}

	// Test label-match routing
	env = &relayv1.Envelope{
		Labels: map[string]string{"topic": "news"},
	}
	subs, mode = router.Route(env)
	if mode != RouteModeLabelMatch {
		t.Errorf("expected label-match mode, got: %v", mode)
	}
	if len(subs) != 1 || subs[0] != topicSub {
		t.Error("label-match routing should find topic subscriber")
	}
}

func TestPubkeyRouting(t *testing.T) {
	table := NewTable()
	router := NewRouter(table)

	// Create subscriber with known pubkey
	var pubkey identity.PublicKey
	pubkeyHex := "7f3a8b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a"
	pubkeyBytes, _ := hex.DecodeString(pubkeyHex)
	pubkey = identity.PublicKey{Algo: identity.AlgEd25519, Bytes: pubkeyBytes}

	bob := NewSubscriber("bob-id", nil, pubkey, 10)
	table.Add(bob)

	// Test routing by pubkey
	env := &relayv1.Envelope{
		Labels: map[string]string{"to": pubkeyHex},
	}
	subs, mode := router.Route(env)
	if mode != RouteModeAddressed {
		t.Errorf("expected addressed mode, got: %v", mode)
	}
	if len(subs) != 1 || subs[0] != bob {
		t.Error("pubkey routing should find bob")
	}

	// Test routing by uppercase pubkey (should also work)
	env = &relayv1.Envelope{
		Labels: map[string]string{"to": "7F3A8B9C2D1E4F5A6B7C8D9E0F1A2B3C4D5E6F7A8B9C0D1E2F3A4B5C6D7E8F9A"},
	}
	subs, mode = router.Route(env)
	if mode != RouteModeAddressed {
		t.Errorf("expected addressed mode for uppercase, got: %v", mode)
	}
	if len(subs) != 1 || subs[0] != bob {
		t.Error("pubkey routing with uppercase should find bob")
	}

	// Test that name routing still works (should not find anything)
	env = &relayv1.Envelope{
		Labels: map[string]string{"to": "bob"},
	}
	subs, mode = router.Route(env)
	if mode != RouteModeAddressed {
		t.Errorf("expected addressed mode, got: %v", mode)
	}
	if len(subs) != 0 {
		t.Error("name routing should not find bob (not registered)")
	}

	// Register name for bob
	_ = table.RegisterName("bob-id", "bob")

	// Now name routing should work
	subs, mode = router.Route(env)
	if mode != RouteModeAddressed {
		t.Errorf("expected addressed mode, got: %v", mode)
	}
	if len(subs) != 1 || subs[0] != bob {
		t.Error("name routing should find bob after registration")
	}
}

func TestSubscriberNonBlockingSend(t *testing.T) {
	sub := NewSubscriber("test", nil, identity.PublicKey{}, 2)

	frame := &relayv1.ServerFrame{}

	// Fill buffer
	if !sub.Send(frame) {
		t.Error("first send should succeed")
	}
	if !sub.Send(frame) {
		t.Error("second send should succeed")
	}

	// Buffer full - should drop
	if sub.Send(frame) {
		t.Error("third send should fail (buffer full)")
	}

	// Verify buffer state
	if !sub.BufferFull() {
		t.Error("buffer should be full")
	}
	if sub.BufferLen() != 2 {
		t.Errorf("buffer length should be 2, got: %d", sub.BufferLen())
	}
}

func TestTableDiscover(t *testing.T) {
	table := NewTable()

	// Create subscribers with different subscriptions
	blobProvider := &Subscriber{
		id:            "blob-id",
		name:          "blob-1",
		subscriptions: make(map[string]*SubscriptionData),
	}
	blobProvider.Subscribe("cap-blob", map[string]any{
		"capability": "blob",
		"transport":  "relay",
	})

	directBlobProvider := &Subscriber{
		id:            "direct-blob-id",
		subscriptions: make(map[string]*SubscriptionData),
	}
	directBlobProvider.Subscribe("cap-blob-direct", map[string]any{
		"capability":  "blob",
		"transport":   "direct",
		"direct_addr": "10.0.0.5:8080",
	})

	indexProvider := &Subscriber{
		id:            "index-id",
		subscriptions: make(map[string]*SubscriptionData),
	}
	indexProvider.Subscribe("cap-index", map[string]any{
		"capability": "index",
	})

	// Add a subscriber with multiple subscriptions
	multiSub := &Subscriber{
		id:            "multi-id",
		subscriptions: make(map[string]*SubscriptionData),
	}
	multiSub.Subscribe("topic-news", map[string]any{"topic": "news"})
	multiSub.Subscribe("topic-sports", map[string]any{"topic": "sports"})

	table.Add(blobProvider)
	table.Add(directBlobProvider)
	table.Add(indexProvider)
	table.Add(multiSub)

	t.Run("discover by capability", func(t *testing.T) {
		results, total := table.Discover(map[string]string{"capability": "blob"}, 100)
		if total != 2 {
			t.Errorf("expected 2 blob providers, got %d", total)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}
	})

	t.Run("discover with multiple filter labels", func(t *testing.T) {
		results, total := table.Discover(map[string]string{
			"capability": "blob",
			"transport":  "direct",
		}, 100)
		if total != 1 {
			t.Errorf("expected 1 direct blob provider, got %d", total)
		}
		if len(results) != 1 {
			t.Errorf("expected 1 result, got %d", len(results))
		}
		if len(results) > 0 && results[0].Labels["direct_addr"] != "10.0.0.5:8080" {
			t.Error("should return direct blob provider")
		}
	})

	t.Run("empty filter returns all subscriptions", func(t *testing.T) {
		results, total := table.Discover(map[string]string{}, 100)
		// 2 blob + 1 index + 2 topics = 5 subscriptions
		if total != 5 {
			t.Errorf("expected 5 total subscriptions, got %d", total)
		}
		if len(results) != 5 {
			t.Errorf("expected 5 results, got %d", len(results))
		}
	})

	t.Run("no matches", func(t *testing.T) {
		results, total := table.Discover(map[string]string{"capability": "naming"}, 100)
		if total != 0 {
			t.Errorf("expected 0 matches, got %d", total)
		}
		if len(results) != 0 {
			t.Errorf("expected 0 results, got %d", len(results))
		}
	})

	t.Run("limit results", func(t *testing.T) {
		results, total := table.Discover(map[string]string{}, 2)
		if total != 5 {
			t.Errorf("total should still be 5, got %d", total)
		}
		if len(results) != 2 {
			t.Errorf("results should be limited to 2, got %d", len(results))
		}
	})

	t.Run("returns subscription details", func(t *testing.T) {
		results, _ := table.Discover(map[string]string{"capability": "index"}, 100)
		if len(results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(results))
		}
		r := results[0]
		if r.Subscriber != indexProvider {
			t.Error("should return correct subscriber")
		}
		if r.SubscriptionID != "cap-index" {
			t.Errorf("subscription ID should be cap-index, got %s", r.SubscriptionID)
		}
		if r.Labels["capability"] != "index" {
			t.Error("should include labels")
		}
	})
}

func TestMatchesFilter(t *testing.T) {
	tests := []struct {
		name      string
		filter    map[string]string
		subLabels map[string]any
		want      bool
	}{
		{
			name:      "empty filter matches all",
			filter:    map[string]string{},
			subLabels: map[string]any{"capability": "blob"},
			want:      true,
		},
		{
			name:      "filter subset of subscription",
			filter:    map[string]string{"capability": "blob"},
			subLabels: map[string]any{"capability": "blob", "transport": "relay"},
			want:      true,
		},
		{
			name:      "exact match",
			filter:    map[string]string{"capability": "blob"},
			subLabels: map[string]any{"capability": "blob"},
			want:      true,
		},
		{
			name:      "filter not subset - extra key",
			filter:    map[string]string{"capability": "blob", "transport": "relay"},
			subLabels: map[string]any{"capability": "blob"},
			want:      false,
		},
		{
			name:      "filter not subset - different value",
			filter:    map[string]string{"capability": "blob"},
			subLabels: map[string]any{"capability": "index"},
			want:      false,
		},
		{
			name:      "empty subscription matches empty filter",
			filter:    map[string]string{},
			subLabels: map[string]any{},
			want:      true,
		},
		{
			name:      "non-string labels skipped in filter match",
			filter:    map[string]string{"capability": "blob"},
			subLabels: map[string]any{"capability": "blob", "capacity": int64(1000)},
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesFilter(tt.filter, tt.subLabels)
			if got != tt.want {
				t.Errorf("matchesFilter(%v, %v) = %v, want %v",
					tt.filter, tt.subLabels, got, tt.want)
			}
		})
	}
}
