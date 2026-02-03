package relay

import (
	"encoding/hex"
	"errors"
	"testing"

	relayv1 "github.com/gezibash/arc-node/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
)

func TestTable(t *testing.T) {
	table := NewTable()

	// Create a mock subscriber
	sub := &Subscriber{
		id:            "sub1",
		subscriptions: make(map[string]map[string]string),
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
		subscriptions: make(map[string]map[string]string),
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
		subscriptions: make(map[string]map[string]string),
	}
	storage := &Subscriber{
		id:            "storage-id",
		subscriptions: make(map[string]map[string]string),
	}
	storage.Subscribe("cap-sub", map[string]string{"capability": "storage"})

	topicSub := &Subscriber{
		id:            "topic-id",
		subscriptions: make(map[string]map[string]string),
	}
	topicSub.Subscribe("s1", map[string]string{"topic": "news"})

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
	copy(pubkey[:], pubkeyBytes)

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
	sub := NewSubscriber("test", nil, [32]byte{}, 2)

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
