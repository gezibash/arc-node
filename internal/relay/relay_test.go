package relay

import (
	"errors"
	"testing"

	relayv1 "github.com/gezibash/arc-node/api/arc/relay/v1"
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

	// Capability registration
	table.RegisterCapability("sub1", "storage")
	caps := table.LookupCapability("storage")
	if len(caps) != 1 || caps[0] != sub {
		t.Error("capability lookup should work")
	}

	// Remove subscriber
	table.Remove("sub1")

	if _, ok := table.Get("sub1"); ok {
		t.Error("subscriber should be removed")
	}
	if _, ok := table.LookupName("alice"); ok {
		t.Error("name should be removed")
	}
	if caps := table.LookupCapability("storage"); len(caps) != 0 {
		t.Error("capability should be removed")
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
		caps:          []string{"storage"},
		subscriptions: make(map[string]map[string]string),
	}
	topicSub := &Subscriber{
		id:            "topic-id",
		subscriptions: make(map[string]map[string]string),
	}
	topicSub.Subscribe("s1", map[string]string{"topic": "news"})

	table.Add(alice)
	table.Add(storage)
	table.Add(topicSub)
	table.RegisterName("alice-id", "alice")
	table.RegisterCapability("storage-id", "storage")

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

	// Test capability routing
	env = &relayv1.Envelope{
		Labels: map[string]string{"capability": "storage"},
	}
	subs, mode = router.Route(env)
	if mode != RouteModeCapability {
		t.Errorf("expected capability mode, got: %v", mode)
	}
	if len(subs) != 1 || subs[0] != storage {
		t.Error("capability routing should find storage")
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
