package gossip

import (
	"context"
	"testing"
	"time"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"google.golang.org/grpc"
)

func TestForwarderResolveTargetByName(t *testing.T) {
	f := newTestForwarder()

	// Register name "bob" on relay-b
	f.gossip.names.Add(&NameEntry{
		Name:      "bob",
		RelayName: "relay-b",
		Pubkey:    "pub-bob",
	})
	f.gossip.relays.Add(&RelayEntry{
		Name:     "relay-b",
		GRPCAddr: "localhost:50052",
	})

	env := &relayv1.Envelope{
		Labels: map[string]string{"to": "bob"},
	}

	name, addr, ok := f.resolveTarget(env)
	if !ok {
		t.Fatal("expected resolve to succeed for name 'bob'")
	}
	if name != "relay-b" {
		t.Errorf("relay name = %q, want relay-b", name)
	}
	if addr != "localhost:50052" {
		t.Errorf("addr = %q, want localhost:50052", addr)
	}
}

func TestForwarderResolveTargetByPubkey(t *testing.T) {
	f := newTestForwarder()

	// Register capability with pubkey on relay-b
	f.gossip.capabilities.Add(&CapabilityEntry{
		RelayName:      "relay-b",
		ProviderPubkey: "ed25519:abc123",
		SubID:          "sub-1",
		Labels:         map[string]string{"capability": "blob"},
	})
	f.gossip.relays.Add(&RelayEntry{
		Name:     "relay-b",
		GRPCAddr: "localhost:50052",
	})

	env := &relayv1.Envelope{
		Labels: map[string]string{"to": "ed25519:abc123"},
	}

	name, addr, ok := f.resolveTarget(env)
	if !ok {
		t.Fatal("expected resolve to succeed for pubkey")
	}
	if name != "relay-b" {
		t.Errorf("relay name = %q, want relay-b", name)
	}
	if addr != "localhost:50052" {
		t.Errorf("addr = %q, want localhost:50052", addr)
	}
}

func TestForwarderResolveTargetByLabelMatch(t *testing.T) {
	f := newTestForwarder()

	// Register capability on relay-b
	f.gossip.capabilities.Add(&CapabilityEntry{
		RelayName:      "relay-b",
		ProviderPubkey: "pub-1",
		SubID:          "sub-1",
		Labels:         map[string]string{"capability": "storage", "backend": "s3"},
	})
	f.gossip.relays.Add(&RelayEntry{
		Name:     "relay-b",
		GRPCAddr: "localhost:50052",
	})

	env := &relayv1.Envelope{
		Labels: map[string]string{"capability": "storage"},
	}

	name, addr, ok := f.resolveTarget(env)
	if !ok {
		t.Fatal("expected resolve to succeed for label match")
	}
	if name != "relay-b" {
		t.Errorf("relay name = %q, want relay-b", name)
	}
	if addr != "localhost:50052" {
		t.Errorf("addr = %q, want localhost:50052", addr)
	}
}

func TestForwarderResolveTargetSkipsLocalRelay(t *testing.T) {
	f := newTestForwarder()

	// Capability on local relay — should be skipped
	f.gossip.capabilities.Add(&CapabilityEntry{
		RelayName:      "relay-a",
		ProviderPubkey: "pub-local",
		SubID:          "sub-1",
		Labels:         map[string]string{"capability": "blob"},
	})

	env := &relayv1.Envelope{
		Labels: map[string]string{"capability": "blob"},
	}

	_, _, ok := f.resolveTarget(env)
	if ok {
		t.Error("should not resolve to local relay")
	}
}

func TestForwarderResolveTargetNoMatch(t *testing.T) {
	f := newTestForwarder()

	env := &relayv1.Envelope{
		Labels: map[string]string{"capability": "nonexistent"},
	}

	_, _, ok := f.resolveTarget(env)
	if ok {
		t.Error("should not resolve when no matching capabilities")
	}
}

func TestForwarderResolveTargetNameSkipsLocal(t *testing.T) {
	f := newTestForwarder()

	// Name registered on local relay — should be skipped
	f.gossip.names.Add(&NameEntry{
		Name:      "alice",
		RelayName: "relay-a",
		Pubkey:    "pub-alice",
	})

	env := &relayv1.Envelope{
		Labels: map[string]string{"to": "alice"},
	}

	_, _, ok := f.resolveTarget(env)
	if ok {
		t.Error("should not resolve name on local relay")
	}
}

func TestForwarderResolveTargetNilLabels(t *testing.T) {
	f := newTestForwarder()

	env := &relayv1.Envelope{}

	_, _, ok := f.resolveTarget(env)
	if ok {
		t.Error("should not resolve with nil labels")
	}
}

func TestForwarderConnectionCaching(t *testing.T) {
	f := newTestForwarder()

	conn1, err := f.getOrDial(context.TODO(), "localhost:50052")
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}

	conn2, err := f.getOrDial(context.TODO(), "localhost:50052")
	if err != nil {
		t.Fatalf("second dial: %v", err)
	}

	if conn1 != conn2 {
		t.Error("expected same connection for same address")
	}

	conn3, err := f.getOrDial(context.TODO(), "localhost:50053")
	if err != nil {
		t.Fatalf("third dial: %v", err)
	}

	if conn1 == conn3 {
		t.Error("expected different connection for different address")
	}

	f.Close()
}

func TestForwarderCloseConn(t *testing.T) {
	f := newTestForwarder()

	_, err := f.getOrDial(context.TODO(), "localhost:50052")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	f.mu.RLock()
	_, exists := f.conns["localhost:50052"]
	f.mu.RUnlock()
	if !exists {
		t.Fatal("connection should be cached")
	}

	f.CloseConn("localhost:50052")

	f.mu.RLock()
	_, exists = f.conns["localhost:50052"]
	f.mu.RUnlock()
	if exists {
		t.Error("connection should be removed after CloseConn")
	}
}

// newTestForwarder creates a Forwarder with a minimal gossip for testing.
func newTestForwarder() *Forwarder {
	g := &Gossip{
		localName:    "relay-a",
		relays:       NewRelaysSection(),
		capabilities: NewCapabilitiesSection(),
		names:        NewNamesSection(),
	}
	return &Forwarder{
		gossip:  g,
		conns:   make(map[string]*grpc.ClientConn),
		timeout: 5 * time.Second,
	}
}
