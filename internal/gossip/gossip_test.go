package gossip

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gezibash/arc/v2/pkg/identity"
)

// =============================================================================
// State encoding round-trip tests
// =============================================================================

func TestRelaysSectionEncodeDecode(t *testing.T) {
	s := NewRelaysSection()
	s.Add(&RelayEntry{
		Name:     "relay-a",
		GRPCAddr: ":50051",
		Pubkey:   "abc123",
		JoinedAt: 1000,
	})
	s.Add(&RelayEntry{
		Name:     "relay-b",
		GRPCAddr: ":50052",
		Pubkey:   "def456",
		JoinedAt: 2000,
	})

	data := s.Encode()

	s2 := NewRelaysSection()
	if err := s2.Merge(data); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	e, ok := s2.Get("relay-a")
	if !ok {
		t.Fatal("relay-a not found after merge")
	}
	if e.GRPCAddr != ":50051" {
		t.Errorf("grpc addr = %q, want :50051", e.GRPCAddr)
	}
	if e.Pubkey != "abc123" {
		t.Errorf("pubkey = %q, want abc123", e.Pubkey)
	}
	if e.JoinedAt != 1000 {
		t.Errorf("joined_at = %d, want 1000", e.JoinedAt)
	}

	e2, ok := s2.Get("relay-b")
	if !ok {
		t.Fatal("relay-b not found after merge")
	}
	if e2.GRPCAddr != ":50052" {
		t.Errorf("grpc addr = %q, want :50052", e2.GRPCAddr)
	}
}

func TestCapabilitiesSectionEncodeDecode(t *testing.T) {
	s := NewCapabilitiesSection()
	s.Add(&CapabilityEntry{
		RelayName:      "relay-a",
		ProviderPubkey: "pub1",
		SubID:          "sub-1",
		Labels:         map[string]string{"capability": "blob", "backend": "s3"},
		ProviderName:   "blob-1",
	})

	data := s.Encode()

	s2 := NewCapabilitiesSection()
	if err := s2.Merge(data); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	entries := s2.Match(map[string]string{"capability": "blob"}, 100)
	if len(entries) != 1 {
		t.Fatalf("expected 1 match, got %d", len(entries))
	}
	e := entries[0]
	if e.RelayName != "relay-a" {
		t.Errorf("relay_name = %q, want relay-a", e.RelayName)
	}
	if e.ProviderPubkey != "pub1" {
		t.Errorf("provider_pubkey = %q, want pub1", e.ProviderPubkey)
	}
	if e.Labels["backend"] != "s3" {
		t.Errorf("labels[backend] = %q, want s3", e.Labels["backend"])
	}
	if e.ProviderName != "blob-1" {
		t.Errorf("provider_name = %q, want blob-1", e.ProviderName)
	}
}

func TestNamesSectionEncodeDecode(t *testing.T) {
	s := NewNamesSection()
	s.Add(&NameEntry{
		Name:      "alice",
		RelayName: "relay-a",
		Pubkey:    "pub-alice",
		UpdatedAt: 5000,
	})

	data := s.Encode()

	s2 := NewNamesSection()
	if err := s2.Merge(data); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	e, ok := s2.Get("alice")
	if !ok {
		t.Fatal("alice not found after merge")
	}
	if e.RelayName != "relay-a" {
		t.Errorf("relay_name = %q, want relay-a", e.RelayName)
	}
	if e.Pubkey != "pub-alice" {
		t.Errorf("pubkey = %q, want pub-alice", e.Pubkey)
	}
}

// =============================================================================
// Merge semantics
// =============================================================================

func TestRelaysMergeLatestWins(t *testing.T) {
	s := NewRelaysSection()
	s.Add(&RelayEntry{
		Name:     "relay-a",
		GRPCAddr: ":50051",
		JoinedAt: 1000,
	})

	// Merge a newer entry for the same relay
	s2 := NewRelaysSection()
	s2.Add(&RelayEntry{
		Name:     "relay-a",
		GRPCAddr: ":50099",
		JoinedAt: 2000, // newer
	})

	data := s2.Encode()
	if err := s.Merge(data); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	e, _ := s.Get("relay-a")
	if e.GRPCAddr != ":50099" {
		t.Errorf("newer entry should win: got %q", e.GRPCAddr)
	}

	// Merge an older entry — should NOT overwrite
	s3 := NewRelaysSection()
	s3.Add(&RelayEntry{
		Name:     "relay-a",
		GRPCAddr: ":50001",
		JoinedAt: 500, // older
	})
	data = s3.Encode()
	if err := s.Merge(data); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	e, _ = s.Get("relay-a")
	if e.GRPCAddr != ":50099" {
		t.Errorf("older entry should not overwrite: got %q", e.GRPCAddr)
	}
}

func TestNamesMergeLatestWins(t *testing.T) {
	s := NewNamesSection()
	s.Add(&NameEntry{
		Name:      "alice",
		RelayName: "relay-a",
		UpdatedAt: 1000,
	})

	// Merge newer
	s2 := NewNamesSection()
	s2.Add(&NameEntry{
		Name:      "alice",
		RelayName: "relay-b",
		UpdatedAt: 2000,
	})
	data := s2.Encode()
	if err := s.Merge(data); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	e, _ := s.Get("alice")
	if e.RelayName != "relay-b" {
		t.Errorf("newer should win: got %q", e.RelayName)
	}
}

func TestCapabilitiesPurgeRelay(t *testing.T) {
	s := NewCapabilitiesSection()
	s.Add(&CapabilityEntry{RelayName: "relay-a", ProviderPubkey: "p1", SubID: "s1", Labels: map[string]string{"cap": "blob"}})
	s.Add(&CapabilityEntry{RelayName: "relay-a", ProviderPubkey: "p2", SubID: "s2", Labels: map[string]string{"cap": "index"}})
	s.Add(&CapabilityEntry{RelayName: "relay-b", ProviderPubkey: "p3", SubID: "s3", Labels: map[string]string{"cap": "blob"}})

	s.PurgeRelay("relay-a")

	entries := s.Match(map[string]string{}, 100)
	if len(entries) != 1 {
		t.Fatalf("expected 1 after purge, got %d", len(entries))
	}
	if entries[0].RelayName != "relay-b" {
		t.Errorf("remaining entry should be relay-b, got %q", entries[0].RelayName)
	}
}

func TestCapabilitiesPurgeProvider(t *testing.T) {
	s := NewCapabilitiesSection()
	s.Add(&CapabilityEntry{RelayName: "relay-a", ProviderPubkey: "p1", SubID: "s1", Labels: map[string]string{"cap": "blob"}})
	s.Add(&CapabilityEntry{RelayName: "relay-a", ProviderPubkey: "p1", SubID: "s2", Labels: map[string]string{"cap": "index"}})
	s.Add(&CapabilityEntry{RelayName: "relay-a", ProviderPubkey: "p2", SubID: "s3", Labels: map[string]string{"cap": "blob"}})

	s.PurgeProvider("relay-a", "p1")

	entries := s.Match(map[string]string{}, 100)
	if len(entries) != 1 {
		t.Fatalf("expected 1 after purge provider, got %d", len(entries))
	}
	if entries[0].ProviderPubkey != "p2" {
		t.Errorf("remaining entry should be p2, got %q", entries[0].ProviderPubkey)
	}
}

func TestNamesPurgeRelay(t *testing.T) {
	s := NewNamesSection()
	s.Add(&NameEntry{Name: "alice", RelayName: "relay-a", UpdatedAt: 1})
	s.Add(&NameEntry{Name: "bob", RelayName: "relay-a", UpdatedAt: 2})
	s.Add(&NameEntry{Name: "charlie", RelayName: "relay-b", UpdatedAt: 3})

	s.PurgeRelay("relay-a")

	if _, ok := s.Get("alice"); ok {
		t.Error("alice should be purged")
	}
	if _, ok := s.Get("bob"); ok {
		t.Error("bob should be purged")
	}
	if _, ok := s.Get("charlie"); !ok {
		t.Error("charlie should remain")
	}
}

// =============================================================================
// HealthMeta
// =============================================================================

func TestHealthMetaEncodeDecode(t *testing.T) {
	h := HealthMeta{
		GRPCAddr:    ":50051",
		Pubkey:      "abcdef1234567890",
		Connections: 42,
		Uptime:      1234567890,
		Version:     "v0.1.0",
		Load:        150,
	}

	data := h.Encode()
	if len(data) > 512 {
		t.Errorf("health meta too large: %d bytes (limit 512)", len(data))
	}

	h2, err := DecodeHealthMeta(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if h2.GRPCAddr != h.GRPCAddr {
		t.Errorf("grpc_addr = %q, want %q", h2.GRPCAddr, h.GRPCAddr)
	}
	if h2.Pubkey != h.Pubkey {
		t.Errorf("pubkey = %q, want %q", h2.Pubkey, h.Pubkey)
	}
	if h2.Connections != h.Connections {
		t.Errorf("connections = %d, want %d", h2.Connections, h.Connections)
	}
	if h2.Uptime != h.Uptime {
		t.Errorf("uptime = %d, want %d", h2.Uptime, h.Uptime)
	}
	if h2.Version != h.Version {
		t.Errorf("version = %q, want %q", h2.Version, h.Version)
	}
	if h2.Load != h.Load {
		t.Errorf("load = %d, want %d", h2.Load, h.Load)
	}
}

func TestHealthMetaFitsIn512Bytes(t *testing.T) {
	// Worst case: long address, long pubkey (hex encoded ed25519)
	h := HealthMeta{
		GRPCAddr:    "192.168.100.200:50051",
		Pubkey:      "ed25519:7f3a8b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a",
		Connections: 99999,
		Uptime:      ^uint64(0),
		Version:     "v1.0.0-rc.1+build.12345",
		Load:        1000,
	}

	data := h.Encode()
	if len(data) > 512 {
		t.Errorf("realistic health meta too large: %d bytes (limit 512)", len(data))
	}
	t.Logf("health meta size: %d bytes", len(data))
}

// =============================================================================
// GossipState full encoding
// =============================================================================

func TestGossipStateEncodeDecodeAll(t *testing.T) {
	state := NewGossipState()
	relays := NewRelaysSection()
	caps := NewCapabilitiesSection()
	names := NewNamesSection()

	state.Register(relays)
	state.Register(caps)
	state.Register(names)

	relays.Add(&RelayEntry{Name: "r1", GRPCAddr: ":50051", JoinedAt: 100})
	caps.Add(&CapabilityEntry{
		RelayName: "r1", ProviderPubkey: "p1", SubID: "s1",
		Labels: map[string]string{"capability": "blob"},
	})
	names.Add(&NameEntry{Name: "alice", RelayName: "r1", Pubkey: "p1", UpdatedAt: 200})

	data := state.EncodeAll()

	// Decode into a fresh state
	state2 := NewGossipState()
	relays2 := NewRelaysSection()
	caps2 := NewCapabilitiesSection()
	names2 := NewNamesSection()
	state2.Register(relays2)
	state2.Register(caps2)
	state2.Register(names2)

	if err := state2.MergeAll(data); err != nil {
		t.Fatalf("merge all failed: %v", err)
	}

	if _, ok := relays2.Get("r1"); !ok {
		t.Error("relay r1 not found after merge all")
	}

	capEntries := caps2.Match(map[string]string{"capability": "blob"}, 100)
	if len(capEntries) != 1 {
		t.Errorf("expected 1 capability match, got %d", len(capEntries))
	}

	if _, ok := names2.Get("alice"); !ok {
		t.Error("name alice not found after merge all")
	}
}

func TestGossipStateIncrementApply(t *testing.T) {
	state := NewGossipState()
	relays := NewRelaysSection()
	state.Register(relays)

	relays.Add(&RelayEntry{Name: "r1", GRPCAddr: ":50051", JoinedAt: 100})

	// Encode as increment
	inc := EncodeIncrement(relays)

	// Apply to fresh state
	state2 := NewGossipState()
	relays2 := NewRelaysSection()
	state2.Register(relays2)

	if err := state2.ApplyIncrement(inc); err != nil {
		t.Fatalf("apply increment failed: %v", err)
	}

	if _, ok := relays2.Get("r1"); !ok {
		t.Error("relay r1 not found after increment")
	}
}

// =============================================================================
// Filter matching on CapabilitiesSection
// =============================================================================

func TestCapabilitiesMatch(t *testing.T) {
	s := NewCapabilitiesSection()
	s.Add(&CapabilityEntry{
		RelayName: "r1", ProviderPubkey: "p1", SubID: "s1",
		Labels: map[string]string{"capability": "blob", "backend": "s3", "region": "us-east-1"},
	})
	s.Add(&CapabilityEntry{
		RelayName: "r1", ProviderPubkey: "p2", SubID: "s2",
		Labels: map[string]string{"capability": "blob", "backend": "local"},
	})
	s.Add(&CapabilityEntry{
		RelayName: "r1", ProviderPubkey: "p3", SubID: "s3",
		Labels: map[string]string{"capability": "index"},
	})

	tests := []struct {
		name   string
		filter map[string]string
		want   int
	}{
		{"all blobs", map[string]string{"capability": "blob"}, 2},
		{"s3 blobs", map[string]string{"capability": "blob", "backend": "s3"}, 1},
		{"index", map[string]string{"capability": "index"}, 1},
		{"no match", map[string]string{"capability": "naming"}, 0},
		{"empty filter", map[string]string{}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := s.Match(tt.filter, 100)
			if len(results) != tt.want {
				t.Errorf("got %d matches, want %d", len(results), tt.want)
			}
		})
	}
}

func TestCapabilitiesMatchLimit(t *testing.T) {
	s := NewCapabilitiesSection()
	for i := range 10 {
		s.Add(&CapabilityEntry{
			RelayName: "r1", ProviderPubkey: fmt.Sprintf("p%d", i), SubID: fmt.Sprintf("s%d", i),
			Labels: map[string]string{"capability": "blob"},
		})
	}

	results := s.Match(map[string]string{"capability": "blob"}, 3)
	if len(results) != 3 {
		t.Errorf("limit should cap at 3, got %d", len(results))
	}
}

// =============================================================================
// Gossip helpers
// =============================================================================

func TestCapabilitiesFindByPubkey(t *testing.T) {
	s := NewCapabilitiesSection()
	s.Add(&CapabilityEntry{
		RelayName: "r1", ProviderPubkey: "pub-alice", SubID: "s1",
		Labels: map[string]string{"capability": "blob"},
	})
	s.Add(&CapabilityEntry{
		RelayName: "r2", ProviderPubkey: "pub-bob", SubID: "s2",
		Labels: map[string]string{"capability": "index"},
	})

	entry, ok := s.FindByPubkey("pub-bob")
	if !ok {
		t.Fatal("expected to find pub-bob")
	}
	if entry.RelayName != "r2" {
		t.Errorf("relay = %q, want r2", entry.RelayName)
	}
	if entry.Labels["capability"] != "index" {
		t.Errorf("capability = %q, want index", entry.Labels["capability"])
	}

	_, ok = s.FindByPubkey("pub-nonexistent")
	if ok {
		t.Error("should not find nonexistent pubkey")
	}
}

func TestResolveRelay(t *testing.T) {
	relays := NewRelaysSection()
	relays.Add(&RelayEntry{Name: "relay-a", GRPCAddr: ":50051"})
	relays.Add(&RelayEntry{Name: "relay-b", GRPCAddr: ":50052"})

	g := &Gossip{relays: relays}

	addr, ok := g.ResolveRelay("relay-a")
	if !ok || addr != ":50051" {
		t.Errorf("ResolveRelay(relay-a) = %q, %v; want :50051, true", addr, ok)
	}

	_, ok = g.ResolveRelay("relay-c")
	if ok {
		t.Error("should not resolve nonexistent relay")
	}
}

func TestIsKnownRelay(t *testing.T) {
	relays := NewRelaysSection()
	relays.Add(&RelayEntry{Name: "relay-a", Pubkey: "pk-a"})
	relays.Add(&RelayEntry{Name: "relay-b", Pubkey: "pk-b"})

	g := &Gossip{relays: relays}

	if !g.IsKnownRelay("pk-a") {
		t.Error("pk-a should be known")
	}
	if !g.IsKnownRelay("pk-b") {
		t.Error("pk-b should be known")
	}
	if g.IsKnownRelay("pk-unknown") {
		t.Error("pk-unknown should not be known")
	}
}

// =============================================================================
// Integration test: 2 nodes with DefaultLocalConfig
// =============================================================================

func TestGossipIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Node A
	gA, err := New(Config{
		NodeName:    "node-a",
		BindAddr:    "127.0.0.1",
		BindPort:    0, // random port
		GRPCAddr:    ":50051",
		RelayPubkey: "pubkey-a",
	})
	if err != nil {
		t.Fatalf("create node-a: %v", err)
	}
	t.Cleanup(func() { _ = gA.Close() })

	if err := gA.Start(ctx); err != nil {
		t.Fatalf("start node-a: %v", err)
	}

	// Get node A's actual address
	membersA := gA.list.Members()
	if len(membersA) == 0 {
		t.Fatal("node-a has no members")
	}
	addrA := membersA[0].Address()

	// Node B — joins A
	gB, err := New(Config{
		NodeName:    "node-b",
		BindAddr:    "127.0.0.1",
		BindPort:    0, // random port
		Seeds:       []string{addrA},
		GRPCAddr:    ":50052",
		RelayPubkey: "pubkey-b",
	})
	if err != nil {
		t.Fatalf("create node-b: %v", err)
	}
	t.Cleanup(func() { _ = gB.Close() })

	if err := gB.Start(ctx); err != nil {
		t.Fatalf("start node-b: %v", err)
	}

	// Wait for convergence
	time.Sleep(500 * time.Millisecond)

	// Verify both nodes see each other
	if len(gA.Members()) != 2 {
		t.Errorf("node-a should see 2 members, got %d", len(gA.Members()))
	}
	if len(gB.Members()) != 2 {
		t.Errorf("node-b should see 2 members, got %d", len(gB.Members()))
	}

	// Node A: add a capability via observer
	gA.OnSubscribe(
		fakePublicKey("cap-provider"),
		"blob-sub-1",
		map[string]string{"capability": "blob", "backend": "s3"},
		"blob-1",
	)

	// Node A: register a name
	gA.OnNameRegistered("alice", fakePublicKey("alice"))

	// Wait for gossip propagation
	time.Sleep(2 * time.Second)

	// Node B: discover capabilities from A
	providers := gB.DiscoverRemote(map[string]string{"capability": "blob"}, 100)
	if len(providers) != 1 {
		t.Errorf("node-b should discover 1 blob provider from node-a, got %d", len(providers))
	}

	// Node B: check names propagated
	if nameEntry, ok := gB.names.Get("alice"); !ok {
		t.Error("node-b should know about alice")
	} else if nameEntry.RelayName != "node-a" {
		t.Errorf("alice relay = %q, want node-a", nameEntry.RelayName)
	}

	// Node A: remove the subscriber
	gA.OnSubscriberRemoved(fakePublicKey("cap-provider"))

	// Wait for propagation
	time.Sleep(2 * time.Second)

	// Node B: should no longer see the capability
	providers = gB.DiscoverRemote(map[string]string{"capability": "blob"}, 100)
	if len(providers) != 0 {
		t.Errorf("node-b should see 0 providers after removal, got %d", len(providers))
	}
}

func TestGossipIntegrationThreeNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Node A (seed)
	gA, err := New(Config{
		NodeName: "node-a",
		BindAddr: "127.0.0.1",
		BindPort: 0,
		GRPCAddr: ":50051",
	})
	if err != nil {
		t.Fatalf("create node-a: %v", err)
	}
	t.Cleanup(func() { _ = gA.Close() })

	if err := gA.Start(ctx); err != nil {
		t.Fatalf("start node-a: %v", err)
	}

	addrA := gA.list.Members()[0].Address()

	// Node B — joins A
	gB, err := New(Config{
		NodeName: "node-b",
		BindAddr: "127.0.0.1",
		BindPort: 0,
		Seeds:    []string{addrA},
		GRPCAddr: ":50052",
	})
	if err != nil {
		t.Fatalf("create node-b: %v", err)
	}
	t.Cleanup(func() { _ = gB.Close() })

	if err := gB.Start(ctx); err != nil {
		t.Fatalf("start node-b: %v", err)
	}

	// Node C — also joins A
	gC, err := New(Config{
		NodeName: "node-c",
		BindAddr: "127.0.0.1",
		BindPort: 0,
		Seeds:    []string{addrA},
		GRPCAddr: ":50053",
	})
	if err != nil {
		t.Fatalf("create node-c: %v", err)
	}
	t.Cleanup(func() { _ = gC.Close() })

	if err := gC.Start(ctx); err != nil {
		t.Fatalf("start node-c: %v", err)
	}

	// Wait for convergence
	time.Sleep(time.Second)

	// All three should see each other
	for name, g := range map[string]*Gossip{"a": gA, "b": gB, "c": gC} {
		if n := len(g.Members()); n != 3 {
			t.Errorf("node-%s should see 3 members, got %d", name, n)
		}
	}
}

// fakePublicKey creates a deterministic identity.PublicKey from a string.
func fakePublicKey(name string) identity.PublicKey {
	b := make([]byte, 32)
	copy(b, name)
	return identity.PublicKey{
		Algo:  identity.AlgEd25519,
		Bytes: b,
	}
}
