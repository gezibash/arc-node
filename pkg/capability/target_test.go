package capability

import (
	"testing"
	"time"

	"github.com/gezibash/arc/v2/pkg/identity"
)

func testPubkey(b byte) identity.PublicKey {
	raw := make([]byte, 32)
	raw[0] = b
	return identity.PublicKey{Algo: identity.AlgEd25519, Bytes: raw}
}

func TestTarget_DisplayName(t *testing.T) {
	tests := []struct {
		name   string
		target Target
		want   string
	}{
		{
			name:   "with registered name",
			target: Target{Name: "alice", Petname: "clever-penguin"},
			want:   "@alice",
		},
		{
			name:   "without name uses petname",
			target: Target{Petname: "clever-penguin"},
			want:   "clever-penguin",
		},
		{
			name:   "both empty returns empty string",
			target: Target{},
			want:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.target.DisplayName()
			if got != tt.want {
				t.Errorf("DisplayName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTarget_HasDirect(t *testing.T) {
	tests := []struct {
		name   string
		target Target
		want   bool
	}{
		{
			name:   "with address",
			target: Target{Address: "192.168.1.1:8080"},
			want:   true,
		},
		{
			name:   "without address",
			target: Target{},
			want:   false,
		},
		{
			name:   "empty address string",
			target: Target{Address: ""},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.target.HasDirect()
			if got != tt.want {
				t.Errorf("HasDirect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEmptyTargetSet(t *testing.T) {
	ts := EmptyTargetSet()

	if ts.Len() != 0 {
		t.Errorf("Len() = %d, want 0", ts.Len())
	}
	if !ts.IsEmpty() {
		t.Error("IsEmpty() = false, want true")
	}
	if ts.All() != nil {
		t.Errorf("All() = %v, want nil", ts.All())
	}
	if ts.First() != nil {
		t.Errorf("First() = %v, want nil", ts.First())
	}
}

func TestNewTargetSet(t *testing.T) {
	targets := []Target{
		{Petname: "alpha", Address: "10.0.0.1:8080"},
		{Petname: "bravo", Address: "10.0.0.2:8080"},
		{Petname: "charlie"},
	}
	ts := NewTargetSet(targets)

	if ts.Len() != 3 {
		t.Errorf("Len() = %d, want 3", ts.Len())
	}
	if ts.IsEmpty() {
		t.Error("IsEmpty() = true, want false")
	}
	all := ts.All()
	if len(all) != 3 {
		t.Fatalf("All() len = %d, want 3", len(all))
	}
	if all[0].Petname != "alpha" {
		t.Errorf("All()[0].Petname = %q, want %q", all[0].Petname, "alpha")
	}
	if all[2].Petname != "charlie" {
		t.Errorf("All()[2].Petname = %q, want %q", all[2].Petname, "charlie")
	}
}

func TestTargetFromPubkey(t *testing.T) {
	pk := testPubkey(0xAB)
	ts := TargetFromPubkey(pk)

	if ts.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", ts.Len())
	}
	if ts.IsEmpty() {
		t.Error("IsEmpty() = true, want false")
	}

	first := ts.First()
	if first == nil {
		t.Fatal("First() = nil, want non-nil")
	}
	if first.PublicKey.Algo != identity.AlgEd25519 {
		t.Errorf("PublicKey.Algo = %q, want %q", first.PublicKey.Algo, identity.AlgEd25519)
	}
	if first.PublicKey.Bytes[0] != 0xAB {
		t.Errorf("PublicKey.Bytes[0] = %x, want %x", first.PublicKey.Bytes[0], 0xAB)
	}
}

func TestTargetFromAddress(t *testing.T) {
	addr := "192.168.1.100:9090"
	ts := TargetFromAddress(addr)

	if ts.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", ts.Len())
	}
	if ts.IsEmpty() {
		t.Error("IsEmpty() = true, want false")
	}

	first := ts.First()
	if first == nil {
		t.Fatal("First() = nil, want non-nil")
	}
	if first.Address != addr {
		t.Errorf("Address = %q, want %q", first.Address, addr)
	}
}

func TestTargetSet_Len(t *testing.T) {
	t.Run("nil target set", func(t *testing.T) {
		var ts *TargetSet
		if ts.Len() != 0 {
			t.Errorf("Len() = %d, want 0", ts.Len())
		}
	})

	t.Run("empty target set", func(t *testing.T) {
		ts := EmptyTargetSet()
		if ts.Len() != 0 {
			t.Errorf("Len() = %d, want 0", ts.Len())
		}
	})

	t.Run("populated target set", func(t *testing.T) {
		ts := NewTargetSet([]Target{{Petname: "a"}, {Petname: "b"}})
		if ts.Len() != 2 {
			t.Errorf("Len() = %d, want 2", ts.Len())
		}
	})
}

func TestTargetSet_IsEmpty(t *testing.T) {
	t.Run("nil target set", func(t *testing.T) {
		var ts *TargetSet
		if !ts.IsEmpty() {
			t.Error("IsEmpty() = false, want true")
		}
	})

	t.Run("empty target set", func(t *testing.T) {
		ts := EmptyTargetSet()
		if !ts.IsEmpty() {
			t.Error("IsEmpty() = false, want true")
		}
	})

	t.Run("populated target set", func(t *testing.T) {
		ts := NewTargetSet([]Target{{Petname: "a"}})
		if ts.IsEmpty() {
			t.Error("IsEmpty() = true, want false")
		}
	})
}

func TestTargetSet_All(t *testing.T) {
	t.Run("nil target set", func(t *testing.T) {
		var ts *TargetSet
		if ts.All() != nil {
			t.Errorf("All() = %v, want nil", ts.All())
		}
	})

	t.Run("empty target set", func(t *testing.T) {
		ts := EmptyTargetSet()
		if ts.All() != nil {
			t.Errorf("All() = %v, want nil", ts.All())
		}
	})

	t.Run("populated target set", func(t *testing.T) {
		targets := []Target{{Petname: "x"}, {Petname: "y"}}
		ts := NewTargetSet(targets)
		all := ts.All()
		if len(all) != 2 {
			t.Fatalf("All() len = %d, want 2", len(all))
		}
		if all[0].Petname != "x" || all[1].Petname != "y" {
			t.Errorf("All() = %v, want petnames [x, y]", all)
		}
	})
}

func TestTargetSet_First(t *testing.T) {
	t.Run("nil target set", func(t *testing.T) {
		var ts *TargetSet
		if ts.First() != nil {
			t.Errorf("First() = %v, want nil", ts.First())
		}
	})

	t.Run("empty target set", func(t *testing.T) {
		ts := EmptyTargetSet()
		if ts.First() != nil {
			t.Errorf("First() = %v, want nil", ts.First())
		}
	})

	t.Run("single target", func(t *testing.T) {
		ts := NewTargetSet([]Target{{Petname: "only"}})
		first := ts.First()
		if first == nil {
			t.Fatal("First() = nil, want non-nil")
		}
		if first.Petname != "only" {
			t.Errorf("First().Petname = %q, want %q", first.Petname, "only")
		}
	})

	t.Run("multiple targets returns first", func(t *testing.T) {
		ts := NewTargetSet([]Target{
			{Petname: "first"},
			{Petname: "second"},
			{Petname: "third"},
		})
		first := ts.First()
		if first == nil {
			t.Fatal("First() = nil, want non-nil")
		}
		if first.Petname != "first" {
			t.Errorf("First().Petname = %q, want %q", first.Petname, "first")
		}
	})
}

func TestTargetSet_Pick(t *testing.T) {
	t.Run("match found", func(t *testing.T) {
		ts := NewTargetSet([]Target{
			{Petname: "alpha", Address: "10.0.0.1:80"},
			{Petname: "bravo"},
			{Petname: "charlie", Address: "10.0.0.3:80"},
		})
		got := ts.Pick(func(tgt Target) bool {
			return tgt.Petname == "charlie"
		})
		if got == nil {
			t.Fatal("Pick() = nil, want non-nil")
		}
		if got.Petname != "charlie" {
			t.Errorf("Pick().Petname = %q, want %q", got.Petname, "charlie")
		}
	})

	t.Run("returns first match", func(t *testing.T) {
		ts := NewTargetSet([]Target{
			{Petname: "alpha", Address: "10.0.0.1:80"},
			{Petname: "bravo", Address: "10.0.0.2:80"},
		})
		got := ts.Pick(func(tgt Target) bool {
			return tgt.HasDirect()
		})
		if got == nil {
			t.Fatal("Pick() = nil, want non-nil")
		}
		if got.Petname != "alpha" {
			t.Errorf("Pick().Petname = %q, want %q", got.Petname, "alpha")
		}
	})

	t.Run("no match", func(t *testing.T) {
		ts := NewTargetSet([]Target{
			{Petname: "alpha"},
			{Petname: "bravo"},
		})
		got := ts.Pick(func(tgt Target) bool {
			return tgt.Petname == "nonexistent"
		})
		if got != nil {
			t.Errorf("Pick() = %v, want nil", got)
		}
	})

	t.Run("nil target set", func(t *testing.T) {
		var ts *TargetSet
		got := ts.Pick(func(tgt Target) bool {
			return true
		})
		if got != nil {
			t.Errorf("Pick() = %v, want nil", got)
		}
	})

	t.Run("empty target set", func(t *testing.T) {
		ts := EmptyTargetSet()
		got := ts.Pick(func(tgt Target) bool {
			return true
		})
		if got != nil {
			t.Errorf("Pick() = %v, want nil", got)
		}
	})
}

func TestTargetSet_Filter(t *testing.T) {
	ts := NewTargetSet([]Target{
		{Petname: "alpha", Address: "10.0.0.1:80"},
		{Petname: "bravo"},
		{Petname: "charlie", Address: "10.0.0.3:80"},
		{Petname: "delta"},
	})

	t.Run("some match", func(t *testing.T) {
		filtered := ts.Filter(func(tgt Target) bool {
			return tgt.HasDirect()
		})
		if filtered.Len() != 2 {
			t.Fatalf("Filter() len = %d, want 2", filtered.Len())
		}
		all := filtered.All()
		if all[0].Petname != "alpha" {
			t.Errorf("Filter()[0].Petname = %q, want %q", all[0].Petname, "alpha")
		}
		if all[1].Petname != "charlie" {
			t.Errorf("Filter()[1].Petname = %q, want %q", all[1].Petname, "charlie")
		}
	})

	t.Run("none match", func(t *testing.T) {
		filtered := ts.Filter(func(tgt Target) bool {
			return tgt.Petname == "nonexistent"
		})
		if filtered.Len() != 0 {
			t.Errorf("Filter() len = %d, want 0", filtered.Len())
		}
		if !filtered.IsEmpty() {
			t.Error("Filter().IsEmpty() = false, want true")
		}
	})

	t.Run("all match", func(t *testing.T) {
		filtered := ts.Filter(func(tgt Target) bool {
			return true
		})
		if filtered.Len() != 4 {
			t.Errorf("Filter() len = %d, want 4", filtered.Len())
		}
	})

	t.Run("nil target set", func(t *testing.T) {
		var nilTS *TargetSet
		filtered := nilTS.Filter(func(tgt Target) bool {
			return true
		})
		if filtered == nil {
			t.Fatal("Filter() on nil = nil, want non-nil empty set")
		}
		if !filtered.IsEmpty() {
			t.Error("Filter() on nil should be empty")
		}
	})

	t.Run("does not mutate original", func(t *testing.T) {
		original := NewTargetSet([]Target{
			{Petname: "keep", Address: "10.0.0.1:80"},
			{Petname: "drop"},
		})
		_ = original.Filter(func(tgt Target) bool {
			return tgt.HasDirect()
		})
		if original.Len() != 2 {
			t.Errorf("original Len() = %d after Filter(), want 2", original.Len())
		}
	})
}

func TestTargetSet_WithDirect(t *testing.T) {
	t.Run("mixed addresses", func(t *testing.T) {
		ts := NewTargetSet([]Target{
			{Petname: "alpha", Address: "10.0.0.1:80"},
			{Petname: "bravo"},
			{Petname: "charlie", Address: "10.0.0.3:80"},
		})
		direct := ts.WithDirect()
		if direct.Len() != 2 {
			t.Fatalf("WithDirect() len = %d, want 2", direct.Len())
		}
		all := direct.All()
		if all[0].Petname != "alpha" || all[1].Petname != "charlie" {
			t.Errorf("WithDirect() petnames = [%q, %q], want [alpha, charlie]",
				all[0].Petname, all[1].Petname)
		}
	})

	t.Run("none with address", func(t *testing.T) {
		ts := NewTargetSet([]Target{
			{Petname: "alpha"},
			{Petname: "bravo"},
		})
		direct := ts.WithDirect()
		if !direct.IsEmpty() {
			t.Errorf("WithDirect() len = %d, want 0", direct.Len())
		}
	})

	t.Run("all with address", func(t *testing.T) {
		ts := NewTargetSet([]Target{
			{Petname: "alpha", Address: "10.0.0.1:80"},
			{Petname: "bravo", Address: "10.0.0.2:80"},
		})
		direct := ts.WithDirect()
		if direct.Len() != 2 {
			t.Errorf("WithDirect() len = %d, want 2", direct.Len())
		}
	})

	t.Run("empty target set", func(t *testing.T) {
		ts := EmptyTargetSet()
		direct := ts.WithDirect()
		if !direct.IsEmpty() {
			t.Errorf("WithDirect() on empty len = %d, want 0", direct.Len())
		}
	})
}

func TestTargetSet_SortByLatency(t *testing.T) {
	t.Run("already sorted", func(t *testing.T) {
		ts := NewTargetSet([]Target{
			{Petname: "fast", Latency: 10 * time.Millisecond},
			{Petname: "medium", Latency: 50 * time.Millisecond},
			{Petname: "slow", Latency: 200 * time.Millisecond},
		})
		sorted := ts.SortByLatency()
		all := sorted.All()
		if len(all) != 3 {
			t.Fatalf("SortByLatency() len = %d, want 3", len(all))
		}
		if all[0].Petname != "fast" || all[1].Petname != "medium" || all[2].Petname != "slow" {
			t.Errorf("SortByLatency() order = [%q, %q, %q], want [fast, medium, slow]",
				all[0].Petname, all[1].Petname, all[2].Petname)
		}
	})

	t.Run("reverse sorted", func(t *testing.T) {
		ts := NewTargetSet([]Target{
			{Petname: "slow", Latency: 200 * time.Millisecond},
			{Petname: "medium", Latency: 50 * time.Millisecond},
			{Petname: "fast", Latency: 10 * time.Millisecond},
		})
		sorted := ts.SortByLatency()
		all := sorted.All()
		if len(all) != 3 {
			t.Fatalf("SortByLatency() len = %d, want 3", len(all))
		}
		if all[0].Petname != "fast" {
			t.Errorf("SortByLatency()[0].Petname = %q, want %q", all[0].Petname, "fast")
		}
		if all[1].Petname != "medium" {
			t.Errorf("SortByLatency()[1].Petname = %q, want %q", all[1].Petname, "medium")
		}
		if all[2].Petname != "slow" {
			t.Errorf("SortByLatency()[2].Petname = %q, want %q", all[2].Petname, "slow")
		}
	})

	t.Run("mixed order", func(t *testing.T) {
		ts := NewTargetSet([]Target{
			{Petname: "medium", Latency: 50 * time.Millisecond},
			{Petname: "fast", Latency: 10 * time.Millisecond},
			{Petname: "slow", Latency: 200 * time.Millisecond},
			{Petname: "fastest", Latency: 1 * time.Millisecond},
		})
		sorted := ts.SortByLatency()
		all := sorted.All()
		if len(all) != 4 {
			t.Fatalf("SortByLatency() len = %d, want 4", len(all))
		}
		expected := []string{"fastest", "fast", "medium", "slow"}
		for i, want := range expected {
			if all[i].Petname != want {
				t.Errorf("SortByLatency()[%d].Petname = %q, want %q", i, all[i].Petname, want)
			}
		}
	})

	t.Run("nil target set", func(t *testing.T) {
		var ts *TargetSet
		sorted := ts.SortByLatency()
		if sorted != nil {
			t.Errorf("SortByLatency() on nil = %v, want nil", sorted)
		}
	})

	t.Run("single target", func(t *testing.T) {
		ts := NewTargetSet([]Target{
			{Petname: "only", Latency: 42 * time.Millisecond},
		})
		sorted := ts.SortByLatency()
		if sorted.Len() != 1 {
			t.Fatalf("SortByLatency() len = %d, want 1", sorted.Len())
		}
		if sorted.First().Petname != "only" {
			t.Errorf("SortByLatency().First().Petname = %q, want %q", sorted.First().Petname, "only")
		}
	})

	t.Run("empty target set", func(t *testing.T) {
		ts := EmptyTargetSet()
		sorted := ts.SortByLatency()
		// EmptyTargetSet has len 0 (nil slice), so <= 1 returns ts itself.
		if sorted.Len() != 0 {
			t.Errorf("SortByLatency() on empty len = %d, want 0", sorted.Len())
		}
	})

	t.Run("does not mutate original", func(t *testing.T) {
		ts := NewTargetSet([]Target{
			{Petname: "slow", Latency: 200 * time.Millisecond},
			{Petname: "fast", Latency: 10 * time.Millisecond},
		})
		_ = ts.SortByLatency()
		// Original should retain its order.
		all := ts.All()
		if all[0].Petname != "slow" {
			t.Errorf("original[0].Petname = %q after sort, want %q", all[0].Petname, "slow")
		}
		if all[1].Petname != "fast" {
			t.Errorf("original[1].Petname = %q after sort, want %q", all[1].Petname, "fast")
		}
	})

	t.Run("equal latencies preserve relative order", func(t *testing.T) {
		ts := NewTargetSet([]Target{
			{Petname: "alpha", Latency: 10 * time.Millisecond},
			{Petname: "bravo", Latency: 10 * time.Millisecond},
			{Petname: "charlie", Latency: 10 * time.Millisecond},
		})
		sorted := ts.SortByLatency()
		all := sorted.All()
		// Insertion sort is stable, so order should be preserved.
		expected := []string{"alpha", "bravo", "charlie"}
		for i, want := range expected {
			if all[i].Petname != want {
				t.Errorf("SortByLatency()[%d].Petname = %q, want %q", i, all[i].Petname, want)
			}
		}
	})

	t.Run("zero latencies", func(t *testing.T) {
		ts := NewTargetSet([]Target{
			{Petname: "alpha", Latency: 0},
			{Petname: "bravo", Latency: 50 * time.Millisecond},
			{Petname: "charlie", Latency: 0},
		})
		sorted := ts.SortByLatency()
		all := sorted.All()
		if all[0].Petname != "alpha" {
			t.Errorf("SortByLatency()[0].Petname = %q, want %q", all[0].Petname, "alpha")
		}
		if all[1].Petname != "charlie" {
			t.Errorf("SortByLatency()[1].Petname = %q, want %q", all[1].Petname, "charlie")
		}
		if all[2].Petname != "bravo" {
			t.Errorf("SortByLatency()[2].Petname = %q, want %q", all[2].Petname, "bravo")
		}
	})
}
