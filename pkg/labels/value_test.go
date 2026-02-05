package labels

import (
	"testing"
)

func TestValidValue(t *testing.T) {
	tests := []struct {
		v    any
		want bool
	}{
		{"hello", true},
		{int64(42), true},
		{float64(3.14), true},
		{true, true},
		{false, true},
		{42, false},          // int, not int64
		{uint64(1), false},   // wrong type
		{[]byte("x"), false}, // wrong type
		{nil, false},         // nil
	}
	for _, tt := range tests {
		if got := ValidValue(tt.v); got != tt.want {
			t.Errorf("ValidValue(%v [%T]) = %v, want %v", tt.v, tt.v, got, tt.want)
		}
	}
}

func TestValidateMap(t *testing.T) {
	if err := ValidateMap(map[string]any{"a": "b", "c": int64(1)}); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if err := ValidateMap(map[string]any{"bad": 42}); err == nil {
		t.Error("expected error for int (not int64)")
	}
}

func TestParseTyped(t *testing.T) {
	tests := []struct {
		input []string
		key   string
		want  any
	}{
		{[]string{"count=42"}, "count", int64(42)},
		{[]string{"rate=3.14"}, "rate", float64(3.14)},
		{[]string{"enabled=true"}, "enabled", true},
		{[]string{"enabled=false"}, "enabled", false},
		{[]string{"name=hello"}, "name", "hello"},
		{[]string{"big=9999999999999"}, "big", int64(9999999999999)},
		{[]string{"sci=1e12"}, "sci", float64(1e12)},
		// Values with = in them
		{[]string{"expr=a=b"}, "expr", "a=b"},
	}
	for _, tt := range tests {
		m, err := ParseTyped(tt.input)
		if err != nil {
			t.Errorf("ParseTyped(%v) error: %v", tt.input, err)
			continue
		}
		got := m[tt.key]
		if got != tt.want {
			t.Errorf("ParseTyped(%v)[%q] = %v (%T), want %v (%T)", tt.input, tt.key, got, got, tt.want, tt.want)
		}
	}
}

func TestParseTypedErrors(t *testing.T) {
	if _, err := ParseTyped([]string{"noeq"}); err == nil {
		t.Error("expected error for missing =")
	}
	if _, err := ParseTyped([]string{"=val"}); err == nil {
		t.Error("expected error for empty key")
	}
}

func TestFormatAny(t *testing.T) {
	if got := FormatAny(nil); got != "-" {
		t.Errorf("FormatAny(nil) = %q, want %q", got, "-")
	}
	m := map[string]any{
		"z":    "hello",
		"a":    int64(42),
		"mid":  true,
		"rate": float64(3.14),
	}
	got := FormatAny(m)
	want := "a=42, mid=true, rate=3.14, z=hello"
	if got != want {
		t.Errorf("FormatAny = %q, want %q", got, want)
	}
}

func TestMergeAny(t *testing.T) {
	a := map[string]any{"x": "1", "y": "2"}
	b := map[string]any{"y": "3", "z": "4"}
	got := MergeAny(a, b)
	if got["x"] != "1" || got["y"] != "3" || got["z"] != "4" {
		t.Errorf("MergeAny = %v", got)
	}
}

func TestToStringMap(t *testing.T) {
	m := map[string]any{
		"s":  "hello",
		"i":  int64(42),
		"f":  float64(1.5),
		"b":  true,
		"s2": "world",
	}
	got := ToStringMap(m)
	if len(got) != 2 {
		t.Errorf("ToStringMap returned %d entries, want 2", len(got))
	}
	if got["s"] != "hello" || got["s2"] != "world" {
		t.Errorf("ToStringMap = %v", got)
	}
}

func TestFromStringMap(t *testing.T) {
	m := map[string]string{"a": "1", "b": "2"}
	got := FromStringMap(m)
	if got["a"] != "1" || got["b"] != "2" {
		t.Errorf("FromStringMap = %v", got)
	}
}

func TestHasAny(t *testing.T) {
	labels := map[string]any{
		"capability": "blob",
		"region":     "us-east",
		"count":      int64(5),
	}
	// Match string entries
	if !HasAny(labels, map[string]string{"capability": "blob"}) {
		t.Error("expected match")
	}
	if !HasAny(labels, map[string]string{"capability": "blob", "region": "us-east"}) {
		t.Error("expected match for multi-key filter")
	}
	// Non-string value doesn't match string filter
	if HasAny(labels, map[string]string{"count": "5"}) {
		t.Error("expected no match: count is int64, not string")
	}
	// Missing key
	if HasAny(labels, map[string]string{"tier": "hot"}) {
		t.Error("expected no match for missing key")
	}
	// Empty filter matches anything
	if !HasAny(labels, map[string]string{}) {
		t.Error("expected empty filter to match")
	}
}

func TestRoundTrip(t *testing.T) {
	input := []string{"name=hello", "count=42", "rate=3.14", "enabled=true"}
	m, err := ParseTyped(input)
	if err != nil {
		t.Fatal(err)
	}
	formatted := FormatAny(m)
	want := "count=42, enabled=true, name=hello, rate=3.14"
	if formatted != want {
		t.Errorf("round-trip = %q, want %q", formatted, want)
	}
}
