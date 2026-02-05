package cel

import (
	"testing"
)

func TestStringEquality(t *testing.T) {
	keys := map[string]bool{"capability": true, "region": true}
	f, err := Compile(`capability == "blob"`, keys)
	if err != nil {
		t.Fatal(err)
	}

	if !f.Match(map[string]any{"capability": "blob", "region": "us"}) {
		t.Error("expected match")
	}
	if f.Match(map[string]any{"capability": "index", "region": "us"}) {
		t.Error("expected no match")
	}
}

func TestNumericComparison(t *testing.T) {
	keys := map[string]bool{"capacity": true, "capability": true}
	f, err := Compile(`capacity > 1000`, keys)
	if err != nil {
		t.Fatal(err)
	}

	if !f.Match(map[string]any{"capacity": int64(5000), "capability": "blob"}) {
		t.Error("expected match for 5000 > 1000")
	}
	if f.Match(map[string]any{"capacity": int64(500), "capability": "blob"}) {
		t.Error("expected no match for 500 > 1000")
	}
}

func TestFloatComparison(t *testing.T) {
	keys := map[string]bool{"load": true}
	f, err := Compile(`load < 0.8`, keys)
	if err != nil {
		t.Fatal(err)
	}

	if !f.Match(map[string]any{"load": float64(0.5)}) {
		t.Error("expected match for 0.5 < 0.8")
	}
	if f.Match(map[string]any{"load": float64(0.9)}) {
		t.Error("expected no match for 0.9 < 0.8")
	}
}

func TestBooleanFilter(t *testing.T) {
	keys := map[string]bool{"healthy": true}
	f, err := Compile(`healthy == true`, keys)
	if err != nil {
		t.Fatal(err)
	}

	if !f.Match(map[string]any{"healthy": true}) {
		t.Error("expected match")
	}
	if f.Match(map[string]any{"healthy": false}) {
		t.Error("expected no match")
	}
}

func TestMissingKeyReturnsFalse(t *testing.T) {
	keys := map[string]bool{"capability": true, "region": true}
	f, err := Compile(`capability == "blob" && region == "us"`, keys)
	if err != nil {
		t.Fatal(err)
	}

	// Missing "region" key — should return false, not error
	if f.Match(map[string]any{"capability": "blob"}) {
		t.Error("expected false for missing key")
	}
}

func TestCompoundExpression(t *testing.T) {
	keys := map[string]bool{"capability": true, "capacity": true, "region": true}
	f, err := Compile(`capability == "blob" && capacity > 1000 && region == "us"`, keys)
	if err != nil {
		t.Fatal(err)
	}

	if !f.Match(map[string]any{
		"capability": "blob",
		"capacity":   int64(5000),
		"region":     "us",
	}) {
		t.Error("expected match")
	}

	if f.Match(map[string]any{
		"capability": "blob",
		"capacity":   int64(500),
		"region":     "us",
	}) {
		t.Error("expected no match for low capacity")
	}
}

func TestCompileError(t *testing.T) {
	keys := map[string]bool{"x": true}
	_, err := Compile(`invalid syntax !!!`, keys)
	if err == nil {
		t.Error("expected compile error")
	}
}

func TestEmptyAttrs(t *testing.T) {
	keys := map[string]bool{"x": true}
	f, err := Compile(`x == "hello"`, keys)
	if err != nil {
		t.Fatal(err)
	}
	if f.Match(map[string]any{}) {
		t.Error("expected false for empty attrs")
	}
}
