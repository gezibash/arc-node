package cel

import (
	"context"
	"testing"

	"github.com/gezibash/arc/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

func newTestEvaluator(t *testing.T) *Evaluator {
	t.Helper()
	eval, err := NewEvaluator()
	if err != nil {
		t.Fatalf("NewEvaluator: %v", err)
	}
	return eval
}

func TestCompileValid(t *testing.T) {
	eval := newTestEvaluator(t)
	_, err := eval.Compile(context.Background(), `labels["type"] == "text"`)
	if err != nil {
		t.Fatalf("Compile valid: %v", err)
	}
}

func TestCompileInvalid(t *testing.T) {
	eval := newTestEvaluator(t)
	_, err := eval.Compile(context.Background(), `invalid!!!`)
	if err == nil {
		t.Fatal("Compile invalid: expected error")
	}
}

func TestCompileCaching(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()
	expr := `labels["x"] == "y"`

	p1, _ := eval.Compile(ctx, expr)
	p2, _ := eval.Compile(ctx, expr)
	if p1 == nil || p2 == nil {
		t.Fatal("Compile returned nil")
	}
}

func TestMatch(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	entry := &physical.Entry{
		Ref:       reference.Compute([]byte("test")),
		Labels:    map[string]string{"type": "text", "lang": "en"},
		Timestamp: 1000,
	}

	match, err := eval.Match(ctx, `labels["type"] == "text"`, entry)
	if err != nil || !match {
		t.Errorf("Match text: %v, %v", match, err)
	}

	match, err = eval.Match(ctx, `labels["type"] == "binary"`, entry)
	if err != nil || match {
		t.Errorf("Match binary: %v, %v", match, err)
	}

	match, err = eval.Match(ctx, `timestamp > 500`, entry)
	if err != nil || !match {
		t.Errorf("Match timestamp: %v, %v", match, err)
	}
}

func TestEvalBatch(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	prg, _ := eval.Compile(ctx, `labels["keep"] == "yes"`)

	entries := []*physical.Entry{
		{Ref: reference.Compute([]byte("1")), Labels: map[string]string{"keep": "yes"}, Timestamp: 1},
		{Ref: reference.Compute([]byte("2")), Labels: map[string]string{"keep": "no"}, Timestamp: 2},
		{Ref: reference.Compute([]byte("3")), Labels: map[string]string{"keep": "yes"}, Timestamp: 3},
	}

	matches, err := eval.EvalBatch(ctx, prg, entries)
	if err != nil {
		t.Fatalf("EvalBatch: %v", err)
	}
	if len(matches) != 2 {
		t.Errorf("EvalBatch matches = %d, want 2", len(matches))
	}
}

func TestValidateExpression(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	if err := eval.ValidateExpression(ctx, `labels["x"] == "y"`); err != nil {
		t.Errorf("ValidateExpression valid: %v", err)
	}
	if err := eval.ValidateExpression(ctx, `bad!!!`); err == nil {
		t.Error("ValidateExpression invalid: expected error")
	}
}
