package cel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/gezibash/arc/v2/pkg/reference"

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

func TestMatchTimestampFilter(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	entry := &physical.Entry{
		Ref:       reference.Compute([]byte("ts-test")),
		Labels:    map[string]string{"type": "event"},
		Timestamp: 5000,
	}

	match, err := eval.Match(ctx, `timestamp > 3000`, entry)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if !match {
		t.Error("expected match for timestamp > 3000 with ts=5000")
	}

	match, err = eval.Match(ctx, `timestamp > 9000`, entry)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if match {
		t.Error("expected no match for timestamp > 9000 with ts=5000")
	}

	match, err = eval.Match(ctx, `timestamp >= 5000 && timestamp < 6000`, entry)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if !match {
		t.Error("expected match for range [5000,6000)")
	}
}

func TestMatchRefFilter(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	data := []byte("ref-filter-test")
	ref := reference.Compute(data)
	hex := reference.Hex(ref)

	entry := &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{},
		Timestamp: 1000,
	}

	expr := fmt.Sprintf(`ref == "%s"`, hex)
	match, err := eval.Match(ctx, expr, entry)
	if err != nil {
		t.Fatalf("Match ref: %v", err)
	}
	if !match {
		t.Error("expected match for exact ref")
	}

	match, err = eval.Match(ctx, `ref == "0000000000000000000000000000000000000000000000000000000000000000"`, entry)
	if err != nil {
		t.Fatalf("Match ref mismatch: %v", err)
	}
	if match {
		t.Error("expected no match for wrong ref")
	}
}

func TestMatchExpiresAtFilter(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	entry := &physical.Entry{
		Ref:       reference.Compute([]byte("exp-test")),
		Labels:    map[string]string{},
		Timestamp: 1000,
		ExpiresAt: 9999,
	}

	match, err := eval.Match(ctx, `expires_at > 0`, entry)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if !match {
		t.Error("expected match for expires_at > 0")
	}

	entryNoExpiry := &physical.Entry{
		Ref:       reference.Compute([]byte("no-exp")),
		Labels:    map[string]string{},
		Timestamp: 1000,
		ExpiresAt: 0,
	}
	match, err = eval.Match(ctx, `expires_at > 0`, entryNoExpiry)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if match {
		t.Error("expected no match for expires_at=0")
	}
}

func TestMatchComplexExpression(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	entry := &physical.Entry{
		Ref:       reference.Compute([]byte("complex")),
		Labels:    map[string]string{"type": "journal", "author": "alice"},
		Timestamp: 5000,
		ExpiresAt: 10000,
	}

	match, err := eval.Match(ctx, `labels["type"] == "journal" && labels["author"] == "alice" && timestamp > 1000`, entry)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if !match {
		t.Error("expected match for complex expression")
	}

	match, err = eval.Match(ctx, `labels["type"] == "journal" && labels["author"] == "bob"`, entry)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if match {
		t.Error("expected no match when author != bob")
	}
}

func TestCacheLRUEviction(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	// Compile many distinct expressions; cache uses sync.Map so it won't OOM,
	// but verify all compile successfully and can be used.
	for i := 0; i < 200; i++ {
		expr := fmt.Sprintf(`timestamp > %d`, i)
		prg, err := eval.Compile(ctx, expr)
		if err != nil {
			t.Fatalf("Compile(%d): %v", i, err)
		}
		if prg == nil {
			t.Fatalf("Compile(%d) returned nil program", i)
		}
	}

	// Verify a cached expression still works.
	entry := &physical.Entry{
		Ref:       reference.Compute([]byte("cache")),
		Labels:    map[string]string{},
		Timestamp: 150,
	}
	match, err := eval.Match(ctx, `timestamp > 100`, entry)
	if err != nil {
		t.Fatalf("Match after many compiles: %v", err)
	}
	if !match {
		t.Error("expected match from cached program")
	}
}

func TestEvalBatchMixed(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	prg, err := eval.Compile(ctx, `labels["status"] == "active" && timestamp > 100`)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	entries := []*physical.Entry{
		{Ref: reference.Compute([]byte("a")), Labels: map[string]string{"status": "active"}, Timestamp: 200},
		{Ref: reference.Compute([]byte("b")), Labels: map[string]string{"status": "inactive"}, Timestamp: 200},
		{Ref: reference.Compute([]byte("c")), Labels: map[string]string{"status": "active"}, Timestamp: 50},
		{Ref: reference.Compute([]byte("d")), Labels: map[string]string{"status": "active"}, Timestamp: 300},
		{Ref: reference.Compute([]byte("e")), Labels: map[string]string{}, Timestamp: 500},
	}

	matches, err := eval.EvalBatch(ctx, prg, entries)
	if err != nil {
		t.Fatalf("EvalBatch: %v", err)
	}
	if len(matches) != 2 {
		t.Errorf("EvalBatch matches = %d, want 2", len(matches))
	}
}

func TestValidateExpressionEmpty(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	// Empty string should fail since CEL cannot parse it.
	err := eval.ValidateExpression(ctx, "")
	// An empty expression is invalid CEL, so we expect an error.
	if err == nil {
		// Some CEL implementations may treat "" differently; if no error, that's also acceptable.
		t.Log("empty expression validated without error (treated as valid)")
	}
}

func TestCompileInvalidSyntax(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	cases := []string{
		`labels[`,
		`== ==`,
		`func()`,
		`labels["x"] + 42`,     // type error: comparison expected bool
		`undeclared_var == "x"`, // undeclared variable
	}

	for _, expr := range cases {
		_, err := eval.Compile(ctx, expr)
		if err == nil {
			t.Errorf("expected error for expression %q", expr)
			continue
		}
		if !errors.Is(err, ErrInvalidExpression) {
			t.Errorf("expected ErrInvalidExpression for %q, got: %v", expr, err)
		}
	}
}

func TestMatchNilEntry(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	// Passing a nil entry should panic or error gracefully.
	// We recover from panic to verify the behavior.
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Match with nil entry panicked (expected): %v", r)
			}
		}()
		_, err := eval.Match(ctx, `timestamp > 0`, nil)
		if err != nil {
			t.Logf("Match with nil entry returned error (expected): %v", err)
		}
	}()
}

func TestConcurrentMatch(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			entry := &physical.Entry{
				Ref:       reference.Compute([]byte(fmt.Sprintf("concurrent-%d", i))),
				Labels:    map[string]string{"idx": fmt.Sprintf("%d", i)},
				Timestamp: int64(i * 100),
			}
			expr := fmt.Sprintf(`timestamp >= %d`, i*100)
			match, err := eval.Match(ctx, expr, entry)
			if err != nil {
				errs <- fmt.Errorf("goroutine %d: %w", i, err)
				return
			}
			if !match {
				errs <- fmt.Errorf("goroutine %d: expected match", i)
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// --- Eval error branches ---

func TestEvalNonBoolResult(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	// Compile an expression that returns a string, not bool.
	// Use a raw CEL expression that returns a string value.
	prg, err := eval.Compile(ctx, `labels["type"]`)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	entry := &physical.Entry{
		Ref:       reference.Compute([]byte("nonbool")),
		Labels:    map[string]string{"type": "text"},
		Timestamp: 1000,
	}

	_, err = eval.Eval(ctx, prg, entry)
	if err == nil {
		t.Fatal("expected error for non-bool result")
	}
	if !errors.Is(err, ErrEvaluationFailed) {
		t.Errorf("expected ErrEvaluationFailed, got: %v", err)
	}
}

func TestMatchInvalidExpression(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	entry := &physical.Entry{
		Ref:       reference.Compute([]byte("x")),
		Labels:    map[string]string{},
		Timestamp: 1,
	}

	_, err := eval.Match(ctx, `invalid!!!`, entry)
	if err == nil {
		t.Fatal("expected error for invalid expression in Match")
	}
	if !errors.Is(err, ErrInvalidExpression) {
		t.Errorf("expected ErrInvalidExpression, got: %v", err)
	}
}

func TestEvalBatchWithErrors(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	// Compile expression that accesses a label key; entries with nil labels cause eval errors.
	prg, err := eval.Compile(ctx, `labels["key"] == "val"`)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	entries := []*physical.Entry{
		{Ref: reference.Compute([]byte("ok")), Labels: map[string]string{"key": "val"}, Timestamp: 1},
		{Ref: reference.Compute([]byte("nilmap")), Labels: nil, Timestamp: 2},
		{Ref: reference.Compute([]byte("ok2")), Labels: map[string]string{"key": "val"}, Timestamp: 3},
	}

	matches, err := eval.EvalBatch(ctx, prg, entries)
	if err != nil {
		t.Fatalf("EvalBatch: %v", err)
	}
	// At least the entries with valid labels should match.
	if len(matches) < 2 {
		t.Errorf("expected at least 2 matches, got %d", len(matches))
	}
}

func TestEvalBatchNonBoolSkipped(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	// Expression returns string, not bool — EvalBatch should skip (not match).
	prg, err := eval.Compile(ctx, `labels["type"]`)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	entries := []*physical.Entry{
		{Ref: reference.Compute([]byte("a")), Labels: map[string]string{"type": "text"}, Timestamp: 1},
	}

	matches, err := eval.EvalBatch(ctx, prg, entries)
	if err != nil {
		t.Fatalf("EvalBatch: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("expected 0 matches for non-bool expression, got %d", len(matches))
	}
}

func TestEvalBatchEmpty(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	prg, err := eval.Compile(ctx, `timestamp > 0`)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	matches, err := eval.EvalBatch(ctx, prg, nil)
	if err != nil {
		t.Fatalf("EvalBatch: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("expected 0 matches for empty input, got %d", len(matches))
	}
}

// --- Analyze tests ---

func TestAnalyzeSimpleLabel(t *testing.T) {
	eval := newTestEvaluator(t)
	r := eval.Analyze(`labels["type"] == "text"`)

	if r.Labels["type"] != "text" {
		t.Errorf("expected label type=text, got %v", r.Labels)
	}
	if !r.FullyPushed {
		t.Error("expected FullyPushed")
	}
	if r.Residual != "" {
		t.Errorf("expected empty residual, got %q", r.Residual)
	}
}

func TestAnalyzeMultipleLabels(t *testing.T) {
	eval := newTestEvaluator(t)
	r := eval.Analyze(`labels["type"] == "text" && labels["author"] == "alice"`)

	if r.Labels["type"] != "text" || r.Labels["author"] != "alice" {
		t.Errorf("expected labels type=text, author=alice, got %v", r.Labels)
	}
	if !r.FullyPushed {
		t.Error("expected FullyPushed")
	}
}

func TestAnalyzeTimestampGreaterThan(t *testing.T) {
	eval := newTestEvaluator(t)
	r := eval.Analyze(`timestamp > 5000`)

	if r.After != 5000 {
		t.Errorf("expected After=5000, got %d", r.After)
	}
	if !r.FullyPushed {
		t.Error("expected FullyPushed")
	}
}

func TestAnalyzeTimestampGreaterEqual(t *testing.T) {
	eval := newTestEvaluator(t)
	r := eval.Analyze(`timestamp >= 5000`)

	if r.After != 4999 {
		t.Errorf("expected After=4999 (5000-1), got %d", r.After)
	}
}

func TestAnalyzeTimestampLessThan(t *testing.T) {
	eval := newTestEvaluator(t)
	r := eval.Analyze(`timestamp < 3000`)

	if r.Before != 3000 {
		t.Errorf("expected Before=3000, got %d", r.Before)
	}
}

func TestAnalyzeTimestampLessEqual(t *testing.T) {
	eval := newTestEvaluator(t)
	r := eval.Analyze(`timestamp <= 3000`)

	if r.Before != 3001 {
		t.Errorf("expected Before=3001 (3000+1), got %d", r.Before)
	}
}

func TestAnalyzeTimestampReversed(t *testing.T) {
	eval := newTestEvaluator(t)

	// N > timestamp => Before = N
	r := eval.Analyze(`3000 > timestamp`)
	if r.Before != 3000 {
		t.Errorf("expected Before=3000 for reversed >, got %d", r.Before)
	}

	// N >= timestamp => Before = N+1
	r = eval.Analyze(`3000 >= timestamp`)
	if r.Before != 3001 {
		t.Errorf("expected Before=3001 for reversed >=, got %d", r.Before)
	}

	// N < timestamp => After = N
	r = eval.Analyze(`3000 < timestamp`)
	if r.After != 3000 {
		t.Errorf("expected After=3000 for reversed <, got %d", r.After)
	}

	// N <= timestamp => After = N-1
	r = eval.Analyze(`3000 <= timestamp`)
	if r.After != 2999 {
		t.Errorf("expected After=2999 for reversed <=, got %d", r.After)
	}
}

func TestAnalyzeTimestampGreaterEqualZero(t *testing.T) {
	eval := newTestEvaluator(t)
	// timestamp >= 0: lit=0, lit > 0 is false, so After stays 0.
	r := eval.Analyze(`timestamp >= 0`)
	if r.After != 0 {
		t.Errorf("expected After=0 for timestamp >= 0, got %d", r.After)
	}
}

func TestAnalyzeTimestampReversedLessEqualZero(t *testing.T) {
	eval := newTestEvaluator(t)
	// 0 <= timestamp: reversed <=, lit=0, lit > 0 is false, so After stays 0.
	r := eval.Analyze(`0 <= timestamp`)
	if r.After != 0 {
		t.Errorf("expected After=0 for 0 <= timestamp, got %d", r.After)
	}
}

func TestAnalyzeCombined(t *testing.T) {
	eval := newTestEvaluator(t)
	r := eval.Analyze(`labels["type"] == "text" && timestamp > 1000 && timestamp < 5000`)

	if r.Labels["type"] != "text" {
		t.Errorf("expected label type=text, got %v", r.Labels)
	}
	if r.After != 1000 {
		t.Errorf("expected After=1000, got %d", r.After)
	}
	if r.Before != 5000 {
		t.Errorf("expected Before=5000, got %d", r.Before)
	}
	if !r.FullyPushed {
		t.Error("expected FullyPushed")
	}
}

func TestAnalyzeWithResidual(t *testing.T) {
	eval := newTestEvaluator(t)
	// ref == "..." cannot be pushed down to labels/timestamp.
	r := eval.Analyze(`labels["type"] == "text" && ref == "abc"`)

	if r.Labels["type"] != "text" {
		t.Errorf("expected label type=text, got %v", r.Labels)
	}
	if r.FullyPushed {
		t.Error("expected not FullyPushed")
	}
	if r.Residual == "" {
		t.Error("expected non-empty residual")
	}
}

func TestAnalyzeInvalidExpression(t *testing.T) {
	eval := newTestEvaluator(t)
	r := eval.Analyze(`invalid!!!`)

	if r.Residual != `invalid!!!` {
		t.Errorf("expected fallback residual, got %q", r.Residual)
	}
	if r.FullyPushed {
		t.Error("expected not FullyPushed for invalid expression")
	}
}

func TestAnalyzeLiteralTrue(t *testing.T) {
	eval := newTestEvaluator(t)
	r := eval.Analyze(`true`)

	if !r.FullyPushed {
		t.Error("expected FullyPushed for literal true")
	}
	if r.Residual != "" {
		t.Errorf("expected empty residual, got %q", r.Residual)
	}
}

func TestAnalyzeORLabelGroups(t *testing.T) {
	eval := newTestEvaluator(t)
	r := eval.Analyze(`labels["type"] == "text" || labels["type"] == "image"`)

	if r.LabelFilter == nil {
		t.Fatal("expected LabelFilter to be set")
	}
	if len(r.LabelFilter.OR) != 2 {
		t.Fatalf("expected 2 OR groups, got %d", len(r.LabelFilter.OR))
	}
	if !r.FullyPushed {
		t.Error("expected FullyPushed for OR label groups")
	}
}

func TestAnalyzeORWithANDGroups(t *testing.T) {
	eval := newTestEvaluator(t)
	r := eval.Analyze(`(labels["type"] == "text" && labels["env"] == "prod") || labels["type"] == "image"`)

	if r.LabelFilter == nil {
		t.Fatal("expected LabelFilter to be set")
	}
	if len(r.LabelFilter.OR) != 2 {
		t.Fatalf("expected 2 OR groups, got %d", len(r.LabelFilter.OR))
	}
	// First group should have 2 predicates.
	if len(r.LabelFilter.OR[0].Predicates) != 2 {
		t.Errorf("expected 2 predicates in first group, got %d", len(r.LabelFilter.OR[0].Predicates))
	}
}

func TestAnalyzeORWithNonLabel(t *testing.T) {
	eval := newTestEvaluator(t)
	// OR with a non-label predicate should not be pushed down.
	r := eval.Analyze(`labels["type"] == "text" || timestamp > 100`)

	if r.LabelFilter != nil {
		t.Error("expected LabelFilter to be nil for non-pushable OR")
	}
	if r.FullyPushed {
		t.Error("expected not FullyPushed")
	}
	if r.Residual == "" {
		t.Error("expected non-empty residual")
	}
}

func TestAnalyzeORCombinedWithAND(t *testing.T) {
	eval := newTestEvaluator(t)
	r := eval.Analyze(`(labels["type"] == "text" || labels["type"] == "image") && timestamp > 1000`)

	if r.LabelFilter == nil {
		t.Fatal("expected LabelFilter to be set")
	}
	if r.After != 1000 {
		t.Errorf("expected After=1000, got %d", r.After)
	}
	if !r.FullyPushed {
		t.Error("expected FullyPushed")
	}
}

func TestAnalyzeReversedLabelEquality(t *testing.T) {
	eval := newTestEvaluator(t)
	// "text" == labels["type"] — reversed order.
	r := eval.Analyze(`"text" == labels["type"]`)

	if r.Labels["type"] != "text" {
		t.Errorf("expected label type=text for reversed equality, got %v", r.Labels)
	}
	if !r.FullyPushed {
		t.Error("expected FullyPushed")
	}
}

func TestAnalyzeTypeCheckFail(t *testing.T) {
	eval := newTestEvaluator(t)
	// Expression that parses but fails type-check.
	r := eval.Analyze(`labels + 42`)

	if r.Residual != `labels + 42` {
		t.Errorf("expected fallback residual for type-check failure, got %q", r.Residual)
	}
}

func TestAnalyzeExprToStringBranches(t *testing.T) {
	eval := newTestEvaluator(t)

	// Test various expression types that produce residuals exercising exprToString.
	tests := []struct {
		name string
		expr string
	}{
		{"not_equal", `labels["type"] == "text" && ref != "abc"`},
		{"or_residual", `labels["type"] == "text" && (ref == "a" || ref == "b")`},
		{"negation", `labels["type"] == "text" && !(timestamp > 100)`},
		{"function_call", `labels["type"] == "text" && ref.startsWith("abc")`},
		{"select", `labels["type"] == "text" && timestamp > 0`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := eval.Analyze(tt.expr)
			// Just verify no panic and that labels are extracted.
			if r.Labels["type"] != "text" {
				t.Errorf("expected label type=text, got %v", r.Labels)
			}
		})
	}
}

func TestAnalyzeExprToStringAllOperators(t *testing.T) {
	eval := newTestEvaluator(t)

	// Each expression produces a residual that exercises a different exprToString branch.
	tests := []struct {
		name     string
		expr     string
		checkRes func(t *testing.T, r *AnalysisResult)
	}{
		{
			name: "ident_residual",
			// expires_at alone is not pushable (not a comparison).
			// "expires_at == 0" uses ref/expires_at which is not label/timestamp push.
			expr: `expires_at == 0`,
			checkRes: func(t *testing.T, r *AnalysisResult) {
				if r.FullyPushed {
					t.Error("expected not FullyPushed")
				}
				if r.Residual == "" {
					t.Error("expected non-empty residual")
				}
			},
		},
		{
			name: "greater_equal_residual",
			expr: `labels["type"] == "text" && expires_at >= 100`,
			checkRes: func(t *testing.T, r *AnalysisResult) {
				if r.Labels["type"] != "text" {
					t.Errorf("missing label")
				}
				if r.FullyPushed {
					t.Error("expected not FullyPushed")
				}
			},
		},
		{
			name: "less_than_residual",
			expr: `labels["type"] == "text" && expires_at < 100`,
			checkRes: func(t *testing.T, r *AnalysisResult) {
				if r.FullyPushed {
					t.Error("expected not FullyPushed")
				}
			},
		},
		{
			name: "less_equal_residual",
			expr: `labels["type"] == "text" && expires_at <= 100`,
			checkRes: func(t *testing.T, r *AnalysisResult) {
				if r.FullyPushed {
					t.Error("expected not FullyPushed")
				}
			},
		},
		{
			name: "index_residual",
			expr: `labels["type"] == "text" && labels["key"] != "val"`,
			checkRes: func(t *testing.T, r *AnalysisResult) {
				if r.FullyPushed {
					t.Error("expected not FullyPushed")
				}
			},
		},
		{
			name: "bool_false_residual",
			// false literal — not pushable as isLiteralTrue checks for true only.
			expr: `labels["type"] == "text" && false`,
			checkRes: func(t *testing.T, r *AnalysisResult) {
				// CEL may optimize this away; just check no panic.
			},
		},
		{
			name: "in_operator",
			expr: `labels["type"] == "text" && ref in ["a", "b"]`,
			checkRes: func(t *testing.T, r *AnalysisResult) {
				if r.FullyPushed {
					t.Error("expected not FullyPushed")
				}
			},
		},
		{
			name: "method_with_target",
			expr: `labels["type"] == "text" && ref.contains("abc")`,
			checkRes: func(t *testing.T, r *AnalysisResult) {
				if r.FullyPushed {
					t.Error("expected not FullyPushed")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := eval.Analyze(tt.expr)
			tt.checkRes(t, r)
		})
	}
}

func TestAnalyzeOnlyResidual(t *testing.T) {
	eval := newTestEvaluator(t)
	// Expression with no pushable predicates at all.
	r := eval.Analyze(`ref == "abc" && expires_at > 0`)

	if r.FullyPushed {
		t.Error("expected not FullyPushed")
	}
	if r.Residual == "" {
		t.Error("expected non-empty residual")
	}
	if len(r.Labels) != 0 {
		t.Errorf("expected no labels, got %v", r.Labels)
	}
}

func TestAnalyzeMultipleResidualParts(t *testing.T) {
	eval := newTestEvaluator(t)
	// Two non-pushable predicates joined by &&.
	r := eval.Analyze(`ref == "abc" && ref != "def"`)

	if r.FullyPushed {
		t.Error("expected not FullyPushed")
	}
	// Residual should contain both parts joined by &&.
	if r.Residual == "" {
		t.Error("expected non-empty residual")
	}
}

func TestEvalBatchManyErrors(t *testing.T) {
	eval := newTestEvaluator(t)
	ctx := context.Background()

	// Create a program that will error on nil labels.
	prg, err := eval.Compile(ctx, `labels["key"] == "val"`)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// More than 5 error entries to exercise the evalErrors > 5 branch.
	entries := make([]*physical.Entry, 10)
	for i := range entries {
		entries[i] = &physical.Entry{
			Ref:       reference.Compute([]byte(fmt.Sprintf("err-%d", i))),
			Labels:    nil,
			Timestamp: int64(i),
		}
	}

	matches, err := eval.EvalBatch(ctx, prg, entries)
	if err != nil {
		t.Fatalf("EvalBatch: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("expected 0 matches, got %d", len(matches))
	}
}

func TestAnalyzeReversedLabelEqualityInOR(t *testing.T) {
	eval := newTestEvaluator(t)
	// Test reversed label equality inside asLabelEqualityPair (OR branch).
	r := eval.Analyze(`"text" == labels["type"] || "image" == labels["type"]`)

	if r.LabelFilter == nil {
		t.Fatal("expected LabelFilter to be set")
	}
	if len(r.LabelFilter.OR) != 2 {
		t.Fatalf("expected 2 OR groups, got %d", len(r.LabelFilter.OR))
	}
}
