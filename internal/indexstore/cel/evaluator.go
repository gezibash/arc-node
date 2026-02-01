// Package cel provides CEL expression evaluation for index store queries.
package cel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"

	"github.com/gezibash/arc/v2/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

var (
	ErrInvalidExpression = errors.New("invalid CEL expression")
	ErrEvaluationFailed  = errors.New("CEL evaluation failed")
)

// Evaluator compiles and evaluates CEL expressions against index entries.
type Evaluator struct {
	env   *cel.Env
	cache sync.Map // map[string]cel.Program
}

// NewEvaluator creates a new CEL evaluator with the index entry schema.
func NewEvaluator() (*Evaluator, error) {
	env, err := cel.NewEnv(
		cel.Declarations(
			decls.NewVar("labels", decls.NewMapType(decls.String, decls.String)),
			decls.NewVar("timestamp", decls.Int),
			decls.NewVar("ref", decls.String),
			decls.NewVar("expires_at", decls.Int),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create CEL env: %w", err)
	}

	return &Evaluator{env: env}, nil
}

// Compile parses and compiles a CEL expression. Compiled programs are cached.
func (e *Evaluator) Compile(_ context.Context, expression string) (cel.Program, error) {
	if cached, ok := e.cache.Load(expression); ok {
		if prg, ok := cached.(cel.Program); ok {
			return prg, nil
		}
	}

	ast, issues := e.env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidExpression, issues.Err())
	}

	prg, err := e.env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidExpression, err)
	}

	e.cache.Store(expression, prg)
	return prg, nil
}

// Match evaluates a CEL expression against an entry and returns whether it matches.
func (e *Evaluator) Match(ctx context.Context, expression string, entry *physical.Entry) (bool, error) {
	prg, err := e.Compile(ctx, expression)
	if err != nil {
		return false, err
	}
	return e.Eval(ctx, prg, entry)
}

// Eval evaluates a compiled program against an entry.
func (e *Evaluator) Eval(_ context.Context, prg cel.Program, entry *physical.Entry) (bool, error) {
	activation := map[string]any{
		"labels":     entry.Labels,
		"timestamp":  entry.Timestamp,
		"ref":        reference.Hex(entry.Ref),
		"expires_at": entry.ExpiresAt,
	}

	out, _, err := prg.Eval(activation)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrEvaluationFailed, err)
	}

	result, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("%w: expression must return bool, got %T", ErrEvaluationFailed, out.Value())
	}
	return result, nil
}

// EvalBatch evaluates a compiled program against multiple entries.
// Returns entries that match the expression.
func (e *Evaluator) EvalBatch(ctx context.Context, prg cel.Program, entries []*physical.Entry) ([]*physical.Entry, error) {
	var matches []*physical.Entry
	evalErrors := 0

	for _, entry := range entries {
		activation := map[string]any{
			"labels":     entry.Labels,
			"timestamp":  entry.Timestamp,
			"ref":        reference.Hex(entry.Ref),
			"expires_at": entry.ExpiresAt,
		}

		out, _, err := prg.Eval(activation)
		if err != nil {
			evalErrors++
			if evalErrors <= 5 {
				slog.DebugContext(ctx, "batch eval entry failed",
					"ref", reference.Hex(entry.Ref),
					"error", err,
				)
			}
			continue
		}

		if result, ok := out.Value().(bool); ok && result {
			matches = append(matches, entry)
		}
	}

	return matches, nil
}

// ValidateExpression checks if an expression is syntactically valid.
func (e *Evaluator) ValidateExpression(ctx context.Context, expression string) error {
	_, err := e.Compile(ctx, expression)
	return err
}
