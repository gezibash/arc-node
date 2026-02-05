// Package cel provides CEL expression evaluation for discovery filtering.
package cel

import (
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
)

// Filter is a compiled CEL expression that can match against typed attribute maps.
type Filter struct {
	program cel.Program
}

// Compile parses and compiles a CEL expression. All keys in knownKeys are
// declared as dynamic-typed variables. Unknown keys at evaluation time
// produce false (not an error).
func Compile(expr string, knownKeys map[string]bool) (*Filter, error) {
	opts := make([]cel.EnvOption, 0, len(knownKeys))
	for k := range knownKeys {
		opts = append(opts, cel.Variable(k, cel.DynType))
	}

	env, err := cel.NewEnv(opts...)
	if err != nil {
		return nil, fmt.Errorf("cel env: %w", err)
	}

	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("cel compile: %w", issues.Err())
	}

	prog, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("cel program: %w", err)
	}

	return &Filter{program: prog}, nil
}

// Match evaluates the filter against the given attributes.
// Returns false (not error) on missing keys, type mismatches, or evaluation errors.
func (f *Filter) Match(attrs map[string]any) bool {
	out, _, err := f.program.Eval(attrs)
	if err != nil {
		return false
	}
	if out.Type() != types.BoolType {
		return false
	}
	b, ok := out.Value().(bool)
	return ok && b
}
