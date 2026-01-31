package cel

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/common/ast"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

// AnalysisResult contains extracted backend-pushable predicates and any
// remaining expression that must be evaluated post-fetch.
type AnalysisResult struct {
	// Labels extracted from labels["k"] == "v" predicates.
	Labels map[string]string

	// After extracted from timestamp > N or timestamp >= N predicates.
	// Zero means no lower bound extracted.
	After int64

	// Before extracted from timestamp < N or timestamp <= N predicates.
	// Zero means no upper bound extracted.
	Before int64

	// LabelFilter extracted from OR predicates on labels.
	// Nil if no OR predicates were found.
	LabelFilter *physical.LabelFilter

	// Residual is the remaining CEL expression after extraction, or ""
	// if the entire expression was pushed down.
	Residual string

	// FullyPushed is true when the entire expression was converted to
	// backend-native filters and no post-fetch CEL evaluation is needed.
	FullyPushed bool
}

// Analyze walks a CEL expression and extracts predicates that can be pushed
// to the physical backend. On any error, returns a result with Residual
// set to the original expression (safe fallback).
func (e *Evaluator) Analyze(expression string) *AnalysisResult {
	fallback := &AnalysisResult{Residual: expression}

	a, issues := e.env.Parse(expression)
	if issues != nil && issues.Err() != nil {
		return fallback
	}

	checked, issues := e.env.Check(a)
	if issues != nil && issues.Err() != nil {
		return fallback
	}

	result := &AnalysisResult{
		Labels: make(map[string]string),
	}

	rootExpr := checked.NativeRep().Expr()
	residualParts := extractPredicates(rootExpr, result)

	if len(residualParts) == 0 {
		result.FullyPushed = true
		result.Residual = ""
	} else {
		result.Residual = joinResidual(residualParts)
		result.FullyPushed = false
	}

	return result
}

// extractPredicates recursively walks AND-connected predicates.
// Returns string representations of sub-expressions that could not be pushed down.
func extractPredicates(expr ast.Expr, result *AnalysisResult) []string {
	// If expr is a logical AND (&&), recurse into both sides
	if isCall(expr, "_&&_") {
		call := expr.AsCall()
		var residual []string
		for _, arg := range call.Args() {
			residual = append(residual, extractPredicates(arg, result)...)
		}
		return residual
	}

	// Try to extract OR groups of label equalities
	if isCall(expr, "_||_") {
		if groups, ok := extractORLabelGroups(expr); ok {
			result.LabelFilter = &physical.LabelFilter{OR: groups}
			return nil
		}
	}

	// Try to extract: labels["key"] == "value"
	if tryExtractLabelEquality(expr, result) {
		return nil
	}

	// Try to extract: timestamp > N, timestamp >= N, timestamp < N, timestamp <= N
	if tryExtractTimestampBound(expr, result) {
		return nil
	}

	// Try to extract: "true" literal
	if isLiteralTrue(expr) {
		return nil
	}

	// Not pushable — return the expression text as residual
	return []string{exprToString(expr)}
}

// tryExtractLabelEquality matches: labels["key"] == "value"
func tryExtractLabelEquality(expr ast.Expr, result *AnalysisResult) bool {
	if !isCall(expr, "_==_") {
		return false
	}
	call := expr.AsCall()
	args := call.Args()
	if len(args) != 2 {
		return false
	}

	var mapKey, litVal string
	if k, ok := asLabelIndex(args[0]); ok {
		if v, ok := asStringLiteral(args[1]); ok {
			mapKey, litVal = k, v
		}
	} else if k, ok := asLabelIndex(args[1]); ok {
		if v, ok := asStringLiteral(args[0]); ok {
			mapKey, litVal = k, v
		}
	}
	if mapKey == "" {
		return false
	}

	result.Labels[mapKey] = litVal
	return true
}

// tryExtractTimestampBound matches: timestamp {>|>=|<|<=} N
func tryExtractTimestampBound(expr ast.Expr, result *AnalysisResult) bool {
	for _, op := range []string{"_>_", "_>=_", "_<_", "_<=_"} {
		if !isCall(expr, op) {
			continue
		}
		call := expr.AsCall()
		args := call.Args()
		if len(args) != 2 {
			return false
		}

		var lit int64
		var identLeft bool
		var found bool

		if isIdent(args[0], "timestamp") {
			if n, ok := asIntLiteral(args[1]); ok {
				lit, identLeft, found = n, true, true
			}
		} else if isIdent(args[1], "timestamp") {
			if n, ok := asIntLiteral(args[0]); ok {
				lit, identLeft, found = n, false, true
			}
		}
		if !found {
			return false
		}

		// Normalize: identLeft means "timestamp {op} N"
		switch op {
		case "_>_":
			if identLeft {
				result.After = lit
			} else {
				result.Before = lit
			}
		case "_>=_":
			if identLeft {
				if lit > 0 {
					result.After = lit - 1
				}
			} else {
				result.Before = lit + 1
			}
		case "_<_":
			if identLeft {
				result.Before = lit
			} else {
				result.After = lit
			}
		case "_<=_":
			if identLeft {
				result.Before = lit + 1
			} else {
				if lit > 0 {
					result.After = lit - 1
				}
			}
		}
		return true
	}
	return false
}

// extractORLabelGroups tries to extract a tree of || connected label equality
// predicates into LabelFilterGroups.
func extractORLabelGroups(expr ast.Expr) ([]physical.LabelFilterGroup, bool) {
	if !isCall(expr, "_||_") {
		// Leaf: must be a single label equality or AND group
		group, ok := extractANDGroup(expr)
		if !ok {
			return nil, false
		}
		return []physical.LabelFilterGroup{group}, true
	}

	call := expr.AsCall()
	var groups []physical.LabelFilterGroup
	for _, arg := range call.Args() {
		sub, ok := extractORLabelGroups(arg)
		if !ok {
			return nil, false
		}
		groups = append(groups, sub...)
	}
	return groups, true
}

// extractANDGroup extracts label equalities from an AND-connected expression.
func extractANDGroup(expr ast.Expr) (physical.LabelFilterGroup, bool) {
	if isCall(expr, "_&&_") {
		call := expr.AsCall()
		var predicates []physical.LabelPredicate
		for _, arg := range call.Args() {
			sub, ok := extractANDGroup(arg)
			if !ok {
				return physical.LabelFilterGroup{}, false
			}
			predicates = append(predicates, sub.Predicates...)
		}
		return physical.LabelFilterGroup{Predicates: predicates}, true
	}

	// Must be a single label equality
	k, v, ok := asLabelEqualityPair(expr)
	if !ok {
		return physical.LabelFilterGroup{}, false
	}
	return physical.LabelFilterGroup{
		Predicates: []physical.LabelPredicate{{Key: k, Value: v}},
	}, true
}

// asLabelEqualityPair extracts key and value from labels["k"] == "v".
func asLabelEqualityPair(expr ast.Expr) (string, string, bool) {
	if !isCall(expr, "_==_") {
		return "", "", false
	}
	call := expr.AsCall()
	args := call.Args()
	if len(args) != 2 {
		return "", "", false
	}

	if k, ok := asLabelIndex(args[0]); ok {
		if v, ok := asStringLiteral(args[1]); ok {
			return k, v, true
		}
	}
	if k, ok := asLabelIndex(args[1]); ok {
		if v, ok := asStringLiteral(args[0]); ok {
			return k, v, true
		}
	}
	return "", "", false
}

// --- AST helpers ---

func isCall(expr ast.Expr, fn string) bool {
	return expr.Kind() == ast.CallKind && expr.AsCall().FunctionName() == fn
}

func isIdent(expr ast.Expr, name string) bool {
	return expr.Kind() == ast.IdentKind && expr.AsIdent() == name
}

func isLiteralTrue(expr ast.Expr) bool {
	if expr.Kind() != ast.LiteralKind {
		return false
	}
	v := expr.AsLiteral()
	if v == nil {
		return false
	}
	b, ok := v.Value().(bool)
	return ok && b
}

// asLabelIndex matches labels["key"] — a map index expression.
func asLabelIndex(expr ast.Expr) (string, bool) {
	if !isCall(expr, "_[_]") {
		return "", false
	}
	call := expr.AsCall()
	args := call.Args()
	if len(args) != 2 {
		return "", false
	}
	if !isIdent(args[0], "labels") {
		return "", false
	}
	return asStringLiteral(args[1])
}

func asStringLiteral(expr ast.Expr) (string, bool) {
	if expr.Kind() != ast.LiteralKind {
		return "", false
	}
	v := expr.AsLiteral()
	if v == nil {
		return "", false
	}
	s, ok := v.Value().(string)
	return s, ok
}

func asIntLiteral(expr ast.Expr) (int64, bool) {
	if expr.Kind() != ast.LiteralKind {
		return 0, false
	}
	v := expr.AsLiteral()
	if v == nil {
		return 0, false
	}
	n, ok := v.Value().(int64)
	return n, ok
}

func joinResidual(parts []string) string {
	return strings.Join(parts, " && ")
}

// exprToString reconstructs a CEL expression string from an AST node.
// This is a best-effort reconstruction; complex expressions may not round-trip perfectly.
func exprToString(expr ast.Expr) string {
	switch expr.Kind() {
	case ast.LiteralKind:
		v := expr.AsLiteral()
		if v == nil {
			return "null"
		}
		switch val := v.Value().(type) {
		case string:
			return fmt.Sprintf("%q", val)
		case int64:
			return fmt.Sprintf("%d", val)
		case bool:
			if val {
				return "true"
			}
			return "false"
		default:
			return fmt.Sprintf("%v", val)
		}

	case ast.IdentKind:
		return expr.AsIdent()

	case ast.CallKind:
		call := expr.AsCall()
		fn := call.FunctionName()
		args := call.Args()

		// Binary operators
		switch fn {
		case "_&&_":
			if len(args) == 2 {
				return exprToString(args[0]) + " && " + exprToString(args[1])
			}
		case "_||_":
			if len(args) == 2 {
				return "(" + exprToString(args[0]) + " || " + exprToString(args[1]) + ")"
			}
		case "_==_":
			if len(args) == 2 {
				return exprToString(args[0]) + " == " + exprToString(args[1])
			}
		case "_!=_":
			if len(args) == 2 {
				return exprToString(args[0]) + " != " + exprToString(args[1])
			}
		case "_>_":
			if len(args) == 2 {
				return exprToString(args[0]) + " > " + exprToString(args[1])
			}
		case "_>=_":
			if len(args) == 2 {
				return exprToString(args[0]) + " >= " + exprToString(args[1])
			}
		case "_<_":
			if len(args) == 2 {
				return exprToString(args[0]) + " < " + exprToString(args[1])
			}
		case "_<=_":
			if len(args) == 2 {
				return exprToString(args[0]) + " <= " + exprToString(args[1])
			}
		case "_[_]":
			if len(args) == 2 {
				return exprToString(args[0]) + "[" + exprToString(args[1]) + "]"
			}
		case "_!_":
			if len(args) == 1 {
				return "!" + exprToString(args[0])
			}
		case "@in":
			if len(args) == 2 {
				return exprToString(args[0]) + " in " + exprToString(args[1])
			}
		}

		// Function call
		argStrs := make([]string, len(args))
		for i, a := range args {
			argStrs[i] = exprToString(a)
		}
		if call.Target().Kind() != ast.UnspecifiedExprKind {
			return exprToString(call.Target()) + "." + fn + "(" + strings.Join(argStrs, ", ") + ")"
		}
		return fn + "(" + strings.Join(argStrs, ", ") + ")"

	case ast.SelectKind:
		sel := expr.AsSelect()
		return exprToString(sel.Operand()) + "." + sel.FieldName()

	default:
		return "/* unknown */"
	}
}
