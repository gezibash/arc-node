package middleware

import (
	"context"
	"fmt"
	"testing"
)

func TestRunPreEmpty(t *testing.T) {
	c := &Chain{
		Pre:  []Hook{},
		Post: []Hook{},
	}

	info := &CallInfo{
		FullMethod: "test.Service/Method",
		IsStream:   false,
	}

	ctx := context.Background()
	resultCtx, err := c.RunPre(ctx, info)

	if err != nil {
		t.Errorf("RunPre() with empty chain returned error: %v", err)
	}

	if resultCtx != ctx {
		t.Error("RunPre() should return the same context when chain is empty")
	}
}

func TestRunPreSuccess(t *testing.T) {
	callCount := 0

	hook1 := func(ctx context.Context, info *CallInfo) (context.Context, error) {
		callCount++
		return ctx, nil
	}

	hook2 := func(ctx context.Context, info *CallInfo) (context.Context, error) {
		callCount++
		return ctx, nil
	}

	c := &Chain{
		Pre: []Hook{hook1, hook2},
	}

	info := &CallInfo{
		FullMethod: "test.Service/Method",
		IsStream:   false,
	}

	ctx := context.Background()
	_, err := c.RunPre(ctx, info)

	if err != nil {
		t.Errorf("RunPre() returned error: %v", err)
	}

	if callCount != 2 {
		t.Errorf("Expected both hooks to be called, got %d calls", callCount)
	}
}

func TestRunPreStopsOnError(t *testing.T) {
	callCount := 0

	hook1 := func(ctx context.Context, info *CallInfo) (context.Context, error) {
		callCount++
		return ctx, fmt.Errorf("hook1 failed")
	}

	hook2 := func(ctx context.Context, info *CallInfo) (context.Context, error) {
		callCount++
		return ctx, nil
	}

	c := &Chain{
		Pre: []Hook{hook1, hook2},
	}

	info := &CallInfo{
		FullMethod: "test.Service/Method",
		IsStream:   false,
	}

	ctx := context.Background()
	_, err := c.RunPre(ctx, info)

	if err == nil {
		t.Error("RunPre() should return error from first hook")
	}

	if err.Error() != "hook1 failed" {
		t.Errorf("Expected 'hook1 failed' error, got: %v", err)
	}

	if callCount != 1 {
		t.Errorf("Expected only first hook to be called, got %d calls", callCount)
	}
}

func TestRunPreContextPropagation(t *testing.T) {
	type ctxKey struct{}

	hook1 := func(ctx context.Context, info *CallInfo) (context.Context, error) {
		return context.WithValue(ctx, ctxKey{}, "value1"), nil
	}

	hook2 := func(ctx context.Context, info *CallInfo) (context.Context, error) {
		v := ctx.Value(ctxKey{})
		if v != "value1" {
			t.Errorf("Expected context value 'value1', got: %v", v)
		}
		return context.WithValue(ctx, ctxKey{}, "value2"), nil
	}

	c := &Chain{
		Pre: []Hook{hook1, hook2},
	}

	info := &CallInfo{
		FullMethod: "test.Service/Method",
		IsStream:   false,
	}

	ctx := context.Background()
	resultCtx, err := c.RunPre(ctx, info)

	if err != nil {
		t.Errorf("RunPre() returned error: %v", err)
	}

	v := resultCtx.Value(ctxKey{})
	if v != "value2" {
		t.Errorf("Expected final context value 'value2', got: %v", v)
	}
}

func TestRunPostEmpty(t *testing.T) {
	c := &Chain{
		Pre:  []Hook{},
		Post: []Hook{},
	}

	info := &CallInfo{
		FullMethod: "test.Service/Method",
		IsStream:   false,
	}

	ctx := context.Background()
	resultCtx, err := c.RunPost(ctx, info)

	if err != nil {
		t.Errorf("RunPost() with empty chain returned error: %v", err)
	}

	if resultCtx != ctx {
		t.Error("RunPost() should return the same context when chain is empty")
	}
}

func TestRunPostSuccess(t *testing.T) {
	callCount := 0

	hook1 := func(ctx context.Context, info *CallInfo) (context.Context, error) {
		callCount++
		return ctx, nil
	}

	hook2 := func(ctx context.Context, info *CallInfo) (context.Context, error) {
		callCount++
		return ctx, nil
	}

	c := &Chain{
		Post: []Hook{hook1, hook2},
	}

	info := &CallInfo{
		FullMethod: "test.Service/Method",
		IsStream:   true,
	}

	ctx := context.Background()
	_, err := c.RunPost(ctx, info)

	if err != nil {
		t.Errorf("RunPost() returned error: %v", err)
	}

	if callCount != 2 {
		t.Errorf("Expected both hooks to be called, got %d calls", callCount)
	}
}

func TestRunPostStopsOnError(t *testing.T) {
	callCount := 0

	hook1 := func(ctx context.Context, info *CallInfo) (context.Context, error) {
		callCount++
		return ctx, fmt.Errorf("hook1 failed")
	}

	hook2 := func(ctx context.Context, info *CallInfo) (context.Context, error) {
		callCount++
		return ctx, nil
	}

	c := &Chain{
		Post: []Hook{hook1, hook2},
	}

	info := &CallInfo{
		FullMethod: "test.Service/Method",
		IsStream:   false,
	}

	ctx := context.Background()
	_, err := c.RunPost(ctx, info)

	if err == nil {
		t.Error("RunPost() should return error from first hook")
	}

	if err.Error() != "hook1 failed" {
		t.Errorf("Expected 'hook1 failed' error, got: %v", err)
	}

	if callCount != 1 {
		t.Errorf("Expected only first hook to be called, got %d calls", callCount)
	}
}

func TestCallInfoFields(t *testing.T) {
	info := &CallInfo{
		FullMethod: "test.Service/Method",
		IsStream:   true,
	}

	if info.FullMethod != "test.Service/Method" {
		t.Errorf("Expected FullMethod 'test.Service/Method', got: %s", info.FullMethod)
	}

	if !info.IsStream {
		t.Error("Expected IsStream to be true")
	}

	info2 := &CallInfo{
		FullMethod: "another.Service/RPC",
		IsStream:   false,
	}

	if info2.IsStream {
		t.Error("Expected IsStream to be false")
	}
}

func TestNilChain(t *testing.T) {
	c := &Chain{
		Pre:  nil,
		Post: nil,
	}

	info := &CallInfo{
		FullMethod: "test.Service/Method",
		IsStream:   false,
	}

	ctx := context.Background()

	resultCtx, err := c.RunPre(ctx, info)
	if err != nil {
		t.Errorf("RunPre() with nil Pre returned error: %v", err)
	}
	if resultCtx != ctx {
		t.Error("RunPre() should return the same context with nil Pre")
	}

	resultCtx, err = c.RunPost(ctx, info)
	if err != nil {
		t.Errorf("RunPost() with nil Post returned error: %v", err)
	}
	if resultCtx != ctx {
		t.Error("RunPost() should return the same context with nil Post")
	}
}
