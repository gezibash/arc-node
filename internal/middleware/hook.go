package middleware

import "context"

// CallInfo describes the current gRPC call for hook processing.
type CallInfo struct {
	FullMethod string
	IsStream   bool
}

// Hook processes a call. Return a gRPC status error to reject.
type Hook func(ctx context.Context, info *CallInfo) (context.Context, error)

// Chain holds ordered pre and post hooks.
type Chain struct {
	Pre  []Hook
	Post []Hook
}

// RunPre executes pre-hooks in order. Stops on first error.
func (c *Chain) RunPre(ctx context.Context, info *CallInfo) (context.Context, error) {
	for _, h := range c.Pre {
		var err error
		ctx, err = h(ctx, info)
		if err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}

// RunPost executes post-hooks in order. Stops on first error.
func (c *Chain) RunPost(ctx context.Context, info *CallInfo) (context.Context, error) {
	for _, h := range c.Post {
		var err error
		ctx, err = h(ctx, info)
		if err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}
