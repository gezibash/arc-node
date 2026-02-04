package capability

import "context"

// Handler processes incoming envelopes for a capability.
//
// Implementations should focus purely on business logic - the Server
// handles all relay communication, subscription management, and response
// routing automatically.
//
// Example:
//
//	type MyHandler struct {
//	    store Store
//	}
//
//	func (h *MyHandler) Handle(ctx context.Context, env *Envelope) (*Response, error) {
//	    switch env.Action {
//	    case "get":
//	        data, err := h.store.Get(env.Labels["key"])
//	        if err != nil {
//	            return Err(err.Error()), nil
//	        }
//	        return OK(data), nil
//	    case "put":
//	        if err := h.store.Put(env.Labels["key"], env.Payload); err != nil {
//	            return nil, err // internal error
//	        }
//	        return OK(nil), nil
//	    default:
//	        return Err("unknown action"), nil
//	    }
//	}
type Handler interface {
	// Handle processes an incoming envelope and returns a response.
	//
	// Return values:
	//   - (*Response, nil): Send response to sender
	//   - (nil, nil): No response (equivalent to NoReply())
	//   - (nil, error): Internal error (logged, no response sent)
	//   - (*Response{Error: ...}, nil): Error response sent to sender
	Handle(ctx context.Context, env *Envelope) (*Response, error)
}

// HandlerFunc is an adapter to allow ordinary functions as Handlers.
type HandlerFunc func(ctx context.Context, env *Envelope) (*Response, error)

// Handle implements Handler by calling f.
func (f HandlerFunc) Handle(ctx context.Context, env *Envelope) (*Response, error) {
	return f(ctx, env)
}
