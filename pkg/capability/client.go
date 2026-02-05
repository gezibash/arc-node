package capability

import (
	"context"
	"fmt"
	"time"

	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/runtime"
	"github.com/gezibash/arc/v2/pkg/transport"
	"github.com/google/uuid"
)

// Client provides helper methods for making capability requests through the relay.
// The relay is encapsulated internally - callers never deal with relay types directly.
type Client struct {
	rt    *runtime.Runtime
	tr    transport.Transport
	subID string // persistent subscription ID for receiving responses
}

// NewClient creates a new capability client from a runtime.
// The runtime must have the relay capability configured.
// Automatically subscribes to receive responses addressed to this client.
func NewClient(rt *runtime.Runtime) (*Client, error) {
	if rt == nil {
		return nil, fmt.Errorf("runtime required")
	}

	tr := transport.From(rt)
	if tr == nil {
		return nil, fmt.Errorf("transport not configured on runtime")
	}

	// Subscribe to receive responses addressed to us
	pk := rt.PublicKey()
	subID := uuid.New().String()
	if err := tr.Subscribe(subID, map[string]any{
		"to": identity.EncodePublicKey(pk),
	}); err != nil {
		return nil, fmt.Errorf("subscribe for responses: %w", err)
	}

	return &Client{
		rt:    rt,
		tr:    tr,
		subID: subID,
	}, nil
}

// Close unsubscribes from the relay.
func (c *Client) Close() error {
	if c.subID != "" {
		return c.tr.Unsubscribe(c.subID)
	}
	return nil
}

// Sender returns our public key (identity).
func (c *Client) Sender() identity.PublicKey {
	return c.rt.PublicKey()
}

// RequestConfig configures a capability request.
type RequestConfig struct {
	// Capability is the target capability name (required).
	Capability string

	// Action is the action to perform (optional, added as action label).
	Action string

	// Labels are additional labels for the request.
	Labels map[string]string

	// Payload is the request data.
	Payload []byte

	// Timeout for waiting for a response (default: no timeout, uses context).
	Timeout time.Duration
}

// RequestResult contains the response from a capability request.
type RequestResult struct {
	// Labels from the response envelope.
	Labels map[string]string

	// Payload is the response data.
	Payload []byte

	// Error from the response (if labels["error"] was set).
	Error string
}

// Request sends a request to a capability and waits for a response.
// Uses the client's persistent subscription to receive the response.
// Routes to any available target providing the capability.
func (c *Client) Request(ctx context.Context, cfg RequestConfig) (*RequestResult, error) {
	return c.RequestTo(ctx, nil, cfg)
}

// RequestTo sends a request to a specific target and waits for a response.
// If target is nil, routes to any available target (same as Request).
func (c *Client) RequestTo(ctx context.Context, target *Target, cfg RequestConfig) (*RequestResult, error) {
	if cfg.Capability == "" {
		return nil, fmt.Errorf("capability name required")
	}

	// Apply timeout if specified
	if cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}

	// Build labels
	labels := make(map[string]string)
	labels["capability"] = cfg.Capability
	if cfg.Action != "" {
		labels["action"] = cfg.Action
	}
	for k, v := range cfg.Labels {
		labels[k] = v
	}

	// Add target routing if specified
	if target != nil {
		// Route to specific target by pubkey
		labels["to"] = identity.EncodePublicKey(target.PublicKey)
	}

	// Send request
	env := &transport.Envelope{
		Labels:      labels,
		Payload:     cfg.Payload,
		Correlation: uuid.New().String(),
	}

	receipt, err := c.tr.Send(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	// Check NACK — no subscribers means no response will come
	if receipt.Delivered == 0 {
		reason := receipt.Reason
		if reason == "" {
			reason = "no subscribers matched"
		}
		return nil, fmt.Errorf("send request: %s", reason)
	}

	// Wait for response (uses persistent subscription)
	delivery, err := c.tr.Receive(ctx)
	if err != nil {
		return nil, fmt.Errorf("receive response: %w", err)
	}

	return &RequestResult{
		Labels:  delivery.Labels,
		Payload: delivery.Payload,
		Error:   delivery.Labels["error"],
	}, nil
}

// Send sends a request without waiting for a response (fire-and-forget).
func (c *Client) Send(ctx context.Context, capability, action string, payload []byte) error {
	labels := map[string]string{
		"capability": capability,
	}
	if action != "" {
		labels["action"] = action
	}

	env := &transport.Envelope{
		Labels:  labels,
		Payload: payload,
	}

	_, err := c.tr.Send(ctx, env)
	return err
}

// SendWithLabels sends a request with custom labels (fire-and-forget).
// This provides more control over envelope labels than Send().
func (c *Client) SendWithLabels(ctx context.Context, labels map[string]string, payload []byte) error {
	env := &transport.Envelope{
		Labels:      labels,
		Payload:     payload,
		Correlation: uuid.New().String(),
	}

	_, err := c.tr.Send(ctx, env)
	return err
}

// Delivery represents a received envelope from a capability.
type Delivery struct {
	// Sender is the public key of the sender.
	Sender identity.PublicKey

	// Labels are the routing labels.
	Labels map[string]string

	// Payload is the message data.
	Payload []byte
}

// Receive waits for the next delivery on the client's subscription.
// Useful for collecting multiple responses (e.g., discovery/want operations).
func (c *Client) Receive(ctx context.Context) (*Delivery, error) {
	d, err := c.tr.Receive(ctx)
	if err != nil {
		return nil, err
	}

	return &Delivery{
		Sender:  d.Sender,
		Labels:  d.Labels,
		Payload: d.Payload,
	}, nil
}

// DiscoverConfig configures a discovery query.
type DiscoverConfig struct {
	// Capability is the capability name to discover (e.g., "blob", "index").
	// Required.
	Capability string

	// Labels are additional filter labels beyond capability.
	// All specified labels must be present in the target's subscription.
	// Only string-valued entries are used for exact-match filtering.
	Labels map[string]string

	// Expression is a CEL expression for discovery filtering.
	// If set, Labels is ignored and this expression is evaluated against
	// merged labels ∪ state for each subscription.
	// Example: 'capability == "blob" && capacity > 1000000000'
	Expression string

	// Limit is the maximum number of results (0 = server default).
	Limit int
}

// Discover finds targets that provide a capability matching the filter.
// Returns a TargetSet that can be filtered, picked from, or iterated.
func (c *Client) Discover(ctx context.Context, cfg DiscoverConfig) (*TargetSet, error) {
	if cfg.Capability == "" {
		return nil, fmt.Errorf("capability name required")
	}

	var result *transport.ProviderSet
	var err error

	if cfg.Expression != "" {
		// CEL expression-based discovery
		result, err = c.tr.DiscoverExpr(ctx, cfg.Expression, cfg.Limit)
	} else {
		// Build filter: capability + any additional labels
		filter := make(map[string]string)
		filter["capability"] = cfg.Capability
		for k, v := range cfg.Labels {
			filter[k] = v
		}
		result, err = c.tr.Discover(ctx, filter, cfg.Limit)
	}
	if err != nil {
		return nil, fmt.Errorf("discover: %w", err)
	}

	// Convert transport.Provider to Target
	targets := make([]Target, 0, len(result.Providers))
	for _, p := range result.Providers {
		// Extract direct address via type assertion
		var addr string
		if d, ok := p.Labels["direct"].(string); ok {
			addr = d
		}

		targets = append(targets, Target{
			PublicKey:         p.Pubkey,
			Name:              p.Name,
			Petname:           p.Petname,
			Labels:            p.Labels,
			State:             p.State,
			Address:           addr,
			RelayPubkey:       p.RelayPubkey,
			Latency:           p.Latency,
			InterRelayLatency: p.InterRelayLatency,
			LastSeen:          p.LastSeen,
			Connected:         p.Connected,
		})
	}

	return NewTargetSet(targets), nil
}
