package relay

import (
	"context"
	"fmt"
	"time"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/transport"
)

// Discover finds subscriptions matching the given filter labels.
// Filter semantics: all filter labels must be present in the subscription.
// Empty filter returns all subscriptions.
// Limit of 0 uses server default (100).
func (c *Client) Discover(ctx context.Context, filter map[string]string, limit int) (*transport.ProviderSet, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}

	// Generate correlation ID
	correlation := fmt.Sprintf("discover-%d", c.nextCorrelation())

	// Register pending channel before sending
	ch := c.registerPending(correlation)
	defer c.deregisterPending(correlation)

	frame := &relayv1.ClientFrame{
		Frame: &relayv1.ClientFrame_Discover{
			Discover: &relayv1.DiscoverFrame{
				Filter:      filter,
				Correlation: correlation,
				Limit:       int32(limit),
			},
		},
	}

	c.mu.Lock()
	err := c.stream.Send(frame)
	c.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("send discover: %w", err)
	}

	return c.waitDiscoverResult(ctx, ch)
}

// DiscoverWithLimit is a convenience wrapper around Discover.
func (c *Client) DiscoverWithLimit(ctx context.Context, filter map[string]string, limit int) (*transport.ProviderSet, error) {
	return c.Discover(ctx, filter, limit)
}

// DiscoverAll uses the server default limit.
func (c *Client) DiscoverAll(ctx context.Context, filter map[string]string) (*transport.ProviderSet, error) {
	return c.Discover(ctx, filter, 0)
}

// waitDiscoverResult waits for a discover result on the dedicated pending channel.
func (c *Client) waitDiscoverResult(ctx context.Context, ch chan *relayv1.ServerFrame) (*transport.ProviderSet, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case frame, ok := <-ch:
		if !ok {
			return nil, c.streamError()
		}
		switch f := frame.GetFrame().(type) {
		case *relayv1.ServerFrame_DiscoverResult:
			r := f.DiscoverResult
			providers := make([]transport.Provider, 0, len(r.GetProviders()))
			for _, p := range r.GetProviders() {
				pubkey, err := identity.DecodePublicKeyBytes(p.GetPubkey())
				if err != nil {
					return nil, fmt.Errorf("invalid provider pubkey: %w", err)
				}
				relayPubkey, err := identity.DecodePublicKeyBytes(p.GetRelayPubkey())
				if err != nil {
					return nil, fmt.Errorf("invalid relay pubkey: %w", err)
				}

				providers = append(providers, transport.Provider{
					Pubkey:            pubkey,
					Name:              p.GetName(),
					Petname:           p.GetPetname(),
					Labels:            p.GetLabels(),
					SubscriptionID:    p.GetSubscriptionId(),
					RelayPubkey:       relayPubkey,
					Latency:           time.Duration(p.GetLatencyNs()),
					InterRelayLatency: time.Duration(p.GetInterRelayLatencyNs()),
					LastSeen:          time.Unix(0, p.GetLastSeenNs()),
					Connected:         time.Duration(p.GetConnectedNs()),
				})
			}
			return &transport.ProviderSet{
				Providers: providers,
				Total:     int(r.GetTotal()),
				HasMore:   r.GetHasMore(),
			}, nil
		case *relayv1.ServerFrame_Error:
			return nil, fmt.Errorf("discover error: %s", f.Error.GetMessage())
		default:
			return nil, fmt.Errorf("unexpected frame type waiting for discover result")
		}
	case <-c.done:
		return nil, c.streamError()
	}
}

// UpdateLabels updates labels on an existing subscription.
// Merged labels overwrite existing keys, remove deletes specified keys.
func (c *Client) UpdateLabels(subscriptionID string, merge map[string]string, remove []string) error {
	if c.closed.Load() {
		return ErrClosed
	}

	frame := &relayv1.ClientFrame{
		Frame: &relayv1.ClientFrame_UpdateLabels{
			UpdateLabels: &relayv1.UpdateLabelsFrame{
				Id:     subscriptionID,
				Labels: merge,
				Remove: remove,
			},
		},
	}

	c.mu.Lock()
	err := c.stream.Send(frame)
	c.mu.Unlock()

	return err
}
