package relay

import (
	"context"
	"fmt"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/transport"
)

// Subscribe registers a subscription with the relay.
// The id is a client-generated identifier for unsubscribing.
// Labels specify exact-match filters; empty means match all.
// Sends both string labels (backward compat) and typed labels.
func (c *Client) Subscribe(id string, labels map[string]any) error {
	if c.closed.Load() {
		return ErrClosed
	}

	frame := &relayv1.ClientFrame{
		Frame: &relayv1.ClientFrame_Subscribe{
			Subscribe: &relayv1.SubscribeFrame{
				Id:          id,
				Labels:      anyToStringMap(labels),
				TypedLabels: anyToProtoMap(labels),
			},
		},
	}

	c.mu.Lock()
	err := c.stream.Send(frame)
	c.mu.Unlock()
	return err
}

// Unsubscribe removes a subscription.
func (c *Client) Unsubscribe(id string) error {
	if c.closed.Load() {
		return ErrClosed
	}

	frame := &relayv1.ClientFrame{
		Frame: &relayv1.ClientFrame_Unsubscribe{
			Unsubscribe: &relayv1.UnsubscribeFrame{
				Id: id,
			},
		},
	}

	c.mu.Lock()
	err := c.stream.Send(frame)
	c.mu.Unlock()
	return err
}

// RegisterName claims an addressed name for this connection.
// Names are prefixed with @ (e.g., "@alice").
func (c *Client) RegisterName(name string) error {
	if c.closed.Load() {
		return ErrClosed
	}

	frame := &relayv1.ClientFrame{
		Frame: &relayv1.ClientFrame_RegisterName{
			RegisterName: &relayv1.RegisterNameFrame{
				Name: name,
			},
		},
	}

	c.mu.Lock()
	err := c.stream.Send(frame)
	c.mu.Unlock()
	return err
}

// Receive waits for the next delivered envelope.
// Returns ErrClosed if the connection is closed.
func (c *Client) Receive(ctx context.Context) (*transport.Delivery, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case frame, ok := <-c.deliverCh:
		if !ok {
			return nil, c.streamError()
		}
		sf, _ := frame.GetFrame().(*relayv1.ServerFrame_Deliver)
		d := sf.Deliver
		env := d.GetEnvelope()

		sender, err := identity.DecodePublicKeyBytes(env.GetSender())
		if err != nil {
			return nil, fmt.Errorf("invalid sender: %w", err)
		}

		return &transport.Delivery{
			Ref:            env.GetRef(),
			Labels:         env.GetLabels(),
			Payload:        env.GetPayload(),
			Sender:         sender,
			SubscriptionID: d.GetSubscriptionId(),
		}, nil
	case <-c.done:
		return nil, c.streamError()
	}
}
