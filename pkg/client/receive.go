package client

import (
	"context"
	"fmt"

	relayv1 "github.com/gezibash/arc-node/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
)

// Delivery is a received envelope.
type Delivery struct {
	Ref            [32]byte
	Labels         map[string]string
	Payload        []byte
	Sender         identity.PublicKey
	SubscriptionID string
}

// Subscribe registers a subscription with the relay.
// The id is a client-generated identifier for unsubscribing.
// Labels specify exact-match filters; empty means match all.
func (c *Client) Subscribe(id string, labels map[string]string) error {
	if c.closed.Load() {
		return ErrClosed
	}

	frame := &relayv1.ClientFrame{
		Frame: &relayv1.ClientFrame_Subscribe{
			Subscribe: &relayv1.SubscribeFrame{
				Id:     id,
				Labels: labels,
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
func (c *Client) Receive(ctx context.Context) (*Delivery, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-c.errCh:
			return nil, err
		case frame, ok := <-c.recvCh:
			if !ok {
				return nil, ErrClosed
			}

			switch f := frame.GetFrame().(type) {
			case *relayv1.ServerFrame_Deliver:
				d := f.Deliver
				env := d.GetEnvelope()

				var ref [32]byte
				copy(ref[:], env.GetRef())

				var sender identity.PublicKey
				copy(sender[:], env.GetSender())

				return &Delivery{
					Ref:            ref,
					Labels:         env.GetLabels(),
					Payload:        env.GetPayload(),
					Sender:         sender,
					SubscriptionID: d.GetSubscriptionId(),
				}, nil

			case *relayv1.ServerFrame_Error:
				e := f.Error
				return nil, fmt.Errorf("error: %s", e.GetMessage())

			// Ignore receipts and pongs while receiving
			default:
				continue
			}
		}
	}
}
