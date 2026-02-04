package relay

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/transport"
)

var ErrNACK = fmt.Errorf("envelope rejected")

// Envelope is the data to send.
type Envelope = transport.Envelope

// Receipt is the result of sending an envelope.
type Receipt = transport.Receipt

// Send sends an envelope through the relay and waits for the receipt.
func (c *Client) Send(ctx context.Context, env *transport.Envelope) (*Receipt, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}

	// Auto-generate correlation if empty
	if env.Correlation == "" {
		env.Correlation = fmt.Sprintf("send-%d", c.nextCorrelation())
	}

	// Register pending channel before sending
	ch := c.registerPending(env.Correlation)
	defer c.deregisterPending(env.Correlation)

	// Build proto envelope
	protoEnv := c.buildEnvelope(env)

	// Send frame
	frame := &relayv1.ClientFrame{
		Frame: &relayv1.ClientFrame_Send{
			Send: &relayv1.SendFrame{
				Envelope:    protoEnv,
				Correlation: env.Correlation,
			},
		},
	}

	c.mu.Lock()
	err := c.stream.Send(frame)
	c.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("send: %w", err)
	}

	// Wait for receipt
	return c.waitReceipt(ctx, ch)
}

// buildEnvelope creates a protobuf envelope.
// The relay will sign the envelope with its keypair.
// The payload (Message) should already be signed by the author.
func (c *Client) buildEnvelope(env *transport.Envelope) *relayv1.Envelope {
	// Compute ref
	ref := computeRef(env.Labels, env.Payload)

	sender := c.sender
	return &relayv1.Envelope{
		Ref:     ref[:],
		Labels:  env.Labels,
		Payload: env.Payload,
		Sender:  []byte(identity.EncodePublicKey(sender)),
	}
}

// computeRef computes SHA-256 of labels + payload.
func computeRef(labels map[string]string, payload []byte) [32]byte {
	h := sha256.New()

	// Hash labels in sorted order for determinism
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(labels[k]))
		h.Write([]byte{0})
	}
	h.Write(payload)

	var ref [32]byte
	copy(ref[:], h.Sum(nil))
	return ref
}

// waitReceipt waits for a receipt on the dedicated pending channel.
func (c *Client) waitReceipt(ctx context.Context, ch chan *relayv1.ServerFrame) (*Receipt, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case frame, ok := <-ch:
		if !ok {
			return nil, c.streamError()
		}
		switch f := frame.GetFrame().(type) {
		case *relayv1.ServerFrame_Receipt:
			r := f.Receipt.GetReceipt()
			return &Receipt{
				Ref:       r.GetRef(),
				Status:    r.GetStatus().String(),
				Delivered: int(r.GetDelivered()),
				Reason:    r.GetReason(),
			}, nil
		case *relayv1.ServerFrame_Error:
			return nil, fmt.Errorf("send error: %s", f.Error.GetMessage())
		default:
			return nil, fmt.Errorf("unexpected frame type waiting for receipt")
		}
	case <-c.done:
		return nil, c.streamError()
	}
}
