package client

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"

	relayv1 "github.com/gezibash/arc-node/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
)

var (
	ErrNACK    = errors.New("envelope rejected")
	ErrTimeout = errors.New("receipt timeout")
)

// Receipt is the result of sending an envelope.
type Receipt struct {
	Ref       [32]byte
	Status    relayv1.ReceiptStatus
	Delivered int
	Reason    string
}

// Send sends an envelope through the relay and waits for the receipt.
func (c *Client) Send(ctx context.Context, env *Envelope) (*Receipt, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}

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
	return c.waitReceipt(ctx, env.Correlation)
}

// Envelope is the data to send.
type Envelope struct {
	Labels      map[string]string
	Payload     []byte
	Correlation string // optional correlation ID
}

// buildEnvelope creates a signed protobuf envelope.
func (c *Client) buildEnvelope(env *Envelope) *relayv1.Envelope {
	// Compute ref
	ref := computeRef(env.Labels, env.Payload)

	// Sign: hash(labels || payload)
	sig := c.sign(ref[:])

	sender := c.sender
	return &relayv1.Envelope{
		Ref:       ref[:],
		Labels:    env.Labels,
		Payload:   env.Payload,
		Signature: sig[:],
		Sender:    sender[:],
	}
}

// sign signs data with the client's keypair using Ed25519.
func (c *Client) sign(data []byte) identity.Signature {
	seed := c.keypair.Seed()
	privateKey := ed25519.NewKeyFromSeed(seed)
	sigBytes := ed25519.Sign(privateKey, data)

	var sig identity.Signature
	copy(sig[:], sigBytes)
	return sig
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

// waitReceipt waits for a receipt matching the correlation ID.
func (c *Client) waitReceipt(ctx context.Context, correlation string) (*Receipt, error) {
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
			case *relayv1.ServerFrame_Receipt:
				r := f.Receipt.GetReceipt()
				// Match by correlation if set, otherwise accept first receipt
				if correlation == "" || r.GetCorrelation() == correlation {
					var ref [32]byte
					copy(ref[:], r.GetRef())
					return &Receipt{
						Ref:       ref,
						Status:    r.GetStatus(),
						Delivered: int(r.GetDelivered()),
						Reason:    r.GetReason(),
					}, nil
				}
			case *relayv1.ServerFrame_Error:
				e := f.Error
				if correlation == "" || e.GetCorrelation() == correlation {
					return nil, fmt.Errorf("error: %s", e.GetMessage())
				}
			}
			// Ignore other frames while waiting for receipt
		}
	}
}
