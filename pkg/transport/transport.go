// Package transport defines the core messaging interface used by capabilities.
package transport

import (
	"context"

	"github.com/gezibash/arc/v2/pkg/identity"
)

// Envelope is the data sent over the transport.
type Envelope struct {
	Labels      map[string]string
	Payload     []byte
	Correlation string
}

// Delivery is a received envelope.
type Delivery struct {
	Ref            []byte
	Labels         map[string]string
	Payload        []byte
	Sender         identity.PublicKey
	SubscriptionID string
}

// Receipt is the result of a send.
type Receipt struct {
	Ref       []byte
	Status    string
	Delivered int
	Reason    string
}

// Transport defines the transport operations required by the capability framework.
type Transport interface {
	Send(ctx context.Context, env *Envelope) (*Receipt, error)
	Receive(ctx context.Context) (*Delivery, error)
	Subscribe(id string, labels map[string]any) error
	Unsubscribe(id string) error
	RegisterName(name string) error
	Discover(ctx context.Context, filter map[string]string, limit int) (*ProviderSet, error)
	DiscoverExpr(ctx context.Context, expr string, limit int) (*ProviderSet, error)
	UpdateState(id string, state map[string]any) error
	Close() error
}
