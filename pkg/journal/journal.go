package journal

import (
	"github.com/gezibash/arc/pkg/identity"
	"github.com/gezibash/arc-node/pkg/client"
)

const contentType = "application/x-arc-journal+encrypted"

// Journal provides encrypted journal operations over an Arc node.
type Journal struct {
	client  *client.Client
	kp      *identity.Keypair
	nodeKey *identity.PublicKey
	symKey  [32]byte
}

// Option configures a Journal.
type Option func(*Journal)

// WithNodeKey sets the node public key used as the default message recipient.
func WithNodeKey(pub identity.PublicKey) Option {
	return func(j *Journal) { j.nodeKey = &pub }
}

// New creates a Journal backed by the given client and keypair.
func New(c *client.Client, kp *identity.Keypair, opts ...Option) *Journal {
	j := &Journal{
		client: c,
		kp:     kp,
		symKey: deriveKey(kp),
	}
	for _, o := range opts {
		o(j)
	}
	return j
}
