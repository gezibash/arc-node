package dm

import (
	"fmt"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/identity"
)

const contentType = "application/x-arc-dm+encrypted"

// DM provides encrypted direct messaging between two keypairs over an Arc node.
type DM struct {
	client    *client.Client
	kp        *identity.Keypair
	nodeKey   *identity.PublicKey
	peerPub   identity.PublicKey
	sharedKey [32]byte
	convID    string
	search    *SearchIndex
}

// Option configures a DM.
type Option func(*DM)

// WithNodeKey sets the node public key used as the message recipient.
func WithNodeKey(pub identity.PublicKey) Option {
	return func(d *DM) { d.nodeKey = &pub }
}

// WithSearchIndex attaches a search index for automatic indexing on send.
func WithSearchIndex(idx *SearchIndex) Option {
	return func(d *DM) { d.search = idx }
}

// New creates a DM session for the given keypair and peer public key.
func New(c *client.Client, kp *identity.Keypair, peerPub identity.PublicKey, opts ...Option) (*DM, error) {
	shared, err := deriveSharedKey(kp, peerPub)
	if err != nil {
		return nil, fmt.Errorf("derive shared key: %w", err)
	}

	d := &DM{
		client:    c,
		kp:        kp,
		peerPub:   peerPub,
		sharedKey: shared,
		convID:    ConversationID(kp.PublicKey(), peerPub),
	}
	for _, o := range opts {
		o(d)
	}
	return d, nil
}

// SelfPublicKey returns the local keypair's public key.
func (d *DM) SelfPublicKey() identity.PublicKey {
	return d.kp.PublicKey()
}

// PeerPublicKey returns the peer's public key.
func (d *DM) PeerPublicKey() identity.PublicKey {
	return d.peerPub
}

func (d *DM) recipientKey() identity.PublicKey {
	if d.client != nil {
		if key, ok := d.client.NodeKey(); ok {
			return key
		}
	}
	if d.nodeKey != nil {
		return *d.nodeKey
	}
	return d.kp.PublicKey()
}
