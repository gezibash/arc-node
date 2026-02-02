package node

import (
	"context"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
)

// NodeClient defines the subset of *client.Client used by node commands.
// *client.Client satisfies this interface without modification.
type NodeClient interface {
	PutContent(ctx context.Context, data []byte) (reference.Reference, error)
	GetContent(ctx context.Context, ref reference.Reference) ([]byte, error)
	ResolveGet(ctx context.Context, prefix string) (*client.GetResult, error)
	SendMessage(ctx context.Context, msg message.Message, labels map[string]string, dims *nodev1.Dimensions) (*client.PublishResult, error)
	QueryMessages(ctx context.Context, opts *client.QueryOptions) (*client.QueryResult, error)
	SubscribeMessages(ctx context.Context, expression string, labels map[string]string, opts ...client.SubscribeOption) (<-chan *client.Entry, <-chan error, error)
	Federate(ctx context.Context, peer string, labels map[string]string) (*client.FederateResult, error)
	ListPeers(ctx context.Context) ([]client.PeerInfo, error)
	NodeKey() (identity.PublicKey, bool)
	Close() error
}

// KeyLoader abstracts keypair loading for testing.
type KeyLoader interface {
	Load(ctx context.Context, nameOrID string) (*identity.Keypair, error)
	LoadDefault(ctx context.Context) (*identity.Keypair, error)
}
