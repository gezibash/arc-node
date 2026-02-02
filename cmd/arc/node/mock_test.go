package node

import (
	"context"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
)

type mockClient struct {
	putContentFn        func(ctx context.Context, data []byte) (reference.Reference, error)
	getContentFn        func(ctx context.Context, ref reference.Reference) ([]byte, error)
	resolveGetFn        func(ctx context.Context, prefix string) (*client.GetResult, error)
	sendMessageFn       func(ctx context.Context, msg message.Message, labels map[string]string, dims *nodev1.Dimensions) (reference.Reference, error)
	queryMessagesFn     func(ctx context.Context, opts *client.QueryOptions) (*client.QueryResult, error)
	subscribeMessagesFn func(ctx context.Context, expression string, labels map[string]string) (<-chan *client.Entry, <-chan error, error)
	federateFn          func(ctx context.Context, peer string, labels map[string]string) (*client.FederateResult, error)
	listPeersFn         func(ctx context.Context) ([]client.PeerInfo, error)
	nodeKeyFn           func() (identity.PublicKey, bool)
	closeFn             func() error
}

func (m *mockClient) PutContent(ctx context.Context, data []byte) (reference.Reference, error) {
	return m.putContentFn(ctx, data)
}

func (m *mockClient) GetContent(ctx context.Context, ref reference.Reference) ([]byte, error) {
	return m.getContentFn(ctx, ref)
}

func (m *mockClient) ResolveGet(ctx context.Context, prefix string) (*client.GetResult, error) {
	return m.resolveGetFn(ctx, prefix)
}

func (m *mockClient) SendMessage(ctx context.Context, msg message.Message, labels map[string]string, dims *nodev1.Dimensions) (reference.Reference, error) {
	return m.sendMessageFn(ctx, msg, labels, dims)
}

func (m *mockClient) QueryMessages(ctx context.Context, opts *client.QueryOptions) (*client.QueryResult, error) {
	return m.queryMessagesFn(ctx, opts)
}

func (m *mockClient) SubscribeMessages(ctx context.Context, expression string, labels map[string]string) (<-chan *client.Entry, <-chan error, error) {
	return m.subscribeMessagesFn(ctx, expression, labels)
}

func (m *mockClient) Federate(ctx context.Context, peer string, labels map[string]string) (*client.FederateResult, error) {
	return m.federateFn(ctx, peer, labels)
}

func (m *mockClient) ListPeers(ctx context.Context) ([]client.PeerInfo, error) {
	return m.listPeersFn(ctx)
}

func (m *mockClient) NodeKey() (identity.PublicKey, bool) {
	if m.nodeKeyFn != nil {
		return m.nodeKeyFn()
	}
	return identity.PublicKey{}, false
}

func (m *mockClient) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

type mockKeyLoader struct {
	loadFn        func(ctx context.Context, nameOrID string) (*identity.Keypair, error)
	loadDefaultFn func(ctx context.Context) (*identity.Keypair, error)
}

func (m *mockKeyLoader) Load(ctx context.Context, nameOrID string) (*identity.Keypair, error) {
	return m.loadFn(ctx, nameOrID)
}

func (m *mockKeyLoader) LoadDefault(ctx context.Context) (*identity.Keypair, error) {
	return m.loadDefaultFn(ctx)
}
