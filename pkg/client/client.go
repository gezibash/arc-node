package client

import (
	"context"
	"fmt"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/internal/envelope"
	"github.com/gezibash/arc/pkg/identity"
	"github.com/gezibash/arc/pkg/message"
	"github.com/gezibash/arc/pkg/reference"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcmd "google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

var marshalOpts = proto.MarshalOptions{Deterministic: true}

type Client struct {
	conn *grpc.ClientConn
	stub nodev1.NodeServiceClient
}

type clientConfig struct {
	kp      *identity.Keypair
	nodeKey *identity.PublicKey
}

// Option configures client behavior.
type Option func(*clientConfig)

// WithIdentity configures the client to sign outgoing requests with the given keypair.
func WithIdentity(kp *identity.Keypair) Option {
	return func(c *clientConfig) { c.kp = kp }
}

// WithNodeKey configures the client to verify response envelopes from the given node key.
func WithNodeKey(pub identity.PublicKey) Option {
	return func(c *clientConfig) { c.nodeKey = &pub }
}

func Dial(addr string, opts ...Option) (*Client, error) {
	cfg := &clientConfig{}
	for _, o := range opts {
		o(cfg)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	if cfg.kp != nil {
		var nodeKey identity.PublicKey
		if cfg.nodeKey != nil {
			nodeKey = *cfg.nodeKey
		}
		dialOpts = append(dialOpts,
			grpc.WithUnaryInterceptor(clientUnaryInterceptor(cfg.kp, nodeKey)),
			grpc.WithStreamInterceptor(clientStreamInterceptor(cfg.kp, nodeKey)),
		)
	}

	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &Client{
		conn: conn,
		stub: nodev1.NewNodeServiceClient(conn),
	}, nil
}

func clientUnaryInterceptor(kp *identity.Keypair, nodeKey identity.PublicKey) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
		// Marshal request to get payload bytes.
		protoMsg, ok := req.(proto.Message)
		if !ok {
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}
		payload, err := marshalOpts.Marshal(protoMsg)
		if err != nil {
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}

		// Seal the outgoing envelope.
		env, err := envelope.Seal(kp, nodeKey, payload, method, kp.PublicKey(), 0, nil)
		if err != nil {
			return fmt.Errorf("seal envelope: %w", err)
		}

		ctx = envelope.InjectOutgoing(ctx, env, *env.Message.Signature)

		// Capture trailing metadata for response verification.
		var trailer grpcmd.MD
		callOpts = append(callOpts, grpc.Trailer(&trailer))

		err = invoker(ctx, method, req, reply, cc, callOpts...)
		if err != nil {
			return err
		}

		// Verify response envelope if we have a node key and trailing metadata.
		if nodeKey != (identity.PublicKey{}) && len(trailer) > 0 {
			respFrom, _, _, respTs, respSig, respCT, _, _, extractErr := envelope.ExtractTrailing(trailer)
			if extractErr == nil {
				if respProto, ok := reply.(proto.Message); ok {
					if respPayload, marshalErr := marshalOpts.Marshal(respProto); marshalErr == nil {
						_, verifyErr := envelope.Open(respFrom, kp.PublicKey(), respPayload, respCT, respTs, respSig, respFrom, 0, nil)
						if verifyErr != nil {
							return fmt.Errorf("verify response envelope: %w", verifyErr)
						}
					}
				}
			}
		}

		return nil
	}
}

func clientStreamInterceptor(kp *identity.Keypair, nodeKey identity.PublicKey) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, callOpts ...grpc.CallOption) (grpc.ClientStream, error) {
		// Seal envelope with empty payload for stream open.
		env, err := envelope.Seal(kp, nodeKey, []byte{}, method, kp.PublicKey(), 0, nil)
		if err != nil {
			return nil, fmt.Errorf("seal stream envelope: %w", err)
		}

		ctx = envelope.InjectOutgoing(ctx, env, *env.Message.Signature)

		return streamer(ctx, desc, cc, method, callOpts...)
	}
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// GetKind indicates whether a ResolveGet result is a blob or message.
type GetKind int

const (
	GetKindBlob    GetKind = 0
	GetKindMessage GetKind = 1
)

// GetResult is the result of a ResolveGet call.
type GetResult struct {
	Kind      GetKind
	Ref       reference.Reference
	Data      []byte            // blob content (GetKindBlob only)
	Labels    map[string]string // message metadata (GetKindMessage only)
	Timestamp int64             // message timestamp (GetKindMessage only)
}

func (c *Client) ResolveGet(ctx context.Context, prefix string) (*GetResult, error) {
	resp, err := c.stub.ResolveGet(ctx, &nodev1.ResolveGetRequest{Prefix: prefix})
	if err != nil {
		return nil, err
	}
	var ref reference.Reference
	copy(ref[:], resp.Reference)
	return &GetResult{
		Kind:      GetKind(resp.Kind),
		Ref:       ref,
		Data:      resp.Data,
		Labels:    resp.Labels,
		Timestamp: resp.Timestamp,
	}, nil
}

func (c *Client) PutContent(ctx context.Context, data []byte) (reference.Reference, error) {
	resp, err := c.stub.PutContent(ctx, &nodev1.PutContentRequest{Data: data})
	if err != nil {
		return reference.Reference{}, err
	}
	var ref reference.Reference
	copy(ref[:], resp.Reference)
	return ref, nil
}

func (c *Client) GetContent(ctx context.Context, ref reference.Reference) ([]byte, error) {
	resp, err := c.stub.GetContent(ctx, &nodev1.GetContentRequest{Reference: ref[:]})
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (c *Client) SendMessage(ctx context.Context, msg message.Message, labels map[string]string) (reference.Reference, error) {
	canonical, err := message.CanonicalBytes(msg)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("canonical bytes: %w", err)
	}
	resp, err := c.stub.SendMessage(ctx, &nodev1.SendMessageRequest{
		Message: canonical,
		Labels:  labels,
	})
	if err != nil {
		return reference.Reference{}, err
	}
	var ref reference.Reference
	copy(ref[:], resp.Reference)
	return ref, nil
}

type QueryOptions struct {
	Expression string
	Labels     map[string]string
	Limit      int
	Cursor     string
	Descending bool
}

type QueryResult struct {
	Entries    []*Entry
	NextCursor string
	HasMore    bool
}

type Entry struct {
	Ref       reference.Reference
	Labels    map[string]string
	Timestamp int64
}

func (c *Client) QueryMessages(ctx context.Context, opts *QueryOptions) (*QueryResult, error) {
	order := nodev1.Order_ORDER_ASCENDING
	if opts.Descending {
		order = nodev1.Order_ORDER_DESCENDING
	}
	resp, err := c.stub.QueryMessages(ctx, &nodev1.QueryMessagesRequest{
		Labels:     opts.Labels,
		Limit:      int32(opts.Limit),
		Expression: opts.Expression,
		Cursor:     opts.Cursor,
		Order:      order,
	})
	if err != nil {
		return nil, err
	}
	entries := make([]*Entry, len(resp.Entries))
	for i, e := range resp.Entries {
		var ref reference.Reference
		copy(ref[:], e.Reference)
		entries[i] = &Entry{
			Ref:       ref,
			Labels:    e.Labels,
			Timestamp: e.Timestamp,
		}
	}
	return &QueryResult{
		Entries:    entries,
		NextCursor: resp.NextCursor,
		HasMore:    resp.HasMore,
	}, nil
}

func (c *Client) SubscribeMessages(ctx context.Context, expression string, labels map[string]string) (<-chan *Entry, <-chan error, error) {
	stream, err := c.stub.SubscribeMessages(ctx, &nodev1.SubscribeMessagesRequest{
		Labels:     labels,
		Expression: expression,
	})
	if err != nil {
		return nil, nil, err
	}

	entries := make(chan *Entry)
	errs := make(chan error, 1)

	go func() {
		defer close(entries)
		defer close(errs)
		for {
			resp, err := stream.Recv()
			if err != nil {
				errs <- err
				return
			}
			e := resp.Entry
			var ref reference.Reference
			copy(ref[:], e.Reference)
			select {
			case entries <- &Entry{
				Ref:       ref,
				Labels:    e.Labels,
				Timestamp: e.Timestamp,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return entries, errs, nil
}
