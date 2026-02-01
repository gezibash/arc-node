package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/internal/envelope"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn     *grpc.ClientConn
	stub     nodev1.NodeServiceClient
	nodeInfo *nodeInfoState

	muxMu sync.Mutex
	mux   *channelMux
}

type clientConfig struct {
	kp      *identity.Keypair
	nodeKey *identity.PublicKey
}

// NodeInfo describes the remote node as observed by the client.
type NodeInfo struct {
	PublicKey      identity.PublicKey
	ServiceName    string
	ServiceVersion string
}

type nodeInfoState struct {
	mu   sync.RWMutex
	info NodeInfo
	ok   bool
}

func (s *nodeInfoState) set(info NodeInfo) {
	s.mu.Lock()
	s.info = info
	s.ok = true
	s.mu.Unlock()
}

func (s *nodeInfoState) get() (NodeInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.ok {
		return NodeInfo{}, false
	}
	return s.info, true
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

	infoState := &nodeInfoState{}
	if cfg.nodeKey != nil {
		infoState.set(NodeInfo{PublicKey: *cfg.nodeKey})
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	if cfg.kp != nil {
		dialOpts = append(dialOpts,
			grpc.WithStreamInterceptor(clientStreamInterceptor(cfg.kp, infoState)),
		)
	}

	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &Client{
		conn:     conn,
		stub:     nodev1.NewNodeServiceClient(conn),
		nodeInfo: infoState,
	}, nil
}

func clientStreamInterceptor(kp *identity.Keypair, infoState *nodeInfoState) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, callOpts ...grpc.CallOption) (grpc.ClientStream, error) {
		var toKey identity.PublicKey
		if infoState != nil {
			if info, ok := infoState.get(); ok {
				toKey = info.PublicKey
			}
		}

		// Seal envelope with empty payload for stream open.
		// toKey may be zero if node key is unknown (e.g. federation bootstrap).
		env, err := envelope.Seal(kp, toKey, []byte{}, method, kp.PublicKey(), 0, nil, &nodev1.Dimensions{Persistence: nodev1.Persistence_PERSISTENCE_DURABLE})
		if err != nil {
			return nil, fmt.Errorf("seal stream envelope: %w", err)
		}

		ctx = envelope.InjectOutgoing(ctx, env, *env.Message.Signature)

		return streamer(ctx, desc, cc, method, callOpts...)
	}
}

func (c *Client) channel(ctx context.Context) (*channelMux, error) {
	_ = ctx
	c.muxMu.Lock()
	defer c.muxMu.Unlock()

	if c.mux != nil {
		return c.mux, nil
	}

	stream, err := c.stub.Channel(context.Background())
	if err != nil {
		return nil, err
	}
	c.mux = newChannelMux(stream)
	return c.mux, nil
}

func (c *Client) Close() error {
	if c.mux != nil {
		c.mux.close()
	}
	return c.conn.Close()
}

// NodeInfo returns the last observed node info from response envelopes.
func (c *Client) NodeInfo() (NodeInfo, bool) {
	if c == nil || c.nodeInfo == nil {
		return NodeInfo{}, false
	}
	return c.nodeInfo.get()
}

// NodeKey returns the last observed node public key.
func (c *Client) NodeKey() (identity.PublicKey, bool) {
	info, ok := c.NodeInfo()
	if !ok || info.PublicKey == (identity.PublicKey{}) {
		return identity.PublicKey{}, false
	}
	return info.PublicKey, true
}

// Ping measures round-trip latency to the node.
func (c *Client) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	_, err := c.QueryMessages(ctx, &QueryOptions{
		Expression: "false",
		Limit:      1,
	})
	return time.Since(start), err
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
	mux, err := c.channel(ctx)
	if err != nil {
		return nil, fmt.Errorf("channel unavailable: %w", err)
	}
	resp, err := mux.roundTrip(ctx, &nodev1.ClientFrame{
		Frame: &nodev1.ClientFrame_ResolveGet{ResolveGet: &nodev1.ResolveGetFrame{Prefix: prefix}},
	})
	if err != nil {
		return nil, err
	}
	rf := resp.Frame.(*nodev1.ServerFrame_ResolveGetResponse).ResolveGetResponse
	var ref reference.Reference
	copy(ref[:], rf.Reference)
	return &GetResult{
		Kind:      GetKind(rf.Kind),
		Ref:       ref,
		Data:      rf.Data,
		Labels:    rf.Labels,
		Timestamp: rf.Timestamp,
	}, nil
}

func (c *Client) PutContent(ctx context.Context, data []byte) (reference.Reference, error) {
	mux, err := c.channel(ctx)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("channel unavailable: %w", err)
	}
	resp, err := mux.roundTrip(ctx, &nodev1.ClientFrame{
		Frame: &nodev1.ClientFrame_Put{Put: &nodev1.PutFrame{Data: data}},
	})
	if err != nil {
		return reference.Reference{}, err
	}
	receipt := resp.Frame.(*nodev1.ServerFrame_Receipt).Receipt
	var ref reference.Reference
	copy(ref[:], receipt.Reference)
	return ref, nil
}

func (c *Client) GetContent(ctx context.Context, ref reference.Reference) ([]byte, error) {
	mux, err := c.channel(ctx)
	if err != nil {
		return nil, fmt.Errorf("channel unavailable: %w", err)
	}
	resp, err := mux.roundTrip(ctx, &nodev1.ClientFrame{
		Frame: &nodev1.ClientFrame_Get{Get: &nodev1.GetFrame{Reference: ref[:]}},
	})
	if err != nil {
		return nil, err
	}
	return resp.Frame.(*nodev1.ServerFrame_Response).Response.Data, nil
}

func (c *Client) SendMessage(ctx context.Context, msg message.Message, labels map[string]string) (reference.Reference, error) {
	canonical, err := message.CanonicalBytes(msg)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("canonical bytes: %w", err)
	}
	mux, err := c.channel(ctx)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("channel unavailable: %w", err)
	}
	resp, err := mux.roundTrip(ctx, &nodev1.ClientFrame{
		Frame: &nodev1.ClientFrame_Publish{Publish: &nodev1.PublishFrame{
			Message: canonical,
			Labels:  labels,
		}},
	})
	if err != nil {
		return reference.Reference{}, err
	}
	receipt := resp.Frame.(*nodev1.ServerFrame_Receipt).Receipt
	var ref reference.Reference
	copy(ref[:], receipt.Reference)
	return ref, nil
}

// SendMessageWithDimensions sends a signed message with explicit dimensions.
func (c *Client) SendMessageWithDimensions(ctx context.Context, msg message.Message, labels map[string]string, dims *nodev1.Dimensions) (reference.Reference, error) {
	canonical, err := message.CanonicalBytes(msg)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("canonical bytes: %w", err)
	}
	mux, err := c.channel(ctx)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("channel unavailable: %w", err)
	}
	resp, err := mux.roundTrip(ctx, &nodev1.ClientFrame{
		Frame: &nodev1.ClientFrame_Publish{Publish: &nodev1.PublishFrame{
			Message:    canonical,
			Labels:     labels,
			Dimensions: dims,
		}},
	})
	if err != nil {
		return reference.Reference{}, err
	}
	receipt := resp.Frame.(*nodev1.ServerFrame_Receipt).Receipt
	var ref reference.Reference
	copy(ref[:], receipt.Reference)
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
	Ref        reference.Reference
	Labels     map[string]string
	Timestamp  int64
	Dimensions *EntryDimensions // nil if not provided by server
}

// EntryDimensions carries dimension metadata from the originating node.
type EntryDimensions struct {
	Visibility       int32
	Persistence      int32
	Delivery         int32
	Pattern          int32
	Affinity         int32
	AffinityKey      string
	TtlMs            int64
	Ordering         int32
	DedupMode        int32
	IdempotencyKey   string
	DeliveryComplete int32
	CompleteN        int32
	Priority         int32
	MaxRedelivery    int32
	AckTimeoutMs     int64
	Correlation      string
}

func (c *Client) QueryMessages(ctx context.Context, opts *QueryOptions) (*QueryResult, error) {
	order := nodev1.Order_ORDER_ASCENDING
	if opts.Descending {
		order = nodev1.Order_ORDER_DESCENDING
	}
	mux, err := c.channel(ctx)
	if err != nil {
		return nil, fmt.Errorf("channel unavailable: %w", err)
	}
	resp, err := mux.roundTrip(ctx, &nodev1.ClientFrame{
		Frame: &nodev1.ClientFrame_Query{Query: &nodev1.QueryFrame{
			Labels:     opts.Labels,
			Limit:      int32(opts.Limit),
			Expression: opts.Expression,
			Cursor:     opts.Cursor,
			Order:      order,
		}},
	})
	if err != nil {
		return nil, err
	}
	rf := resp.Frame.(*nodev1.ServerFrame_Response).Response
	return protoToQueryResult(rf.Entries, rf.NextCursor, rf.HasMore), nil
}

func protoToEntryDimensions(d *nodev1.Dimensions) *EntryDimensions {
	if d == nil {
		return nil
	}
	return &EntryDimensions{
		Visibility:       int32(d.Visibility),
		Persistence:      int32(d.Persistence),
		Delivery:         int32(d.Delivery),
		Pattern:          int32(d.Pattern),
		Affinity:         int32(d.Affinity),
		AffinityKey:      d.AffinityKey,
		TtlMs:            d.TtlMs,
		Ordering:         int32(d.Ordering),
		DedupMode:        int32(d.Dedup),
		IdempotencyKey:   d.IdempotencyKey,
		DeliveryComplete: int32(d.Complete),
		CompleteN:        d.CompleteN,
		Priority:         int32(d.Priority),
		MaxRedelivery:    int32(d.MaxRedelivery),
		AckTimeoutMs:     d.AckTimeoutMs,
		Correlation:      d.Correlation,
	}
}

func protoToEntry(e *nodev1.IndexEntry) *Entry {
	var ref reference.Reference
	copy(ref[:], e.Reference)
	return &Entry{
		Ref:        ref,
		Labels:     e.Labels,
		Timestamp:  e.Timestamp,
		Dimensions: protoToEntryDimensions(e.Dimensions),
	}
}

func protoToQueryResult(entries []*nodev1.IndexEntry, nextCursor string, hasMore bool) *QueryResult {
	out := make([]*Entry, len(entries))
	for i, e := range entries {
		out[i] = protoToEntry(e)
	}
	return &QueryResult{
		Entries:    out,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}
}

func (c *Client) SubscribeMessages(ctx context.Context, expression string, labels map[string]string) (<-chan *Entry, <-chan error, error) {
	mux, err := c.channel(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("channel unavailable: %w", err)
	}
	return c.subscribeViaChannel(ctx, mux, expression, labels)
}

// SubscribeChannel subscribes using only the Channel bidi stream.
func (c *Client) SubscribeChannel(ctx context.Context, expression string, labels map[string]string) (<-chan *Entry, <-chan error, error) {
	mux, err := c.channel(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("channel unavailable: %w", err)
	}
	return c.subscribeViaChannel(ctx, mux, expression, labels)
}

func (c *Client) subscribeViaChannel(ctx context.Context, mux *channelMux, expression string, labels map[string]string) (<-chan *Entry, <-chan error, error) {
	channel := "default"

	// Register delivery channel before sending subscribe frame.
	deliveries := mux.subscribe(channel)

	_, err := mux.roundTrip(ctx, &nodev1.ClientFrame{
		Frame: &nodev1.ClientFrame_Subscribe{Subscribe: &nodev1.SubscribeFrame{
			Channel:    channel,
			Labels:     labels,
			Expression: expression,
		}},
	})
	if err != nil {
		mux.unsubscribe(channel)
		return nil, nil, err
	}

	entries := make(chan *Entry)
	errs := make(chan error, 1)

	go func() {
		defer close(entries)
		defer close(errs)
		for {
			select {
			case <-ctx.Done():
				// Send unsubscribe (best effort).
				mux.roundTrip(context.Background(), &nodev1.ClientFrame{
					Frame: &nodev1.ClientFrame_Unsubscribe{Unsubscribe: &nodev1.UnsubscribeFrame{Channel: channel}},
				})
				mux.unsubscribe(channel)
				return
			case df, ok := <-deliveries:
				if !ok {
					return
				}
				entry := protoToEntry(df.Entry)
				select {
				case entries <- entry:
					// Auto-ack if delivery_id is set (at-least-once).
					if df.DeliveryId > 0 {
						mux.send(&nodev1.ClientFrame{
							Frame: &nodev1.ClientFrame_Ack{Ack: &nodev1.AckFrame{
								DeliveryId: df.DeliveryId,
							}},
						})
					}
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return entries, errs, nil
}

// PeerDirection indicates whether a peer is inbound or outbound.
type PeerDirection int

const (
	PeerDirectionOutbound PeerDirection = 0 // we subscribe to them
	PeerDirectionInbound  PeerDirection = 1 // they subscribe to us
)

// PeerInfo describes an active peer connection.
type PeerInfo struct {
	Address           string            `json:"address,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	BytesReceived     int64             `json:"bytes_received"`
	EntriesReplicated int64             `json:"entries_replicated"`
	EntriesSent       int64             `json:"entries_sent"`
	StartedAt         int64             `json:"started_at"`
	Direction         PeerDirection     `json:"direction"`
	PublicKey         []byte            `json:"public_key,omitempty"`
}

func (c *Client) ListPeers(ctx context.Context) ([]PeerInfo, error) {
	mux, err := c.channel(ctx)
	if err != nil {
		return nil, fmt.Errorf("channel unavailable: %w", err)
	}
	resp, err := mux.roundTrip(ctx, &nodev1.ClientFrame{
		Frame: &nodev1.ClientFrame_ListPeers{ListPeers: &nodev1.ListPeersFrame{}},
	})
	if err != nil {
		return nil, err
	}
	lpr := resp.Frame.(*nodev1.ServerFrame_ListPeersResponse).ListPeersResponse
	peers := make([]PeerInfo, len(lpr.Peers))
	for i, p := range lpr.Peers {
		peers[i] = PeerInfo{
			Address:           p.Address,
			Labels:            p.Labels,
			BytesReceived:     p.BytesReceived,
			EntriesReplicated: p.EntriesReplicated,
			EntriesSent:       p.EntriesSent,
			StartedAt:         p.StartedAt,
			Direction:         PeerDirection(p.Direction),
			PublicKey:         p.PublicKey,
		}
	}
	return peers, nil
}

type FederateResult struct {
	Status  string
	Message string
}

func (c *Client) Federate(ctx context.Context, peer string, labels map[string]string) (*FederateResult, error) {
	mux, err := c.channel(ctx)
	if err != nil {
		return nil, fmt.Errorf("channel unavailable: %w", err)
	}
	resp, err := mux.roundTrip(ctx, &nodev1.ClientFrame{
		Frame: &nodev1.ClientFrame_Federate{Federate: &nodev1.FederateFrame{
			Peer:   peer,
			Labels: labels,
		}},
	})
	if err != nil {
		return nil, err
	}
	fr := resp.Frame.(*nodev1.ServerFrame_FederateResponse).FederateResponse
	return &FederateResult{
		Status:  fr.Status,
		Message: fr.Message,
	}, nil
}

// Seek repositions an active subscription's cursor.
func (c *Client) Seek(ctx context.Context, channel string, timestamp int64) error {
	mux, err := c.channel(ctx)
	if err != nil {
		return fmt.Errorf("channel unavailable: %w", err)
	}
	_, err = mux.roundTrip(ctx, &nodev1.ClientFrame{
		Frame: &nodev1.ClientFrame_Seek{Seek: &nodev1.SeekFrame{
			Channel:   channel,
			Timestamp: timestamp,
		}},
	})
	return err
}
