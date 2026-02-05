package relay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RemoteDiscovery provides cross-relay capability discovery via gossip.
type RemoteDiscovery interface {
	DiscoverRemote(filter map[string]string, limit int) []*relayv1.ProviderInfo
	DiscoverRemoteCEL(expr string, limit int) []*relayv1.ProviderInfo
}

// GossipAdmin provides gossip cluster management operations.
type GossipAdmin interface {
	Join(peers []string) (int, error)
	Members() []GossipMemberInfo
	Leave() error
}

// GossipMemberInfo describes a cluster member (decoupled from gossip package).
type GossipMemberInfo struct {
	Name        string
	Addr        string
	GRPCAddr    string
	Status      string
	Pubkey      string
	Connections uint32
	Uptime      uint64
	LatencyNs   int64
	IsLocal     bool
}

// RemoteForwarder forwards envelopes to remote relays when local routing fails.
type RemoteForwarder interface {
	Forward(ctx context.Context, env *relayv1.Envelope) (delivered int, err error)
	// ForwardToRelay forwards an envelope to a specific relay by name.
	// Used for response routing when we know the target relay from the return path cache.
	ForwardToRelay(ctx context.Context, env *relayv1.Envelope, relayName string) (delivered int, err error)
}

// Observer receives notifications about local relay state changes.
// Used by gossip to propagate state to the cluster.
type Observer interface {
	OnConnected(pubkey identity.PublicKey)
	OnSubscribe(pubkey identity.PublicKey, subID string, labels map[string]any, name string)
	OnUnsubscribe(pubkey identity.PublicKey, subID string)
	OnSubscriberRemoved(pubkey identity.PublicKey)
	OnNameRegistered(name string, pubkey identity.PublicKey)
	OnLatencyMeasured(pubkey identity.PublicKey, latency time.Duration)
	OnStateUpdated(pubkey identity.PublicKey, subID string, state map[string]any)
}

// Service implements the RelayService gRPC interface.
type Service struct {
	relayv1.UnimplementedRelayServiceServer
	relay           *Relay
	bufferSize      int
	remoteDiscovery RemoteDiscovery
	remoteForwarder RemoteForwarder
	observer        Observer
	gossipAdmin     GossipAdmin
	returnPaths     *ReturnPathCache // caches return paths for cross-relay responses
}

// SetRemoteDiscovery sets the remote discovery provider (gossip).
func (s *Service) SetRemoteDiscovery(rd RemoteDiscovery) {
	s.remoteDiscovery = rd
}

// SetObserver sets the observer for state change notifications.
func (s *Service) SetObserver(obs Observer) {
	s.observer = obs
}

// SetRemoteForwarder sets the remote forwarder for cross-relay envelope routing.
func (s *Service) SetRemoteForwarder(f RemoteForwarder) {
	s.remoteForwarder = f
}

// SetGossipAdmin sets the gossip admin for cluster management RPCs.
func (s *Service) SetGossipAdmin(ga GossipAdmin) {
	s.gossipAdmin = ga
}

// NewService creates a new relay service.
func NewService(relay *Relay, bufferSize int) *Service {
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	return &Service{
		relay:       relay,
		bufferSize:  bufferSize,
		returnPaths: NewReturnPathCache(5 * time.Minute),
	}
}

// Connect handles a bidirectional stream connection.
func (s *Service) Connect(stream relayv1.RelayService_ConnectServer) error {
	ctx := stream.Context()

	// Get sender from context (set by interceptor)
	sender, ok := SenderFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "no sender in context")
	}

	// Create subscriber
	id := uuid.New().String()
	sub := NewSubscriber(id, stream, sender, s.bufferSize)

	slog.Info("client connected",
		"component", "service",
		"subscriber_id", id,
		"sender", identity.EncodePublicKey(sender),
	)

	// Register with table
	s.relay.Table().Add(sub)
	if s.observer != nil {
		s.observer.OnConnected(sender)
	}
	defer func() {
		slog.Info("client disconnected",
			"component", "service",
			"subscriber_id", id,
			"subscriber", sub.DisplayName(),
		)
		if s.observer != nil {
			s.observer.OnSubscriberRemoved(sub.Sender())
		}
		sub.Close()
		s.relay.Table().Remove(id)
	}()

	// Start sender goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- sub.Run()
	}()

	// Receive loop
	for {
		frame, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		sub.TouchRecv()

		if err := s.handleFrame(ctx, sub, frame); err != nil {
			// Send error frame but don't disconnect
			errFrame := WrapError(
				int32(status.Code(err)),
				err.Error(),
				"",
				"",
				false,
			)
			sub.Send(errFrame)
		}
	}
}

func (s *Service) handleFrame(ctx context.Context, sub *Subscriber, frame *relayv1.ClientFrame) error {
	switch f := frame.GetFrame().(type) {
	case *relayv1.ClientFrame_Send:
		return s.handleSend(ctx, sub, f.Send)
	case *relayv1.ClientFrame_Subscribe:
		return s.handleSubscribe(ctx, sub, f.Subscribe)
	case *relayv1.ClientFrame_Unsubscribe:
		return s.handleUnsubscribe(ctx, sub, f.Unsubscribe)
	case *relayv1.ClientFrame_RegisterName:
		return s.handleRegisterName(ctx, sub, f.RegisterName)
	case *relayv1.ClientFrame_Ping:
		return s.handlePing(ctx, sub, f.Ping)
	case *relayv1.ClientFrame_Discover:
		return s.handleDiscover(ctx, sub, f.Discover)
	case *relayv1.ClientFrame_UpdateLabels:
		return s.handleUpdateLabels(ctx, sub, f.UpdateLabels)
	case *relayv1.ClientFrame_UpdateState:
		return s.handleUpdateState(ctx, sub, f.UpdateState)
	default:
		return status.Error(codes.Unimplemented, "unknown frame type")
	}
}

func (s *Service) handleSend(ctx context.Context, sub *Subscriber, send *relayv1.SendFrame) error {
	env := send.GetEnvelope()
	if env == nil {
		return status.Error(codes.InvalidArgument, "missing envelope")
	}

	delivered, err := s.relay.Route(env)

	// Try cross-relay forwarding when local routing fails
	if errors.Is(err, ErrNoRoute) && s.remoteForwarder != nil {
		// First try gossip-based forwarding (for capability providers)
		fwdDelivered, fwdErr := s.remoteForwarder.Forward(ctx, env)
		if fwdErr == nil && fwdDelivered > 0 {
			delivered = fwdDelivered
			err = nil
			slog.Debug("envelope forwarded to remote relay",
				"component", "service",
				"subscriber_id", sub.ID(),
				"delivered", delivered,
				"labels", env.GetLabels(),
			)
		} else if to := env.GetLabels()["to"]; to != "" {
			// Gossip forwarding failed for addressed envelope — check return path cache
			// This handles responses going back to clients on other relays
			if relayName, ok := s.returnPaths.Get(to); ok {
				fwdDelivered, fwdErr = s.remoteForwarder.ForwardToRelay(ctx, env, relayName)
				if fwdErr == nil && fwdDelivered > 0 {
					delivered = fwdDelivered
					err = nil
					slog.Debug("envelope forwarded via return path",
						"component", "service",
						"subscriber_id", sub.ID(),
						"to", to[:min(32, len(to))],
						"relay", relayName,
						"delivered", delivered,
					)
				}
			}
		}
	}

	slog.Debug("envelope routed",
		"component", "service",
		"subscriber_id", sub.ID(),
		"delivered", delivered,
		"labels", env.GetLabels(),
	)

	// Send receipt (signed by relay)
	var receipt *relayv1.Receipt
	if err != nil {
		switch {
		case errors.Is(err, ErrNoRoute):
			receipt = NewNACK(env.GetRef(), send.GetCorrelation(), ReasonNoRoute)
		default:
			return err
		}
	} else {
		receipt = NewACK(env.GetRef(), send.GetCorrelation(), delivered)
	}

	// Sign the receipt with relay's keypair
	SignReceipt(receipt, s.relay.Signer())

	sub.Send(WrapReceipt(receipt))
	return nil
}

func (s *Service) handleSubscribe(_ context.Context, sub *Subscriber, subscribe *relayv1.SubscribeFrame) error {
	id := subscribe.GetId()
	if id == "" {
		return status.Error(codes.InvalidArgument, "subscription ID required")
	}

	// Build typed labels: typed_labels first, fall back to string labels
	labels := mergeProtoLabels(subscribe.GetLabels(), subscribe.GetTypedLabels())
	sub.Subscribe(id, labels)

	if s.observer != nil {
		s.observer.OnSubscribe(sub.Sender(), id, labels, sub.Name())
	}

	slog.Debug("subscription added",
		"subscriber", sub.ID(),
		"subscription", id,
		"labels", labels,
	)
	return nil
}

func (s *Service) handleUnsubscribe(_ context.Context, sub *Subscriber, unsubscribe *relayv1.UnsubscribeFrame) error {
	id := unsubscribe.GetId()
	sub.Unsubscribe(id)

	if s.observer != nil {
		s.observer.OnUnsubscribe(sub.Sender(), id)
	}
	return nil
}

func (s *Service) handleRegisterName(_ context.Context, sub *Subscriber, reg *relayv1.RegisterNameFrame) error {
	name := reg.GetName()
	if name == "" {
		return status.Error(codes.InvalidArgument, "name required")
	}

	err := s.relay.Table().RegisterName(sub.ID(), name)
	if errors.Is(err, ErrNameTaken) {
		return status.Error(codes.AlreadyExists, "name already registered")
	}
	if err == nil {
		if s.observer != nil {
			s.observer.OnNameRegistered(name, sub.Sender())
		}
		slog.Info("name registered",
			"component", "service",
			"subscriber_id", sub.ID(),
			"name", name,
		)
	}
	return err
}

func (s *Service) handlePing(_ context.Context, sub *Subscriber, ping *relayv1.PingFrame) error {
	pong := WrapPong(ping.GetNonce(), time.Now().UnixNano())
	sub.Send(pong)

	// Client reports its measured RTT — store it as subscriber latency.
	if rtt := ping.GetMeasuredLatencyNs(); rtt > 0 {
		sub.SetLatency(time.Duration(rtt))
		if s.observer != nil {
			s.observer.OnLatencyMeasured(sub.Sender(), time.Duration(rtt))
		}
	}

	return nil
}

func (s *Service) handleDiscover(_ context.Context, sub *Subscriber, discover *relayv1.DiscoverFrame) error {
	expr := discover.GetExpression()
	filter := discover.GetFilter()
	limit := int(discover.GetLimit())
	if limit <= 0 {
		limit = 100 // default limit
	}

	var results []DiscoveryResult
	var total int
	var err error

	if expr != "" {
		results, total, err = s.relay.Table().DiscoverCEL(expr, limit)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid expression: %v", err)
		}
	} else {
		results, total = s.relay.Table().Discover(filter, limit)
	}

	slog.Debug("local discovery",
		"component", "service",
		"filter", filter,
		"expression", expr,
		"local_results", len(results),
		"local_total", total,
		"table_count", s.relay.Table().Count(),
	)

	// Build provider info list
	providers := make([]*relayv1.ProviderInfo, 0, len(results))
	relayPubkey := s.relay.Signer().PublicKey()
	now := time.Now().UnixNano()

	for _, r := range results {
		sender := r.Subscriber.Sender()
		providers = append(providers, &relayv1.ProviderInfo{
			Pubkey:         []byte(identity.EncodePublicKey(sender)),
			Name:           r.Subscriber.Name(),
			Labels:         anyToStringMap(r.Labels),
			TypedLabels:    anyToProtoMap(r.Labels),
			State:          anyToProtoMap(r.State),
			SubscriptionId: r.SubscriptionID,
			RelayPubkey:    []byte(identity.EncodePublicKey(relayPubkey)),
			Petname:        r.Subscriber.DisplayName(),
			LatencyNs:      int64(r.Subscriber.Latency()),
			LastSeenNs:     r.Subscriber.LastRecv().UnixNano(),
			ConnectedNs:    now - r.Subscriber.ConnectedAt().UnixNano(),
		})
	}

	// Append remote discovery results if available
	if s.remoteDiscovery != nil {
		remaining := limit - len(providers)
		if remaining <= 0 {
			remaining = limit
		}
		slog.Debug("calling remote discovery",
			"component", "service",
			"filter", filter,
			"remaining", remaining,
			"local_providers", len(providers),
		)
		var remote []*relayv1.ProviderInfo
		if expr != "" {
			remote = s.remoteDiscovery.DiscoverRemoteCEL(expr, remaining)
		} else {
			remote = s.remoteDiscovery.DiscoverRemote(filter, remaining)
		}
		slog.Debug("remote discovery returned",
			"component", "service",
			"remote_count", len(remote),
		)
		providers = append(providers, remote...)
		total += len(remote)
	} else {
		slog.Debug("no remote discovery configured", "component", "service")
	}

	hasMore := total > len(providers)
	frame := WrapDiscoverResult(discover.GetCorrelation(), providers, int32(total), hasMore)
	sub.Send(frame)

	slog.Debug("discovery completed",
		"component", "service",
		"subscriber_id", sub.ID(),
		"filter", filter,
		"expression", expr,
		"results", len(providers),
		"total", total,
	)
	return nil
}

func (s *Service) handleUpdateLabels(_ context.Context, sub *Subscriber, update *relayv1.UpdateLabelsFrame) error {
	id := update.GetId()
	if id == "" {
		return status.Error(codes.InvalidArgument, "subscription ID required")
	}

	merge := mergeProtoLabels(update.GetLabels(), update.GetTypedLabels())
	if !sub.UpdateLabels(id, merge, update.GetRemove()) {
		return status.Error(codes.NotFound, "subscription not found")
	}

	slog.Debug("labels updated",
		"component", "service",
		"subscriber_id", sub.ID(),
		"subscription_id", id,
		"merged", len(merge),
		"removed", len(update.GetRemove()),
	)
	return nil
}

func (s *Service) handleUpdateState(_ context.Context, sub *Subscriber, update *relayv1.UpdateStateFrame) error {
	id := update.GetId()
	if id == "" {
		return status.Error(codes.InvalidArgument, "subscription ID required")
	}

	state := protoToAnyMap(update.GetState())
	if !sub.UpdateState(id, state, update.GetRemove()) {
		return status.Error(codes.NotFound, "subscription not found")
	}

	if s.observer != nil {
		s.observer.OnStateUpdated(sub.Sender(), id, state)
	}

	slog.Debug("state updated",
		"component", "service",
		"subscriber_id", sub.ID(),
		"subscription_id", id,
		"state_keys", len(state),
		"removed", len(update.GetRemove()),
	)
	return nil
}

// Context key for sender
type senderKey struct{}

// WithSender stores the sender public key in context.
func WithSender(ctx context.Context, sender identity.PublicKey) context.Context {
	return context.WithValue(ctx, senderKey{}, sender)
}

// SenderFromContext retrieves the sender public key from context.
func SenderFromContext(ctx context.Context) (identity.PublicKey, bool) {
	v, ok := ctx.Value(senderKey{}).(identity.PublicKey)
	return v, ok
}

// ForwardEnvelope handles a forwarded envelope from a peer relay.
// Auth is standard keypair auth (same as Connect).
// Routes locally only — never forwards again (1-hop limit).
func (s *Service) ForwardEnvelope(ctx context.Context, req *relayv1.ForwardEnvelopeRequest) (*relayv1.ForwardEnvelopeResponse, error) {
	// Sender is verified by the unary interceptor (same keypair auth as Connect)
	if _, ok := SenderFromContext(ctx); !ok {
		return nil, status.Error(codes.Unauthenticated, "no sender in context")
	}

	env := req.GetEnvelope()
	if env == nil {
		return nil, status.Error(codes.InvalidArgument, "missing envelope")
	}

	// Cache return path: sender pubkey → source relay
	// This allows responses to be routed back to the original client
	sourceRelay := req.GetSourceRelay()
	if sourceRelay != "" && env.GetSender() != nil {
		senderPubkey := string(env.GetSender())
		s.returnPaths.Set(senderPubkey, sourceRelay)
		slog.Debug("cached return path",
			"component", "service",
			"sender", senderPubkey[:min(32, len(senderPubkey))],
			"source_relay", sourceRelay,
		)
	}

	// Route locally only — no forwarding (1-hop limit enforced structurally)
	delivered, err := s.relay.Route(env)

	slog.Debug("forwarded envelope routed locally",
		"component", "service",
		"source_relay", sourceRelay,
		"delivered", delivered,
		"labels", env.GetLabels(),
	)

	if err != nil {
		if errors.Is(err, ErrNoRoute) {
			return &relayv1.ForwardEnvelopeResponse{
				Delivered: 0,
				Reason:    string(ReasonNoRoute),
			}, nil
		}
		return nil, status.Errorf(codes.Internal, "route: %v", err)
	}

	return &relayv1.ForwardEnvelopeResponse{
		Delivered: int32(delivered),
	}, nil
}

// GossipJoin joins additional peers to the gossip cluster.
func (s *Service) GossipJoin(_ context.Context, req *relayv1.GossipJoinRequest) (*relayv1.GossipJoinResponse, error) {
	if s.gossipAdmin == nil {
		return nil, status.Error(codes.Unavailable, "gossip not enabled")
	}
	n, err := s.gossipAdmin.Join(req.GetPeers())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "join: %v", err)
	}
	return &relayv1.GossipJoinResponse{Joined: int32(n)}, nil
}

// GossipMembers lists all members in the gossip cluster.
func (s *Service) GossipMembers(_ context.Context, _ *relayv1.GossipMembersRequest) (*relayv1.GossipMembersResponse, error) {
	if s.gossipAdmin == nil {
		return nil, status.Error(codes.Unavailable, "gossip not enabled")
	}
	members := s.gossipAdmin.Members()
	pbMembers := make([]*relayv1.GossipMember, 0, len(members))
	for _, m := range members {
		pbMembers = append(pbMembers, &relayv1.GossipMember{
			Name:        m.Name,
			Addr:        m.Addr,
			GrpcAddr:    m.GRPCAddr,
			Status:      m.Status,
			Pubkey:      []byte(m.Pubkey),
			Connections: int32(m.Connections),
			UptimeNs:    int64(m.Uptime),
			LatencyNs:   m.LatencyNs,
			IsLocal:     m.IsLocal,
		})
	}
	return &relayv1.GossipMembersResponse{Members: pbMembers}, nil
}

// GossipLeave gracefully leaves the gossip cluster.
func (s *Service) GossipLeave(_ context.Context, _ *relayv1.GossipLeaveRequest) (*relayv1.GossipLeaveResponse, error) {
	if s.gossipAdmin == nil {
		return nil, status.Error(codes.Unavailable, "gossip not enabled")
	}
	if err := s.gossipAdmin.Leave(); err != nil {
		return nil, status.Errorf(codes.Internal, "leave: %v", err)
	}
	return &relayv1.GossipLeaveResponse{}, nil
}
