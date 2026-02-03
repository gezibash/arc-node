package relay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	relayv1 "github.com/gezibash/arc-node/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service implements the RelayService gRPC interface.
type Service struct {
	relayv1.UnimplementedRelayServiceServer
	relay      *Relay
	bufferSize int
}

// NewService creates a new relay service.
func NewService(relay *Relay, bufferSize int) *Service {
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	return &Service{
		relay:      relay,
		bufferSize: bufferSize,
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

	// Register with table
	s.relay.Table().Add(sub)
	defer func() {
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
	case *relayv1.ClientFrame_RegisterCapability:
		return s.handleRegisterCapability(ctx, sub, f.RegisterCapability)
	case *relayv1.ClientFrame_Ping:
		return s.handlePing(ctx, sub, f.Ping)
	default:
		return status.Error(codes.Unimplemented, "unknown frame type")
	}
}

func (s *Service) handleSend(_ context.Context, sub *Subscriber, send *relayv1.SendFrame) error {
	env := send.GetEnvelope()
	if env == nil {
		return status.Error(codes.InvalidArgument, "missing envelope")
	}

	delivered, err := s.relay.Route(env)

	// Send receipt
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

	sub.Send(WrapReceipt(receipt))
	return nil
}

func (s *Service) handleSubscribe(_ context.Context, sub *Subscriber, subscribe *relayv1.SubscribeFrame) error {
	id := subscribe.GetId()
	if id == "" {
		return status.Error(codes.InvalidArgument, "subscription ID required")
	}

	labels := subscribe.GetLabels()
	sub.Subscribe(id, labels)

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
	return err
}

func (s *Service) handleRegisterCapability(_ context.Context, sub *Subscriber, reg *relayv1.RegisterCapabilityFrame) error {
	cap := reg.GetCapability()
	if cap == "" {
		return status.Error(codes.InvalidArgument, "capability required")
	}

	s.relay.Table().RegisterCapability(sub.ID(), cap)
	return nil
}

func (s *Service) handlePing(_ context.Context, sub *Subscriber, ping *relayv1.PingFrame) error {
	pong := WrapPong(ping.GetNonce(), time.Now().UnixNano())
	sub.Send(pong)
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
