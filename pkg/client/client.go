package client

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	relayv1 "github.com/gezibash/arc-node/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// DefaultRelayAddr is the default relay address.
const DefaultRelayAddr = "localhost:50051"

// Metadata keys for authentication (must match internal/relay/interceptor.go).
const (
	MetaFrom      = "arc-from"
	MetaSignature = "arc-signature"
	MetaTimestamp = "arc-timestamp"
)

var (
	ErrClosed       = errors.New("client closed")
	ErrNotConnected = errors.New("not connected")
)

// Client connects to a relay and provides send/receive operations.
type Client struct {
	conn    *grpc.ClientConn
	stream  relayv1.RelayService_ConnectClient
	keypair *identity.Keypair
	sender  identity.PublicKey

	recvCh chan *relayv1.ServerFrame
	errCh  chan error

	mu     sync.Mutex
	closed atomic.Bool
}

// Dial connects to a relay at the given address using the provided keypair.
func Dial(ctx context.Context, addr string, kp *identity.Keypair) (*Client, error) {
	if addr == "" {
		addr = DefaultRelayAddr
	}

	// Create gRPC connection
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	// Create auth metadata
	ts := time.Now().UnixMilli()
	from := kp.PublicKey()

	// Sign auth message (from || timestamp)
	authMsg := message.Message{
		From:      from,
		Timestamp: ts,
	}
	if err := message.Sign(&authMsg, kp); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("sign auth: %w", err)
	}

	md := metadata.New(map[string]string{
		MetaFrom:      hex.EncodeToString(from[:]),
		MetaSignature: hex.EncodeToString(authMsg.Signature[:]),
		MetaTimestamp: strconv.FormatInt(ts, 10),
	})
	ctx = metadata.NewOutgoingContext(ctx, md)

	// Connect stream
	client := relayv1.NewRelayServiceClient(conn)
	stream, err := client.Connect(ctx)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("connect: %w", err)
	}

	c := &Client{
		conn:    conn,
		stream:  stream,
		keypair: kp,
		sender:  from,
		recvCh:  make(chan *relayv1.ServerFrame, 64),
		errCh:   make(chan error, 1),
	}

	// Start receiver goroutine
	go c.receiveLoop()

	return c, nil
}

// Sender returns the client's public key.
func (c *Client) Sender() identity.PublicKey {
	return c.sender
}

// Close closes the connection.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stream != nil {
		_ = c.stream.CloseSend()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// receiveLoop reads frames from the stream and dispatches them.
func (c *Client) receiveLoop() {
	for {
		frame, err := c.stream.Recv()
		if err != nil {
			if !c.closed.Load() {
				select {
				case c.errCh <- err:
				default:
				}
			}
			close(c.recvCh)
			return
		}

		select {
		case c.recvCh <- frame:
		default:
			// Drop if buffer full
		}
	}
}
