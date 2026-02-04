package relay

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
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

const (
	// ClientPingInterval is how often the client pings the relay.
	ClientPingInterval = 10 * time.Second
	// clientPingNonceLen is the nonce length for client pings.
	clientPingNonceLen = 8
)

// Client connects to a relay and provides send/receive operations.
type Client struct {
	conn   *grpc.ClientConn
	stream relayv1.RelayService_ConnectClient
	signer identity.Signer
	sender identity.PublicKey

	// deliverCh receives Deliver frames for Receive().
	deliverCh chan *relayv1.ServerFrame

	// done is closed when receiveLoop exits.
	done chan struct{}

	// streamErr holds the error that caused receiveLoop to exit.
	streamErr error

	// pending maps correlation IDs to channels for request/response frames.
	pendingMu sync.Mutex
	pending   map[string]chan *relayv1.ServerFrame

	// Latency measurement (client → relay)
	latency      atomic.Int64 // RTT in nanoseconds
	pendingPing  atomic.Int64 // send timestamp (unix nanos), 0 = no pending
	pendingNonce atomic.Value // []byte nonce of pending ping

	mu          sync.Mutex
	closed      atomic.Bool
	correlation atomic.Uint64
}

// Dial connects to a relay at the given address using the provided keypair.
func Dial(ctx context.Context, addr string, signer identity.Signer) (*Client, error) {
	if addr == "" {
		addr = DefaultRelayAddr
	}
	if signer == nil {
		return nil, fmt.Errorf("signer required")
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
	from := signer.PublicKey()

	payload := authPayload(from, ts)
	sig, err := signer.Sign(payload)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("sign auth: %w", err)
	}

	md := metadata.New(map[string]string{
		MetaFrom:      identity.EncodePublicKey(from),
		MetaSignature: identity.EncodeSignature(sig),
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
		conn:      conn,
		stream:    stream,
		signer:    signer,
		sender:    from,
		deliverCh: make(chan *relayv1.ServerFrame, 64),
		done:      make(chan struct{}),
		pending:   make(map[string]chan *relayv1.ServerFrame),
	}

	// Start receiver goroutine
	go c.receiveLoop()

	// Start background pinger for latency measurement
	go c.pingLoop()

	return c, nil
}

// Sender returns the client's public key.
func (c *Client) Sender() identity.PublicKey {
	return c.sender
}

// Latency returns the last measured RTT to the relay.
func (c *Client) Latency() time.Duration {
	return time.Duration(c.latency.Load())
}

// Ping sends a ping to the relay for latency measurement.
func (c *Client) Ping() error {
	if c.closed.Load() {
		return ErrClosed
	}

	nonce := make([]byte, clientPingNonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("generate nonce: %w", err)
	}

	c.pendingNonce.Store(nonce)
	c.pendingPing.Store(time.Now().UnixNano())

	frame := &relayv1.ClientFrame{
		Frame: &relayv1.ClientFrame_Ping{
			Ping: &relayv1.PingFrame{
				Nonce:             nonce,
				MeasuredLatencyNs: c.latency.Load(),
			},
		},
	}

	c.mu.Lock()
	err := c.stream.Send(frame)
	c.mu.Unlock()
	return err
}

// completePing calculates and stores latency from a pong response.
func (c *Client) completePing(nonce []byte) {
	sentAt := c.pendingPing.Swap(0)
	if sentAt == 0 {
		return
	}

	// Verify nonce matches
	pending, _ := c.pendingNonce.Load().([]byte)
	if len(nonce) != len(pending) {
		return
	}
	for i := range nonce {
		if nonce[i] != pending[i] {
			return
		}
	}

	rtt := time.Now().UnixNano() - sentAt
	c.latency.Store(rtt)
}

// pingLoop periodically pings the relay.
// Fires immediately on start, then every ClientPingInterval.
func (c *Client) pingLoop() {
	// Initial ping — measure before any discover calls.
	_ = c.Ping()

	ticker := time.NewTicker(ClientPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if c.closed.Load() {
				return
			}
			_ = c.Ping()
		}
	}
}

// nextCorrelation returns a unique correlation ID.
func (c *Client) nextCorrelation() uint64 {
	return c.correlation.Add(1)
}

// registerPending creates a buffered(1) channel for the given correlation ID.
// The caller must call deregisterPending when done.
func (c *Client) registerPending(correlation string) chan *relayv1.ServerFrame {
	ch := make(chan *relayv1.ServerFrame, 1)
	c.pendingMu.Lock()
	c.pending[correlation] = ch
	c.pendingMu.Unlock()
	return ch
}

// deregisterPending removes the pending channel for the given correlation ID.
func (c *Client) deregisterPending(correlation string) {
	c.pendingMu.Lock()
	delete(c.pending, correlation)
	c.pendingMu.Unlock()
}

// dispatch sends a frame to the pending channel for the given correlation ID.
func (c *Client) dispatch(correlation string, frame *relayv1.ServerFrame) {
	c.pendingMu.Lock()
	ch, ok := c.pending[correlation]
	c.pendingMu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- frame:
	default:
	}
}

// cancelAllPending closes all pending channels to wake blocked readers.
func (c *Client) cancelAllPending() {
	c.pendingMu.Lock()
	for id, ch := range c.pending {
		close(ch)
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()
}

// streamError returns the error that caused receiveLoop to exit, or ErrClosed.
func (c *Client) streamError() error {
	if c.streamErr != nil {
		return c.streamErr
	}
	return ErrClosed
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

// receiveLoop reads frames from the stream and demuxes them by type.
func (c *Client) receiveLoop() {
	defer func() {
		c.cancelAllPending()
		close(c.deliverCh)
		close(c.done)
	}()

	for {
		frame, err := c.stream.Recv()
		if err != nil {
			if !c.closed.Load() {
				c.streamErr = err
			}
			return
		}

		switch f := frame.GetFrame().(type) {
		case *relayv1.ServerFrame_Deliver:
			select {
			case c.deliverCh <- frame:
			default:
				// Drop if buffer full
			}

		case *relayv1.ServerFrame_Receipt:
			corr := f.Receipt.GetReceipt().GetCorrelation()
			if corr != "" {
				c.dispatch(corr, frame)
			}

		case *relayv1.ServerFrame_DiscoverResult:
			corr := f.DiscoverResult.GetCorrelation()
			if corr != "" {
				c.dispatch(corr, frame)
			}

		case *relayv1.ServerFrame_Error:
			corr := f.Error.GetCorrelation()
			if corr != "" {
				c.dispatch(corr, frame)
			}
			// Errors without correlation are dropped (no global error channel)

		case *relayv1.ServerFrame_Pong:
			c.completePing(f.Pong.GetNonce())
		}
	}
}
