package gossip

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Metadata keys for relay-to-relay authentication.
const (
	metaFrom      = "arc-from"
	metaSignature = "arc-signature"
	metaTimestamp = "arc-timestamp"
)

// Forwarder forwards envelopes to remote relays via gRPC.
// Implements relay.RemoteForwarder.
type Forwarder struct {
	gossip  *Gossip
	signer  identity.Signer
	mu      sync.RWMutex
	conns   map[string]*grpc.ClientConn // grpcAddr → conn
	timeout time.Duration
}

// NewForwarder creates a new Forwarder.
func NewForwarder(g *Gossip, signer identity.Signer) *Forwarder {
	return &Forwarder{
		gossip:  g,
		signer:  signer,
		conns:   make(map[string]*grpc.ClientConn),
		timeout: 5 * time.Second,
	}
}

// Forward resolves the target relay from gossip state and sends via ForwardEnvelope RPC.
// Returns delivered count from remote relay.
func (f *Forwarder) Forward(ctx context.Context, env *relayv1.Envelope) (int, error) {
	relayName, grpcAddr, ok := f.resolveTarget(env)
	if !ok {
		return 0, fmt.Errorf("no remote relay found for envelope")
	}

	// Skip forwarding to ourselves
	if relayName == f.gossip.LocalName() {
		return 0, fmt.Errorf("resolved to local relay")
	}

	conn, err := f.getOrDial(ctx, grpcAddr)
	if err != nil {
		return 0, fmt.Errorf("dial %s: %w", grpcAddr, err)
	}

	client := relayv1.NewRelayServiceClient(conn)

	// Add auth metadata
	ctx = f.authContext(ctx)

	// Apply timeout
	ctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()

	resp, err := client.ForwardEnvelope(ctx, &relayv1.ForwardEnvelopeRequest{
		Envelope:    env,
		SourceRelay: f.gossip.LocalName(),
	})
	if err != nil {
		return 0, fmt.Errorf("forward to %s: %w", relayName, err)
	}

	if resp.GetDelivered() == 0 {
		return 0, fmt.Errorf("remote relay %s: %s", relayName, resp.GetReason())
	}

	slog.Debug("envelope forwarded",
		"component", "forwarder",
		"target_relay", relayName,
		"target_addr", grpcAddr,
		"delivered", resp.GetDelivered(),
	)

	return int(resp.GetDelivered()), nil
}

// Close closes all cached connections.
func (f *Forwarder) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for addr, conn := range f.conns {
		_ = conn.Close()
		delete(f.conns, addr)
	}
	return nil
}

// CloseConn closes a cached connection for the given address.
// Used when a relay leaves the cluster.
func (f *Forwarder) CloseConn(grpcAddr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if conn, ok := f.conns[grpcAddr]; ok {
		_ = conn.Close()
		delete(f.conns, grpcAddr)
	}
}

// resolveTarget determines which relay should receive this envelope.
// For addressed routing: checks NamesSection, then CapabilitiesSection by pubkey.
// For label-match: checks CapabilitiesSection.Match.
// Returns (relayName, grpcAddr, ok).
func (f *Forwarder) resolveTarget(env *relayv1.Envelope) (string, string, bool) {
	labels := env.GetLabels()
	if labels == nil {
		return "", "", false
	}

	// 1. Addressed routing: "to" label → resolve name or pubkey
	if to := labels["to"]; to != "" {
		return f.resolveAddressed(to)
	}

	// 2. Label-match routing: find a remote capability that matches
	return f.resolveLabelMatch(labels)
}

// resolveAddressed resolves an addressed target (name or pubkey) to a relay.
func (f *Forwarder) resolveAddressed(to string) (string, string, bool) {
	// Try name resolution first
	if nameEntry, ok := f.gossip.Names().Get(to); ok {
		// Skip local relay entries
		if nameEntry.RelayName == f.gossip.LocalName() {
			return "", "", false
		}
		if grpcAddr, ok := f.gossip.ResolveRelay(nameEntry.RelayName); ok {
			return nameEntry.RelayName, grpcAddr, true
		}
	}

	// Try pubkey resolution: scan capabilities for matching provider pubkey
	if entry, ok := f.gossip.Capabilities().FindByPubkey(to); ok {
		if entry.RelayName == f.gossip.LocalName() {
			return "", "", false
		}
		if grpcAddr, ok := f.gossip.ResolveRelay(entry.RelayName); ok {
			return entry.RelayName, grpcAddr, true
		}
	}

	return "", "", false
}

// resolveLabelMatch finds a remote capability matching the labels.
func (f *Forwarder) resolveLabelMatch(labels map[string]string) (string, string, bool) {
	entries := f.gossip.Capabilities().Match(labels, 100)
	for _, entry := range entries {
		// Skip local relay entries
		if entry.RelayName == f.gossip.LocalName() {
			continue
		}
		if grpcAddr, ok := f.gossip.ResolveRelay(entry.RelayName); ok {
			return entry.RelayName, grpcAddr, true
		}
	}
	return "", "", false
}

// getOrDial returns a cached connection or creates a new one.
func (f *Forwarder) getOrDial(_ context.Context, grpcAddr string) (*grpc.ClientConn, error) {
	f.mu.RLock()
	conn, ok := f.conns[grpcAddr]
	f.mu.RUnlock()
	if ok {
		return conn, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, ok := f.conns[grpcAddr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	f.conns[grpcAddr] = conn
	return conn, nil
}

// authContext adds relay authentication metadata to the context.
func (f *Forwarder) authContext(ctx context.Context) context.Context {
	ts := time.Now().UnixMilli()
	from := f.signer.PublicKey()

	payload := authPayload(from, ts)
	sig, err := f.signer.Sign(payload)
	if err != nil {
		return ctx
	}

	md := metadata.New(map[string]string{
		metaFrom:      identity.EncodePublicKey(from),
		metaSignature: identity.EncodeSignature(sig),
		metaTimestamp: strconv.FormatInt(ts, 10),
	})
	return metadata.NewOutgoingContext(ctx, md)
}

// authPayload builds the authentication payload for relay-to-relay auth.
// Must match internal/relay/interceptor.go's authPayload.
func authPayload(pub identity.PublicKey, ts int64) []byte {
	algo := pub.Algo
	if algo == "" {
		algo = identity.AlgEd25519
	}
	return []byte(fmt.Sprintf("arc-auth-v1:%s:%x:%d", algo, pub.Bytes, ts))
}
