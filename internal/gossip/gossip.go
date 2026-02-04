package gossip

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gezibash/arc/v2/internal/names"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/logging"
	"github.com/hashicorp/memberlist"
)

// Gossip manages the memberlist cluster and gossip state.
type Gossip struct {
	list       *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue
	state      *GossipState
	localName  string
	health     *HealthMeta
	config     Config
	pings      *pingDelegate

	// Sections for direct access
	relays       *RelaysSection
	capabilities *CapabilitiesSection
	names        *NamesSection

	connections atomic.Int32

	closed     atomic.Bool
	wg         sync.WaitGroup
	cancelFunc context.CancelFunc
}

// MemberInfo describes a cluster member.
type MemberInfo struct {
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

// New creates a new gossip instance. Call Start() to join the cluster.
func New(cfg Config) (*Gossip, error) {
	if cfg.NodeName == "" {
		if cfg.RelayPubkey != "" {
			cfg.NodeName = names.PetnameFromEncoded(cfg.RelayPubkey)
		} else {
			hostname, err := os.Hostname()
			if err != nil {
				return nil, fmt.Errorf("get hostname: %w", err)
			}
			cfg.NodeName = hostname
		}
	}
	if cfg.BindAddr == "" {
		cfg.BindAddr = "0.0.0.0"
	}
	// BindPort 0 means let the OS pick a free port (useful for tests).
	// Production defaults should be set at the config layer.

	// Create sections
	relays := NewRelaysSection()
	capabilities := NewCapabilitiesSection()
	names := NewNamesSection()

	// Create state registry
	state := NewGossipState()
	state.Register(relays)
	state.Register(capabilities)
	state.Register(names)

	health := &HealthMeta{
		GRPCAddr: cfg.GRPCAddr,
		Pubkey:   cfg.RelayPubkey,
	}

	// Create broadcast queue
	broadcasts := &memberlist.TransmitLimitedQueue{
		RetransmitMult: 3,
	}

	d := &delegate{
		state:      state,
		health:     health,
		broadcasts: broadcasts,
	}
	events := &eventDelegate{
		relays:       relays,
		capabilities: capabilities,
		names:        names,
	}

	pings := newPingDelegate()

	// Configure memberlist
	mlConfig := memberlist.DefaultLANConfig()
	mlConfig.Name = cfg.NodeName
	mlConfig.BindAddr = cfg.BindAddr
	mlConfig.BindPort = cfg.BindPort
	if cfg.AdvertiseAddr != "" {
		mlConfig.AdvertiseAddr = cfg.AdvertiseAddr
	}
	if cfg.AdvertisePort != 0 {
		mlConfig.AdvertisePort = cfg.AdvertisePort
	}
	mlConfig.Delegate = d
	mlConfig.Events = events
	mlConfig.Ping = pings
	mlConfig.LogOutput = &slogWriter{
		log: logging.New(nil).WithComponent("memberlist"),
	}

	list, err := memberlist.Create(mlConfig)
	if err != nil {
		return nil, fmt.Errorf("create memberlist: %w", err)
	}

	broadcasts.NumNodes = func() int {
		return list.NumMembers()
	}

	g := &Gossip{
		list:         list,
		broadcasts:   broadcasts,
		state:        state,
		localName:    cfg.NodeName,
		health:       health,
		config:       cfg,
		pings:        pings,
		relays:       relays,
		capabilities: capabilities,
		names:        names,
	}

	return g, nil
}

// Start joins the cluster (if seeds given) and starts background tasks.
func (g *Gossip) Start(ctx context.Context) error {
	ctx, g.cancelFunc = context.WithCancel(ctx)

	// Join seeds if configured
	if len(g.config.Seeds) > 0 {
		n, err := g.list.Join(g.config.Seeds)
		if err != nil {
			slog.Warn("partial join",
				"component", "gossip",
				"joined", n,
				"seeds", g.config.Seeds,
				"error", err,
			)
		} else {
			slog.Info("joined cluster",
				"component", "gossip",
				"joined", n,
				"seeds", g.config.Seeds,
			)
		}
	}

	// Start health updater
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.healthUpdater(ctx)
	}()

	slog.Info("gossip started",
		"component", "gossip",
		"name", g.localName,
		"bind", fmt.Sprintf("%s:%d", g.config.BindAddr, g.config.BindPort),
		"members", g.list.NumMembers(),
	)

	return nil
}

// healthUpdater periodically updates node metadata.
func (g *Gossip) healthUpdater(ctx context.Context) {
	startTime := time.Now()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.health.Uptime = uint64(time.Since(startTime).Nanoseconds())
			if err := g.list.UpdateNode(5 * time.Second); err != nil {
				slog.Debug("update node meta failed",
					"component", "gossip",
					"error", err,
				)
			}
		}
	}
}

// Join adds peers to the cluster at runtime.
func (g *Gossip) Join(peers []string) (int, error) {
	return g.list.Join(peers)
}

// Members returns information about all cluster members.
func (g *Gossip) Members() []MemberInfo {
	members := g.list.Members()
	infos := make([]MemberInfo, 0, len(members))

	for _, m := range members {
		info := MemberInfo{
			Name:      m.Name,
			Addr:      m.Address(),
			Status:    memberStatusString(m.State),
			IsLocal:   m.Name == g.localName,
			LatencyNs: g.RelayRTT(m.Name).Nanoseconds(),
		}

		if meta, err := DecodeHealthMeta(m.Meta); err == nil {
			info.GRPCAddr = meta.GRPCAddr
			info.Pubkey = meta.Pubkey
			info.Connections = meta.Connections
			info.Uptime = meta.Uptime
		}

		infos = append(infos, info)
	}
	return infos
}

// Leave gracefully leaves the cluster.
func (g *Gossip) Leave() error {
	return g.list.Leave(5 * time.Second)
}

// Close shuts down gossip entirely.
func (g *Gossip) Close() error {
	if g.closed.Swap(true) {
		return nil
	}

	if g.cancelFunc != nil {
		g.cancelFunc()
	}

	if err := g.list.Leave(5 * time.Second); err != nil {
		slog.Warn("leave failed during close",
			"component", "gossip",
			"error", err,
		)
	}

	err := g.list.Shutdown()
	g.wg.Wait()
	return err
}

// LocalName returns this node's name.
func (g *Gossip) LocalName() string {
	return g.localName
}

// State returns the gossip state for direct access.
func (g *Gossip) State() *GossipState {
	return g.state
}

// Broadcast queues an increment for gossip broadcast.
// For relay-scoped sections (capabilities, names), includes the source relay name
// so receivers can purge-and-replace entries for this relay.
func (g *Gossip) Broadcast(s Section) {
	var data []byte
	if _, ok := s.(RelayScopedSection); ok {
		data = EncodeRelayIncrement(s, g.localName)
	} else {
		data = EncodeIncrement(s)
	}
	g.broadcasts.QueueBroadcast(&broadcast{
		key:  fmt.Sprintf("%d", s.ID()),
		data: data,
	})
}

// UpdateConnections updates the local connection count for health metadata.
func (g *Gossip) UpdateConnections(count uint32) {
	g.health.Connections = count
}

// OnConnected is called when a client connects to the local relay.
// Implements relay.Observer.
func (g *Gossip) OnConnected(_ identity.PublicKey) {
	count := g.connections.Add(1)
	g.UpdateConnections(uint32(count))
}

// OnSubscribe is called when a client subscribes on the local relay.
// Implements relay.Observer.
func (g *Gossip) OnSubscribe(pubkey identity.PublicKey, subID string, labels map[string]string, name string) {
	g.capabilities.Add(&CapabilityEntry{
		RelayName:      g.localName,
		ProviderPubkey: identity.EncodePublicKey(pubkey),
		SubID:          subID,
		Labels:         labels,
		ProviderName:   name,
	})
	g.Broadcast(g.capabilities)
}

// OnUnsubscribe is called when a client unsubscribes from the local relay.
// Implements relay.Observer.
func (g *Gossip) OnUnsubscribe(pubkey identity.PublicKey, subID string) {
	g.capabilities.Remove(g.localName, identity.EncodePublicKey(pubkey), subID)
	g.Broadcast(g.capabilities)
}

// OnSubscriberRemoved is called when a client disconnects.
// Implements relay.Observer.
func (g *Gossip) OnSubscriberRemoved(pubkey identity.PublicKey) {
	count := g.connections.Add(-1)
	g.UpdateConnections(uint32(count))

	g.capabilities.PurgeProvider(g.localName, identity.EncodePublicKey(pubkey))
	g.Broadcast(g.capabilities)
}

// OnLatencyMeasured is called when the relay measures RTT to a subscriber.
// Implements relay.Observer.
func (g *Gossip) OnLatencyMeasured(pubkey identity.PublicKey, latency time.Duration) {
	pubkeyStr := identity.EncodePublicKey(pubkey)
	if g.capabilities.UpdateLatency(g.localName, pubkeyStr, latency.Nanoseconds()) {
		g.Broadcast(g.capabilities)
	}
}

// OnNameRegistered is called when a name is registered on the local relay.
// Implements relay.Observer.
func (g *Gossip) OnNameRegistered(name string, pubkey identity.PublicKey) {
	pubkeyStr := identity.EncodePublicKey(pubkey)

	g.names.Add(&NameEntry{
		Name:      name,
		RelayName: g.localName,
		Pubkey:    pubkeyStr,
	})
	g.Broadcast(g.names)

	// Patch capability entries so the name propagates with capabilities too.
	g.capabilities.UpdateProviderName(g.localName, pubkeyStr, name)
	g.Broadcast(g.capabilities)
}

// ResolveRelay returns the gRPC address for a relay name.
func (g *Gossip) ResolveRelay(name string) (grpcAddr string, ok bool) {
	entry, ok := g.relays.Get(name)
	if !ok {
		return "", false
	}
	return entry.GRPCAddr, true
}

// IsKnownRelay checks if a pubkey belongs to a relay in the cluster.
func (g *Gossip) IsKnownRelay(pubkey string) bool {
	for _, entry := range g.relays.All() {
		if entry.Pubkey == pubkey {
			return true
		}
	}
	return false
}

// RelayRTT returns the last measured RTT to a gossip peer by node name.
// Returns 0 if not yet measured or if the node is the local relay.
func (g *Gossip) RelayRTT(nodeName string) time.Duration {
	if nodeName == g.localName {
		return 0
	}
	return g.pings.RTT(nodeName)
}

// Capabilities returns the capabilities section for direct access.
func (g *Gossip) Capabilities() *CapabilitiesSection {
	return g.capabilities
}

// Names returns the names section for direct access.
func (g *Gossip) Names() *NamesSection {
	return g.names
}

func memberStatusString(state memberlist.NodeStateType) string {
	switch state {
	case memberlist.StateAlive:
		return "alive"
	case memberlist.StateSuspect:
		return "suspect"
	case memberlist.StateDead:
		return "dead"
	case memberlist.StateLeft:
		return "left"
	default:
		return "unknown"
	}
}

// broadcast implements memberlist.Broadcast.
type broadcast struct {
	key  string
	data []byte
}

func (b *broadcast) Invalidates(other memberlist.Broadcast) bool {
	ob, ok := other.(*broadcast)
	if !ok {
		return false
	}
	return b.key == ob.key
}

func (b *broadcast) Message() []byte {
	return b.data
}

func (b *broadcast) Finished() {}

// slogWriter adapts memberlist's io.Writer log output to slog.
// Parses memberlist's [ERR], [WARN], [DEBUG], [INFO] prefixes
// and maps them to proper slog levels so errors surface without
// needing --log-level debug.
type slogWriter struct {
	log *logging.Logger
}

func (w *slogWriter) Write(p []byte) (int, error) {
	msg := string(p)
	if len(msg) > 0 && msg[len(msg)-1] == '\n' {
		msg = msg[:len(msg)-1]
	}

	// Parse memberlist's level prefix and strip timestamp
	switch {
	case strings.Contains(msg, "[ERR]"):
		w.log.Warn(stripMemberlistPrefix(msg, "[ERR]"))
	case strings.Contains(msg, "[WARN]"):
		w.log.Warn(stripMemberlistPrefix(msg, "[WARN]"))
	case strings.Contains(msg, "[INFO]"):
		w.log.Info(stripMemberlistPrefix(msg, "[INFO]"))
	default:
		w.log.Debug(stripMemberlistPrefix(msg, "[DEBUG]"))
	}
	return len(p), nil
}

// stripMemberlistPrefix removes memberlist's timestamp and level prefix.
// Input: "2026/02/04 14:13:51 [ERR] memberlist: Failed to send..."
// Output: "memberlist: Failed to send..."
func stripMemberlistPrefix(msg, level string) string {
	if idx := strings.Index(msg, level); idx != -1 {
		msg = strings.TrimSpace(msg[idx+len(level):])
	}
	return msg
}
