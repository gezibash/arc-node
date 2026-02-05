package gossip

import (
	"log/slog"
	"net"
	"time"

	"github.com/hashicorp/memberlist"
)

// resolveGRPCAddr returns a fully-qualified gRPC address.
// If the advertised address is just a port (e.g., ":50051"), it's combined
// with the node's memberlist IP to form a routable address.
func resolveGRPCAddr(advertised string, nodeAddr net.IP) string {
	if len(advertised) == 0 {
		return ""
	}
	host, port, err := net.SplitHostPort(advertised)
	if err != nil {
		return advertised
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return net.JoinHostPort(nodeAddr.String(), port)
	}
	return advertised
}

// eventDelegate implements memberlist.EventDelegate for join/leave/update.
type eventDelegate struct {
	relays       *RelaysSection
	capabilities *CapabilitiesSection
	names        *NamesSection
}

var _ memberlist.EventDelegate = (*eventDelegate)(nil)

// NotifyJoin is called when a node joins the cluster.
func (e *eventDelegate) NotifyJoin(node *memberlist.Node) {
	meta, err := DecodeHealthMeta(node.Meta)
	if err != nil {
		slog.Warn("decode join meta failed",
			"component", "gossip",
			"node", node.Name,
			"error", err,
		)
		// Still register with what we have
		meta = HealthMeta{}
	}

	grpcAddr := resolveGRPCAddr(meta.GRPCAddr, node.Addr)

	e.relays.Add(&RelayEntry{
		Name:     node.Name,
		GRPCAddr: grpcAddr,
		Pubkey:   meta.Pubkey,
		JoinedAt: time.Now().UnixNano(),
	})

	slog.Info("node joined",
		"component", "gossip",
		"node", node.Name,
		"addr", node.Address(),
		"grpc_addr", grpcAddr,
	)
}

// NotifyLeave is called when a node leaves the cluster.
func (e *eventDelegate) NotifyLeave(node *memberlist.Node) {
	e.relays.Remove(node.Name)
	e.capabilities.PurgeRelay(node.Name)
	e.names.PurgeRelay(node.Name)

	slog.Info("node left",
		"component", "gossip",
		"node", node.Name,
		"addr", node.Address(),
	)
}

// NotifyUpdate is called when a node's metadata is updated.
func (e *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	meta, err := DecodeHealthMeta(node.Meta)
	if err != nil {
		slog.Debug("decode update meta failed",
			"component", "gossip",
			"node", node.Name,
			"error", err,
		)
		return
	}

	// Update the relay entry with fresh metadata
	e.relays.Add(&RelayEntry{
		Name:     node.Name,
		GRPCAddr: resolveGRPCAddr(meta.GRPCAddr, node.Addr),
		Pubkey:   meta.Pubkey,
		JoinedAt: time.Now().UnixNano(),
	})

	slog.Debug("node updated",
		"component", "gossip",
		"node", node.Name,
		"connections", meta.Connections,
	)
}
