package gossip

import (
	"log/slog"
	"time"

	"github.com/hashicorp/memberlist"
)

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

	e.relays.Add(&RelayEntry{
		Name:     node.Name,
		GRPCAddr: meta.GRPCAddr,
		Pubkey:   meta.Pubkey,
		JoinedAt: time.Now().UnixNano(),
	})

	slog.Info("node joined",
		"component", "gossip",
		"node", node.Name,
		"addr", node.Address(),
		"grpc_addr", meta.GRPCAddr,
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
		GRPCAddr: meta.GRPCAddr,
		Pubkey:   meta.Pubkey,
		JoinedAt: time.Now().UnixNano(),
	})

	slog.Debug("node updated",
		"component", "gossip",
		"node", node.Name,
		"connections", meta.Connections,
	)
}
