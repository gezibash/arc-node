package gossip

import (
	"log/slog"

	"github.com/hashicorp/memberlist"
)

// delegate implements memberlist.Delegate to hook into gossip.
type delegate struct {
	state      *GossipState
	health     *HealthMeta
	broadcasts *memberlist.TransmitLimitedQueue
}

var _ memberlist.Delegate = (*delegate)(nil)

// NodeMeta returns metadata about this node (must fit in 512 bytes).
func (d *delegate) NodeMeta(limit int) []byte {
	data := d.health.Encode()
	if len(data) > limit {
		slog.Warn("node meta exceeds limit",
			"component", "gossip",
			"size", len(data),
			"limit", limit,
		)
		return data[:limit]
	}
	return data
}

// NotifyMsg is called when a user-data message is received (broadcasts).
// Must not block.
func (d *delegate) NotifyMsg(msg []byte) {
	if len(msg) == 0 {
		return
	}

	var err error
	switch msg[0] {
	case MsgTypeIncrement:
		err = d.state.ApplyIncrement(msg)
	case MsgTypeRelayIncrement:
		err = d.state.ApplyRelayIncrement(msg)
	default:
		slog.Debug("unknown gossip message type",
			"component", "gossip",
			"type", msg[0],
		)
		return
	}
	if err != nil {
		slog.Debug("apply broadcast failed",
			"component", "gossip",
			"error", err,
		)
	}
}

// GetBroadcasts returns queued broadcasts (called by memberlist protocol).
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.broadcasts.GetBroadcasts(overhead, limit)
}

// LocalState returns the full local state for push/pull sync.
func (d *delegate) LocalState(join bool) []byte {
	return d.state.EncodeAll()
}

// MergeRemoteState merges a full state from a remote node (push/pull sync).
func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	if err := d.state.MergeAll(buf); err != nil {
		slog.Warn("merge remote state failed",
			"component", "gossip",
			"error", err,
		)
	}
}
