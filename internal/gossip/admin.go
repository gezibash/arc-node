package gossip

import (
	"github.com/gezibash/arc/v2/internal/relay"
)

// GossipAdmin adapts Gossip to implement relay.GossipAdmin.
type GossipAdmin struct {
	g *Gossip
}

var _ relay.GossipAdmin = (*GossipAdmin)(nil)

// NewGossipAdmin wraps a Gossip instance as a relay.GossipAdmin.
func NewGossipAdmin(g *Gossip) *GossipAdmin {
	return &GossipAdmin{g: g}
}

func (a *GossipAdmin) Join(peers []string) (int, error) {
	return a.g.Join(peers)
}

func (a *GossipAdmin) Members() []relay.GossipMemberInfo {
	members := a.g.Members()
	out := make([]relay.GossipMemberInfo, len(members))
	for i, m := range members {
		out[i] = relay.GossipMemberInfo{
			Name:        m.Name,
			Addr:        m.Addr,
			GRPCAddr:    m.GRPCAddr,
			Status:      m.Status,
			Pubkey:      m.Pubkey,
			Connections: m.Connections,
			Uptime:      m.Uptime,
			LatencyNs:   m.LatencyNs,
			IsLocal:     m.IsLocal,
		}
	}
	return out
}

func (a *GossipAdmin) Leave() error {
	return a.g.Leave()
}
