package gossip

import (
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// pingDelegate implements memberlist.PingDelegate to track inter-relay RTT
// from SWIM protocol probes.
type pingDelegate struct {
	mu        sync.RWMutex
	latencies map[string]time.Duration // nodeName → RTT
}

var _ memberlist.PingDelegate = (*pingDelegate)(nil)

func newPingDelegate() *pingDelegate {
	return &pingDelegate{
		latencies: make(map[string]time.Duration),
	}
}

// AckPayload returns optional payload for ack (unused).
func (p *pingDelegate) AckPayload() []byte { return nil }

// NotifyPingComplete is called by memberlist after each successful SWIM probe.
func (p *pingDelegate) NotifyPingComplete(node *memberlist.Node, rtt time.Duration, _ []byte) {
	p.mu.Lock()
	p.latencies[node.Name] = rtt
	p.mu.Unlock()
}

// RTT returns the last measured RTT to a gossip peer by node name.
// Returns 0 if not yet measured.
func (p *pingDelegate) RTT(nodeName string) time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.latencies[nodeName]
}
