package transport

import (
	"time"

	"github.com/gezibash/arc/v2/pkg/identity"
)

// Provider describes a capability provider discovered via the transport.
type Provider struct {
	Pubkey            identity.PublicKey
	Name              string
	Petname           string
	Labels            map[string]any // structural labels (typed)
	State             map[string]any // dynamic metrics (typed)
	SubscriptionID    string
	RelayPubkey       identity.PublicKey
	Latency           time.Duration // relay → capability RTT
	InterRelayLatency time.Duration // local relay → remote relay RTT (0 for local)
	LastSeen          time.Time
	Connected         time.Duration
}

// ProviderSet is a discovery result set.
type ProviderSet struct {
	Providers []Provider
	Total     int
	HasMore   bool
}
