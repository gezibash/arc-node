package capability

import (
	"github.com/gezibash/arc/v2/pkg/relay"
	"github.com/gezibash/arc/v2/pkg/runtime"
)

// RelayConfig configures the relay connection for capabilities.
type RelayConfig struct {
	// Addr is the relay server address.
	// Defaults to ARC_RELAY env or localhost:50051.
	Addr string
}

// Relay returns a runtime extension that connects to a relay.
// This is a convenience wrapper that allows capability users to configure
// relay without directly importing the relay package.
func Relay(cfg RelayConfig) runtime.Extension {
	return relay.Capability(relay.Config{
		Addr: cfg.Addr,
	})
}
