package relay

import (
	"fmt"
	"os"

	"github.com/gezibash/arc/v2/pkg/runtime"
	"github.com/gezibash/arc/v2/pkg/transport"
)

const componentKey = "relay"

// Config configures the relay capability.
type Config struct {
	// Addr is the relay server address.
	// Defaults to ARC_RELAY env or localhost:50051.
	Addr string
}

// Capability returns a runtime extension that connects to a relay.
func Capability(cfg Config) runtime.Extension {
	return func(rt *runtime.Runtime) error {
		// Resolve address
		addr := cfg.Addr
		if addr == "" {
			addr = os.Getenv("ARC_RELAY")
		}
		if addr == "" {
			addr = DefaultRelayAddr
		}

		rt.Log().Info("connecting to relay", "addr", addr)

		client, err := Dial(rt.Context(), addr, rt.Signer())
		if err != nil {
			return fmt.Errorf("connect to relay: %w", err)
		}

		rt.Set(componentKey, client)
		_ = transport.Attach(client)(rt)

		return nil
	}
}

// From retrieves the relay client from the runtime.
// Returns nil if relay capability was not configured.
func From(rt *runtime.Runtime) *Client {
	c, _ := rt.Get(componentKey).(*Client)
	return c
}
