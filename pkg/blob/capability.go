package blob

import (
	"fmt"

	"github.com/gezibash/arc/v2/pkg/runtime"
)

const componentKey = "blob"

// Config configures the blob capability.
type Config struct {
	// Currently empty, but kept for future extensibility.
	// The relay is obtained from the runtime's relay capability.
}

// Capability returns a runtime extension that provides blob client functionality.
// Requires the relay capability to be configured on the runtime.
func Capability(cfg Config) runtime.Extension {
	return func(rt *runtime.Runtime) error {
		client, err := NewClient(rt)
		if err != nil {
			return fmt.Errorf("create blob client: %w", err)
		}

		rt.Set(componentKey, client)
		rt.OnClose(client.Close)

		return nil
	}
}

// From retrieves the blob client from the runtime.
// Returns nil if blob capability was not configured.
func From(rt *runtime.Runtime) *Client {
	c, _ := rt.Get(componentKey).(*Client)
	return c
}
