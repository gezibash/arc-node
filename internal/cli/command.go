package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/gezibash/arc/v2/pkg/runtime"
	"github.com/spf13/viper"
)

// CommandConfig configures a CLI command that uses the runtime pattern.
type CommandConfig struct {
	// Name identifies this command (for runtime/logging).
	Name string

	// KeyName is the default key alias for identity loading.
	KeyName string

	// Viper holds the command's configuration.
	Viper *viper.Viper

	// Timeout for the command operation. Zero means no timeout.
	Timeout time.Duration

	// Extensions are applied to the runtime (e.g., WithRelay, blob.Capability).
	Extensions []runtime.Extension

	// Run is the command's business logic.
	Run func(ctx context.Context, rt *runtime.Runtime, out *Output) error
}

// RunCommand executes a CLI command with standard infrastructure setup.
// Handles: LoadSigner -> NewBuilder -> Use(extensions) -> Build -> timeout -> Output -> Run -> Close.
func RunCommand(cfg CommandConfig) error {
	if cfg.Name == "" {
		return fmt.Errorf("command name required")
	}
	if cfg.Viper == nil {
		return fmt.Errorf("viper required")
	}
	if cfg.Run == nil {
		return fmt.Errorf("run function required")
	}

	// Load identity
	signer, err := LoadSigner(cfg.Viper, cfg.KeyName)
	if err != nil {
		return fmt.Errorf("load signer: %w", err)
	}

	// Build runtime
	builder := NewBuilder(cfg.Name, cfg.Viper).Signer(signer)
	for _, ext := range cfg.Extensions {
		builder = builder.Use(ext)
	}

	rt, err := builder.Build()
	if err != nil {
		return fmt.Errorf("init: %w", err)
	}
	defer func() { _ = rt.Close() }()

	// Apply timeout
	ctx := rt.Context()
	if cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}

	// Create output
	out := NewOutputFromViper(cfg.Viper)

	return cfg.Run(ctx, rt, out)
}
