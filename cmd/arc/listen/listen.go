package listen

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/gezibash/arc-node/internal/config"
	"github.com/gezibash/arc-node/internal/keyring"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Entrypoint returns the listen command.
func Entrypoint(v *viper.Viper) *cobra.Command {
	var (
		labels []string
		name   string
		relay  string
		raw    bool
	)

	cmd := &cobra.Command{
		Use:   "listen",
		Short: "Subscribe and receive envelopes from the relay",
		Long: `Connect to the relay and receive envelopes matching your subscription.

Outputs received envelopes as JSON (one per line).

Examples:
  arc listen                              # listen for all (match-all subscription)
  arc listen --labels topic=news          # filter by labels
  arc listen --labels capability=storage  # subscribe as capability provider
  arc listen --name alice                 # register name, receive addressed messages
  arc listen --relay localhost:50051      # specify relay`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			// Set up signal handling for graceful shutdown
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigCh
				cancel()
			}()

			// Load key
			kr := keyring.New(dataDir(v))
			keyName := v.GetString("key")
			var key *keyring.Key
			var err error
			if keyName != "" {
				key, err = kr.Load(ctx, keyName)
			} else {
				key, err = kr.LoadDefault(ctx)
			}
			if err != nil {
				return fmt.Errorf("load key: %w", err)
			}

			// Build labels
			subLabels := make(map[string]string)
			for _, l := range labels {
				parts := strings.SplitN(l, "=", 2)
				if len(parts) != 2 {
					return fmt.Errorf("invalid label format: %s (expected key=value)", l)
				}
				subLabels[parts[0]] = parts[1]
			}

			// Connect to relay
			relayAddr := relay
			if relayAddr == "" {
				relayAddr = os.Getenv("ARC_RELAY")
			}
			if relayAddr == "" {
				relayAddr = client.DefaultRelayAddr
			}

			c, err := client.Dial(ctx, relayAddr, key.Keypair)
			if err != nil {
				return fmt.Errorf("connect: %w", err)
			}
			defer func() { _ = c.Close() }()

			// Register name if specified
			if name != "" {
				if err := c.RegisterName(name); err != nil {
					return fmt.Errorf("register name: %w", err)
				}
			}

			// Subscribe
			subID := uuid.New().String()
			if err := c.Subscribe(subID, subLabels); err != nil {
				return fmt.Errorf("subscribe: %w", err)
			}

			enc := json.NewEncoder(os.Stdout)

			// Receive loop
			for {
				delivery, err := c.Receive(ctx)
				if err != nil {
					if ctx.Err() != nil {
						// Graceful shutdown
						return nil
					}
					return fmt.Errorf("receive: %w", err)
				}

				// Output as JSON
				output := map[string]any{
					"from":   hex.EncodeToString(delivery.Sender[:]),
					"labels": delivery.Labels,
				}

				if raw {
					output["payload"] = base64.StdEncoding.EncodeToString(delivery.Payload)
				} else {
					// Try to output as string if it looks like text
					if isText(delivery.Payload) {
						output["payload"] = string(delivery.Payload)
					} else {
						output["payload"] = base64.StdEncoding.EncodeToString(delivery.Payload)
						output["encoding"] = "base64"
					}
				}

				if err := enc.Encode(output); err != nil {
					return fmt.Errorf("encode: %w", err)
				}
			}
		},
	}

	cmd.Flags().StringSliceVarP(&labels, "labels", "l", nil, "filter labels (key=value, can repeat)")
	cmd.Flags().StringVar(&name, "name", "", "register addressed name (without @ prefix)")
	cmd.Flags().StringVar(&relay, "relay", "", "relay address (default localhost:50051, or ARC_RELAY env)")
	cmd.Flags().BoolVar(&raw, "raw", false, "always output payload as base64")

	return cmd
}

func dataDir(v *viper.Viper) string {
	if d := v.GetString("data_dir"); d != "" {
		return d
	}
	return config.DefaultDataDir()
}

// isText returns true if the data looks like UTF-8 text.
func isText(data []byte) bool {
	for _, b := range data {
		// Allow printable ASCII and common whitespace
		if b < 0x20 && b != '\t' && b != '\n' && b != '\r' {
			return false
		}
		if b == 0x7f {
			return false
		}
	}
	return true
}
