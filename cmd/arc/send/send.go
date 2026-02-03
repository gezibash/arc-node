package send

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	relayv1 "github.com/gezibash/arc-node/api/arc/relay/v1"
	"github.com/gezibash/arc-node/internal/config"
	"github.com/gezibash/arc-node/internal/keyring"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Entrypoint returns the send command.
func Entrypoint(v *viper.Viper) *cobra.Command {
	var (
		to         string
		labels     []string
		capability string
		relay      string
		timeout    time.Duration
	)

	cmd := &cobra.Command{
		Use:   "send [data]",
		Short: "Send an envelope through the relay",
		Long: `Send an envelope to the relay. Data can be provided as an argument or via stdin.

Examples:
  arc send "hello world"                    # send to default relay
  arc send --to @alice "hello"              # addressed send
  arc send --labels topic=news "breaking"   # label-match send
  arc send --capability storage <file       # capability send
  arc send --relay localhost:50051 "test"   # specify relay`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}

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

			// Read data
			var data []byte
			if len(args) > 0 {
				data = []byte(args[0])
			} else {
				data, err = io.ReadAll(os.Stdin)
				if err != nil {
					return fmt.Errorf("read stdin: %w", err)
				}
			}

			// Build labels
			envLabels := make(map[string]string)
			for _, l := range labels {
				parts := strings.SplitN(l, "=", 2)
				if len(parts) != 2 {
					return fmt.Errorf("invalid label format: %s (expected key=value)", l)
				}
				envLabels[parts[0]] = parts[1]
			}

			// Add special routing labels
			if to != "" {
				envLabels["to"] = to
			}
			if capability != "" {
				envLabels["capability"] = capability
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

			// Send envelope
			env := &client.Envelope{
				Labels:      envLabels,
				Payload:     data,
				Correlation: uuid.New().String(),
			}

			receipt, err := c.Send(ctx, env)
			if err != nil {
				return fmt.Errorf("send: %w", err)
			}

			// Output receipt as JSON
			output := map[string]any{
				"ref":    hex.EncodeToString(receipt.Ref[:]),
				"status": receiptStatus(receipt.Status),
			}
			if receipt.Delivered > 0 {
				output["delivered"] = receipt.Delivered
			}
			if receipt.Reason != "" {
				output["reason"] = receipt.Reason
			}

			enc := json.NewEncoder(os.Stdout)
			return enc.Encode(output)
		},
	}

	cmd.Flags().StringVar(&to, "to", "", "recipient name (@name) or public key (hex)")
	cmd.Flags().StringSliceVarP(&labels, "labels", "l", nil, "labels (key=value, can repeat)")
	cmd.Flags().StringVar(&capability, "capability", "", "target capability")
	cmd.Flags().StringVar(&relay, "relay", "", "relay address (default localhost:50051, or ARC_RELAY env)")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "send timeout")

	return cmd
}

func dataDir(v *viper.Viper) string {
	if d := v.GetString("data_dir"); d != "" {
		return d
	}
	return config.DefaultDataDir()
}

func receiptStatus(s relayv1.ReceiptStatus) string {
	switch s {
	case relayv1.ReceiptStatus_RECEIPT_STATUS_ACK:
		return "ACK"
	case relayv1.ReceiptStatus_RECEIPT_STATUS_NACK:
		return "NACK"
	default:
		return "UNKNOWN"
	}
}
