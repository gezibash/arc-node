package node

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"os"
	"path/filepath"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
	"github.com/spf13/cobra"
)

func newPublishCmd(n *nodeCmd) *cobra.Command {
	var (
		to          string
		contentType string
		labels      []string
		output      string
	)

	cmd := &cobra.Command{
		Use:   "publish [file | text]",
		Short: "Store content, sign a message, and index it",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Read content
			data, err := n.readInputFn(args)
			if err != nil {
				return err
			}

			// Auto-detect content type: extension first, then byte sniffing
			if contentType == "" {
				if len(args) == 1 {
					contentType = mime.TypeByExtension(filepath.Ext(args[0]))
				}
				if contentType == "" {
					contentType = http.DetectContentType(data)
				}
			}

			// Load keypair
			kp, err := n.loadKeypair(cmd)
			if err != nil {
				return fmt.Errorf("load key: %w", err)
			}

			// Put content
			contentRef, err := n.client.PutContent(cmd.Context(), data)
			if err != nil {
				return fmt.Errorf("put content: %w", err)
			}

			// Determine recipient
			pub := kp.PublicKey()
			var toPub identity.PublicKey
			if to != "" {
				b, err := hex.DecodeString(to)
				if err != nil || len(b) != identity.PublicKeySize {
					return fmt.Errorf("parse --to: expected %d-byte hex public key", identity.PublicKeySize)
				}
				copy(toPub[:], b)
			} else {
				// Default to the node's public key
				nodePub, err := n.loadNodeKey(cmd)
				if err != nil {
					return fmt.Errorf("load node key for default --to: %w", err)
				}
				toPub = nodePub
			}

			// Build and sign message using arc protocol
			msg := message.New(pub, toPub, contentRef, contentType)
			if err := message.Sign(&msg, kp); err != nil {
				return fmt.Errorf("sign message: %w", err)
			}

			// Parse labels
			labelMap, err := parseLabels(labels)
			if err != nil {
				return err
			}

			slog.DebugContext(cmd.Context(), "sending message", "content_ref", reference.Hex(contentRef), "labels", labelMap)
			result, err := n.client.SendMessage(cmd.Context(), msg, labelMap, &nodev1.Dimensions{
				Persistence: nodev1.Persistence_PERSISTENCE_DURABLE,
			})
			if err != nil {
				return fmt.Errorf("send message: %w", err)
			}
			if output == "json" {
				return writeJSON(os.Stdout, map[string]string{"reference": reference.Hex(result.Ref)})
			}
			fmt.Println(reference.Hex(result.Ref))
			return nil
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text or json")
	cmd.Flags().StringVar(&to, "to", "", "recipient public key (hex, defaults to node)")
	cmd.Flags().StringVar(&contentType, "content-type", "", "content MIME type (auto-detected if omitted)")
	cmd.Flags().StringSliceVarP(&labels, "label", "l", nil, "label key=value (repeatable)")
	return cmd
}
