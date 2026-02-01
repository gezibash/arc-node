package node

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"os"
	"path/filepath"

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
			data, err := readInput(args)
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
			kr := openKeyring(n.v)
			key, err := loadKey(cmd, n.v, kr)
			if err != nil {
				return fmt.Errorf("load key: %w", err)
			}

			// Put content
			contentRef, err := n.client.PutContent(cmd.Context(), data)
			if err != nil {
				return fmt.Errorf("put content: %w", err)
			}

			// Determine recipient
			pub := key.Keypair.PublicKey()
			var toPub identity.PublicKey
			if to != "" {
				b, err := hex.DecodeString(to)
				if err != nil || len(b) != identity.PublicKeySize {
					return fmt.Errorf("parse --to: expected %d-byte hex public key", identity.PublicKeySize)
				}
				copy(toPub[:], b)
			} else {
				// Default to the node's public key
				nodeKey, err := openKeyring(n.v).Load(cmd.Context(), "node")
				if err != nil {
					return fmt.Errorf("load node key for default --to: %w", err)
				}
				toPub = nodeKey.Keypair.PublicKey()
			}

			// Build and sign message using arc protocol
			msg := message.New(pub, toPub, contentRef, contentType)
			if err := message.Sign(&msg, key.Keypair); err != nil {
				return fmt.Errorf("sign message: %w", err)
			}

			// Parse labels
			labelMap, err := parseLabels(labels)
			if err != nil {
				return err
			}

			slog.DebugContext(cmd.Context(), "sending message", "content_ref", reference.Hex(contentRef), "labels", labelMap)
			ref, err := n.client.SendMessage(cmd.Context(), msg, labelMap)
			if err != nil {
				return fmt.Errorf("send message: %w", err)
			}
			if output == "json" {
				return writeJSON(os.Stdout, map[string]string{"reference": reference.Hex(ref)})
			}
			fmt.Println(reference.Hex(ref))
			return nil
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text or json")
	cmd.Flags().StringVar(&to, "to", "", "recipient public key (hex, defaults to node)")
	cmd.Flags().StringVar(&contentType, "content-type", "", "content MIME type (auto-detected if omitted)")
	cmd.Flags().StringSliceVarP(&labels, "label", "l", nil, "label key=value (repeatable)")
	return cmd
}
