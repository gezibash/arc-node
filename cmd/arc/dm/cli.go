package dm

import (
	"fmt"
	"os"

	"github.com/gezibash/arc-node/pkg/dm"
	"github.com/gezibash/arc/v2/pkg/reference"
	"github.com/spf13/cobra"
)

func newSendCmd(d *dmCmd) *cobra.Command {
	var (
		labels []string
		output string
	)

	cmd := &cobra.Command{
		Use:   "send <peer-pubkey> [file | text]",
		Short: "Encrypt and send a direct message",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sdk, err := d.openConversation(args[0])
			if err != nil {
				return err
			}

			data, err := readInput(args[1:])
			if err != nil {
				return err
			}

			labelMap, err := parseLabels(labels)
			if err != nil {
				return err
			}

			result, err := sdk.Send(cmd.Context(), data, labelMap)
			if err != nil {
				return err
			}

			if output == "json" {
				return writeJSON(os.Stdout, map[string]string{"reference": reference.Hex(result.Ref)})
			}
			fmt.Println(reference.Hex(result.Ref))
			return nil
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text or json")
	cmd.Flags().StringSliceVarP(&labels, "label", "l", nil, "label key=value (repeatable)")
	return cmd
}

func newListCmd(d *dmCmd) *cobra.Command {
	var (
		limit      int
		cursor     string
		descending bool
		labels     []string
		output     string
		preview    bool
	)

	cmd := &cobra.Command{
		Use:   "list <peer-pubkey> [expression]",
		Short: "Query conversation messages",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sdk, err := d.openConversation(args[0])
			if err != nil {
				return err
			}

			expr := ""
			if len(args) == 2 {
				expr = args[1]
			}

			labelMap, err := parseLabels(labels)
			if err != nil {
				return err
			}

			result, err := sdk.List(cmd.Context(), dm.ListOptions{
				Expression: expr,
				Labels:     labelMap,
				Limit:      limit,
				Cursor:     cursor,
				Descending: descending,
			})
			if err != nil {
				return err
			}

			selfPub := sdk.SelfPublicKey()

			switch output {
			case "json":
				return formatListJSON(os.Stdout, result, preview, sdk, selfPub)
			case "md", "markdown":
				return formatListMarkdown(os.Stdout, result, preview, sdk, selfPub)
			default:
				return formatListText(os.Stdout, result, preview, sdk, selfPub)
			}
		},
	}

	cmd.Flags().IntVar(&limit, "limit", 10, "max messages to return")
	cmd.Flags().StringVar(&cursor, "cursor", "", "pagination cursor")
	cmd.Flags().BoolVar(&descending, "descending", true, "reverse chronological order")
	cmd.Flags().StringSliceVarP(&labels, "label", "l", nil, "label key=value (repeatable)")
	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text, json, or md")
	cmd.Flags().BoolVar(&preview, "preview", false, "show decrypted content preview")
	return cmd
}

func newReadCmd(d *dmCmd) *cobra.Command {
	var file string

	cmd := &cobra.Command{
		Use:   "read <peer-pubkey> <ref>",
		Short: "Fetch, decrypt, and display a message",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sdk, err := d.openConversation(args[0])
			if err != nil {
				return err
			}

			ref, err := reference.FromHex(args[1])
			if err != nil {
				return fmt.Errorf("invalid reference: %w", err)
			}

			msg, err := sdk.Read(cmd.Context(), ref)
			if err != nil {
				return err
			}

			if file != "" {
				return os.WriteFile(file, msg.Content, 0600)
			}
			_, err = os.Stdout.Write(msg.Content)
			return err
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "write to file instead of stdout")
	return cmd
}
