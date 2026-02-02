package dm

import (
	"fmt"
	"os"
	"time"

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
	var (
		file   string
		output string
	)

	cmd := &cobra.Command{
		Use:   "read <peer-pubkey> <ref>",
		Short: "Fetch, decrypt, and display a message",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sdk, err := d.openConversation(args[0])
			if err != nil {
				return err
			}

			// Ref prefix resolution.
			refStr := args[1]
			var ref reference.Reference
			if len(refStr) < 64 {
				resolved, err := d.client.ResolveGet(cmd.Context(), refStr)
				if err != nil {
					return fmt.Errorf("resolve ref prefix %q: %w", refStr, err)
				}
				ref = resolved.Ref
			} else {
				var err error
				ref, err = reference.FromHex(refStr)
				if err != nil {
					return fmt.Errorf("invalid reference: %w", err)
				}
			}

			msg, err := sdk.Read(cmd.Context(), ref)
			if err != nil {
				return err
			}

			if file != "" {
				return os.WriteFile(file, msg.Content, 0600)
			}

			switch output {
			case "json":
				return formatReadJSON(os.Stdout, msg)
			case "md", "markdown":
				return formatReadMarkdown(os.Stdout, msg)
			default:
				_, err = os.Stdout.Write(msg.Content)
				return err
			}
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "write to file instead of stdout")
	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text, json, or md")
	return cmd
}

func newWatchCmd(d *dmCmd) *cobra.Command {
	var output string

	cmd := &cobra.Command{
		Use:   "watch [peer-pubkey]",
		Short: "Stream realtime DMs to stdout",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			self := d.kp.PublicKey()

			var msgs <-chan *dm.Message
			var errs <-chan error

			if len(args) == 1 {
				sdk, err := d.openConversation(args[0])
				if err != nil {
					return err
				}
				msgs, errs, err = sdk.Subscribe(ctx, dm.ListOptions{})
				if err != nil {
					return err
				}
			} else {
				var err error
				msgs, errs, err = d.threads.SubscribeAll(ctx)
				if err != nil {
					return err
				}
			}

			for {
				select {
				case m, ok := <-msgs:
					if !ok {
						return nil
					}
					switch output {
					case "json":
						_ = formatWatchJSON(os.Stdout, *m, "", self)
					case "md", "markdown":
						formatWatchMarkdown(os.Stdout, *m, "", self)
					default:
						formatWatchText(os.Stdout, *m, self)
					}
				case err, ok := <-errs:
					if !ok {
						return nil
					}
					return err
				case <-ctx.Done():
					return nil
				}
			}
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text, json, or md")
	return cmd
}

func newSearchCmd(d *dmCmd) *cobra.Command {
	var (
		output string
		limit  int
		offset int
	)

	cmd := &cobra.Command{
		Use:   "search <query>",
		Short: "Full-text search across DM messages",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := d.threads.Search(cmd.Context(), args[0], dm.SearchOptions{
				Limit:  limit,
				Offset: offset,
			})
			if err != nil {
				return err
			}
			if len(resp.Results) == 0 {
				fmt.Fprintln(os.Stderr, "no results")
				return nil
			}

			switch output {
			case "json":
				return formatSearchJSON(os.Stdout, resp.Results, resp.TotalCount, args[0], limit, offset)
			case "md", "markdown":
				return formatSearchMarkdown(os.Stdout, resp.Results, resp.TotalCount, args[0], limit, offset)
			default:
				formatSearchText(os.Stdout, resp.Results)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text, json, or md")
	cmd.Flags().IntVar(&limit, "limit", 10, "max results to return")
	cmd.Flags().IntVar(&offset, "offset", 0, "skip first N results")
	return cmd
}

func newFetchCmd(d *dmCmd) *cobra.Command {
	var output string

	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Check the remote search index status",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			info, err := d.threads.FetchRemoteSearchInfo(ctx)
			if err != nil {
				return err
			}
			if info == nil {
				if output == "json" {
					return writeJSON(os.Stdout, map[string]any{"error": "no remote search index"})
				}
				fmt.Fprintln(os.Stderr, "no remote search index")
				return nil
			}

			state := dm.LoadSearchState(d.searchDir)
			state.RemoteHash = info.DBHash
			if err := state.Save(); err != nil {
				return fmt.Errorf("save state: %w", err)
			}

			localHash, _ := d.search.ContentHash(ctx)
			localHex := reference.Hex(localHash)
			localTS, _ := d.search.LastIndexedTimestamp(ctx)
			var localCount int
			_ = d.search.Count(ctx, &localCount)

			if output == "json" {
				return writeJSON(os.Stdout, map[string]any{
					"local_hash":   localHex,
					"local_count":  localCount,
					"remote_hash":  info.DBHash,
					"remote_count": info.EntryCount,
					"in_sync":      localHex == info.DBHash,
				})
			}

			fmt.Fprintf(os.Stderr, "local  %s  %d entries  %s\n", localHex[:8], localCount, formatTimestamp(localTS))
			if info.DBHash != "" {
				fmt.Fprintf(os.Stderr, "remote %s  %d entries  %s\n", info.DBHash[:8], info.EntryCount, formatTimestamp(info.LastUpdate))
			} else {
				fmt.Fprintln(os.Stderr, "remote (no hash)")
			}
			if localHex == info.DBHash {
				fmt.Fprintln(os.Stderr, "up to date")
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text or json")
	return cmd
}

func newPullCmd(d *dmCmd) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "pull",
		Short: "Pull the remote search index if it differs from local",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			state := dm.LoadSearchState(d.searchDir)

			info, err := d.threads.FetchRemoteSearchInfo(ctx)
			if err != nil {
				return err
			}
			if info == nil {
				fmt.Fprintln(os.Stderr, "no remote search index")
				return nil
			}

			localHash, _ := d.search.ContentHash(ctx)
			localHex := reference.Hex(localHash)
			if localHex == info.DBHash {
				fmt.Fprintln(os.Stderr, "already up to date")
				state.RemoteHash = info.DBHash
				return state.Save()
			}

			localTS, _ := d.search.LastIndexedTimestamp(ctx)
			if !force && localTS > 0 && info.LastUpdate > 0 && localTS > info.LastUpdate {
				var localCount int
				_ = d.search.Count(ctx, &localCount)
				fmt.Fprintf(os.Stderr, "warning: local index appears newer than remote\n")
				fmt.Fprintf(os.Stderr, "  local:  %d entries, last-update %s\n", localCount, formatTimestamp(localTS))
				fmt.Fprintf(os.Stderr, "  remote: %d entries, last-update %s\n", info.EntryCount, formatTimestamp(info.LastUpdate))
				fmt.Fprintln(os.Stderr, "use --force to pull anyway")
				return nil
			}

			fmt.Fprintln(os.Stderr, "pulling remote search index...")
			_ = d.search.Close()
			pulled, err := d.threads.PullSearchIndex(ctx, d.searchPath)
			if err != nil {
				d.search, _ = dm.OpenSearchIndex(ctx, d.searchPath)
				d.threads.SetSearchIndex(d.search)
				return err
			}
			d.search = pulled
			d.threads.SetSearchIndex(pulled)

			pulledHash, _ := pulled.ContentHash(ctx)
			state.LocalHash = reference.Hex(pulledHash)
			state.RemoteHash = info.DBHash
			if err := state.Save(); err != nil {
				return fmt.Errorf("save state: %w", err)
			}

			var count int
			_ = pulled.Count(ctx, &count)
			ts, _ := pulled.LastIndexedTimestamp(ctx)
			fmt.Fprintf(os.Stderr, "pulled (%d entries, last-update: %s)\n", count, formatTimestamp(ts))
			return nil
		},
	}
	cmd.Flags().BoolVarP(&force, "force", "f", false, "pull even if local index is newer")
	return cmd
}

func newPushCmd(d *dmCmd) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "push",
		Short: "Push the local search index to the node",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			state := dm.LoadSearchState(d.searchDir)

			localHash, err := d.search.ContentHash(ctx)
			if err != nil {
				return err
			}
			localHex := reference.Hex(localHash)

			if localHex == state.RemoteHash {
				fmt.Fprintln(os.Stderr, "everything up to date")
				return nil
			}

			info, err := d.threads.FetchRemoteSearchInfo(ctx)
			if err == nil && info != nil && info.DBHash == localHex {
				state.LocalHash = localHex
				state.RemoteHash = localHex
				_ = state.Save()
				fmt.Fprintln(os.Stderr, "everything up to date")
				return nil
			}

			if !force && info != nil && info.LastUpdate > 0 {
				localTS, _ := d.search.LastIndexedTimestamp(ctx)
				if localTS > 0 && info.LastUpdate > localTS {
					var localCount int
					_ = d.search.Count(ctx, &localCount)
					fmt.Fprintf(os.Stderr, "warning: remote index appears newer than local\n")
					fmt.Fprintf(os.Stderr, "  local:  %d entries, last-update %s\n", localCount, formatTimestamp(localTS))
					fmt.Fprintf(os.Stderr, "  remote: %d entries, last-update %s\n", info.EntryCount, formatTimestamp(info.LastUpdate))
					fmt.Fprintln(os.Stderr, "use --force to push anyway")
					return nil
				}
			}

			fmt.Fprintln(os.Stderr, "pushing search index...")
			ref, err := d.threads.PushSearchIndex(ctx, d.search)
			if err != nil {
				return err
			}

			state.LocalHash = localHex
			state.RemoteHash = localHex
			if err := state.Save(); err != nil {
				return fmt.Errorf("save state: %w", err)
			}

			fmt.Fprintf(os.Stderr, "pushed %s\n", reference.Hex(ref)[:8])
			return nil
		},
	}
	cmd.Flags().BoolVarP(&force, "force", "f", false, "push even if remote index is newer")
	return cmd
}

func newReindexCmd(d *dmCmd) *cobra.Command {
	return &cobra.Command{
		Use:   "reindex",
		Short: "Rebuild local search index from DM messages on the node",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(os.Stderr, "reindexing messages from node...")
			result, err := d.threads.Reindex(cmd.Context())
			if err != nil {
				return err
			}
			if result.Errors > 0 {
				fmt.Fprintf(os.Stderr, "warning: %d errors during reindex\n", result.Errors)
			}
			ctx := cmd.Context()
			var n int
			_ = d.search.Count(ctx, &n)
			ts, _ := d.search.LastIndexedTimestamp(ctx)
			fmt.Fprintf(os.Stderr, "reindex complete (%d messages, last-update: %d)\n", n, ts)
			return nil
		},
	}
}

func formatTimestamp(ms int64) string {
	if ms == 0 {
		return "—"
	}
	return time.UnixMilli(ms).Format("Jan 2 15:04")
}
