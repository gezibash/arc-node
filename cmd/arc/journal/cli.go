package journal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/gezibash/arc-node/cmd/arc/render"
	"github.com/gezibash/arc-node/pkg/journal"
	"github.com/gezibash/arc/v2/pkg/reference"
	"github.com/spf13/cobra"
)

func newWriteCmd(j *journalCmd) *cobra.Command {
	var (
		labels []string
		output string
	)

	cmd := &cobra.Command{
		Use:   "write [file | text]",
		Short: "Encrypt and publish a journal entry",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := readInput(args)
			if err != nil {
				return err
			}

			labelMap, err := parseLabels(labels)
			if err != nil {
				return err
			}

			result, err := j.sdk.Write(cmd.Context(), data, labelMap)
			if err != nil {
				return err
			}

			if output == "json" {
				return writeJSON(os.Stdout, map[string]string{
					"reference": reference.Hex(result.Ref),
					"entry_ref": reference.Hex(result.EntryRef),
				})
			}
			fmt.Println(reference.Hex(result.Ref))
			return nil
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text or json")
	cmd.Flags().StringSliceVarP(&labels, "label", "l", nil, "label key=value (repeatable)")
	return cmd
}

func newListCmd(j *journalCmd) *cobra.Command {
	var (
		limit      int
		cursor     string
		descending bool
		labels     []string
		output     string
		preview    bool
		content    bool
	)

	cmd := &cobra.Command{
		Use:   "list [expression]",
		Short: "Query journal entries",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			expr := ""
			if len(args) == 1 {
				expr = args[0]
			}

			labelMap, err := parseLabels(labels)
			if err != nil {
				return err
			}

			result, err := j.sdk.List(cmd.Context(), journal.ListOptions{
				Expression: expr,
				Labels:     labelMap,
				Limit:      limit,
				Cursor:     cursor,
				Descending: descending,
			})
			if err != nil {
				return err
			}

			switch output {
			case "json":
				return formatListJSON(os.Stdout, result, preview, content, j.sdk)
			case "md", "markdown":
				return formatListMarkdown(os.Stdout, result, preview, content, j.sdk)
			default:
				return formatListText(os.Stdout, result, preview, content, j.sdk)
			}
		},
	}

	cmd.Flags().IntVar(&limit, "limit", 10, "max entries to return")
	cmd.Flags().StringVar(&cursor, "cursor", "", "pagination cursor")
	cmd.Flags().BoolVar(&descending, "descending", true, "reverse chronological order")
	cmd.Flags().StringSliceVarP(&labels, "label", "l", nil, "label key=value (repeatable)")
	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text, json, or md")
	cmd.Flags().BoolVar(&preview, "preview", false, "show decrypted content preview")
	cmd.Flags().BoolVar(&content, "content", false, "inline full decrypted content")
	return cmd
}

func newReadCmd(j *journalCmd) *cobra.Command {
	var (
		file   string
		output string
	)

	cmd := &cobra.Command{
		Use:   "read <ref>",
		Short: "Fetch, decrypt, and display a journal entry",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			entry, err := resolveAndRead(cmd.Context(), j, args[0])
			if err != nil {
				return err
			}

			if file != "" {
				return os.WriteFile(file, entry.Content, 0600)
			}

			switch output {
			case "json":
				je := map[string]any{
					"reference": reference.Hex(entry.Ref),
					"labels":    entry.Labels,
					"timestamp": entry.Timestamp,
					"content":   string(entry.Content),
				}
				var zeroRef reference.Reference
				if entry.EntryRef != zeroRef {
					je["entry_ref"] = reference.Hex(entry.EntryRef)
				}
				return writeJSON(os.Stdout, je)
			case "md", "markdown":
				ts := time.UnixMilli(entry.Timestamp)
				short := reference.Hex(entry.Ref)[:8]
				var zeroRef reference.Reference
				if entry.EntryRef != zeroRef {
					short = reference.Hex(entry.EntryRef)[:8]
				}
				_, _ = fmt.Fprintf(os.Stdout, "## %s [%s]\n", ts.Format("2006-01-02 15:04"), short)
				if len(entry.Labels) > 0 {
					var parts []string
					for k, v := range entry.Labels {
						if k == "app" || k == "type" || k == "content" {
							continue
						}
						parts = append(parts, k+"="+v)
					}
					if len(parts) > 0 {
						_, _ = fmt.Fprintf(os.Stdout, "Labels: %s\n", strings.Join(parts, ", "))
					}
				}
				_, _ = fmt.Fprintln(os.Stdout)
				_, err = os.Stdout.Write(entry.Content)
				_, _ = fmt.Fprintln(os.Stdout)
				return err
			default:
				_, err = os.Stdout.Write(entry.Content)
				return err
			}
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "write to file instead of stdout")
	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text, json, or md")
	return cmd
}

// resolveAndRead resolves a ref argument (full hex, short entryRef prefix, or
// content ref) and returns the full decrypted entry with metadata.
func resolveAndRead(ctx context.Context, j *journalCmd, arg string) (*journal.Entry, error) {
	// Try as full 64-char entryRef first.
	if len(arg) == 64 {
		result, err := j.sdk.List(ctx, journal.ListOptions{
			Labels: map[string]string{"entry": arg},
			Limit:  1,
		})
		if err == nil && len(result.Entries) > 0 {
			e := result.Entries[0]
			contentRef, err := reference.FromHex(e.Labels["content"])
			if err != nil {
				return nil, fmt.Errorf("parse content ref: %w", err)
			}
			entry, err := j.sdk.Read(ctx, contentRef)
			if err != nil {
				return nil, err
			}
			entry.EntryRef = e.EntryRef
			entry.Labels = e.Labels
			entry.Timestamp = e.Timestamp
			return entry, nil
		}
	}

	// Try as short entryRef prefix via search index.
	if len(arg) < 64 && len(arg) >= 8 && j.search != nil {
		entryRef, err := j.search.ResolvePrefix(ctx, arg)
		if err == nil {
			entryHex := reference.Hex(entryRef)
			result, err := j.sdk.List(ctx, journal.ListOptions{
				Labels: map[string]string{"entry": entryHex},
				Limit:  1,
			})
			if err == nil && len(result.Entries) > 0 {
				e := result.Entries[0]
				contentRef, err := reference.FromHex(e.Labels["content"])
				if err != nil {
					return nil, fmt.Errorf("parse content ref: %w", err)
				}
				entry, err := j.sdk.Read(ctx, contentRef)
				if err != nil {
					return nil, err
				}
				entry.EntryRef = e.EntryRef
				entry.Labels = e.Labels
				entry.Timestamp = e.Timestamp
				return entry, nil
			}
		}
	}

	// Fall back to treating as content ref.
	ref, err := reference.FromHex(arg)
	if err != nil {
		return nil, fmt.Errorf("invalid reference: %w", err)
	}
	return j.sdk.Read(ctx, ref)
}

func newSearchCmd(j *journalCmd) *cobra.Command {
	var (
		output  string
		readAll bool
		limit   int
		offset  int
	)

	cmd := &cobra.Command{
		Use:   "search <query>",
		Short: "Full-text search across journal entries",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := j.sdk.Search(cmd.Context(), args[0], journal.SearchOptions{
				Limit:  limit,
				Offset: offset,
			})
			if err != nil {
				return err
			}
			results := resp.Results
			if len(results) == 0 {
				fmt.Fprintln(os.Stderr, "no results")
				return nil
			}

			hasMore := resp.TotalCount > offset+len(results)

			// Compute time range.
			var oldest, newest int64
			for i, r := range results {
				if i == 0 || r.Timestamp < oldest {
					oldest = r.Timestamp
				}
				if r.Timestamp > newest {
					newest = r.Timestamp
				}
			}

			meta := render.Metadata{
				Command:      fmt.Sprintf("arc journal search %q", args[0]),
				Query:        args[0],
				TotalCount:   resp.TotalCount,
				ShowingCount: len(results),
				Limit:        limit,
				Offset:       offset,
				HasMore:      hasMore,
				TimeRange:    &render.TimeRange{Oldest: oldest, Newest: newest},
			}

			switch output {
			case "json":
				type jsonSearchResult struct {
					EntryRef  string `json:"entry_ref"`
					Snippet   string `json:"snippet"`
					Timestamp int64  `json:"timestamp"`
					Content   string `json:"content,omitempty"`
				}
				var jsonResults []jsonSearchResult
				for _, r := range results {
					jr := jsonSearchResult{
						EntryRef:  reference.Hex(r.Ref),
						Snippet:   r.Snippet,
						Timestamp: r.Timestamp,
					}
					if readAll {
						entry, err := resolveAndRead(cmd.Context(), j, reference.Hex(r.Ref))
						if err == nil {
							jr.Content = string(entry.Content)
						}
					}
					jsonResults = append(jsonResults, jr)
				}
				return render.JSONEnvelope(os.Stdout, meta, jsonResults)
			case "md", "markdown":
				return render.MarkdownWithFrontmatter(os.Stdout, meta, func(w io.Writer) error {
					_, _ = fmt.Fprintf(w, "## Search: %q\n\n", args[0])
					for _, r := range results {
						short := reference.Hex(r.Ref)[:8]
						ts := time.UnixMilli(r.Timestamp)
						_, _ = fmt.Fprintf(w, "### [%s] %s\n", short, ts.Format("Jan 2 15:04"))
						_, _ = fmt.Fprintln(w, r.Snippet)
						if readAll {
							entry, err := resolveAndRead(cmd.Context(), j, reference.Hex(r.Ref))
							if err == nil {
								_, _ = fmt.Fprintln(w)
								_, _ = fmt.Fprintln(w, string(entry.Content))
							}
						}
						_, _ = fmt.Fprintln(w, "---")
						_, _ = fmt.Fprintln(w)
					}
					return nil
				})
			default:
				for _, r := range results {
					short := reference.Hex(r.Ref)[:8]
					_, _ = fmt.Fprintf(os.Stdout, "%-10s %s\n", short, r.Snippet)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text, json, or md")
	cmd.Flags().BoolVar(&readAll, "read", false, "inline full decrypted content in results")
	cmd.Flags().IntVar(&limit, "limit", 10, "max results to return (0 for unlimited)")
	cmd.Flags().IntVar(&offset, "offset", 0, "skip first N results")
	return cmd
}

func newFetchCmd(j *journalCmd) *cobra.Command {
	var output string

	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Check the remote search index status",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			info, err := j.sdk.FetchRemoteSearchInfo(ctx)
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

			// Update tracked remote hash.
			state := journal.LoadSearchState(j.searchDir)
			state.RemoteHash = info.DBHash
			if err := state.Save(); err != nil {
				return fmt.Errorf("save state: %w", err)
			}

			localHash, _ := j.search.ContentHash(ctx)
			localHex := reference.Hex(localHash)

			localTS, _ := j.search.LastIndexedTimestamp(ctx)
			localCount := entryCount(ctx, j.search)

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

func newPullCmd(j *journalCmd) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "pull",
		Short: "Pull the remote search index if it differs from local",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			state := journal.LoadSearchState(j.searchDir)

			// Fetch remote info.
			info, err := j.sdk.FetchRemoteSearchInfo(ctx)
			if err != nil {
				return err
			}
			if info == nil {
				fmt.Fprintln(os.Stderr, "no remote search index")
				return nil
			}

			// Compare hashes.
			localHash, _ := j.search.ContentHash(ctx)
			localHex := reference.Hex(localHash)
			if localHex == info.DBHash {
				fmt.Fprintln(os.Stderr, "already up to date")
				state.RemoteHash = info.DBHash
				return state.Save()
			}

			// Safety check: warn if local is newer than remote.
			localTS, _ := j.search.LastIndexedTimestamp(ctx)
			if !force && localTS > 0 && info.LastUpdate > 0 && localTS > info.LastUpdate {
				localCount := entryCount(ctx, j.search)
				fmt.Fprintf(os.Stderr, "warning: local index appears newer than remote\n")
				fmt.Fprintf(os.Stderr, "  local:  %d entries, last-update %s\n", localCount, formatTimestamp(localTS))
				fmt.Fprintf(os.Stderr, "  remote: %d entries, last-update %s\n", info.EntryCount, formatTimestamp(info.LastUpdate))
				fmt.Fprintln(os.Stderr, "use --force to pull anyway")
				return nil
			}

			// Download and replace.
			fmt.Fprintln(os.Stderr, "pulling remote search index...")
			_ = j.search.Close()
			pulled, err := j.sdk.PullSearchIndex(ctx, j.searchPath)
			if err != nil {
				// Reopen local on failure.
				j.search, _ = journal.OpenSearchIndex(ctx, j.searchPath)
				j.sdk.SetSearchIndex(j.search)
				return err
			}
			j.search = pulled
			j.sdk.SetSearchIndex(pulled)

			pulledHash, _ := pulled.ContentHash(ctx)
			state.LocalHash = reference.Hex(pulledHash)
			state.RemoteHash = info.DBHash
			if err := state.Save(); err != nil {
				return fmt.Errorf("save state: %w", err)
			}

			ts, _ := pulled.LastIndexedTimestamp(ctx)
			fmt.Fprintf(os.Stderr, "pulled (%d entries, last-update: %s)\n", entryCount(ctx, pulled), formatTimestamp(ts))
			return nil
		},
	}
	cmd.Flags().BoolVarP(&force, "force", "f", false, "pull even if local index is newer")
	return cmd
}

func newPushCmd(j *journalCmd) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "push",
		Short: "Push the local search index to the node",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			state := journal.LoadSearchState(j.searchDir)

			localHash, err := j.search.ContentHash(ctx)
			if err != nil {
				return err
			}
			localHex := reference.Hex(localHash)

			// Check if remote already has this content.
			if localHex == state.RemoteHash {
				fmt.Fprintln(os.Stderr, "everything up to date")
				return nil
			}

			// Double-check against actual remote.
			info, err := j.sdk.FetchRemoteSearchInfo(ctx)
			if err == nil && info != nil && info.DBHash == localHex {
				state.LocalHash = localHex
				state.RemoteHash = localHex
				_ = state.Save()
				fmt.Fprintln(os.Stderr, "everything up to date")
				return nil
			}

			// Safety check: warn if remote is newer than local.
			if !force && info != nil && info.LastUpdate > 0 {
				localTS, _ := j.search.LastIndexedTimestamp(ctx)
				if localTS > 0 && info.LastUpdate > localTS {
					localCount := entryCount(ctx, j.search)
					fmt.Fprintf(os.Stderr, "warning: remote index appears newer than local\n")
					fmt.Fprintf(os.Stderr, "  local:  %d entries, last-update %s\n", localCount, formatTimestamp(localTS))
					fmt.Fprintf(os.Stderr, "  remote: %d entries, last-update %s\n", info.EntryCount, formatTimestamp(info.LastUpdate))
					fmt.Fprintln(os.Stderr, "use --force to push anyway")
					return nil
				}
			}

			fmt.Fprintln(os.Stderr, "pushing search index...")
			ref, err := j.sdk.PushSearchIndex(ctx, j.search)
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

func newReindexCmd(j *journalCmd) *cobra.Command {
	return &cobra.Command{
		Use:   "reindex",
		Short: "Rebuild local search index from journal entries on the node",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(os.Stderr, "reindexing entries from node...")
			if err := j.sdk.Reindex(cmd.Context()); err != nil {
				return err
			}
			ctx := cmd.Context()
			n := entryCount(ctx, j.search)
			ts, _ := j.search.LastIndexedTimestamp(ctx)
			fmt.Fprintf(os.Stderr, "reindex complete (%d entries, last-update: %d)\n", n, ts)
			return nil
		},
	}
}

func entryCount(ctx context.Context, idx *journal.SearchIndex) int {
	if idx == nil {
		return 0
	}
	var n int
	_ = idx.Count(ctx, &n)
	return n
}

func formatTimestamp(ms int64) string {
	if ms == 0 {
		return "(none)"
	}
	return time.UnixMilli(ms).Format("Jan 2, 2006 15:04")
}

func newEditCmd(j *journalCmd) *cobra.Command {
	var output string

	cmd := &cobra.Command{
		Use:   "edit <ref>",
		Short: "Edit an existing journal entry",
		Long:  "Fetches and decrypts the entry, opens $EDITOR, then publishes the replacement.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			entryRef, err := reference.FromHex(args[0])
			if err != nil {
				return fmt.Errorf("invalid reference: %w", err)
			}

			// Look up the entry by its entryRef label to get the msgRef for ReadForEdit.
			result, err := j.sdk.List(cmd.Context(), journal.ListOptions{
				Labels: map[string]string{"entry": reference.Hex(entryRef)},
				Limit:  1,
			})
			if err != nil {
				return fmt.Errorf("lookup entry: %w", err)
			}
			if len(result.Entries) == 0 {
				return fmt.Errorf("entry not found for ref %s", args[0])
			}
			msgRefHex := reference.Hex(result.Entries[0].Ref)

			plaintext, origLabels, err := j.sdk.ReadForEdit(cmd.Context(), msgRefHex)
			if err != nil {
				return err
			}

			tmpFile, err := os.CreateTemp("", "arc-journal-*.txt")
			if err != nil {
				return fmt.Errorf("create temp file: %w", err)
			}
			tmpPath := tmpFile.Name()
			defer func() { _ = os.Remove(tmpPath) }()

			if _, err := tmpFile.Write(plaintext); err != nil {
				_ = tmpFile.Close()
				return fmt.Errorf("write temp file: %w", err)
			}
			_ = tmpFile.Close()

			editor := os.Getenv("EDITOR")
			if editor == "" {
				editor = "vi"
			}
			editorPath, err := exec.LookPath(editor)
			if err != nil {
				return fmt.Errorf("editor %q not found: %w", editor, err)
			}
			editorCmd := exec.CommandContext(cmd.Context(), editorPath, tmpPath)
			editorCmd.Stdin = os.Stdin
			editorCmd.Stdout = os.Stdout
			editorCmd.Stderr = os.Stderr
			if err := editorCmd.Run(); err != nil {
				return fmt.Errorf("editor: %w", err)
			}

			edited, err := os.ReadFile(filepath.Clean(tmpPath))
			if err != nil {
				return fmt.Errorf("read edited file: %w", err)
			}

			if bytes.Equal(plaintext, edited) {
				fmt.Fprintln(os.Stderr, "no changes")
				return nil
			}

			editResult, err := j.sdk.Edit(cmd.Context(), entryRef, edited, origLabels)
			if err != nil {
				return err
			}

			if output == "json" {
				return writeJSON(os.Stdout, map[string]string{
					"reference": reference.Hex(editResult.Ref),
					"entry_ref": reference.Hex(editResult.EntryRef),
				})
			}
			fmt.Println(reference.Hex(editResult.Ref))
			return nil
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text or json")
	return cmd
}
