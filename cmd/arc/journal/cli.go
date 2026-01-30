package journal

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"

	"github.com/gezibash/arc/pkg/reference"
	"github.com/gezibash/arc-node/pkg/journal"
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

func newListCmd(j *journalCmd) *cobra.Command {
	var (
		limit      int
		cursor     string
		descending bool
		labels     []string
		output     string
		preview    bool
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
				return formatListJSON(os.Stdout, result, preview, j.sdk)
			case "md", "markdown":
				return formatListMarkdown(os.Stdout, result, preview, j.sdk)
			default:
				return formatListText(os.Stdout, result, preview, j.sdk)
			}
		},
	}

	cmd.Flags().IntVar(&limit, "limit", 10, "max entries to return")
	cmd.Flags().StringVar(&cursor, "cursor", "", "pagination cursor")
	cmd.Flags().BoolVar(&descending, "descending", true, "reverse chronological order")
	cmd.Flags().StringSliceVarP(&labels, "label", "l", nil, "label key=value (repeatable)")
	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text, json, or md")
	cmd.Flags().BoolVar(&preview, "preview", false, "show decrypted content preview")
	return cmd
}

func newReadCmd(j *journalCmd) *cobra.Command {
	var file string

	cmd := &cobra.Command{
		Use:   "read <ref>",
		Short: "Fetch, decrypt, and display a journal entry",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ref, err := reference.FromHex(args[0])
			if err != nil {
				return fmt.Errorf("invalid reference: %w", err)
			}

			entry, err := j.sdk.Read(cmd.Context(), ref)
			if err != nil {
				return err
			}

			if file != "" {
				return os.WriteFile(file, entry.Content, 0644)
			}
			_, err = os.Stdout.Write(entry.Content)
			return err
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "write to file instead of stdout")
	return cmd
}

func newEditCmd(j *journalCmd) *cobra.Command {
	var output string

	cmd := &cobra.Command{
		Use:   "edit <ref>",
		Short: "Edit an existing journal entry",
		Long:  "Fetches and decrypts the entry, opens $EDITOR, then publishes the replacement.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			msgRef, err := reference.FromHex(args[0])
			if err != nil {
				return fmt.Errorf("invalid reference: %w", err)
			}

			plaintext, origLabels, err := j.sdk.ReadForEdit(cmd.Context(), args[0])
			if err != nil {
				return err
			}

			tmpFile, err := os.CreateTemp("", "arc-journal-*.txt")
			if err != nil {
				return fmt.Errorf("create temp file: %w", err)
			}
			tmpPath := tmpFile.Name()
			defer os.Remove(tmpPath)

			if _, err := tmpFile.Write(plaintext); err != nil {
				tmpFile.Close()
				return fmt.Errorf("write temp file: %w", err)
			}
			tmpFile.Close()

			editor := os.Getenv("EDITOR")
			if editor == "" {
				editor = "vi"
			}
			editorCmd := exec.CommandContext(cmd.Context(), editor, tmpPath)
			editorCmd.Stdin = os.Stdin
			editorCmd.Stdout = os.Stdout
			editorCmd.Stderr = os.Stderr
			if err := editorCmd.Run(); err != nil {
				return fmt.Errorf("editor: %w", err)
			}

			edited, err := os.ReadFile(tmpPath)
			if err != nil {
				return fmt.Errorf("read edited file: %w", err)
			}

			if bytes.Equal(plaintext, edited) {
				fmt.Fprintln(os.Stderr, "no changes")
				return nil
			}

			result, err := j.sdk.Edit(cmd.Context(), msgRef, edited, origLabels)
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
	return cmd
}
