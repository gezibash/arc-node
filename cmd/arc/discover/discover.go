package discover

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

// DiscoverCommands finds arc-* binaries in PATH and creates delegating commands.
func DiscoverCommands() []*cobra.Command {
	var commands []*cobra.Command

	// Find arc-* binaries in PATH
	pathDirs := filepath.SplitList(os.Getenv("PATH"))
	seen := make(map[string]bool)

	for _, dir := range pathDirs {
		matches, _ := filepath.Glob(filepath.Join(dir, "arc-*"))
		for _, match := range matches {
			info, err := os.Stat(match)
			if err != nil || info.IsDir() {
				continue
			}
			if info.Mode()&0111 == 0 {
				continue // not executable
			}

			name := filepath.Base(match)
			subCmd := strings.TrimPrefix(name, "arc-")

			if seen[subCmd] {
				continue
			}
			seen[subCmd] = true

			cmd := newDelegateCmd(subCmd, match)
			commands = append(commands, cmd)
		}
	}

	return commands
}

func newDelegateCmd(name, binPath string) *cobra.Command {
	return &cobra.Command{
		Use:                name,
		Short:              "Delegates to " + filepath.Base(binPath),
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return delegate(cmd.Context(), binPath, args)
		},
	}
}

func delegate(ctx context.Context, binPath string, args []string) error {
	cmd := exec.CommandContext(ctx, binPath, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
