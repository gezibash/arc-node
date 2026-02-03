package main

import (
	"os"

	"github.com/gezibash/arc-node/cmd/arc/discover"
	"github.com/gezibash/arc-node/cmd/arc/keys"
	"github.com/gezibash/arc-node/cmd/arc/listen"
	"github.com/gezibash/arc-node/cmd/arc/relay"
	"github.com/gezibash/arc-node/cmd/arc/send"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	v := viper.New()

	rootCmd := &cobra.Command{
		Use:   "arc",
		Short: "Arc messaging client",
	}

	rootCmd.PersistentFlags().String("data-dir", "", "data directory (default ~/.arc)")
	_ = v.BindPFlag("data_dir", rootCmd.PersistentFlags().Lookup("data-dir"))

	rootCmd.PersistentFlags().StringP("key", "k", "", "key alias or public key hex")
	_ = v.BindPFlag("key", rootCmd.PersistentFlags().Lookup("key"))

	// Client commands (primary)
	rootCmd.AddCommand(send.Entrypoint(v))
	rootCmd.AddCommand(listen.Entrypoint(v))

	// Admin/utility commands
	rootCmd.AddCommand(relay.Entrypoint(v))
	rootCmd.AddCommand(keys.Entrypoint(v))
	rootCmd.AddCommand(newVersionCmd())
	rootCmd.AddCommand(newCompletionCmd())

	// Discover arc-* binaries in PATH
	for _, cmd := range discover.DiscoverCommands() {
		rootCmd.AddCommand(cmd)
	}

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
