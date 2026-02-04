package main

import (
	"os"

	"github.com/gezibash/arc/v2/cmd/arc/discover"
	"github.com/gezibash/arc/v2/cmd/arc/keys"
	"github.com/gezibash/arc/v2/cmd/arc/names"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	v := viper.New()

	rootCmd := &cobra.Command{
		Use:   "arc",
		Short: "Arc - identity and messaging",
		Long: `Arc is a decentralized messaging system built on cryptographic identity.

Core commands (built-in):
  arc keys          Manage cryptographic keys
  arc names         Manage local name directory

Plugin commands (discovered from PATH):
  arc relay         Message routing (arc-relay)
  arc blob          Content storage (arc-blob)

Plugins are discovered automatically from arc-* binaries in your PATH.`,
	}

	rootCmd.PersistentFlags().String("data-dir", "", "data directory (default ~/.arc)")
	_ = v.BindPFlag("data_dir", rootCmd.PersistentFlags().Lookup("data-dir"))

	rootCmd.PersistentFlags().StringP("key", "k", "", "key alias or public key hex")
	_ = v.BindPFlag("key", rootCmd.PersistentFlags().Lookup("key"))

	rootCmd.PersistentFlags().StringP("output", "o", "text", "output format (text, json, markdown)")
	_ = v.BindPFlag("output", rootCmd.PersistentFlags().Lookup("output"))

	// Core commands (identity + local names)
	rootCmd.AddCommand(keys.Entrypoint(v))
	rootCmd.AddCommand(names.Entrypoint(v))
	rootCmd.AddCommand(newVersionCmd())
	rootCmd.AddCommand(newCompletionCmd())

	// Discover arc-* plugin binaries in PATH (relay, blob, etc.)
	for _, cmd := range discover.DiscoverCommands() {
		rootCmd.AddCommand(cmd)
	}

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
