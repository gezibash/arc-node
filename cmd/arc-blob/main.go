package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	v := viper.New()

	rootCmd := &cobra.Command{
		Use:   "arc-blob",
		Short: "Arc blob storage capability",
		Long: `Arc blob storage - content-addressed storage for Arc.

Server commands:
  arc-blob start           Run blob storage server

Client commands:
  arc-blob put <file>      Store a blob
  arc-blob get <cid>       Retrieve a blob
  arc-blob want <cid>      Discover who has a blob
  arc-blob discover        Discover blob providers`,
	}

	// Global flags for client commands
	rootCmd.PersistentFlags().String("data-dir", "", "data directory (default ~/.arc)")
	_ = v.BindPFlag("data_dir", rootCmd.PersistentFlags().Lookup("data-dir"))

	rootCmd.PersistentFlags().StringP("key", "k", "", "key alias or public key hex")
	_ = v.BindPFlag("key", rootCmd.PersistentFlags().Lookup("key"))

	rootCmd.PersistentFlags().StringP("output", "o", "text", "output format (text, json, markdown)")
	_ = v.BindPFlag("output", rootCmd.PersistentFlags().Lookup("output"))

	// Server command
	rootCmd.AddCommand(newStartCmd())

	// Client commands
	rootCmd.AddCommand(newPutCmd(v))
	rootCmd.AddCommand(newGetCmd(v))
	rootCmd.AddCommand(newWantCmd(v))
	rootCmd.AddCommand(newDiscoverCmd(v))

	return rootCmd.ExecuteContext(context.Background())
}
