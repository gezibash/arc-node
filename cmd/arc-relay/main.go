package main

import (
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
		Use:   "arc-relay",
		Short: "Arc relay - message routing",
		Long: `Arc relay server and client commands.

Server:
  arc-relay start        Start the relay server

Client:
  arc-relay send         Send an envelope through the relay
  arc-relay listen       Subscribe and receive envelopes
  arc-relay discover     Find capability providers`,
	}

	// Global flags
	rootCmd.PersistentFlags().String("data-dir", "", "data directory (default ~/.arc)")
	_ = v.BindPFlag("data_dir", rootCmd.PersistentFlags().Lookup("data-dir"))

	rootCmd.PersistentFlags().StringP("key", "k", "", "key alias or public key hex")
	_ = v.BindPFlag("key", rootCmd.PersistentFlags().Lookup("key"))

	rootCmd.PersistentFlags().StringP("output", "o", "text", "output format (text, json, markdown)")
	_ = v.BindPFlag("output", rootCmd.PersistentFlags().Lookup("output"))

	// Commands
	rootCmd.AddCommand(
		newStartCmd(),
		newSendCmd(v),
		newListenCmd(v),
		newDiscoverCmd(v),
		newJoinCmd(),
		newMembersCmd(),
		newLeaveCmd(),
	)

	return rootCmd.Execute()
}
