package main

import (
	"os"

	"github.com/gezibash/arc-node/cmd/arc/journal"
	"github.com/gezibash/arc-node/cmd/arc/keys"
	"github.com/gezibash/arc-node/cmd/arc/node"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	v := viper.New()

	rootCmd := &cobra.Command{
		Use:   "arc",
		Short: "Arc node",
	}

	rootCmd.PersistentFlags().String("data-dir", "", "data directory (default ~/.arc)")
	_ = v.BindPFlag("data_dir", rootCmd.PersistentFlags().Lookup("data-dir"))

	rootCmd.PersistentFlags().StringP("key", "k", "", "key alias or public key hex")
	_ = v.BindPFlag("key", rootCmd.PersistentFlags().Lookup("key"))

	rootCmd.AddCommand(node.Entrypoint(v))
	rootCmd.AddCommand(keys.Entrypoint(v))
	rootCmd.AddCommand(journal.Entrypoint(v))
	rootCmd.AddCommand(newVersionCmd())
	rootCmd.AddCommand(newWhoamiCmd(v))
	rootCmd.AddCommand(newCompletionCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
