package main

import (
	"fmt"
	"strings"

	"github.com/gezibash/arc-node/internal/config"
	"github.com/gezibash/arc-node/internal/keyring"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newWhoamiCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use:   "whoami",
		Short: "Print the active identity",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dataDir := v.GetString("data_dir")
			if dataDir == "" {
				dataDir = config.DefaultDataDir()
			}
			kr := keyring.New(dataDir)

			name := v.GetString("key")
			var key *keyring.Key
			var err error
			if name != "" {
				key, err = kr.Load(cmd.Context(), name)
			} else {
				key, err = kr.LoadDefault(cmd.Context())
			}
			if err != nil {
				return fmt.Errorf("load key: %w", err)
			}

			infos, err := kr.List(cmd.Context())
			if err != nil {
				return fmt.Errorf("list keys: %w", err)
			}

			fmt.Println(key.PublicKey)
			for _, info := range infos {
				if info.PublicKey == key.PublicKey {
					if len(info.Aliases) > 0 {
						fmt.Printf("  aliases: %s\n", strings.Join(info.Aliases, ", "))
					}
					if info.IsDefault {
						fmt.Println("  default: yes")
					}
					break
				}
			}
			return nil
		},
	}
}
