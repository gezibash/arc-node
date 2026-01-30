package keys

import (
	"encoding/json"
	"io"

	"github.com/gezibash/arc-node/internal/config"
	"github.com/gezibash/arc-node/internal/keyring"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func Entrypoint(v *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "keys",
		Short: "Manage keypairs",
		Long:  "Manage Ed25519 keys with alias support.\nKeys are stored in <data-dir>/keys/ with a keyring.json alias map.",
	}

	cmd.AddCommand(
		newGenerateCmd(v),
		newImportCmd(v),
		newListCmd(v),
		newShowCmd(v),
		newAliasCmd(v),
		newDefaultCmd(v),
		newDeleteCmd(v),
		newExportCmd(v),
	)

	return cmd
}

func dataDir(v *viper.Viper) string {
	if d := v.GetString("data_dir"); d != "" {
		return d
	}
	return config.DefaultDataDir()
}

func openKeyring(v *viper.Viper) *keyring.Keyring {
	return keyring.New(dataDir(v))
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
