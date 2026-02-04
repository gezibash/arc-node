package cli

import (
	"context"

	"github.com/gezibash/arc/v2/internal/config"
	"github.com/gezibash/arc/v2/internal/keyring"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/spf13/viper"
)

// LoadSigner loads a signer from viper configuration.
// Resolution order: key_path flag → key/key_name flag → defaultKeyName.
// Keys are loaded (or auto-generated) from the keyring in data_dir.
func LoadSigner(v *viper.Viper, defaultKeyName string) (identity.Signer, error) {
	dataDir := v.GetString("data_dir")
	if dataDir == "" {
		dataDir = config.DefaultDataDir()
	}

	keyPath := v.GetString("key_path")

	keyName := v.GetString("key")
	if keyName == "" {
		keyName = v.GetString("key_name")
	}
	if keyName == "" {
		keyName = defaultKeyName
	}

	kr := keyring.New(dataDir)
	return kr.LoadSigner(context.Background(), keyName, keyPath)
}
