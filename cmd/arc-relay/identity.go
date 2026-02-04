package main

import (
	"context"

	"github.com/gezibash/arc/v2/internal/config"
	"github.com/gezibash/arc/v2/internal/keyring"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/spf13/viper"
)

func loadSigner(v *viper.Viper, defaultName string) (identity.Signer, error) {
	dataDir := v.GetString("data_dir")
	if dataDir == "" {
		dataDir = config.DefaultDataDir()
	}

	keyName := v.GetString("key")
	if keyName == "" {
		keyName = defaultName
	}

	kr := keyring.New(dataDir)
	return kr.LoadSigner(context.Background(), keyName, "")
}
