package cli

import (
	"context"
	"os"
	"testing"

	"github.com/gezibash/arc/v2/internal/keyring"
	"github.com/spf13/viper"
)

func TestLoadSigner(t *testing.T) {
	t.Run("loads by default key name", func(t *testing.T) {
		dir := t.TempDir()
		kr := keyring.New(dir)

		// Generate a key with the default name
		key, err := kr.Generate(context.Background(), "blob")
		if err != nil {
			t.Fatalf("Generate key: %v", err)
		}

		v := viper.New()
		v.Set("data_dir", dir)

		signer, err := LoadSigner(v, "blob")
		if err != nil {
			t.Fatalf("LoadSigner() error = %v", err)
		}

		pk := signer.PublicKey()
		if pk.Algo != key.Keypair.PublicKey().Algo {
			t.Errorf("Algo = %q, want %q", pk.Algo, key.Keypair.PublicKey().Algo)
		}
	})

	t.Run("prefers key flag over default", func(t *testing.T) {
		dir := t.TempDir()
		kr := keyring.New(dir)

		// Generate two keys
		_, err := kr.Generate(context.Background(), "default-key")
		if err != nil {
			t.Fatalf("Generate default key: %v", err)
		}
		customKey, err := kr.Generate(context.Background(), "custom-key")
		if err != nil {
			t.Fatalf("Generate custom key: %v", err)
		}

		v := viper.New()
		v.Set("data_dir", dir)
		v.Set("key", "custom-key") // flag takes priority

		signer, err := LoadSigner(v, "default-key")
		if err != nil {
			t.Fatalf("LoadSigner() error = %v", err)
		}

		got := signer.PublicKey()
		want := customKey.Keypair.PublicKey()
		if got.Algo != want.Algo {
			t.Errorf("wrong key loaded")
		}
	})

	t.Run("prefers key_name over default", func(t *testing.T) {
		dir := t.TempDir()
		kr := keyring.New(dir)

		namedKey, err := kr.Generate(context.Background(), "named")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}

		v := viper.New()
		v.Set("data_dir", dir)
		v.Set("key_name", "named")

		signer, err := LoadSigner(v, "fallback")
		if err != nil {
			t.Fatalf("LoadSigner() error = %v", err)
		}

		got := signer.PublicKey()
		want := namedKey.Keypair.PublicKey()
		if got.Algo != want.Algo {
			t.Errorf("wrong key loaded")
		}
	})

	t.Run("loads from key_path", func(t *testing.T) {
		dir := t.TempDir()
		kr := keyring.New(dir)

		// Generate and export seed to a file
		key, err := kr.Generate(context.Background(), "test")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}

		seedPath := dir + "/seed.key"
		seed := key.Keypair.Seed()
		if err := writeFile(seedPath, seed); err != nil {
			t.Fatalf("write seed: %v", err)
		}

		v := viper.New()
		v.Set("data_dir", t.TempDir()) // different dir, no keys here
		v.Set("key_path", seedPath)

		signer, err := LoadSigner(v, "anything")
		if err != nil {
			t.Fatalf("LoadSigner() error = %v", err)
		}

		got := signer.PublicKey()
		want := key.Keypair.PublicKey()
		if got.Algo != want.Algo {
			t.Errorf("Algo mismatch")
		}
	})

	t.Run("auto-generates when key does not exist", func(t *testing.T) {
		dir := t.TempDir()

		v := viper.New()
		v.Set("data_dir", dir)

		signer, err := LoadSigner(v, "auto-gen")
		if err != nil {
			t.Fatalf("LoadSigner() error = %v", err)
		}

		if signer == nil {
			t.Fatal("expected signer, got nil")
		}

		// Verify the key was persisted
		kr := keyring.New(dir)
		key, err := kr.Load(context.Background(), "auto-gen")
		if err != nil {
			t.Fatalf("key should have been auto-generated: %v", err)
		}
		if key == nil {
			t.Fatal("key not found after auto-generate")
		}
	})
}

func writeFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0600)
}
