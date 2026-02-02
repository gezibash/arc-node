package node

import (
	"context"
	"testing"

	"github.com/gezibash/arc-node/internal/keyring"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func TestLoadKeypair_WithKeyLoader(t *testing.T) {
	kp := testKeypair(t)
	n := &nodeCmd{
		v:         viper.New(),
		keyLoader: staticKeyLoader(kp),
	}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	got, err := n.loadKeypair(cmd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PublicKey() != kp.PublicKey() {
		t.Fatalf("wrong keypair returned")
	}
}

func TestLoadKeypair_WithKeyLoaderNamed(t *testing.T) {
	kp := testKeypair(t)
	v := viper.New()
	v.Set("key", "mykey")

	var gotName string
	n := &nodeCmd{
		v: v,
		keyLoader: &mockKeyLoader{
			loadFn: func(_ context.Context, name string) (*identity.Keypair, error) {
				gotName = name
				return kp, nil
			},
		},
	}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	_, err := n.loadKeypair(cmd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotName != "mykey" {
		t.Fatalf("expected name %q, got %q", "mykey", gotName)
	}
}

func TestLoadKeypair_RealKeyring(t *testing.T) {
	dir := t.TempDir()
	kr := keyring.New(dir)

	key, err := kr.Generate(context.Background(), "default")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if err := kr.SetDefault("default"); err != nil {
		t.Fatalf("set default: %v", err)
	}

	v := viper.New()
	v.Set("data_dir", dir)

	n := &nodeCmd{v: v} // no keyLoader → real keyring path
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	got, err := n.loadKeypair(cmd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PublicKey() != key.Keypair.PublicKey() {
		t.Fatalf("wrong keypair: got %x, want %x", got.PublicKey(), key.Keypair.PublicKey())
	}
}

func TestLoadNodeKey_WithKeyLoader(t *testing.T) {
	kp := testKeypair(t)
	n := &nodeCmd{
		v:         viper.New(),
		keyLoader: staticKeyLoader(kp),
	}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	pub, err := n.loadNodeKey(cmd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pub != kp.PublicKey() {
		t.Fatalf("wrong public key")
	}
}

func TestLoadNodeKey_RealKeyring(t *testing.T) {
	dir := t.TempDir()
	kr := keyring.New(dir)

	key, err := kr.Generate(context.Background(), "node")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	v := viper.New()
	v.Set("data_dir", dir)

	n := &nodeCmd{v: v}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	pub, err := n.loadNodeKey(cmd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pub != key.Keypair.PublicKey() {
		t.Fatalf("wrong public key")
	}
}

func TestReadInputFn_WithOverride(t *testing.T) {
	n := &nodeCmd{
		readInput: func(args []string) ([]byte, error) {
			return []byte("override"), nil
		},
	}
	data, err := n.readInputFn([]string{"anything"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "override" {
		t.Fatalf("got %q, want %q", data, "override")
	}
}

func TestReadInputFn_Fallback(t *testing.T) {
	n := &nodeCmd{} // no readInput override
	data, err := n.readInputFn([]string{"literal text"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "literal text" {
		t.Fatalf("got %q, want %q", data, "literal text")
	}
}

func TestOpenKeyring(t *testing.T) {
	dir := t.TempDir()
	v := viper.New()
	v.Set("data_dir", dir)

	kr := openKeyring(v)
	if kr == nil {
		t.Fatal("expected non-nil keyring")
	}
}

func TestOpenKeyring_DefaultDir(t *testing.T) {
	v := viper.New()
	kr := openKeyring(v)
	if kr == nil {
		t.Fatal("expected non-nil keyring")
	}
}
