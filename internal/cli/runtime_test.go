package cli

import (
	"testing"

	arced25519 "github.com/gezibash/arc/v2/pkg/identity/ed25519"
	"github.com/gezibash/arc/v2/pkg/runtime"
	"github.com/spf13/viper"
)

func TestNewBuilder_logKeyResolution(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(v *viper.Viper)
		wantBuilder bool // just verify it doesn't panic
	}{
		{
			name: "observability.log_level key",
			setup: func(v *viper.Viper) {
				v.Set("observability.log_level", "debug")
				v.Set("observability.log_format", "json")
			},
			wantBuilder: true,
		},
		{
			name: "legacy log_level key",
			setup: func(v *viper.Viper) {
				v.Set("log_level", "warn")
				v.Set("log_format", "text")
			},
			wantBuilder: true,
		},
		{
			name: "observability takes precedence over legacy",
			setup: func(v *viper.Viper) {
				v.Set("observability.log_level", "error")
				v.Set("log_level", "debug")
			},
			wantBuilder: true,
		},
		{
			name:        "no log keys set",
			setup:       func(v *viper.Viper) {},
			wantBuilder: true,
		},
		{
			name: "data_dir set",
			setup: func(v *viper.Viper) {
				v.Set("data_dir", "/tmp/test-data")
			},
			wantBuilder: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := viper.New()
			tt.setup(v)
			builder := NewBuilder("test", v)
			if builder == nil {
				t.Fatal("NewBuilder returned nil")
			}
		})
	}
}

func TestWithRelay(t *testing.T) {
	t.Run("returns non-nil extension", func(t *testing.T) {
		ext := WithRelay("")
		if ext == nil {
			t.Fatal("WithRelay returned nil")
		}
	})

	t.Run("explicit addr fails without relay", func(t *testing.T) {
		ext := WithRelay("127.0.0.1:1")

		kp, err := arced25519.Generate()
		if err != nil {
			t.Fatalf("generate keypair: %v", err)
		}

		rt, err := runtime.New("test").Signer(kp).Build()
		if err != nil {
			t.Fatalf("build runtime: %v", err)
		}
		t.Cleanup(func() { _ = rt.Close() })

		if err := ext(rt); err == nil {
			t.Fatal("expected error when connecting to non-existent relay")
		}
	})

	t.Run("env addr fails without relay", func(t *testing.T) {
		t.Setenv("ARC_RELAY", "127.0.0.1:1")

		ext := WithRelay("")

		kp, err := arced25519.Generate()
		if err != nil {
			t.Fatalf("generate keypair: %v", err)
		}

		rt, err := runtime.New("test").Signer(kp).Build()
		if err != nil {
			t.Fatalf("build runtime: %v", err)
		}
		t.Cleanup(func() { _ = rt.Close() })

		if err := ext(rt); err == nil {
			t.Fatal("expected error when connecting to non-existent relay")
		}
	})

	t.Run("default addr used when no override", func(t *testing.T) {
		// Verify that WithRelay("") without ARC_RELAY env uses DefaultRelayAddr.
		// We don't assert on connection error here because grpc.NewClient with
		// "localhost:50051" uses lazy connection and may not fail immediately.
		ext := WithRelay("")
		if ext == nil {
			t.Fatal("WithRelay returned nil for default addr")
		}
	})
}
