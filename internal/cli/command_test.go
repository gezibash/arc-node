package cli

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gezibash/arc/v2/internal/keyring"
	"github.com/gezibash/arc/v2/pkg/runtime"
	"github.com/spf13/viper"
)

func TestRunCommand(t *testing.T) {
	t.Run("executes run function with runtime", func(t *testing.T) {
		dir := t.TempDir()
		kr := keyring.New(dir)
		_, err := kr.Generate(context.Background(), "test-cmd")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}

		v := viper.New()
		v.Set("data_dir", dir)

		var called bool
		err = RunCommand(CommandConfig{
			Name:    "test-cmd",
			KeyName: "test-cmd",
			Viper:   v,
			Run: func(ctx context.Context, rt *runtime.Runtime, out *Output) error {
				called = true
				if rt == nil {
					return fmt.Errorf("runtime is nil")
				}
				if out == nil {
					return fmt.Errorf("output is nil")
				}
				if rt.Name() != "test-cmd" {
					return fmt.Errorf("name = %q, want %q", rt.Name(), "test-cmd")
				}
				return nil
			},
		})

		if err != nil {
			t.Fatalf("RunCommand() error = %v", err)
		}
		if !called {
			t.Fatal("Run function was not called")
		}
	})

	t.Run("applies timeout to context", func(t *testing.T) {
		dir := t.TempDir()
		kr := keyring.New(dir)
		_, err := kr.Generate(context.Background(), "timeout-test")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}

		v := viper.New()
		v.Set("data_dir", dir)

		err = RunCommand(CommandConfig{
			Name:    "timeout-test",
			KeyName: "timeout-test",
			Viper:   v,
			Timeout: 5 * time.Second,
			Run: func(ctx context.Context, rt *runtime.Runtime, out *Output) error {
				deadline, ok := ctx.Deadline()
				if !ok {
					return fmt.Errorf("context should have deadline")
				}
				if time.Until(deadline) > 6*time.Second {
					return fmt.Errorf("deadline too far: %v", time.Until(deadline))
				}
				return nil
			},
		})

		if err != nil {
			t.Fatalf("RunCommand() error = %v", err)
		}
	})

	t.Run("applies extensions", func(t *testing.T) {
		dir := t.TempDir()
		kr := keyring.New(dir)
		_, err := kr.Generate(context.Background(), "ext-test")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}

		v := viper.New()
		v.Set("data_dir", dir)

		var extApplied bool
		testExt := func(rt *runtime.Runtime) error {
			extApplied = true
			rt.Set("test-component", "hello")
			return nil
		}

		err = RunCommand(CommandConfig{
			Name:    "ext-test",
			KeyName: "ext-test",
			Viper:   v,
			Extensions: []runtime.Extension{
				testExt,
			},
			Run: func(ctx context.Context, rt *runtime.Runtime, out *Output) error {
				val, ok := rt.Get("test-component").(string)
				if !ok || val != "hello" {
					return fmt.Errorf("extension did not register component")
				}
				return nil
			},
		})

		if err != nil {
			t.Fatalf("RunCommand() error = %v", err)
		}
		if !extApplied {
			t.Fatal("extension was not applied")
		}
	})

	t.Run("propagates run error", func(t *testing.T) {
		dir := t.TempDir()
		kr := keyring.New(dir)
		_, err := kr.Generate(context.Background(), "err-test")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}

		v := viper.New()
		v.Set("data_dir", dir)

		err = RunCommand(CommandConfig{
			Name:    "err-test",
			KeyName: "err-test",
			Viper:   v,
			Run: func(ctx context.Context, rt *runtime.Runtime, out *Output) error {
				return fmt.Errorf("business logic failed")
			},
		})

		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != "business logic failed" {
			t.Errorf("error = %q, want %q", err.Error(), "business logic failed")
		}
	})

	t.Run("propagates extension error", func(t *testing.T) {
		dir := t.TempDir()
		kr := keyring.New(dir)
		_, err := kr.Generate(context.Background(), "ext-err")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}

		v := viper.New()
		v.Set("data_dir", dir)

		badExt := func(rt *runtime.Runtime) error {
			return fmt.Errorf("extension init failed")
		}

		err = RunCommand(CommandConfig{
			Name:    "ext-err",
			KeyName: "ext-err",
			Viper:   v,
			Extensions: []runtime.Extension{
				badExt,
			},
			Run: func(ctx context.Context, rt *runtime.Runtime, out *Output) error {
				t.Fatal("run should not be called when extension fails")
				return nil
			},
		})

		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("creates output with format from viper", func(t *testing.T) {
		dir := t.TempDir()
		kr := keyring.New(dir)
		_, err := kr.Generate(context.Background(), "output-test")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}

		v := viper.New()
		v.Set("data_dir", dir)
		v.Set("output", "json")

		err = RunCommand(CommandConfig{
			Name:    "output-test",
			KeyName: "output-test",
			Viper:   v,
			Run: func(ctx context.Context, rt *runtime.Runtime, out *Output) error {
				if out.Format() != FormatJSON {
					return fmt.Errorf("format = %q, want %q", out.Format(), FormatJSON)
				}
				return nil
			},
		})

		if err != nil {
			t.Fatalf("RunCommand() error = %v", err)
		}
	})

	t.Run("auto-generates key when not found", func(t *testing.T) {
		dir := t.TempDir()

		v := viper.New()
		v.Set("data_dir", dir)

		err := RunCommand(CommandConfig{
			Name:    "auto-gen-test",
			KeyName: "new-key",
			Viper:   v,
			Run: func(ctx context.Context, rt *runtime.Runtime, out *Output) error {
				if rt.Signer() == nil {
					return fmt.Errorf("signer is nil")
				}
				return nil
			},
		})

		if err != nil {
			t.Fatalf("RunCommand() error = %v", err)
		}

		// Verify key was persisted
		kr := keyring.New(dir)
		_, err = kr.Load(context.Background(), "new-key")
		if err != nil {
			t.Fatalf("auto-generated key not found: %v", err)
		}
	})
}

func TestRunCommand_validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     CommandConfig
		wantErr string
	}{
		{
			name:    "missing name",
			cfg:     CommandConfig{Viper: viper.New(), Run: func(context.Context, *runtime.Runtime, *Output) error { return nil }},
			wantErr: "command name required",
		},
		{
			name:    "missing viper",
			cfg:     CommandConfig{Name: "test", Run: func(context.Context, *runtime.Runtime, *Output) error { return nil }},
			wantErr: "viper required",
		},
		{
			name:    "missing run",
			cfg:     CommandConfig{Name: "test", Viper: viper.New()},
			wantErr: "run function required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := RunCommand(tt.cfg)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}
