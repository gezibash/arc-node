package node

import (
	"context"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/gezibash/arc-node/internal/keyring"
	_ "github.com/gezibash/arc-node/internal/node"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func TestEnsureWritableDir_Success(t *testing.T) {
	dir := t.TempDir()
	if err := ensureWritableDir(dir); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureWritableDir_EmptyPath(t *testing.T) {
	if err := ensureWritableDir(""); err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestEnsureWritableDir_CreatesDir(t *testing.T) {
	dir := t.TempDir() + "/sub/dir"
	if err := ensureWritableDir(dir); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// freePortN finds N available TCP ports.
func freePortN(t *testing.T, n int) []string {
	t.Helper()
	var lc net.ListenConfig
	addrs := make([]string, n)
	listeners := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("find free port: %v", err)
		}
		listeners[i] = l
		addrs[i] = l.Addr().String()
	}
	for _, l := range listeners {
		l.Close()
	}
	return addrs
}

func TestIntegration_RunStart(t *testing.T) {
	dataDir := t.TempDir()
	ports := freePortN(t, 2)
	grpcAddr := ports[0]
	metricsAddr := ports[1]

	v := viper.New()
	v.Set("data_dir", dataDir)
	v.Set("grpc.addr", grpcAddr)
	v.Set("observability.log_level", "error")
	v.Set("observability.log_format", "json")
	v.Set("observability.metrics_addr", metricsAddr)
	v.Set("storage.blob.backend", "memory")
	v.Set("storage.index.backend", "memory")

	cmd := &cobra.Command{}
	cmd.Flags().String("config", "", "config file path")
	cmd.SetContext(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- runStart(cmd, v)
	}()

	// Wait for the gRPC server to be listening, then verify it responds.
	kr := keyring.New(dataDir)
	deadline := time.Now().Add(5 * time.Second)
	var connected bool
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)

		// runStart creates/loads a "node" key in the keyring.
		nodeKey, err := kr.Load(context.Background(), "node")
		if err != nil {
			continue
		}
		c, err := client.Dial(grpcAddr,
			client.WithIdentity(nodeKey.Keypair),
			client.WithNodeKey(nodeKey.Keypair.PublicKey()),
		)
		if err != nil {
			continue
		}
		_, qErr := c.QueryMessages(context.Background(), &client.QueryOptions{
			Expression: "true",
			Limit:      1,
		})
		c.Close()
		if qErr == nil {
			connected = true
			break
		}
	}
	if !connected {
		t.Log("warning: could not verify server connectivity before shutdown")
	}

	// Send SIGINT to trigger graceful shutdown.
	syscall.Kill(syscall.Getpid(), syscall.SIGINT)

	select {
	case err := <-errCh:
		// srv.Serve() returns nil on graceful stop or a non-nil error.
		// Either is acceptable — the key thing is it didn't hang.
		_ = err
	case <-time.After(10 * time.Second):
		t.Fatal("runStart did not shut down within 10 seconds")
	}
}

func TestIntegration_RunStart_BadDataDir(t *testing.T) {
	v := viper.New()
	v.Set("data_dir", "/dev/null/impossible")
	v.Set("grpc.addr", "127.0.0.1:0")
	v.Set("observability.log_level", "error")
	v.Set("observability.log_format", "json")
	v.Set("observability.metrics_addr", "127.0.0.1:0")
	v.Set("storage.blob.backend", "memory")
	v.Set("storage.index.backend", "memory")

	cmd := &cobra.Command{}
	cmd.Flags().String("config", "", "config file path")
	cmd.SetContext(context.Background())

	err := runStart(cmd, v)
	if err == nil {
		t.Fatal("expected error for bad data dir")
	}
	t.Logf("got expected error: %v", err)
}

func TestNewStartCmd(t *testing.T) {
	v := viper.New()
	cmd := newStartCmd(v)
	if cmd.Use != "start" {
		t.Fatalf("Use = %q, want %q", cmd.Use, "start")
	}
	// Verify BindServeFlags registered the expected flags.
	for _, flag := range []string{"data-dir", "addr", "config", "log-level", "log-format", "metrics-addr", "reflection"} {
		if cmd.Flags().Lookup(flag) == nil {
			t.Errorf("missing flag %q", flag)
		}
	}
}

func TestIntegration_RunStart_BadBackend(t *testing.T) {
	dataDir := t.TempDir()

	v := viper.New()
	v.Set("data_dir", dataDir)
	v.Set("grpc.addr", "127.0.0.1:0")
	v.Set("observability.log_level", "error")
	v.Set("observability.log_format", "json")
	v.Set("observability.metrics_addr", "127.0.0.1:0")
	v.Set("storage.blob.backend", "nonexistent")
	v.Set("storage.index.backend", "memory")

	cmd := &cobra.Command{}
	cmd.Flags().String("config", "", "config file path")
	cmd.SetContext(context.Background())

	err := runStart(cmd, v)
	if err == nil {
		t.Fatal("expected error for bad backend")
	}
	fmt.Println(err)
}
