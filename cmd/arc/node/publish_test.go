package node

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
	"github.com/spf13/viper"
)

func TestPublishCmd_Success(t *testing.T) {
	kp := testKeypair(t)
	nodeKP := testKeypair(t)
	contentRef := reference.Compute([]byte("hello"))
	msgRef := reference.Compute([]byte("msg"))

	var gotLabels map[string]string
	mc := &mockClient{
		putContentFn: func(_ context.Context, data []byte) (reference.Reference, error) {
			if !bytes.Equal(data, []byte("hello")) {
				t.Errorf("data = %q, want %q", data, "hello")
			}
			return contentRef, nil
		},
		sendMessageFn: func(_ context.Context, msg message.Message, labels map[string]string, dims *nodev1.Dimensions) (*client.PublishResult, error) {
			gotLabels = labels
			if dims.Persistence != nodev1.Persistence_PERSISTENCE_DURABLE {
				t.Errorf("persistence = %v, want durable", dims.Persistence)
			}
			return &client.PublishResult{Ref: msgRef}, nil
		},
	}

	kl := &mockKeyLoader{
		loadFn: func(_ context.Context, name string) (*identity.Keypair, error) {
			if name == "node" {
				return nodeKP, nil
			}
			return kp, nil
		},
		loadDefaultFn: func(_ context.Context) (*identity.Keypair, error) {
			return kp, nil
		},
	}

	n := &nodeCmd{
		v:         viper.New(),
		client:    mc,
		keyLoader: kl,
		readInput: func(_ []string) ([]byte, error) { return []byte("hello"), nil },
	}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newPublishCmd(n)
	cmd.SetArgs([]string{"-l", "app=test"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotLabels["app"] != "test" {
		t.Errorf("labels = %v, want app=test", gotLabels)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	if got := buf.String(); got != reference.Hex(msgRef)+"\n" {
		t.Errorf("output = %q, want %q", got, reference.Hex(msgRef)+"\n")
	}
}

func TestPublishCmd_WithExplicitTo(t *testing.T) {
	kp := testKeypair(t)
	recipientKP := testKeypair(t)
	pub := recipientKP.PublicKey()
	recipientHex := hex.EncodeToString(pub[:])

	mc := &mockClient{
		putContentFn: func(_ context.Context, _ []byte) (reference.Reference, error) {
			return reference.Compute([]byte("x")), nil
		},
		sendMessageFn: func(_ context.Context, msg message.Message, _ map[string]string, _ *nodev1.Dimensions) (*client.PublishResult, error) {
			if msg.To != recipientKP.PublicKey() {
				t.Errorf("msg.To = %x, want %x", msg.To, recipientKP.PublicKey())
			}
			return &client.PublishResult{Ref: reference.Compute([]byte("msg"))}, nil
		},
	}

	n := &nodeCmd{
		v:         viper.New(),
		client:    mc,
		keyLoader: staticKeyLoader(kp),
		readInput: func(_ []string) ([]byte, error) { return []byte("data"), nil },
	}

	cmd := newPublishCmd(n)
	cmd.SilenceUsage = true

	// redirect stdout
	old := os.Stdout
	_, w, _ := os.Pipe()
	os.Stdout = w

	cmd.SetArgs([]string{"--to", recipientHex})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPublishCmd_JSONOutput(t *testing.T) {
	kp := testKeypair(t)
	nodeKP := testKeypair(t)

	mc := &mockClient{
		putContentFn: func(_ context.Context, _ []byte) (reference.Reference, error) {
			return reference.Compute([]byte("x")), nil
		},
		sendMessageFn: func(_ context.Context, _ message.Message, _ map[string]string, _ *nodev1.Dimensions) (*client.PublishResult, error) {
			return &client.PublishResult{Ref: reference.Compute([]byte("msg"))}, nil
		},
	}

	n := &nodeCmd{
		v:      viper.New(),
		client: mc,
		keyLoader: &mockKeyLoader{
			loadFn:        func(_ context.Context, name string) (*identity.Keypair, error) { return nodeKP, nil },
			loadDefaultFn: func(_ context.Context) (*identity.Keypair, error) { return kp, nil },
		},
		readInput: func(_ []string) ([]byte, error) { return []byte("data"), nil },
	}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newPublishCmd(n)
	cmd.SetArgs([]string{"-o", "json"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	if !strings.Contains(buf.String(), `"reference"`) {
		t.Fatalf("expected JSON output, got %q", buf.String())
	}
}

func TestPublishCmd_ContentTypeFlag(t *testing.T) {
	kp := testKeypair(t)
	nodeKP := testKeypair(t)

	var gotMsg message.Message
	mc := &mockClient{
		putContentFn: func(_ context.Context, _ []byte) (reference.Reference, error) {
			return reference.Compute([]byte("x")), nil
		},
		sendMessageFn: func(_ context.Context, msg message.Message, _ map[string]string, _ *nodev1.Dimensions) (*client.PublishResult, error) {
			gotMsg = msg
			return &client.PublishResult{Ref: reference.Compute([]byte("msg"))}, nil
		},
	}

	n := &nodeCmd{
		v:      viper.New(),
		client: mc,
		keyLoader: &mockKeyLoader{
			loadFn:        func(_ context.Context, _ string) (*identity.Keypair, error) { return nodeKP, nil },
			loadDefaultFn: func(_ context.Context) (*identity.Keypair, error) { return kp, nil },
		},
		readInput: func(_ []string) ([]byte, error) { return []byte("data"), nil },
	}

	old := os.Stdout
	_, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newPublishCmd(n)
	cmd.SetArgs([]string{"--content-type", "text/markdown"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotMsg.ContentType != "text/markdown" {
		t.Errorf("content type = %q, want %q", gotMsg.ContentType, "text/markdown")
	}
}

func TestPublishCmd_KeyLoadError(t *testing.T) {
	mc := &mockClient{}

	n := &nodeCmd{
		v:      viper.New(),
		client: mc,
		keyLoader: &mockKeyLoader{
			loadDefaultFn: func(_ context.Context) (*identity.Keypair, error) {
				return nil, fmt.Errorf("no keys")
			},
		},
		readInput: func(_ []string) ([]byte, error) { return []byte("data"), nil },
	}

	cmd := newPublishCmd(n)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPublishCmd_InvalidTo(t *testing.T) {
	kp := testKeypair(t)

	mc := &mockClient{
		putContentFn: func(_ context.Context, _ []byte) (reference.Reference, error) {
			return reference.Compute([]byte("x")), nil
		},
	}

	n := &nodeCmd{
		v:         viper.New(),
		client:    mc,
		keyLoader: staticKeyLoader(kp),
		readInput: func(_ []string) ([]byte, error) { return []byte("data"), nil },
	}

	cmd := newPublishCmd(n)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{"--to", "invalid-hex"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --to")
	}
}

func TestPublishCmd_PutError(t *testing.T) {
	kp := testKeypair(t)

	mc := &mockClient{
		putContentFn: func(_ context.Context, _ []byte) (reference.Reference, error) {
			return reference.Reference{}, fmt.Errorf("storage full")
		},
	}

	n := &nodeCmd{
		v:         viper.New(),
		client:    mc,
		keyLoader: staticKeyLoader(kp),
		readInput: func(_ []string) ([]byte, error) { return []byte("data"), nil },
	}

	cmd := newPublishCmd(n)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error")
	}
}
