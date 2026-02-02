package node

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/reference"
)

func TestGetCmd_FullRef(t *testing.T) {
	data := []byte("blob content")
	ref := reference.Compute(data)
	refHex := reference.Hex(ref)

	mc := &mockClient{
		getContentFn: func(_ context.Context, r reference.Reference) ([]byte, error) {
			if r != ref {
				return nil, fmt.Errorf("unexpected ref")
			}
			return data, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newGetCmd(n)
	cmd.SetArgs([]string{refHex})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	if buf.String() != "blob content" {
		t.Fatalf("got %q, want %q", buf.String(), "blob content")
	}
}

func TestGetCmd_WriteToFile(t *testing.T) {
	data := []byte("file output")
	ref := reference.Compute(data)

	mc := &mockClient{
		getContentFn: func(_ context.Context, _ reference.Reference) ([]byte, error) {
			return data, nil
		},
	}

	n := &nodeCmd{client: mc}
	dir := t.TempDir()
	outPath := filepath.Join(dir, "out.bin")

	cmd := newGetCmd(n)
	cmd.SetArgs([]string{reference.Hex(ref), "-f", outPath})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("file content = %q, want %q", got, data)
	}
}

func TestGetCmd_Prefix_Blob(t *testing.T) {
	ref := reference.Compute([]byte("test"))

	mc := &mockClient{
		resolveGetFn: func(_ context.Context, prefix string) (*client.GetResult, error) {
			return &client.GetResult{
				Kind: client.GetKindBlob,
				Ref:  ref,
				Data: []byte("resolved blob"),
			}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newGetCmd(n)
	cmd.SetArgs([]string{"abcd1234"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	if buf.String() != "resolved blob" {
		t.Fatalf("got %q, want %q", buf.String(), "resolved blob")
	}
}

func TestGetCmd_Prefix_Message(t *testing.T) {
	ref := reference.Compute([]byte("msg"))

	mc := &mockClient{
		resolveGetFn: func(_ context.Context, _ string) (*client.GetResult, error) {
			return &client.GetResult{
				Kind:      client.GetKindMessage,
				Ref:       ref,
				Labels:    map[string]string{"app": "test"},
				Timestamp: 1700000000000,
			}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newGetCmd(n)
	cmd.SetArgs([]string{"abcd"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	out := buf.String()
	if !strings.Contains(out, "ref:") {
		t.Fatalf("expected message output, got %q", out)
	}
	if !strings.Contains(out, "app: test") {
		t.Fatalf("expected label in output, got %q", out)
	}
}

func TestGetCmd_NotFound(t *testing.T) {
	mc := &mockClient{
		getContentFn: func(_ context.Context, _ reference.Reference) ([]byte, error) {
			return nil, fmt.Errorf("not found")
		},
	}

	n := &nodeCmd{client: mc}
	ref := reference.Compute([]byte("missing"))

	cmd := newGetCmd(n)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{reference.Hex(ref)})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error")
	}
}
