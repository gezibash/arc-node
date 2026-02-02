package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/gezibash/arc/v2/pkg/reference"
)

func TestPutCmd_Success(t *testing.T) {
	var captured []byte
	ref := reference.Compute([]byte("test data"))

	mc := &mockClient{
		putContentFn: func(_ context.Context, data []byte) (reference.Reference, error) {
			captured = data
			return ref, nil
		},
	}

	n := &nodeCmd{client: mc, readInput: func(args []string) ([]byte, error) {
		return []byte("test data"), nil
	}}

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newPutCmd(n)
	cmd.SetArgs([]string{})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(captured, []byte("test data")) {
		t.Fatalf("captured data = %q, want %q", captured, "test data")
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	got := buf.String()
	want := reference.Hex(ref) + "\n"
	if got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestPutCmd_JSON(t *testing.T) {
	ref := reference.Compute([]byte("data"))

	mc := &mockClient{
		putContentFn: func(_ context.Context, _ []byte) (reference.Reference, error) {
			return ref, nil
		},
	}

	n := &nodeCmd{client: mc, readInput: func(_ []string) ([]byte, error) {
		return []byte("data"), nil
	}}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newPutCmd(n)
	cmd.SetArgs([]string{"-o", "json"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var result map[string]string
	data, _ := bytes.NewBuffer(nil), bytes.NewBuffer(nil)
	data.ReadFrom(r)
	if err := json.Unmarshal(data.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if result["reference"] != reference.Hex(ref) {
		t.Fatalf("ref = %q, want %q", result["reference"], reference.Hex(ref))
	}
}

func TestPutCmd_ServerError(t *testing.T) {
	mc := &mockClient{
		putContentFn: func(_ context.Context, _ []byte) (reference.Reference, error) {
			return reference.Reference{}, fmt.Errorf("connection refused")
		},
	}

	n := &nodeCmd{client: mc, readInput: func(_ []string) ([]byte, error) {
		return []byte("data"), nil
	}}

	cmd := newPutCmd(n)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error")
	}
}
