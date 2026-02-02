package node

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadInput_FromArgs(t *testing.T) {
	data, err := readInput([]string{"hello world"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("got %q, want %q", data, "hello world")
	}
}

func TestReadInput_FromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(path, []byte("file content"), 0600); err != nil {
		t.Fatal(err)
	}
	data, err := readInput([]string{path})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "file content" {
		t.Fatalf("got %q, want %q", data, "file content")
	}
}

func TestReadInput_FromStdin(t *testing.T) {
	// Replace stdin with a pipe
	old := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r

	go func() {
		w.Write([]byte("stdin data"))
		w.Close()
	}()

	data, err := readInput(nil)
	os.Stdin = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "stdin data" {
		t.Fatalf("got %q, want %q", data, "stdin data")
	}
}

func TestReadInput_FilePermissionError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "noperm.txt")
	if err := os.WriteFile(path, []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	// Remove read permission
	if err := os.Chmod(path, 0000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(path, 0600) })

	_, err := readInput([]string{path})
	if err == nil {
		t.Fatal("expected error for unreadable file")
	}
}

func TestReadInput_FileNotExist_TreatsAsLiteral(t *testing.T) {
	data, err := readInput([]string{"/nonexistent/path/to/file.txt"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "/nonexistent/path/to/file.txt" {
		t.Fatalf("got %q, want literal path", data)
	}
}
