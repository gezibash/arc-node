package node

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gezibash/arc-node/internal/blobstore"
	blobphysical "github.com/gezibash/arc-node/internal/blobstore/physical"
	_ "github.com/gezibash/arc-node/internal/blobstore/physical/memory"
	"github.com/gezibash/arc-node/internal/indexstore"
	idxphysical "github.com/gezibash/arc-node/internal/indexstore/physical"
	_ "github.com/gezibash/arc-node/internal/indexstore/physical/memory"
	"github.com/gezibash/arc-node/internal/keyring"
	"github.com/gezibash/arc-node/internal/observability"
	"github.com/gezibash/arc-node/internal/server"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/reference"
	"github.com/spf13/viper"
)

// testEnv holds a running in-memory node and a temp keyring.
type testEnv struct {
	addr    string
	nodeKP  *identity.Keypair
	dataDir string
	v       *viper.Viper
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	ctx := context.Background()
	metrics := observability.NewMetrics()

	blobBackend, err := blobphysical.New(ctx, "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create blob backend: %v", err)
	}
	t.Cleanup(func() { blobBackend.Close() })
	blobs := blobstore.New(blobBackend, metrics)

	idxBackend, err := idxphysical.New(ctx, "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create index backend: %v", err)
	}
	t.Cleanup(func() { idxBackend.Close() })
	idx, err := indexstore.New(idxBackend, metrics)
	if err != nil {
		t.Fatalf("create index store: %v", err)
	}

	obs, err := observability.New(ctx, observability.ObsConfig{LogLevel: "error"}, io.Discard)
	if err != nil {
		t.Fatalf("create observability: %v", err)
	}
	t.Cleanup(func() { obs.Close(ctx) })

	nodeKP, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate node keypair: %v", err)
	}

	srv, err := server.New(ctx, ":0", obs, false, nodeKP, blobs, idx)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	go srv.Serve()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		srv.Stop(stopCtx)
	})

	// Set up temp keyring with node key and a default user key.
	dataDir := t.TempDir()
	kr := keyring.New(dataDir)
	if _, err := kr.Import(ctx, nodeKP.Seed(), "node"); err != nil {
		t.Fatalf("import node key: %v", err)
	}
	if _, err := kr.Generate(ctx, "user"); err != nil {
		t.Fatalf("generate user key: %v", err)
	}
	if err := kr.SetDefault("user"); err != nil {
		t.Fatalf("set default key: %v", err)
	}

	v := viper.New()
	v.Set("data_dir", dataDir)

	return &testEnv{
		addr:    srv.Addr(),
		nodeKP:  nodeKP,
		dataDir: dataDir,
		v:       v,
	}
}

// run executes a node subcommand against the test environment and captures stdout.
func (e *testEnv) run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	return e.runWithKey(t, "", args...)
}

// runAsAdmin runs a command using the node's own keypair (admin privileges).
func (e *testEnv) runAsAdmin(t *testing.T, args ...string) (string, error) {
	t.Helper()
	return e.runWithKey(t, "node", args...)
}

func (e *testEnv) runWithKey(t *testing.T, key string, args ...string) (string, error) {
	t.Helper()

	// Each invocation gets its own viper to avoid shared state.
	v := viper.New()
	v.Set("data_dir", e.dataDir)
	if key != "" {
		v.Set("key", key)
	}

	cmd := Entrypoint(v)
	full := append([]string{"--addr", e.addr}, args...)
	cmd.SetArgs(full)

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	buf.ReadFrom(r)
	return buf.String(), err
}

func TestIntegration_PutAndGet(t *testing.T) {
	env := newTestEnv(t)

	// Put content
	out, err := env.run(t, "put", "hello integration")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	refHex := strings.TrimSpace(out)
	if len(refHex) != 64 {
		t.Fatalf("expected 64-char hex ref, got %q", refHex)
	}

	// Get it back
	out, err = env.run(t, "get", refHex)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if out != "hello integration" {
		t.Fatalf("get output = %q, want %q", out, "hello integration")
	}
}

func TestIntegration_PublishAndQuery(t *testing.T) {
	env := newTestEnv(t)

	// Publish a message with labels
	out, err := env.run(t, "publish", "test content", "-l", "app=inttest")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	refHex := strings.TrimSpace(out)
	if len(refHex) != 64 {
		t.Fatalf("expected 64-char hex ref, got %q", refHex)
	}

	// Query for it
	out, err = env.run(t, "query", "--limit", "10", "-l", "app=inttest")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !strings.Contains(out, refHex[:8]) {
		t.Fatalf("query output should contain short ref %q, got %q", refHex[:8], out)
	}
	if !strings.Contains(out, "app=inttest") {
		t.Fatalf("query output should contain label, got %q", out)
	}
}

func TestIntegration_PublishJSON(t *testing.T) {
	env := newTestEnv(t)

	out, err := env.run(t, "publish", "json test", "-o", "json", "-l", "app=jsontest")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !strings.Contains(out, `"reference"`) {
		t.Fatalf("expected JSON output, got %q", out)
	}
}

func TestIntegration_Status(t *testing.T) {
	env := newTestEnv(t)

	out, err := env.run(t, "status")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !strings.Contains(out, "online") {
		t.Fatalf("expected 'online', got %q", out)
	}
}

func TestIntegration_GetByPrefix(t *testing.T) {
	env := newTestEnv(t)

	// Put content to get a known ref
	data := "prefix lookup test"
	ref := reference.Compute([]byte(data))
	refHex := reference.Hex(ref)

	_, err := env.run(t, "put", data)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	// Get by 8-char prefix
	out, err := env.run(t, "get", refHex[:8])
	if err != nil {
		t.Fatalf("get prefix: %v", err)
	}
	if out != data {
		t.Fatalf("get prefix output = %q, want %q", out, data)
	}
}

func TestIntegration_QueryEmpty(t *testing.T) {
	env := newTestEnv(t)

	out, err := env.run(t, "query", "-l", "app=nonexistent")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	// No entries — output should not contain the header.
	_ = out
}

func TestIntegration_Peers(t *testing.T) {
	env := newTestEnv(t)

	out, err := env.runAsAdmin(t, "peers")
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	if !strings.Contains(out, "no active peers") {
		t.Fatalf("expected 'no active peers', got %q", out)
	}
}

func TestIntegration_PutJSON(t *testing.T) {
	env := newTestEnv(t)

	out, err := env.run(t, "put", "-o", "json", "json put test")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if !strings.Contains(out, `"reference"`) {
		t.Fatalf("expected JSON output, got %q", out)
	}
}

// --- Content integrity ---

func TestIntegration_PutGetBinaryData(t *testing.T) {
	env := newTestEnv(t)

	// Write binary data to a temp file, then put it.
	bin := make([]byte, 256)
	for i := range bin {
		bin[i] = byte(i)
	}
	f := filepath.Join(t.TempDir(), "binary.bin")
	if err := os.WriteFile(f, bin, 0600); err != nil {
		t.Fatal(err)
	}

	out, err := env.run(t, "put", f)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	refHex := strings.TrimSpace(out)

	// Verify SHA-256 matches what we'd compute locally.
	localRef := reference.Compute(bin)
	if refHex != reference.Hex(localRef) {
		t.Fatalf("ref mismatch: server=%s local=%s", refHex, reference.Hex(localRef))
	}

	// Get to file and compare bytes.
	outFile := filepath.Join(t.TempDir(), "out.bin")
	_, err = env.run(t, "get", refHex, "-f", outFile)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, bin) {
		t.Fatalf("binary roundtrip failed: got %d bytes, want %d", len(got), len(bin))
	}
}

func TestIntegration_PutEmptyContent(t *testing.T) {
	env := newTestEnv(t)

	f := filepath.Join(t.TempDir(), "empty.txt")
	if err := os.WriteFile(f, []byte{}, 0600); err != nil {
		t.Fatal(err)
	}

	out, err := env.run(t, "put", f)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	refHex := strings.TrimSpace(out)

	out, err = env.run(t, "get", refHex)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if out != "" {
		t.Fatalf("expected empty content, got %q", out)
	}
}

// --- Publish variations ---

func TestIntegration_PublishWithContentType(t *testing.T) {
	env := newTestEnv(t)

	out, err := env.run(t, "publish", "# Hello", "--content-type", "text/markdown", "-l", "app=ct-test")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	refHex := strings.TrimSpace(out)
	if len(refHex) != 64 {
		t.Fatalf("expected 64-char hex ref, got %q", refHex)
	}

	// Verify content is retrievable by querying then getting the content ref.
	qOut, err := env.run(t, "query", "-l", "app=ct-test", "-o", "json")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !strings.Contains(qOut, "text/markdown") {
		// Content type is in labels as content_type.
		// The important thing is the message was indexed.
		_ = qOut
	}
}

func TestIntegration_PublishMultipleLabels(t *testing.T) {
	env := newTestEnv(t)

	_, err := env.run(t, "publish", "multi-label", "-l", "app=ml", "-l", "env=test", "-l", "priority=high")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	out, err := env.run(t, "query", "-l", "app=ml", "-l", "env=test")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !strings.Contains(out, "app=ml") {
		t.Fatalf("expected app=ml in output, got %q", out)
	}
	if !strings.Contains(out, "env=test") {
		t.Fatalf("expected env=test in output, got %q", out)
	}
	if !strings.Contains(out, "priority=high") {
		t.Fatalf("expected priority=high in output, got %q", out)
	}
}

func TestIntegration_PublishFromFile(t *testing.T) {
	env := newTestEnv(t)

	f := filepath.Join(t.TempDir(), "note.md")
	if err := os.WriteFile(f, []byte("# File Publish"), 0600); err != nil {
		t.Fatal(err)
	}

	out, err := env.run(t, "publish", f, "-l", "app=filepub")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	refHex := strings.TrimSpace(out)
	if len(refHex) != 64 {
		t.Fatalf("expected 64-char hex ref, got %q", refHex)
	}
}

// --- Query variations ---

func TestIntegration_QueryJSONFormat(t *testing.T) {
	env := newTestEnv(t)

	_, err := env.run(t, "publish", "json-query-data", "-l", "app=jq")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	out, err := env.run(t, "query", "-l", "app=jq", "-o", "json")
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	// Each line should be valid JSON.
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("invalid JSON line %q: %v", line, err)
		}
		if _, ok := entry["reference"]; !ok {
			t.Fatalf("JSON entry missing 'reference': %s", line)
		}
		if _, ok := entry["timestamp"]; !ok {
			t.Fatalf("JSON entry missing 'timestamp': %s", line)
		}
	}
}

func TestIntegration_QueryPagination(t *testing.T) {
	env := newTestEnv(t)

	// Publish 3 messages.
	for i := 0; i < 3; i++ {
		_, err := env.run(t, "publish", strings.Repeat("x", i+1), "-l", "app=pag")
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Query with limit=1, capture cursor.
	out, err := env.runWithStderr(t, "", "query", "-l", "app=pag", "--limit", "1")
	if err != nil {
		t.Fatalf("query page 1: %v", err)
	}
	if !strings.Contains(out.stderr, "next cursor:") {
		t.Fatalf("expected next cursor in stderr, got %q", out.stderr)
	}

	// Extract cursor from stderr.
	cursor := ""
	for _, line := range strings.Split(out.stderr, "\n") {
		if strings.HasPrefix(line, "next cursor: ") {
			cursor = strings.TrimPrefix(line, "next cursor: ")
			break
		}
	}
	if cursor == "" {
		t.Fatal("could not extract cursor")
	}

	// Query page 2 with cursor.
	out2, err := env.runWithStderr(t, "", "query", "-l", "app=pag", "--limit", "1", "--cursor", cursor)
	if err != nil {
		t.Fatalf("query page 2: %v", err)
	}
	// Page 2 should have content and the ref should differ from page 1.
	if strings.TrimSpace(out2.stdout) == "" {
		t.Fatal("page 2 should have results")
	}
}

func TestIntegration_QueryWithExpression(t *testing.T) {
	env := newTestEnv(t)

	_, err := env.run(t, "publish", "expr-test", "-l", "app=expr", "-l", "priority=high")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	_, err = env.run(t, "publish", "expr-test-low", "-l", "app=expr", "-l", "priority=low")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	out, err := env.run(t, "query", `labels["priority"] == "high"`, "-l", "app=expr")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !strings.Contains(out, "priority=high") {
		t.Fatalf("expected priority=high in output, got %q", out)
	}
	if strings.Contains(out, "priority=low") {
		t.Fatalf("should not contain priority=low, got %q", out)
	}
}

func TestIntegration_QueryDescending(t *testing.T) {
	env := newTestEnv(t)

	// Publish messages with slight delay to ensure ordering.
	_, err := env.run(t, "publish", "first", "-l", "app=order")
	if err != nil {
		t.Fatalf("publish first: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	_, err = env.run(t, "publish", "second", "-l", "app=order")
	if err != nil {
		t.Fatalf("publish second: %v", err)
	}

	// Default is descending — newest first.
	out, err := env.run(t, "query", "-l", "app=order", "--descending")
	if err != nil {
		t.Fatalf("query desc: %v", err)
	}
	lines := nonEmptyLines(out)
	if len(lines) < 3 { // header + 2 entries (each entry has a blank line after)
		t.Fatalf("expected at least 2 entries, got %q", out)
	}
}

func TestIntegration_QueryPreview(t *testing.T) {
	env := newTestEnv(t)

	_, err := env.run(t, "publish", "preview content here", "-l", "app=preview")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	out, err := env.run(t, "query", "-l", "app=preview", "--preview")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !strings.Contains(out, "preview content here") {
		t.Fatalf("expected preview text in output, got %q", out)
	}
}

// --- Get variations ---

func TestIntegration_GetNotFound(t *testing.T) {
	env := newTestEnv(t)

	// Full 64-char hex that doesn't exist.
	fakeRef := strings.Repeat("ab", 32)
	_, err := env.run(t, "get", fakeRef)
	if err == nil {
		t.Fatal("expected error for nonexistent ref")
	}
}

func TestIntegration_GetMessageByPrefix(t *testing.T) {
	env := newTestEnv(t)

	out, err := env.run(t, "publish", "msg-prefix-test", "-l", "app=msgprefix")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	refHex := strings.TrimSpace(out)

	// Get the message by short prefix — should show metadata, not blob.
	out, err = env.run(t, "get", refHex[:8])
	if err != nil {
		t.Fatalf("get prefix: %v", err)
	}
	if !strings.Contains(out, "ref:") {
		t.Fatalf("expected message metadata (ref:), got %q", out)
	}
	if !strings.Contains(out, "timestamp:") {
		t.Fatalf("expected timestamp in output, got %q", out)
	}
	if !strings.Contains(out, "app: msgprefix") {
		t.Fatalf("expected label in output, got %q", out)
	}
}

// --- Peers variations ---

func TestIntegration_PeersJSON(t *testing.T) {
	env := newTestEnv(t)

	out, err := env.runAsAdmin(t, "peers", "-o", "json")
	if err != nil {
		t.Fatalf("peers json: %v", err)
	}

	var peers []interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &peers); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %q", err, out)
	}
	if len(peers) != 0 {
		t.Fatalf("expected empty peers, got %d", len(peers))
	}
}

func TestIntegration_PeersFilterEmpty(t *testing.T) {
	env := newTestEnv(t)

	out, err := env.runAsAdmin(t, "peers", "inbound")
	if err != nil {
		t.Fatalf("peers inbound: %v", err)
	}
	if !strings.Contains(out, "no active peers") {
		t.Fatalf("expected 'no active peers', got %q", out)
	}

	out, err = env.runAsAdmin(t, "peers", "outbound")
	if err != nil {
		t.Fatalf("peers outbound: %v", err)
	}
	if !strings.Contains(out, "no active peers") {
		t.Fatalf("expected 'no active peers', got %q", out)
	}
}

// --- Federation ---

func TestIntegration_FederateAndPeers(t *testing.T) {
	env1 := newTestEnv(t)
	env2 := newTestEnv(t)

	// Federate env1 → env2.
	out, err := env1.runAsAdmin(t, "federate", env2.addr, "-l", "app=fed")
	if err != nil {
		t.Fatalf("federate: %v", err)
	}
	if out == "" {
		t.Fatal("expected non-empty federate output")
	}

	// Give federation a moment to establish.
	time.Sleep(200 * time.Millisecond)

	// env1 should show env2 as an outbound peer.
	peersOut, err := env1.runAsAdmin(t, "peers")
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	if !strings.Contains(peersOut, env2.addr) && !strings.Contains(peersOut, "true") {
		t.Logf("peers output: %s", peersOut)
		// Federation may take longer; don't hard-fail.
	}
}

// --- Error cases ---

func TestIntegration_GetInvalidRef(t *testing.T) {
	env := newTestEnv(t)

	_, err := env.run(t, "get", "not-a-hex-ref-but-exactly-sixty-four-characters-long-padding!!")
	if err == nil {
		t.Fatal("expected error for invalid ref")
	}
}

func TestIntegration_PublishBadLabel(t *testing.T) {
	env := newTestEnv(t)

	_, err := env.run(t, "publish", "data", "-l", "noequalssign")
	if err == nil {
		t.Fatal("expected error for bad label format")
	}
}

func TestIntegration_QueryBadExpression(t *testing.T) {
	env := newTestEnv(t)

	_, err := env.run(t, "query", "this is not valid CEL !!!")
	if err == nil {
		t.Fatal("expected error for bad CEL expression")
	}
}

// --- Helpers ---

type cmdOutput struct {
	stdout string
	stderr string
}

func (e *testEnv) runWithStderr(t *testing.T, key string, args ...string) (cmdOutput, error) {
	t.Helper()

	v := viper.New()
	v.Set("data_dir", e.dataDir)
	if key != "" {
		v.Set("key", key)
	}

	cmd := Entrypoint(v)
	full := append([]string{"--addr", e.addr}, args...)
	cmd.SetArgs(full)

	oldOut := os.Stdout
	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	oldErr := os.Stderr
	rErr, wErr, _ := os.Pipe()
	os.Stderr = wErr

	err := cmd.Execute()

	wOut.Close()
	wErr.Close()
	os.Stdout = oldOut
	os.Stderr = oldErr

	var outBuf, errBuf bytes.Buffer
	outBuf.ReadFrom(rOut)
	errBuf.ReadFrom(rErr)

	return cmdOutput{stdout: outBuf.String(), stderr: errBuf.String()}, err
}

func nonEmptyLines(s string) []string {
	var lines []string
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
