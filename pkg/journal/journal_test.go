package journal

import (
	"context"
	"encoding/hex"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/gezibash/arc-node/internal/blobstore"
	blobphysical "github.com/gezibash/arc-node/internal/blobstore/physical"
	_ "github.com/gezibash/arc-node/internal/blobstore/physical/memory"
	"github.com/gezibash/arc-node/internal/indexstore"
	idxphysical "github.com/gezibash/arc-node/internal/indexstore/physical"
	_ "github.com/gezibash/arc-node/internal/indexstore/physical/memory"
	"github.com/gezibash/arc-node/internal/observability"
	"github.com/gezibash/arc-node/internal/server"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/reference"
)

// newTestJournal spins up an in-memory server and returns a Journal backed by
// a real client connection. The server and client are cleaned up when t ends.
func newTestJournal(t *testing.T) *Journal {
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

	srv, err := server.New(":0", obs, false, nodeKP, blobs, idx)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Stop(ctx) })

	callerKP, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate caller keypair: %v", err)
	}

	nodePub := nodeKP.PublicKey()
	c, err := client.Dial(srv.Addr(),
		client.WithIdentity(callerKP),
		client.WithNodeKey(nodePub),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	return New(c, callerKP, WithNodeKey(nodePub))
}

func TestWriteAndRead(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	plaintext := []byte("Today I learned about Ed25519 signatures.")
	result, err := j.Write(ctx, plaintext, nil)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if result.Ref == (reference.Reference{}) {
		t.Fatal("Write returned zero Ref")
	}
	if result.EntryRef == (reference.Reference{}) {
		t.Fatal("Write returned zero EntryRef")
	}

	// Read back via the content label from List.
	list, err := j.List(ctx, ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(list.Entries))
	}
	contentHex := list.Entries[0].Labels["content"]
	contentRef, err := reference.FromHex(contentHex)
	if err != nil {
		t.Fatalf("parse content ref: %v", err)
	}

	entry, err := j.Read(ctx, contentRef)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(entry.Content) != string(plaintext) {
		t.Errorf("Read content = %q, want %q", entry.Content, plaintext)
	}
}

func TestWriteAndList(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	texts := []string{"Entry one", "Entry two", "Entry three"}
	for _, text := range texts {
		if _, err := j.Write(ctx, []byte(text), nil); err != nil {
			t.Fatalf("Write(%q): %v", text, err)
		}
	}

	list, err := j.List(ctx, ListOptions{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(list.Entries))
	}
}

func TestListPagination(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := j.Write(ctx, []byte("entry"), nil); err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
	}

	// Page 1
	r1, err := j.List(ctx, ListOptions{Limit: 2})
	if err != nil {
		t.Fatalf("List page 1: %v", err)
	}
	if len(r1.Entries) != 2 {
		t.Fatalf("page 1: expected 2, got %d", len(r1.Entries))
	}
	if !r1.HasMore {
		t.Error("page 1: HasMore should be true")
	}

	// Page 2
	r2, err := j.List(ctx, ListOptions{Limit: 2, Cursor: r1.NextCursor})
	if err != nil {
		t.Fatalf("List page 2: %v", err)
	}
	if len(r2.Entries) != 2 {
		t.Fatalf("page 2: expected 2, got %d", len(r2.Entries))
	}

	// Page 3
	r3, err := j.List(ctx, ListOptions{Limit: 2, Cursor: r2.NextCursor})
	if err != nil {
		t.Fatalf("List page 3: %v", err)
	}
	if len(r3.Entries) != 1 {
		t.Fatalf("page 3: expected 1, got %d", len(r3.Entries))
	}
	if r3.HasMore {
		t.Error("page 3: HasMore should be false")
	}
}

func TestPreview(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	text := "A short journal entry for preview testing."
	if _, err := j.Write(ctx, []byte(text), nil); err != nil {
		t.Fatalf("Write: %v", err)
	}

	list, err := j.List(ctx, ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(list.Entries))
	}

	preview, err := j.Preview(ctx, list.Entries[0])
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if preview != text {
		t.Errorf("Preview = %q, want %q", preview, text)
	}
}

func TestTruncatePreview(t *testing.T) {
	// Test byte truncation.
	long := strings.Repeat("x", 300)
	got := truncatePreview([]byte(long))
	if len(got) > previewMaxBytes+3 { // +3 for "..."
		t.Errorf("truncatePreview did not truncate bytes: len=%d", len(got))
	}
	if !strings.HasSuffix(got, "...") {
		t.Error("truncatePreview should end with ... for long content")
	}

	// Test line truncation.
	manyLines := strings.Repeat("line\n", 10)
	got = truncatePreview([]byte(manyLines))
	lines := strings.Split(strings.TrimSuffix(got, "..."), "\n")
	if len(lines) > previewMaxLines {
		t.Errorf("truncatePreview did not truncate lines: got %d", len(lines))
	}

	// Short content is untouched.
	short := "hello"
	got = truncatePreview([]byte(short))
	if got != short {
		t.Errorf("truncatePreview(%q) = %q", short, got)
	}
}

func TestSubscribe(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, _, err := j.Subscribe(subCtx, ListOptions{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Give subscription time to establish.
	time.Sleep(100 * time.Millisecond)

	if _, err := j.Write(ctx, []byte("subscribed entry"), nil); err != nil {
		t.Fatalf("Write: %v", err)
	}

	select {
	case e := <-entries:
		if e == nil {
			t.Fatal("received nil entry")
		}
		if e.Labels["app"] != "journal" {
			t.Errorf("label app = %q, want journal", e.Labels["app"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive entry within timeout")
	}
}

func TestComputeEntryRef(t *testing.T) {
	kp, _ := identity.Generate()
	pub := kp.PublicKey()
	contentRef := reference.Compute([]byte("content"))
	ts := int64(1700000000000)

	// Deterministic: same inputs produce the same ref.
	r1 := ComputeEntryRef(contentRef, ts, pub)
	r2 := ComputeEntryRef(contentRef, ts, pub)
	if r1 != r2 {
		t.Error("ComputeEntryRef is not deterministic")
	}

	// Different content ref produces a different entry ref.
	otherContent := reference.Compute([]byte("other"))
	r3 := ComputeEntryRef(otherContent, ts, pub)
	if r1 == r3 {
		t.Error("different content should produce different entry ref")
	}

	// Different timestamp produces a different entry ref.
	r4 := ComputeEntryRef(contentRef, ts+1, pub)
	if r1 == r4 {
		t.Error("different timestamp should produce different entry ref")
	}

	// Different owner produces a different entry ref.
	kp2, _ := identity.Generate()
	r5 := ComputeEntryRef(contentRef, ts, kp2.PublicKey())
	if r1 == r5 {
		t.Error("different owner should produce different entry ref")
	}
}

func TestSearchIndexIntegration(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	dir := t.TempDir()
	idx, err := OpenSearchIndex(dir + "/search.db")
	if err != nil {
		t.Fatalf("OpenSearchIndex: %v", err)
	}
	t.Cleanup(func() { idx.Close() })
	j.SetSearchIndex(idx)

	if _, err := j.Write(ctx, []byte("the quick brown fox"), nil); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := j.Write(ctx, []byte("lazy dog in the sun"), nil); err != nil {
		t.Fatalf("Write: %v", err)
	}

	resp, err := j.Search(ctx, "fox", SearchOptions{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if resp.TotalCount != 1 {
		t.Errorf("Search(fox): expected 1 result, got %d", resp.TotalCount)
	}

	resp, err = j.Search(ctx, "dog", SearchOptions{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if resp.TotalCount != 1 {
		t.Errorf("Search(dog): expected 1 result, got %d", resp.TotalCount)
	}
}

func TestEdit(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	// Write original entry.
	orig, err := j.Write(ctx, []byte("original content"), map[string]string{"mood": "happy"})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// List to get the labels needed for edit.
	list, err := j.List(ctx, ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(list.Entries))
	}

	// Edit the entry.
	editResult, err := j.Edit(ctx, orig.EntryRef, []byte("edited content"), list.Entries[0].Labels)
	if err != nil {
		t.Fatalf("Edit: %v", err)
	}
	if editResult.Ref == (reference.Reference{}) {
		t.Fatal("Edit returned zero Ref")
	}
	if editResult.EntryRef == orig.EntryRef {
		t.Error("edited EntryRef should differ from original")
	}

	// The edited message should have a "replaces" label pointing to the old entry ref.
	list, err = j.List(ctx, ListOptions{Limit: 10, Descending: true})
	if err != nil {
		t.Fatalf("List after edit: %v", err)
	}

	var found bool
	for _, e := range list.Entries {
		if e.Labels["replaces"] == reference.Hex(orig.EntryRef) {
			found = true
			// Verify mood label carried forward.
			if e.Labels["mood"] != "happy" {
				t.Errorf("mood label not carried forward: %q", e.Labels["mood"])
			}
			break
		}
	}
	if !found {
		t.Error("did not find edited entry with replaces label")
	}
}

func TestMergeLabels(t *testing.T) {
	m := mergeLabels(map[string]string{"mood": "happy", "weather": "sunny"})
	if m["app"] != "journal" {
		t.Errorf("app = %q, want journal", m["app"])
	}
	if m["type"] != "entry" {
		t.Errorf("type = %q, want entry", m["type"])
	}
	if m["mood"] != "happy" {
		t.Errorf("mood = %q, want happy", m["mood"])
	}
	if m["weather"] != "sunny" {
		t.Errorf("weather = %q, want sunny", m["weather"])
	}

	// App and type labels override user-supplied ones.
	m2 := mergeLabels(map[string]string{"app": "custom", "type": "custom"})
	if m2["app"] != "journal" {
		t.Errorf("app should be forced to journal, got %q", m2["app"])
	}
	if m2["type"] != "entry" {
		t.Errorf("type should be forced to entry, got %q", m2["type"])
	}
}

func TestOwnerLabels(t *testing.T) {
	kp, _ := identity.Generate()
	j := &Journal{kp: kp, symKey: deriveKey(kp)}

	m := j.ownerLabels(nil)
	pub := kp.PublicKey()
	wantOwner := hex.EncodeToString(pub[:])
	if m["owner"] != wantOwner {
		t.Errorf("owner = %q, want %q", m["owner"], wantOwner)
	}
	if m["app"] != "journal" {
		t.Errorf("app = %q, want journal", m["app"])
	}
	if m["type"] != "entry" {
		t.Errorf("type = %q, want entry", m["type"])
	}
}
