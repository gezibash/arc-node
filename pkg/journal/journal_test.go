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
	t.Cleanup(func() { _ = blobBackend.Close() })
	blobs := blobstore.New(blobBackend, metrics)

	idxBackend, err := idxphysical.New(ctx, "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create index backend: %v", err)
	}
	t.Cleanup(func() { _ = idxBackend.Close() })
	idx, err := indexstore.New(idxBackend, metrics)
	if err != nil {
		t.Fatalf("create index store: %v", err)
	}

	obs, err := observability.New(ctx, observability.ObsConfig{LogLevel: "error"}, io.Discard)
	if err != nil {
		t.Fatalf("create observability: %v", err)
	}
	t.Cleanup(func() { _ = obs.Close(ctx) })

	nodeKP, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate node keypair: %v", err)
	}

	srv, err := server.New(context.Background(), ":0", obs, false, nodeKP, blobs, idx)
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
	t.Cleanup(func() { _ = c.Close() })

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
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatalf("OpenSearchIndex: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })
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

func TestReadForEdit(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	plaintext := []byte("editable entry content")
	result, err := j.Write(ctx, plaintext, map[string]string{"mood": "calm"})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// List to get the message ref hex.
	list, err := j.List(ctx, ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(list.Entries))
	}
	msgRefHex := reference.Hex(list.Entries[0].Ref)

	got, labels, err := j.ReadForEdit(ctx, msgRefHex)
	if err != nil {
		t.Fatalf("ReadForEdit: %v", err)
	}
	if string(got) != string(plaintext) {
		t.Errorf("ReadForEdit content = %q, want %q", got, plaintext)
	}
	if labels["mood"] != "calm" {
		t.Errorf("label mood = %q, want calm", labels["mood"])
	}
	_ = result
}

func TestEditWithSearchIndex(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	dir := t.TempDir()
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatalf("OpenSearchIndex: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	j.SetSearchIndex(idx)

	orig, err := j.Write(ctx, []byte("original searchable"), nil)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify original is searchable.
	resp, err := j.Search(ctx, "original", SearchOptions{})
	if err != nil {
		t.Fatalf("Search original: %v", err)
	}
	if resp.TotalCount != 1 {
		t.Fatalf("expected 1 search result, got %d", resp.TotalCount)
	}

	// Edit.
	list, err := j.List(ctx, ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	_, err = j.Edit(ctx, orig.EntryRef, []byte("edited searchable"), list.Entries[0].Labels)
	if err != nil {
		t.Fatalf("Edit: %v", err)
	}

	// Old content gone from search.
	resp, err = j.Search(ctx, "original", SearchOptions{})
	if err != nil {
		t.Fatalf("Search original after edit: %v", err)
	}
	if resp.TotalCount != 0 {
		t.Errorf("expected 0 results for old content, got %d", resp.TotalCount)
	}

	// New content in search.
	resp, err = j.Search(ctx, "edited", SearchOptions{})
	if err != nil {
		t.Fatalf("Search edited: %v", err)
	}
	if resp.TotalCount != 1 {
		t.Errorf("expected 1 result for new content, got %d", resp.TotalCount)
	}
}

func TestReindex(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	dir := t.TempDir()
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatalf("OpenSearchIndex: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	j.SetSearchIndex(idx)

	// Write 3 entries.
	orig, err := j.Write(ctx, []byte("entry alpha"), nil)
	if err != nil {
		t.Fatalf("Write alpha: %v", err)
	}
	if _, err := j.Write(ctx, []byte("entry beta"), nil); err != nil {
		t.Fatalf("Write beta: %v", err)
	}
	if _, err := j.Write(ctx, []byte("entry gamma"), nil); err != nil {
		t.Fatalf("Write gamma: %v", err)
	}

	// Edit alpha → replaces it.
	list, err := j.List(ctx, ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var origLabels map[string]string
	for _, e := range list.Entries {
		if e.EntryRef == orig.EntryRef {
			origLabels = e.Labels
			break
		}
	}
	if _, err := j.Edit(ctx, orig.EntryRef, []byte("entry alpha v2"), origLabels); err != nil {
		t.Fatalf("Edit: %v", err)
	}

	// Clear and reindex.
	if err := j.Reindex(ctx); err != nil {
		t.Fatalf("Reindex: %v", err)
	}

	// Should find beta, gamma, alpha-v2 but not original alpha.
	var count int
	if err := idx.Count(ctx, &count); err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 indexed entries, got %d", count)
	}

	resp, err := j.Search(ctx, "alpha", SearchOptions{})
	if err != nil {
		t.Fatalf("Search alpha: %v", err)
	}
	if resp.TotalCount != 1 {
		t.Errorf("expected 1 alpha result, got %d", resp.TotalCount)
	}
}

func TestReindexNoSearchIndex(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	err := j.Reindex(ctx)
	if err == nil {
		t.Error("expected error reindexing without search index")
	}
}

func TestSearchNoIndex(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	_, err := j.Search(ctx, "test", SearchOptions{})
	if err == nil {
		t.Error("expected error searching without search index")
	}
}

func TestSearchStateLoadSave(t *testing.T) {
	dir := t.TempDir()
	s := LoadSearchState(dir)
	s.LocalHash = "aaaa"
	s.RemoteHash = "bbbb"
	if err := s.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	s2 := LoadSearchState(dir)
	if s2.LocalHash != "aaaa" {
		t.Errorf("LocalHash = %q, want aaaa", s2.LocalHash)
	}
	if s2.RemoteHash != "bbbb" {
		t.Errorf("RemoteHash = %q, want bbbb", s2.RemoteHash)
	}
}

func TestSearchStateLoadMissing(t *testing.T) {
	dir := t.TempDir()
	s := LoadSearchState(dir)
	if s.LocalHash != "" || s.RemoteHash != "" {
		t.Error("missing state file should return zero state")
	}
}

func TestSearchStateLocalRefRemoteRef(t *testing.T) {
	s := &SearchState{
		LocalHash:  "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		RemoteHash: "invalid-hex",
	}
	lr := s.LocalRef()
	if lr == (reference.Reference{}) {
		t.Error("LocalRef should be non-zero for valid hex")
	}
	rr := s.RemoteRef()
	if rr != (reference.Reference{}) {
		t.Error("RemoteRef should be zero for invalid hex")
	}
}

func TestPushAndPullSearchIndex(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	dir := t.TempDir()
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatalf("OpenSearchIndex: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	j.SetSearchIndex(idx)

	// Write entries to build search index.
	if _, err := j.Write(ctx, []byte("push pull test alpha"), nil); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := j.Write(ctx, []byte("push pull test beta"), nil); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Push.
	_, err = j.PushSearchIndex(ctx, idx)
	if err != nil {
		t.Fatalf("PushSearchIndex: %v", err)
	}

	// Pull to new path.
	pullDir := t.TempDir()
	pulled, err := j.PullSearchIndex(ctx, pullDir+"/pulled.db")
	if err != nil {
		t.Fatalf("PullSearchIndex: %v", err)
	}
	t.Cleanup(func() { pulled.Close() })

	// Verify search works on pulled DB.
	resp, err := pulled.Search(ctx, "alpha", SearchOptions{})
	if err != nil {
		t.Fatalf("Search on pulled: %v", err)
	}
	if resp.TotalCount != 1 {
		t.Errorf("expected 1 result on pulled index, got %d", resp.TotalCount)
	}
}

func TestFetchRemoteSearchInfo(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	dir := t.TempDir()
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatalf("OpenSearchIndex: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	j.SetSearchIndex(idx)

	if _, err := j.Write(ctx, []byte("info test"), nil); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if _, err := j.PushSearchIndex(ctx, idx); err != nil {
		t.Fatalf("PushSearchIndex: %v", err)
	}

	info, err := j.FetchRemoteSearchInfo(ctx)
	if err != nil {
		t.Fatalf("FetchRemoteSearchInfo: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil info")
	}
	if info.EntryCount != 1 {
		t.Errorf("EntryCount = %d, want 1", info.EntryCount)
	}
	if info.DBHash == "" {
		t.Error("DBHash should not be empty")
	}
}

func TestFetchRemoteSearchInfoEmpty(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	info, err := j.FetchRemoteSearchInfo(ctx)
	if err != nil {
		t.Fatalf("FetchRemoteSearchInfo: %v", err)
	}
	if info != nil {
		t.Errorf("expected nil info, got %+v", info)
	}
}
