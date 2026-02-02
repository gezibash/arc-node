package dm

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/reference"
)

func TestSearchIndexOpenAndClose(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSearchIndexIndexAndSearch(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	contentRef1 := reference.Compute([]byte("content one"))
	msgRef1 := reference.Compute([]byte("msg one"))
	contentRef2 := reference.Compute([]byte("content two"))
	msgRef2 := reference.Compute([]byte("msg two"))

	if err := idx.Index(ctx, contentRef1, msgRef1, "hello world test entry", 1000); err != nil {
		t.Fatal(err)
	}
	if err := idx.Index(ctx, contentRef2, msgRef2, "goodbye world another entry", 2000); err != nil {
		t.Fatal(err)
	}

	resp, err := idx.Search(ctx, "hello", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.TotalCount != 1 {
		t.Fatalf("expected 1 result, got %d", resp.TotalCount)
	}
	if resp.Results[0].Ref != msgRef1 {
		t.Error("wrong ref returned")
	}

	resp, err = idx.Search(ctx, "world", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.TotalCount != 2 {
		t.Fatalf("expected 2 results, got %d", resp.TotalCount)
	}

	resp, err = idx.Search(ctx, "hel", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.TotalCount != 1 {
		t.Fatalf("prefix search: expected 1, got %d", resp.TotalCount)
	}
}

func TestSearchIndexPagination(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	for i := 0; i < 5; i++ {
		cRef := reference.Compute([]byte(fmt.Sprintf("c-%d", i)))
		mRef := reference.Compute([]byte(fmt.Sprintf("m-%d", i)))
		if err := idx.Index(ctx, cRef, mRef, fmt.Sprintf("test entry number %d", i), int64(1000+i)); err != nil {
			t.Fatal(err)
		}
	}

	resp, err := idx.Search(ctx, "test", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.TotalCount != 5 {
		t.Fatalf("expected 5, got %d", resp.TotalCount)
	}

	resp, err = idx.Search(ctx, "test", SearchOptions{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(resp.Results))
	}
	if resp.TotalCount != 5 {
		t.Fatalf("total should still be 5, got %d", resp.TotalCount)
	}

	resp, err = idx.Search(ctx, "test", SearchOptions{Limit: 2, Offset: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 2 {
		t.Fatalf("expected 2 results with offset, got %d", len(resp.Results))
	}
}

func TestSearchIndexCount(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	var n int
	if err := idx.Count(ctx, &n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("empty: expected 0, got %d", n)
	}

	for i := 0; i < 3; i++ {
		cRef := reference.Compute([]byte(fmt.Sprintf("c-%d", i)))
		mRef := reference.Compute([]byte(fmt.Sprintf("m-%d", i)))
		if err := idx.Index(ctx, cRef, mRef, fmt.Sprintf("count %d", i), int64(1000+i)); err != nil {
			t.Fatal(err)
		}
	}

	if err := idx.Count(ctx, &n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("expected 3, got %d", n)
	}
}

func TestSearchIndexClear(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	cRef := reference.Compute([]byte("c"))
	mRef := reference.Compute([]byte("m"))
	if err := idx.Index(ctx, cRef, mRef, "clear test", 1000); err != nil {
		t.Fatal(err)
	}

	if err := idx.Clear(ctx); err != nil {
		t.Fatal(err)
	}

	var n int
	if err := idx.Count(ctx, &n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 after clear, got %d", n)
	}
}

func TestSearchIndexResolvePrefix(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	cRef := reference.Compute([]byte("c"))
	mRef1 := reference.Compute([]byte("msg-1"))
	mRef2 := reference.Compute([]byte("msg-2"))
	if err := idx.Index(ctx, cRef, mRef1, "prefix one", 1000); err != nil {
		t.Fatal(err)
	}
	if err := idx.Index(ctx, cRef, mRef2, "prefix two", 2000); err != nil {
		t.Fatal(err)
	}

	hex1 := reference.Hex(mRef1)
	resolved, err := idx.ResolvePrefix(ctx, hex1[:16])
	if err != nil {
		t.Fatalf("ResolvePrefix unique: %v", err)
	}
	if resolved != mRef1 {
		t.Error("resolved wrong ref")
	}

	_, err = idx.ResolvePrefix(ctx, "0000000000000000")
	if err == nil {
		t.Error("expected error for no match")
	}
}

func TestSearchIndexMarkReadAndLastRead(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	ts, err := idx.LastRead(ctx, "conv1")
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0 {
		t.Fatalf("expected 0, got %d", ts)
	}

	if err := idx.MarkRead(ctx, "conv1", 5000); err != nil {
		t.Fatal(err)
	}
	ts, err = idx.LastRead(ctx, "conv1")
	if err != nil {
		t.Fatal(err)
	}
	if ts != 5000 {
		t.Fatalf("expected 5000, got %d", ts)
	}

	if err := idx.MarkRead(ctx, "conv1", 3000); err != nil {
		t.Fatal(err)
	}
	ts, _ = idx.LastRead(ctx, "conv1")
	if ts != 5000 {
		t.Fatalf("expected 5000 (no downgrade), got %d", ts)
	}

	if err := idx.MarkRead(ctx, "conv1", 8000); err != nil {
		t.Fatal(err)
	}
	ts, _ = idx.LastRead(ctx, "conv1")
	if ts != 8000 {
		t.Fatalf("expected 8000, got %d", ts)
	}
}

func TestSearchIndexAllLastRead(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	m, err := idx.AllLastRead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 0 {
		t.Fatalf("expected empty map, got %d", len(m))
	}

	if err := idx.MarkRead(ctx, "conv1", 1000); err != nil {
		t.Fatal(err)
	}
	if err := idx.MarkRead(ctx, "conv2", 2000); err != nil {
		t.Fatal(err)
	}

	m, err = idx.AllLastRead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 2 {
		t.Fatalf("expected 2, got %d", len(m))
	}
	if m["conv1"] != 1000 {
		t.Errorf("conv1 = %d, want 1000", m["conv1"])
	}
	if m["conv2"] != 2000 {
		t.Errorf("conv2 = %d, want 2000", m["conv2"])
	}
}

func TestSearchIndexLastIndexedTimestamp(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	ts, err := idx.LastIndexedTimestamp(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0 {
		t.Fatalf("empty: expected 0, got %d", ts)
	}

	cRef := reference.Compute([]byte("c"))
	mRef := reference.Compute([]byte("m"))
	if err := idx.Index(ctx, cRef, mRef, "ts test", 42000); err != nil {
		t.Fatal(err)
	}

	ts, _ = idx.LastIndexedTimestamp(ctx)
	if ts != 42000 {
		t.Fatalf("expected 42000, got %d", ts)
	}
}

func TestSearchIndexContentHash(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	cRef := reference.Compute([]byte("c"))
	mRef := reference.Compute([]byte("m"))
	if err := idx.Index(ctx, cRef, mRef, "hash test", 1000); err != nil {
		t.Fatal(err)
	}

	h1, err := idx.ContentHash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h2, _ := idx.ContentHash(ctx)
	if h1 != h2 {
		t.Error("ContentHash not deterministic")
	}
	if h1 == (reference.Reference{}) {
		t.Error("ContentHash should be non-zero")
	}
}

func TestSearchIndexDBPath(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	got, err := idx.DBPath()
	if err != nil {
		t.Fatal(err)
	}
	if got == "" {
		t.Error("DBPath should be non-empty")
	}
}

func TestSearchIndexSchemaMigration(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "search.db")

	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	cRef := reference.Compute([]byte("c"))
	mRef := reference.Compute([]byte("m"))
	if err := idx.Index(ctx, cRef, mRef, "migration test", 1000); err != nil {
		t.Fatal(err)
	}
	idx.Close()

	idx2, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx2.Close() })

	resp, err := idx2.Search(ctx, "migration", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.TotalCount != 1 {
		t.Errorf("expected 1 after reopen, got %d", resp.TotalCount)
	}
}

func TestSearchIndexReindexContent(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	cRef := reference.Compute([]byte("c"))
	mRef := reference.Compute([]byte("m"))
	if err := idx.Index(ctx, cRef, mRef, "hello world", 1000); err != nil {
		t.Fatal(err)
	}
	if err := idx.Index(ctx, cRef, mRef, "hello updated", 1500); err != nil {
		t.Fatal(err)
	}

	resp, _ := idx.Search(ctx, "world", SearchOptions{})
	if resp.TotalCount != 0 {
		t.Errorf("old content should not match, got %d", resp.TotalCount)
	}

	resp, _ = idx.Search(ctx, "updated", SearchOptions{})
	if resp.TotalCount != 1 {
		t.Errorf("updated content should match, got %d", resp.TotalCount)
	}
}

func TestPrefixQuery(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"hello", `"hello"*`},
		{"hello world", `"hello"* "world"*`},
	}
	for _, tt := range tests {
		got := prefixQuery(tt.input)
		if got != tt.want {
			t.Errorf("prefixQuery(%q) = %q, want %q", tt.input, got, tt.want)
		}
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

func TestDeriveSymKey(t *testing.T) {
	seed := []byte("0123456789abcdef0123456789abcdef")
	k1 := deriveSymKey(seed)
	k2 := deriveSymKey(seed)
	if k1 != k2 {
		t.Error("deriveSymKey should be deterministic")
	}
	if k1 == [32]byte{} {
		t.Error("deriveSymKey should be non-zero")
	}
}

func TestParseEntryCount(t *testing.T) {
	tests := []struct {
		labels map[string]string
		want   int
	}{
		{map[string]string{}, 0},
		{map[string]string{"entry-count": ""}, 0},
		{map[string]string{"entry-count": "42"}, 42},
		{map[string]string{"entry-count": "bad"}, 0},
	}
	for _, tt := range tests {
		got := parseEntryCount(tt.labels)
		if got != tt.want {
			t.Errorf("parseEntryCount(%v) = %d, want %d", tt.labels, got, tt.want)
		}
	}
}

func TestParseLastUpdate(t *testing.T) {
	tests := []struct {
		labels map[string]string
		want   int64
	}{
		{map[string]string{}, 0},
		{map[string]string{"last-update": ""}, 0},
		{map[string]string{"last-update": "ff"}, 255},
		{map[string]string{"last-update": "zzzz"}, 0},
	}
	for _, tt := range tests {
		got := parseLastUpdate(tt.labels)
		if got != tt.want {
			t.Errorf("parseLastUpdate(%v) = %d, want %d", tt.labels, got, tt.want)
		}
	}
}

func TestDMSelfPublicKey(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	dm, _, senderKP, _ := newTestDM(t, addr, nodeKP)
	if dm.SelfPublicKey() != senderKP.PublicKey() {
		t.Error("SelfPublicKey mismatch")
	}
}

func TestDMPeerPublicKey(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	dm, _, _, peerKP := newTestDM(t, addr, nodeKP)
	if dm.PeerPublicKey() != peerKP.PublicKey() {
		t.Error("PeerPublicKey mismatch")
	}
}

func TestQueryLabels(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	dm, _, _, _ := newTestDM(t, addr, nodeKP)

	labels := dm.queryLabels(map[string]string{"extra": "value"})
	if labels["app"] != "dm" {
		t.Errorf("app = %q, want dm", labels["app"])
	}
	if labels["type"] != "message" {
		t.Errorf("type = %q, want message", labels["type"])
	}
	if labels["extra"] != "value" {
		t.Errorf("extra = %q, want value", labels["extra"])
	}
	if labels["conversation"] == "" {
		t.Error("conversation should not be empty")
	}
}

func TestQueryLabelsNilExtra(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	dm, _, _, _ := newTestDM(t, addr, nodeKP)

	labels := dm.queryLabels(nil)
	if labels["app"] != "dm" {
		t.Errorf("app = %q, want dm", labels["app"])
	}
}

func TestThreadsSearchNoIndex(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	nodePub := nodeKP.PublicKey()
	threads := NewThreads(c, senderKP, WithNodeKey(nodePub))

	_, err := threads.Search(context.Background(), "test", SearchOptions{})
	if err == nil {
		t.Error("expected error searching without index")
	}
}

func TestThreadsSearchWithIndex(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	nodePub := nodeKP.PublicKey()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	threads := NewThreads(c, senderKP, WithNodeKey(nodePub))
	threads.SetSearchIndex(idx)

	if threads.SearchIndex() != idx {
		t.Error("SearchIndex getter mismatch")
	}

	cRef := reference.Compute([]byte("content"))
	mRef := reference.Compute([]byte("msg"))
	threads.IndexMessage(ctx, cRef, mRef, "searchable text", 1000)

	resp, err := threads.Search(ctx, "searchable", SearchOptions{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if resp.TotalCount != 1 {
		t.Errorf("expected 1 result, got %d", resp.TotalCount)
	}
}

func TestThreadsIndexMessageNoIndex(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	nodePub := nodeKP.PublicKey()
	threads := NewThreads(c, senderKP, WithNodeKey(nodePub))

	cRef := reference.Compute([]byte("content"))
	mRef := reference.Compute([]byte("msg"))
	threads.IndexMessage(context.Background(), cRef, mRef, "text", 1000)
}

func TestThreadsReindexNoIndex(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	nodePub := nodeKP.PublicKey()
	threads := NewThreads(c, senderKP, WithNodeKey(nodePub))

	_, err := threads.Reindex(context.Background())
	if err == nil {
		t.Error("expected error reindexing without index")
	}
}

func TestThreadsReindex(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	nodePub := nodeKP.PublicKey()
	ctx := context.Background()

	peer1KP, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	dm, err := New(c, senderKP, peer1KP.PublicKey(), WithNodeKey(nodePub))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dm.Send(ctx, []byte("reindex test msg"), nil); err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	threads := NewThreads(c, senderKP, WithNodeKey(nodePub))
	threads.SetSearchIndex(idx)

	result, err := threads.Reindex(ctx)
	if err != nil {
		t.Fatalf("Reindex: %v", err)
	}
	_ = result
}

func TestThreadsWithSearchIndexOption(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	nodePub := nodeKP.PublicKey()
	ctx := context.Background()

	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	threads := NewThreads(c, senderKP, WithNodeKey(nodePub))
	threads.SetSearchIndex(idx)

	peer, _ := identity.Generate()
	dm, err := threads.OpenConversation(peer.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	if dm.search != idx {
		t.Error("search index not passed to DM")
	}
	_ = ctx
}

func TestDMSendWithSearchIndex(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	nodePub := nodeKP.PublicKey()
	ctx := context.Background()

	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	peer, _ := identity.Generate()
	dm, err := New(c, senderKP, peer.PublicKey(), WithNodeKey(nodePub), WithSearchIndex(idx))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := dm.Send(ctx, []byte("indexed message"), nil); err != nil {
		t.Fatal(err)
	}

	var n int
	if err := idx.Count(ctx, &n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 indexed, got %d", n)
	}
}

func TestThreadsRecipientKeyWithNodeKey(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	nodePub := nodeKP.PublicKey()

	threads := NewThreads(c, senderKP, WithNodeKey(nodePub))
	got := threads.recipientKey()
	if got != nodePub {
		t.Errorf("recipientKey = %x, want nodeKey %x", got, nodePub)
	}
}

func TestThreadsRecipientKeyFallback(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	_ = addr
	_ = nodeKP

	threads := &Threads{client: c, kp: senderKP}
	got := threads.recipientKey()
	// Without nodeKey option, falls back to client.NodeKey() or self pubkey
	_ = got
}

func TestEd25519KeyConversions(t *testing.T) {
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i)
	}
	priv := ed25519SeedToCurve25519Private(seed)
	if priv[0]&7 != 0 {
		t.Error("clamping failed: low bits")
	}
	if priv[31]&128 != 0 {
		t.Error("clamping failed: high bit")
	}
	if priv[31]&64 == 0 {
		t.Error("clamping failed: second high bit")
	}
}

func TestEd25519PublicToCurve25519(t *testing.T) {
	kp, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	pub := kp.PublicKey()
	curve, err := ed25519PublicToCurve25519(pub)
	if err != nil {
		t.Fatal(err)
	}
	if len(curve) != 32 {
		t.Errorf("expected 32 bytes, got %d", len(curve))
	}
}

func TestFetchRemoteSearchInfo(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	nodePub := nodeKP.PublicKey()

	threads := NewThreads(c, senderKP, WithNodeKey(nodePub))
	ctx := context.Background()

	// No remote index exists yet
	info, err := threads.FetchRemoteSearchInfo(ctx)
	if err != nil {
		t.Fatalf("FetchRemoteSearchInfo: %v", err)
	}
	if info != nil {
		t.Error("expected nil info when no remote index exists")
	}
}

func TestPushAndPullSearchIndex(t *testing.T) {
	addr, nodeKP := newTestServer(t)
	c, senderKP := newTestClient(t, addr, nodeKP)
	nodePub := nodeKP.PublicKey()
	ctx := context.Background()

	// Create and populate a local search index
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}

	cRef := reference.Compute([]byte("c"))
	mRef := reference.Compute([]byte("m"))
	if err := idx.Index(ctx, cRef, mRef, "push test", 1000); err != nil {
		t.Fatal(err)
	}

	threads := NewThreads(c, senderKP, WithNodeKey(nodePub))
	threads.SetSearchIndex(idx)

	// Push
	ref, err := threads.PushSearchIndex(ctx, idx)
	if err != nil {
		t.Fatalf("PushSearchIndex: %v", err)
	}
	if ref == (reference.Reference{}) {
		t.Error("PushSearchIndex returned zero ref")
	}
	idx.Close()

	// Verify remote info
	info, err := threads.FetchRemoteSearchInfo(ctx)
	if err != nil {
		t.Fatalf("FetchRemoteSearchInfo: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil info after push")
	}
	if info.EntryCount != 1 {
		t.Errorf("EntryCount = %d, want 1", info.EntryCount)
	}

	// Pull
	destPath := filepath.Join(t.TempDir(), "pulled.db")
	pulled, err := threads.PullSearchIndex(ctx, destPath)
	if err != nil {
		t.Fatalf("PullSearchIndex: %v", err)
	}
	t.Cleanup(func() { pulled.Close() })

	var n int
	if err := pulled.Count(ctx, &n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("pulled index count = %d, want 1", n)
	}
}
