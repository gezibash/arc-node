package journal

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
)

func TestWithSearchIndexOption(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatalf("OpenSearchIndex: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	kp, _ := identity.Generate()
	j := New(nil, kp, WithSearchIndex(idx))
	if j.SearchIndex() != idx {
		t.Error("SearchIndex() should return the attached index")
	}
}

func TestSearchIndexNilReturn(t *testing.T) {
	kp, _ := identity.Generate()
	j := New(nil, kp)
	if j.SearchIndex() != nil {
		t.Error("SearchIndex() should return nil when none attached")
	}
}

func TestIndexEntryNoSearch(t *testing.T) {
	kp, _ := identity.Generate()
	j := New(nil, kp)
	j.IndexEntry(context.Background(), reference.Reference{}, reference.Reference{}, "text", 123)
}

func TestParseEntryCount(t *testing.T) {
	tests := []struct {
		labels map[string]string
		want   int
	}{
		{nil, 0},
		{map[string]string{}, 0},
		{map[string]string{"entry-count": ""}, 0},
		{map[string]string{"entry-count": "5"}, 5},
		{map[string]string{"entry-count": "invalid"}, 0},
	}
	for _, tc := range tests {
		got := parseEntryCount(tc.labels)
		if got != tc.want {
			t.Errorf("parseEntryCount(%v) = %d, want %d", tc.labels, got, tc.want)
		}
	}
}

func TestParseLastUpdate(t *testing.T) {
	tests := []struct {
		labels map[string]string
		want   int64
	}{
		{nil, 0},
		{map[string]string{}, 0},
		{map[string]string{"last-update": ""}, 0},
		{map[string]string{"last-update": "ff"}, 255},
		{map[string]string{"last-update": "invalid"}, 0},
	}
	for _, tc := range tests {
		got := parseLastUpdate(tc.labels)
		if got != tc.want {
			t.Errorf("parseLastUpdate(%v) = %d, want %d", tc.labels, got, tc.want)
		}
	}
}

func TestPrefixQueryEmpty(t *testing.T) {
	got := prefixQuery("")
	if got != "" {
		t.Errorf("prefixQuery empty = %q, want empty", got)
	}
}

func TestPrefixQueryWithQuotes(t *testing.T) {
	got := prefixQuery("hello \"world\"")
	if !strings.Contains(got, "\"\"") {
		t.Errorf("prefixQuery should escape quotes, got %q", got)
	}
}

func TestPreviewNoContentLabel(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	_, err := j.Preview(ctx, Entry{Labels: map[string]string{}})
	if err == nil {
		t.Error("expected error for missing content label")
	}
}

func TestPreviewInvalidContentRef(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	_, err := j.Preview(ctx, Entry{Labels: map[string]string{"content": "not-hex"}})
	if err == nil {
		t.Error("expected error for invalid content ref")
	}
}

func TestReadDecryptError(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	raw := []byte("this is not encrypted content that will fail decryption")
	ref, err := j.client.PutContent(ctx, raw)
	if err != nil {
		t.Fatalf("PutContent: %v", err)
	}
	_, err = j.Read(ctx, ref)
	if err == nil {
		t.Error("expected decrypt error for raw content")
	}
}

func TestSearchStateSaveInvalidDir(t *testing.T) {
	s := &SearchState{path: "/nonexistent/dir/state.json"}
	err := s.Save()
	if err == nil {
		t.Error("expected error saving to nonexistent dir")
	}
}

func TestContentHashEmptyIndex(t *testing.T) {
	ctx := context.Background()
	idx, err := OpenSearchIndex(ctx, t.TempDir()+"/search.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	h, err := idx.ContentHash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h2, _ := idx.ContentHash(ctx)
	if h != h2 {
		t.Error("ContentHash of empty index should be deterministic")
	}
}

func TestResolvePrefixAmbiguous(t *testing.T) {
	ctx := context.Background()
	idx, err := OpenSearchIndex(ctx, t.TempDir()+"/search.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	cRef := reference.Compute([]byte("c"))
	eRef1 := reference.Compute([]byte("entry-1"))
	eRef2 := reference.Compute([]byte("entry-2"))
	_ = idx.Index(ctx, cRef, eRef1, "one", 1000)
	_ = idx.Index(ctx, cRef, eRef2, "two", 2000)
	_, err = idx.ResolvePrefix(ctx, "")
	if err == nil {
		t.Error("expected ambiguous error for empty prefix")
	}
	if !strings.Contains(err.Error(), "ambiguous") {
		t.Errorf("expected ambiguous error, got: %v", err)
	}
}

func TestRecipientKeyFallback(t *testing.T) {
	kp, _ := identity.Generate()
	j := &Journal{kp: kp, symKey: deriveKey(kp)}
	got := j.recipientKey()
	if got != kp.PublicKey() {
		t.Error("recipientKey should fall back to own public key")
	}
}

func TestRecipientKeyNodeKeyOption(t *testing.T) {
	kp, _ := identity.Generate()
	nodeKP, _ := identity.Generate()
	nodePub := nodeKP.PublicKey()
	j := &Journal{kp: kp, symKey: deriveKey(kp), nodeKey: &nodePub}
	got := j.recipientKey()
	if got != nodePub {
		t.Error("recipientKey should return nodeKey when set")
	}
}

func TestPullSearchIndexNoRemote(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	_, err := j.PullSearchIndex(ctx, t.TempDir()+"/pulled.db")
	if err == nil {
		t.Error("expected error pulling with no remote index")
	}
}

func TestSubscribeWithExpression(t *testing.T) {
	j := newTestJournal(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entries, _, err := j.Subscribe(ctx, ListOptions{Expression: "true"})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := j.Write(context.Background(), []byte("expr sub"), nil); err != nil {
		t.Fatalf("Write: %v", err)
	}
	select {
	case e := <-entries:
		if e == nil {
			t.Fatal("nil entry")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}
}

func TestPreviewLongContent(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	long := strings.Repeat("x", 500)
	if _, err := j.Write(ctx, []byte(long), nil); err != nil {
		t.Fatalf("Write: %v", err)
	}
	list, err := j.List(ctx, ListOptions{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	preview, err := j.Preview(ctx, list.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(preview, "...") {
		t.Error("long preview should be truncated")
	}
}

func TestPullSearchIndexInvalidDecrypt(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	dir := t.TempDir()
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	j.SetSearchIndex(idx)
	if _, err := j.Write(ctx, []byte("test"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := j.PushSearchIndex(ctx, idx); err != nil {
		t.Fatal(err)
	}
	kp2, _ := identity.Generate()
	j2 := New(j.client, kp2, WithNodeKey(*j.nodeKey))
	_, err = j2.PullSearchIndex(ctx, t.TempDir()+"/pulled.db")
	if err == nil {
		t.Error("expected error pulling with wrong key")
	}
}

func TestSubscribeCancelContext(t *testing.T) {
	j := newTestJournal(t)
	ctx, cancel := context.WithCancel(context.Background())
	entries, _, err := j.Subscribe(ctx, ListOptions{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cancel()
	select {
	case <-entries:
	case <-time.After(3 * time.Second):
		t.Fatal("channel did not close after cancel")
	}
}

func TestPreviewDecryptError(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	raw := []byte("not encrypted at all for preview test")
	ref, err := j.client.PutContent(ctx, raw)
	if err != nil {
		t.Fatalf("PutContent: %v", err)
	}
	e := Entry{Labels: map[string]string{"content": reference.Hex(ref)}}
	_, err = j.Preview(ctx, e)
	if err == nil {
		t.Error("expected decrypt error in Preview")
	}
}

func TestEditDecryptError(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	// Put raw unencrypted content and try Edit referencing it.
	// First write something real to get labels.
	orig, err := j.Write(ctx, []byte("original"), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Edit with valid new plaintext should work.
	list, err := j.List(ctx, ListOptions{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = j.Edit(ctx, orig.EntryRef, []byte("new content"), list.Entries[0].Labels)
	if err != nil {
		t.Fatalf("Edit should succeed: %v", err)
	}
}

func TestWriteWithLabels(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	dir := t.TempDir()
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	j.SetSearchIndex(idx)

	result, err := j.Write(ctx, []byte("labeled entry"), map[string]string{"mood": "happy"})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if result.Ref == (reference.Reference{}) {
		t.Error("zero ref")
	}

	// Search for it.
	resp, err := j.Search(ctx, "labeled", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.TotalCount != 1 {
		t.Errorf("expected 1, got %d", resp.TotalCount)
	}
}

func TestListDescending(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := j.Write(ctx, []byte("entry"), nil); err != nil {
			t.Fatal(err)
		}
	}

	list, err := j.List(ctx, ListOptions{Descending: true, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Entries) != 3 {
		t.Fatalf("expected 3, got %d", len(list.Entries))
	}
	// Descending: first entry should have latest timestamp.
	if list.Entries[0].Timestamp < list.Entries[2].Timestamp {
		t.Error("descending order not respected")
	}
}

func TestSearchWithPagination(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	dir := t.TempDir()
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	j.SetSearchIndex(idx)

	for i := 0; i < 5; i++ {
		if _, err := j.Write(ctx, []byte("searchable entry"), nil); err != nil {
			t.Fatal(err)
		}
	}

	resp, err := j.Search(ctx, "searchable", SearchOptions{Limit: 2, Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	if resp.TotalCount != 5 {
		t.Errorf("total = %d, want 5", resp.TotalCount)
	}
	if len(resp.Results) != 2 {
		t.Errorf("results = %d, want 2", len(resp.Results))
	}
}

func TestEditWithSearchIndexDeleteOld(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	dir := t.TempDir()
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	j.SetSearchIndex(idx)

	orig, err := j.Write(ctx, []byte("before edit"), nil)
	if err != nil {
		t.Fatal(err)
	}

	list, err := j.List(ctx, ListOptions{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}

	editResult, err := j.Edit(ctx, orig.EntryRef, []byte("after edit"), list.Entries[0].Labels)
	if err != nil {
		t.Fatal(err)
	}
	if editResult.EntryRef == (reference.Reference{}) {
		t.Error("zero entry ref")
	}

	// Old content should not be searchable.
	resp, _ := j.Search(ctx, "before", SearchOptions{})
	if resp.TotalCount != 0 {
		t.Errorf("old content still searchable: %d", resp.TotalCount)
	}

	// New content should be searchable.
	resp, _ = j.Search(ctx, "after", SearchOptions{})
	if resp.TotalCount != 1 {
		t.Errorf("new content not searchable: %d", resp.TotalCount)
	}
}

func TestFetchRemoteSearchInfoWithData(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	dir := t.TempDir()
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	j.SetSearchIndex(idx)

	// Write multiple entries.
	for i := 0; i < 3; i++ {
		if _, err := j.Write(ctx, []byte("info entry"), nil); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := j.PushSearchIndex(ctx, idx); err != nil {
		t.Fatal(err)
	}

	info, err := j.FetchRemoteSearchInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info == nil {
		t.Fatal("nil info")
	}
	if info.EntryCount != 3 {
		t.Errorf("entry count = %d, want 3", info.EntryCount)
	}
	if info.LastUpdate == 0 {
		t.Error("last update should be non-zero")
	}
	if info.DBHash == "" {
		t.Error("db hash should not be empty")
	}
}

func TestReindexWithReplacedEntries(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	dir := t.TempDir()
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	j.SetSearchIndex(idx)

	// Write original.
	orig, err := j.Write(ctx, []byte("original alpha"), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Edit it.
	list, _ := j.List(ctx, ListOptions{Limit: 1})
	_, err = j.Edit(ctx, orig.EntryRef, []byte("edited alpha"), list.Entries[0].Labels)
	if err != nil {
		t.Fatal(err)
	}

	// Write another.
	if _, err := j.Write(ctx, []byte("entry beta"), nil); err != nil {
		t.Fatal(err)
	}

	// Reindex.
	if err := j.Reindex(ctx); err != nil {
		t.Fatal(err)
	}

	// Should have 2 entries: edited alpha + beta.
	var count int
	idx.Count(ctx, &count)
	if count != 2 {
		t.Errorf("expected 2 entries after reindex, got %d", count)
	}
}

func TestReadForEditResolveError(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	// Non-existent ref should fail at resolve.
	_, _, err := j.ReadForEdit(ctx, "0000000000000000000000000000000000000000000000000000000000000000")
	if err == nil {
		t.Error("expected error for non-existent ref")
	}
}

func TestReadForEditNoContentLabel(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	// We need a message with no content label. Send a raw message.
	pub := j.kp.PublicKey()
	toPub := j.recipientKey()
	contentRef := reference.Compute([]byte("raw"))
	msg := message.New(pub, toPub, contentRef, "text/plain")
	if err := message.Sign(&msg, j.kp); err != nil {
		t.Fatal(err)
	}
	result, err := j.client.SendMessage(ctx, msg, map[string]string{"app": "journal", "type": "entry"}, nil)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	_, _, err = j.ReadForEdit(ctx, reference.Hex(result.Ref))
	if err == nil {
		t.Error("expected error for missing content label")
	}
}

func TestReadForEditDecryptError(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	// Store raw (unencrypted) content, create a message pointing to it.
	raw := []byte("not encrypted for edit test purposes")
	contentRef, err := j.client.PutContent(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	pub := j.kp.PublicKey()
	toPub := j.recipientKey()
	msg := message.New(pub, toPub, contentRef, contentType)
	if err := message.Sign(&msg, j.kp); err != nil {
		t.Fatal(err)
	}
	result, err := j.client.SendMessage(ctx, msg, map[string]string{
		"app":     "journal",
		"type":    "entry",
		"content": reference.Hex(contentRef),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = j.ReadForEdit(ctx, reference.Hex(result.Ref))
	if err == nil {
		t.Error("expected decrypt error")
	}
}

func TestSubscribeError(t *testing.T) {
	j := newTestJournal(t)
	ctx, cancel := context.WithCancel(context.Background())

	entries, errs, err := j.Subscribe(ctx, ListOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Cancel should cause channels to close.
	cancel()
	time.Sleep(200 * time.Millisecond)

	// Drain both channels.
	select {
	case <-entries:
	case <-errs:
	case <-time.After(3 * time.Second):
		t.Fatal("channels did not close")
	}
}

func TestWriteWithClosedClient(t *testing.T) {
	// Create a journal, close the client, then try to write.
	j := newTestJournal(t)
	ctx := context.Background()

	// First write succeeds.
	_, err := j.Write(ctx, []byte("test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Close client connection.
	_ = j.client.Close()

	// Write should fail.
	_, err = j.Write(ctx, []byte("should fail"), nil)
	if err == nil {
		t.Error("expected error after closing client")
	}
}

func TestEditWithClosedClient(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	orig, err := j.Write(ctx, []byte("original"), nil)
	if err != nil {
		t.Fatal(err)
	}

	list, _ := j.List(ctx, ListOptions{Limit: 1})

	_ = j.client.Close()

	_, err = j.Edit(ctx, orig.EntryRef, []byte("edited"), list.Entries[0].Labels)
	if err == nil {
		t.Error("expected error after closing client")
	}
}

func TestReadForEditWithClosedClient(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	result, _ := j.Write(ctx, []byte("test"), nil)
	list, _ := j.List(ctx, ListOptions{Limit: 1})
	refHex := reference.Hex(list.Entries[0].Ref)
	_ = result

	_ = j.client.Close()

	_, _, err := j.ReadForEdit(ctx, refHex)
	if err == nil {
		t.Error("expected error after closing client")
	}
}

func TestListWithClosedClient(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	_ = j.client.Close()
	_, err := j.List(ctx, ListOptions{})
	if err == nil {
		t.Error("expected error after closing client")
	}
}

func TestPreviewWithClosedClient(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	_, _ = j.Write(ctx, []byte("test"), nil)
	list, _ := j.List(ctx, ListOptions{Limit: 1})

	_ = j.client.Close()

	_, err := j.Preview(ctx, list.Entries[0])
	if err == nil {
		t.Error("expected error after closing client")
	}
}

func TestFetchRemoteSearchInfoWithClosedClient(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	_ = j.client.Close()
	_, err := j.FetchRemoteSearchInfo(ctx)
	if err == nil {
		t.Error("expected error after closing client")
	}
}

func TestPushSearchIndexWithClosedClient(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	dir := t.TempDir()
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	cRef := reference.Compute([]byte("c"))
	eRef := reference.Compute([]byte("e"))
	_ = idx.Index(ctx, cRef, eRef, "test", 1000)

	_ = j.client.Close()

	_, err = j.PushSearchIndex(ctx, idx)
	if err == nil {
		t.Error("expected error after closing client")
	}
}

func TestPullSearchIndexWithClosedClient(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	_ = j.client.Close()
	_, err := j.PullSearchIndex(ctx, t.TempDir()+"/pulled.db")
	if err == nil {
		t.Error("expected error after closing client")
	}
}

func TestSubscribeWithClosedClient(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	_ = j.client.Close()
	_, _, err := j.Subscribe(ctx, ListOptions{})
	if err == nil {
		t.Error("expected error after closing client")
	}
}

func TestReindexWithClosedClient(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	dir := t.TempDir()
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	j.SetSearchIndex(idx)

	_ = j.client.Close()

	err = j.Reindex(ctx)
	if err == nil {
		t.Error("expected error after closing client")
	}
}

func TestSearchOnClosedIndex(t *testing.T) {
	ctx := context.Background()
	idx, err := OpenSearchIndex(ctx, t.TempDir()+"/search.db")
	if err != nil {
		t.Fatal(err)
	}
	cRef := reference.Compute([]byte("c"))
	eRef := reference.Compute([]byte("e"))
	_ = idx.Index(ctx, cRef, eRef, "test", 1000)
	idx.Close()

	// Operations on closed DB should fail.
	_, err = idx.Search(ctx, "test", SearchOptions{})
	if err == nil {
		t.Error("expected error on closed index")
	}

	_, err = idx.ContentHash(ctx)
	if err == nil {
		t.Error("expected error on closed index ContentHash")
	}

	_, err = idx.LastIndexedTimestamp(ctx)
	if err == nil {
		t.Error("expected error on closed index LastIndexedTimestamp")
	}

	err = idx.Index(ctx, cRef, eRef, "test2", 2000)
	if err == nil {
		t.Error("expected error on closed index Index")
	}

	_, err = idx.ResolvePrefix(ctx, "abc")
	if err == nil {
		t.Error("expected error on closed index ResolvePrefix")
	}

	var n int
	err = idx.Count(ctx, &n)
	if err == nil {
		t.Error("expected error on closed index Count")
	}
}

func TestReadForEditNotMessage(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	// Store content directly and try ReadForEdit with the content ref.
	// ResolveGet for a content ref should return GetKindContent, not GetKindMessage.
	raw := []byte("just some content")
	contentRef, err := j.client.PutContent(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = j.ReadForEdit(ctx, reference.Hex(contentRef))
	if err == nil {
		t.Error("expected error: reference is not a message")
	}
}

func TestReindexClearError(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()

	dir := t.TempDir()
	idx, err := OpenSearchIndex(ctx, dir+"/search.db")
	if err != nil {
		t.Fatal(err)
	}
	j.SetSearchIndex(idx)

	// Close the index to force clear error.
	idx.Close()

	err = j.Reindex(ctx)
	if err == nil {
		t.Error("expected error reindexing with closed index")
	}
}

func TestReadGetContentError(t *testing.T) {
	j := newTestJournal(t)
	ctx := context.Background()
	// Non-existent content ref.
	var fakeRef reference.Reference
	fakeRef[0] = 0xFF
	_, err := j.Read(ctx, fakeRef)
	if err == nil {
		t.Error("expected error for non-existent content")
	}
}
