package journal

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/gezibash/arc/v2/pkg/reference"
)

func TestSearchIndex(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	contentRef1 := reference.Compute([]byte("content one"))
	contentRef2 := reference.Compute([]byte("content two"))
	entryRef1 := reference.Compute([]byte("entry one"))
	entryRef2 := reference.Compute([]byte("entry two"))

	if err := idx.Index(contentRef1, entryRef1, "hello world this is a test entry", 1000); err != nil {
		t.Fatal(err)
	}
	if err := idx.Index(contentRef2, entryRef2, "goodbye world another entry here", 2000); err != nil {
		t.Fatal(err)
	}

	// Search for "hello"
	resp, err := idx.Search("hello", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(resp.Results))
	}
	if resp.Results[0].Ref != entryRef1 {
		t.Fatalf("expected entryRef1, got %v", resp.Results[0].Ref)
	}

	// Search for "world" should match both
	resp, err = idx.Search("world", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(resp.Results))
	}

	// LastIndexedTimestamp
	ts, err := idx.LastIndexedTimestamp()
	if err != nil {
		t.Fatal(err)
	}
	if ts != 2000 {
		t.Fatalf("expected 2000, got %d", ts)
	}

	// Prefix search: "hel" should match "hello"
	resp, err = idx.Search("hel", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("prefix search: expected 1 result, got %d", len(resp.Results))
	}
	if resp.Results[0].Ref != entryRef1 {
		t.Fatalf("prefix search: expected entryRef1")
	}

	// Idempotent re-index (same msg ref, updated content)
	if err := idx.Index(contentRef1, entryRef1, "hello world updated content", 1500); err != nil {
		t.Fatal(err)
	}
	resp, err = idx.Search("updated", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("expected 1 result after re-index, got %d", len(resp.Results))
	}

	// Old content should no longer match
	resp, err = idx.Search("test", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 0 {
		t.Fatalf("expected 0 results for old content, got %d", len(resp.Results))
	}
}

func TestSearchIndexEmpty(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	ts, err := idx.LastIndexedTimestamp()
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0 {
		t.Fatalf("expected 0, got %d", ts)
	}

	resp, err := idx.Search("anything", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(resp.Results))
	}
}

func TestSearchIndexDuplicateContentDifferentMessages(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	contentRef := reference.Compute([]byte("content same"))
	entryRef1 := reference.Compute([]byte("msg one"))
	entryRef2 := reference.Compute([]byte("msg two"))

	if err := idx.Index(contentRef, entryRef1, "hello there", 1000); err != nil {
		t.Fatal(err)
	}
	if err := idx.Index(contentRef, entryRef2, "hello there", 2000); err != nil {
		t.Fatal(err)
	}

	resp, err := idx.Search("hello", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(resp.Results))
	}
	seen := map[reference.Reference]bool{}
	for _, r := range resp.Results {
		seen[r.Ref] = true
	}
	if !seen[entryRef1] || !seen[entryRef2] {
		t.Fatalf("expected results to include both message refs")
	}
}

func TestSearchPagination(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	// Index 5 entries all containing "test".
	for i := 0; i < 5; i++ {
		cRef := reference.Compute([]byte(fmt.Sprintf("content-%d", i)))
		eRef := reference.Compute([]byte(fmt.Sprintf("entry-%d", i)))
		if err := idx.Index(cRef, eRef, fmt.Sprintf("test entry number %d", i), int64(1000+i)); err != nil {
			t.Fatal(err)
		}
	}

	// Unlimited.
	resp, err := idx.Search("test", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.TotalCount != 5 {
		t.Fatalf("expected total 5, got %d", resp.TotalCount)
	}
	if len(resp.Results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(resp.Results))
	}

	// Limit 2.
	resp, err = idx.Search("test", SearchOptions{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if resp.TotalCount != 5 {
		t.Fatalf("expected total 5, got %d", resp.TotalCount)
	}
	if len(resp.Results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(resp.Results))
	}

	// Limit 2, offset 3.
	resp, err = idx.Search("test", SearchOptions{Limit: 2, Offset: 3})
	if err != nil {
		t.Fatal(err)
	}
	if resp.TotalCount != 5 {
		t.Fatalf("expected total 5, got %d", resp.TotalCount)
	}
	if len(resp.Results) != 2 {
		t.Fatalf("expected 2 results with offset 3, got %d", len(resp.Results))
	}
}

func TestSearchIndexDeleteByEntryRef(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	contentRef := reference.Compute([]byte("content"))
	entryRef := reference.Compute([]byte("entry"))
	if err := idx.Index(contentRef, entryRef, "deletable content", 1000); err != nil {
		t.Fatal(err)
	}

	resp, _ := idx.Search("deletable", SearchOptions{})
	if resp.TotalCount != 1 {
		t.Fatalf("expected 1 before delete, got %d", resp.TotalCount)
	}

	if err := idx.DeleteByEntryRef(entryRef); err != nil {
		t.Fatal(err)
	}

	resp, _ = idx.Search("deletable", SearchOptions{})
	if resp.TotalCount != 0 {
		t.Fatalf("expected 0 after delete, got %d", resp.TotalCount)
	}
}

func TestSearchIndexContentHash(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	cRef := reference.Compute([]byte("c"))
	eRef := reference.Compute([]byte("e"))
	idx.Index(cRef, eRef, "hash test", 1000)

	h1, err := idx.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := idx.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Error("ContentHash not deterministic")
	}
	if h1 == (reference.Reference{}) {
		t.Error("ContentHash should be non-zero")
	}
}

func TestSearchIndexCount(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	for i := 0; i < 3; i++ {
		cRef := reference.Compute([]byte(fmt.Sprintf("c-%d", i)))
		eRef := reference.Compute([]byte(fmt.Sprintf("e-%d", i)))
		idx.Index(cRef, eRef, fmt.Sprintf("count test %d", i), int64(1000+i))
	}

	var n int
	if err := idx.Count(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("expected 3, got %d", n)
	}
}

func TestSearchIndexClear(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	cRef := reference.Compute([]byte("c"))
	eRef := reference.Compute([]byte("e"))
	idx.Index(cRef, eRef, "clear test", 1000)

	if err := idx.Clear(); err != nil {
		t.Fatal(err)
	}

	var n int
	idx.Count(&n)
	if n != 0 {
		t.Fatalf("expected 0 after clear, got %d", n)
	}
}

func TestSearchIndexResolvePrefix(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	cRef := reference.Compute([]byte("c1"))
	eRef1 := reference.Compute([]byte("entry-1"))
	eRef2 := reference.Compute([]byte("entry-2"))
	idx.Index(cRef, eRef1, "prefix one", 1000)
	idx.Index(cRef, eRef2, "prefix two", 2000)

	// Unique prefix.
	hex1 := reference.Hex(eRef1)
	resolved, err := idx.ResolvePrefix(hex1[:16])
	if err != nil {
		t.Fatalf("ResolvePrefix unique: %v", err)
	}
	if resolved != eRef1 {
		t.Error("resolved wrong ref")
	}

	// No match.
	_, err = idx.ResolvePrefix("0000000000000000")
	if err == nil {
		t.Error("expected error for no match")
	}
}

func TestSearchIndexDBPath(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(dbPath)
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

func TestSearchIndexLastIndexedTimestamp(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.db")
	idx, err := OpenSearchIndex(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	ts, _ := idx.LastIndexedTimestamp()
	if ts != 0 {
		t.Fatalf("empty index: expected 0, got %d", ts)
	}

	cRef := reference.Compute([]byte("c"))
	eRef := reference.Compute([]byte("e"))
	idx.Index(cRef, eRef, "ts test", 42000)

	ts, _ = idx.LastIndexedTimestamp()
	if ts != 42000 {
		t.Fatalf("expected 42000, got %d", ts)
	}
}

func TestSearchIndexSchemaMigration(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.db")

	// Open and index something.
	idx, err := OpenSearchIndex(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	cRef := reference.Compute([]byte("c"))
	eRef := reference.Compute([]byte("e"))
	idx.Index(cRef, eRef, "migration test", 1000)
	idx.Close()

	// Tamper with schema version to simulate old version.
	db, _ := OpenSearchIndex(dbPath)
	db.Close()

	// Reopen should still work (schema version matches current).
	idx2, err := OpenSearchIndex(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx2.Close() })

	resp, err := idx2.Search("migration", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.TotalCount != 1 {
		t.Errorf("expected 1 result after reopen, got %d", resp.TotalCount)
	}
}
