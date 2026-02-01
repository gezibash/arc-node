package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/gezibash/arc/v2/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

func newTestBackend(t *testing.T) physical.Backend {
	t.Helper()
	cfg := map[string]string{"path": filepath.Join(t.TempDir(), "test.db")}
	be, err := NewFactory(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { be.Close() })
	return be
}

func testRef(b byte) reference.Reference {
	var r reference.Reference
	r[0] = b
	return r
}

func TestPutAndGet(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("hello"))
	entry := &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"type": "text"},
		Timestamp: time.Now().UnixMilli(),
		ExpiresAt: time.Now().Add(time.Hour).UnixMilli(),
	}

	if err := be.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := be.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !reference.Equal(got.Ref, ref) {
		t.Errorf("ref = %s, want %s", reference.Hex(got.Ref), reference.Hex(ref))
	}
	if got.Labels["type"] != "text" {
		t.Errorf("labels = %v, want type=text", got.Labels)
	}
	if got.Timestamp != entry.Timestamp {
		t.Errorf("timestamp = %d, want %d", got.Timestamp, entry.Timestamp)
	}
	if got.ExpiresAt != entry.ExpiresAt {
		t.Errorf("expires_at = %d, want %d", got.ExpiresAt, entry.ExpiresAt)
	}
}

func TestGetNotFound(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	_, err := be.Get(ctx, testRef(0xFF))
	if !errors.Is(err, physical.ErrNotFound) {
		t.Errorf("Get = %v, want ErrNotFound", err)
	}
}

func TestPutReplacement(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("replace-me"))
	be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"v": "1"},
		Timestamp: 1000,
	})

	be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"v": "2"},
		Timestamp: 2000,
	})

	got, err := be.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Labels["v"] != "2" {
		t.Errorf("label v = %s, want 2", got.Labels["v"])
	}
	if got.Timestamp != 2000 {
		t.Errorf("timestamp = %d, want 2000", got.Timestamp)
	}
}

func TestDeleteIdempotent(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("delete-me"))
	be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"k": "v"},
		Timestamp: time.Now().UnixMilli(),
	})

	if err := be.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := be.Get(ctx, ref)
	if !errors.Is(err, physical.ErrNotFound) {
		t.Errorf("Get after delete = %v, want ErrNotFound", err)
	}

	// Second delete should not error.
	if err := be.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete (idempotent): %v", err)
	}
}

func TestQueryNoFilter(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 5 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"x": "y"},
			Timestamp: int64(1000 + i),
		})
	}

	// Ascending
	res, err := be.Query(ctx, &physical.QueryOptions{Limit: 100})
	if err != nil {
		t.Fatalf("Query ASC: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Fatalf("got %d entries, want 5", len(res.Entries))
	}
	for i := 1; i < len(res.Entries); i++ {
		if res.Entries[i].Timestamp < res.Entries[i-1].Timestamp {
			t.Error("entries not in ascending order")
		}
	}

	// Descending
	res, err = be.Query(ctx, &physical.QueryOptions{Limit: 100, Descending: true})
	if err != nil {
		t.Fatalf("Query DESC: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Fatalf("got %d entries, want 5", len(res.Entries))
	}
	for i := 1; i < len(res.Entries); i++ {
		if res.Entries[i].Timestamp > res.Entries[i-1].Timestamp {
			t.Error("entries not in descending order")
		}
	}
}

func TestQuerySingleLabel(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		label := "a"
		if i%2 == 0 {
			label = "b"
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"group": label},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"group": "b"},
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Errorf("got %d entries, want 5", len(res.Entries))
	}
}

func TestQueryMultiLabel(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		grp := "a"
		if i%2 == 0 {
			grp = "b"
		}
		env := "dev"
		if i%3 == 0 {
			env = "prod"
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"group": grp, "env": env},
			Timestamp: int64(1000 + i),
		})
	}

	// group=b AND env=prod -> indices 0, 6 (even AND divisible by 3)
	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"group": "b", "env": "prod"},
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 2 {
		t.Errorf("got %d entries, want 2", len(res.Entries))
	}
}

func TestQueryTimeRange(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"x": "y"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		After:  1002,
		Before: 1007,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// timestamps 1003, 1004, 1005, 1006
	if len(res.Entries) != 4 {
		t.Errorf("got %d entries, want 4", len(res.Entries))
	}
}

func TestQueryLabelAndTimeRange(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		grp := "a"
		if i%2 == 0 {
			grp = "b"
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"group": grp},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"group": "b"},
		After:  1001,
		Before: 1008,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// group=b (even): 0,2,4,6,8. After 1001 and before 1008: 2,4,6
	if len(res.Entries) != 3 {
		t.Errorf("got %d entries, want 3", len(res.Entries))
	}
}

func TestQueryPagination(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"x": "y"},
			Timestamp: int64(1000 + i),
		})
	}

	var total int
	cursor := ""
	for {
		res, err := be.Query(ctx, &physical.QueryOptions{Limit: 3, Cursor: cursor})
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		total += len(res.Entries)
		if !res.HasMore {
			break
		}
		cursor = res.NextCursor
	}
	if total != 10 {
		t.Errorf("paginated total = %d, want 10", total)
	}
}

func TestDeleteExpired(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()
	now := time.Now()

	for i := range 6 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		exp := now.Add(time.Hour).UnixMilli()
		if i < 3 {
			exp = now.Add(-time.Hour).UnixMilli()
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"i": "v"},
			Timestamp: int64(1000 + i),
			ExpiresAt: exp,
		})
	}

	n, err := be.DeleteExpired(ctx, now)
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}
	if n != 3 {
		t.Errorf("deleted = %d, want 3", n)
	}
}

func TestClosedBackend(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	be.Close()

	if err := be.Put(ctx, &physical.Entry{}); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Put = %v, want ErrClosed", err)
	}
	if _, err := be.Get(ctx, testRef(0)); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Get = %v, want ErrClosed", err)
	}
	if err := be.Delete(ctx, testRef(0)); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Delete = %v, want ErrClosed", err)
	}
	if _, err := be.Query(ctx, nil); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Query = %v, want ErrClosed", err)
	}
	if _, err := be.DeleteExpired(ctx, time.Now()); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("DeleteExpired = %v, want ErrClosed", err)
	}
	if _, err := be.Stats(ctx); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Stats = %v, want ErrClosed", err)
	}
}

func TestStats(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	stats, err := be.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.BackendType != "sqlite" {
		t.Errorf("BackendType = %s, want sqlite", stats.BackendType)
	}
}

func TestDimensionFieldsRoundTrip(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("dim-roundtrip"))
	entry := &physical.Entry{
		Ref:              ref,
		Labels:           map[string]string{"type": "test"},
		Timestamp:        time.Now().UnixMilli(),
		ExpiresAt:        time.Now().Add(time.Hour).UnixMilli(),
		Persistence:      1,
		Visibility:       2,
		DeliveryMode:     1,
		Pattern:          3,
		Affinity:         2,
		AffinityKey:      "my-key",
		Ordering:         1,
		DedupMode:        2,
		IdempotencyKey:   "idem-123",
		DeliveryComplete: 1,
		CompleteN:        3,
		Priority:         42,
		MaxRedelivery:    5,
		AckTimeoutMs:     15000,
		Correlation:      "corr-abc",
	}

	if err := be.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := be.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Persistence != 1 {
		t.Errorf("Persistence = %d, want 1", got.Persistence)
	}
	if got.Visibility != 2 {
		t.Errorf("Visibility = %d, want 2", got.Visibility)
	}
	if got.DeliveryMode != 1 {
		t.Errorf("DeliveryMode = %d, want 1", got.DeliveryMode)
	}
	if got.Pattern != 3 {
		t.Errorf("Pattern = %d, want 3", got.Pattern)
	}
	if got.Affinity != 2 {
		t.Errorf("Affinity = %d, want 2", got.Affinity)
	}
	if got.AffinityKey != "my-key" {
		t.Errorf("AffinityKey = %q, want %q", got.AffinityKey, "my-key")
	}
	if got.Ordering != 1 {
		t.Errorf("Ordering = %d, want 1", got.Ordering)
	}
	if got.DedupMode != 2 {
		t.Errorf("DedupMode = %d, want 2", got.DedupMode)
	}
	if got.IdempotencyKey != "idem-123" {
		t.Errorf("IdempotencyKey = %q, want %q", got.IdempotencyKey, "idem-123")
	}
	if got.DeliveryComplete != 1 {
		t.Errorf("DeliveryComplete = %d, want 1", got.DeliveryComplete)
	}
	if got.CompleteN != 3 {
		t.Errorf("CompleteN = %d, want 3", got.CompleteN)
	}
	if got.Priority != 42 {
		t.Errorf("Priority = %d, want 42", got.Priority)
	}
	if got.MaxRedelivery != 5 {
		t.Errorf("MaxRedelivery = %d, want 5", got.MaxRedelivery)
	}
	if got.AckTimeoutMs != 15000 {
		t.Errorf("AckTimeoutMs = %d, want 15000", got.AckTimeoutMs)
	}
	if got.Correlation != "corr-abc" {
		t.Errorf("Correlation = %q, want %q", got.Correlation, "corr-abc")
	}
}

func TestDefaults(t *testing.T) {
	d := Defaults()
	if d[KeyPath] != "~/.arc/index.db" {
		t.Errorf("path = %s, want ~/.arc/index.db", d[KeyPath])
	}
	if d[KeyJournalMode] != "wal" {
		t.Errorf("journal_mode = %s, want wal", d[KeyJournalMode])
	}
	if d[KeyBusyTimeout] != "5000" {
		t.Errorf("busy_timeout = %s, want 5000", d[KeyBusyTimeout])
	}
	if d[KeyCacheSize] != "-64000" {
		t.Errorf("cache_size = %s, want -64000", d[KeyCacheSize])
	}
}

func TestNewFactoryEmptyPath(t *testing.T) {
	_, err := NewFactory(context.Background(), map[string]string{"path": ""})
	if err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestPutBatch(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	entries := make([]*physical.Entry, 5)
	for i := range entries {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(100+i))
		entries[i] = &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"batch": "yes"},
			Timestamp: int64(2000 + i),
		}
	}

	if err := be.PutBatch(ctx, entries); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}

	for _, e := range entries {
		got, err := be.Get(ctx, e.Ref)
		if err != nil {
			t.Fatalf("Get after PutBatch: %v", err)
		}
		if got.Labels["batch"] != "yes" {
			t.Errorf("label batch = %s, want yes", got.Labels["batch"])
		}
	}
}

func TestPutBatchClosed(t *testing.T) {
	be := newTestBackend(t)
	be.Close()

	err := be.PutBatch(context.Background(), []*physical.Entry{{Ref: testRef(1)}})
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("PutBatch on closed = %v, want ErrClosed", err)
	}
}

func TestPutNoLabels(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("no-labels"))
	if err := be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Timestamp: 5000,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := be.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Labels) != 0 {
		t.Errorf("labels = %v, want empty", got.Labels)
	}
}

func TestQueryByLabelFilter(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		grp := "a"
		if i%2 == 0 {
			grp = "b"
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(200+i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"group": grp},
			Timestamp: int64(3000 + i),
		})
	}

	// OR filter: group=a OR group=b -> all 10
	res, err := be.Query(ctx, &physical.QueryOptions{
		LabelFilter: &physical.LabelFilter{
			OR: []physical.LabelFilterGroup{
				{Predicates: []physical.LabelPredicate{{Key: "group", Value: "a"}}},
				{Predicates: []physical.LabelPredicate{{Key: "group", Value: "b"}}},
			},
		},
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 10 {
		t.Errorf("got %d entries, want 10", len(res.Entries))
	}

	// OR filter: just group=a -> 5
	res, err = be.Query(ctx, &physical.QueryOptions{
		LabelFilter: &physical.LabelFilter{
			OR: []physical.LabelFilterGroup{
				{Predicates: []physical.LabelPredicate{{Key: "group", Value: "a"}}},
			},
		},
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Errorf("got %d entries, want 5", len(res.Entries))
	}
}

func TestQueryByLabelFilterDescending(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 6 {
		grp := "x"
		if i < 3 {
			grp = "y"
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(300+i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"g": grp},
			Timestamp: int64(4000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		LabelFilter: &physical.LabelFilter{
			OR: []physical.LabelFilterGroup{
				{Predicates: []physical.LabelPredicate{{Key: "g", Value: "x"}}},
				{Predicates: []physical.LabelPredicate{{Key: "g", Value: "y"}}},
			},
		},
		Limit:      100,
		Descending: true,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 6 {
		t.Fatalf("got %d entries, want 6", len(res.Entries))
	}
	for i := 1; i < len(res.Entries); i++ {
		if res.Entries[i].Timestamp > res.Entries[i-1].Timestamp {
			t.Error("entries not in descending order")
		}
	}
}

func TestQueryByLabelFilterWithPagination(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 8 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(400+i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"k": "v"},
			Timestamp: int64(5000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		LabelFilter: &physical.LabelFilter{
			OR: []physical.LabelFilterGroup{
				{Predicates: []physical.LabelPredicate{{Key: "k", Value: "v"}}},
			},
		},
		Limit: 3,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !res.HasMore {
		t.Error("expected HasMore=true")
	}
	if res.NextCursor == "" {
		t.Error("expected non-empty NextCursor")
	}
	if len(res.Entries) != 3 {
		t.Errorf("got %d entries, want 3", len(res.Entries))
	}
}

func TestCount(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 8 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(500+i))
		label := "aa"
		if i < 3 {
			label = "bb"
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"t": label},
			Timestamp: int64(6000 + i),
		})
	}

	// Count all
	n, err := be.Count(ctx, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 8 {
		t.Errorf("count = %d, want 8", n)
	}

	// Count with label
	n, err = be.Count(ctx, &physical.QueryOptions{
		Labels: map[string]string{"t": "bb"},
	})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 3 {
		t.Errorf("count = %d, want 3", n)
	}

	// Count with time range
	n, err = be.Count(ctx, &physical.QueryOptions{
		After:  6002,
		Before: 6006,
	})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 3 {
		t.Errorf("count = %d, want 3 (6003,6004,6005)", n)
	}
}

func TestCountClosed(t *testing.T) {
	be := newTestBackend(t)
	be.Close()

	_, err := be.Count(context.Background(), nil)
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Count on closed = %v, want ErrClosed", err)
	}
}

func TestCountIncludeExpired(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()
	now := time.Now()

	// 2 expired, 2 not
	for i := range 4 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(600+i))
		exp := now.Add(time.Hour).UnixMilli()
		if i < 2 {
			exp = now.Add(-time.Hour).UnixMilli()
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"z": "z"},
			Timestamp: int64(7000 + i),
			ExpiresAt: exp,
		})
	}

	// Without IncludeExpired
	n, err := be.Count(ctx, &physical.QueryOptions{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 2 {
		t.Errorf("count = %d, want 2 (non-expired)", n)
	}

	// With IncludeExpired
	n, err = be.Count(ctx, &physical.QueryOptions{IncludeExpired: true})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 4 {
		t.Errorf("count = %d, want 4 (all)", n)
	}
}

func TestCursorOperations(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	// GetCursor not found
	_, err := be.GetCursor(ctx, "missing")
	if !errors.Is(err, physical.ErrCursorNotFound) {
		t.Errorf("GetCursor = %v, want ErrCursorNotFound", err)
	}

	// PutCursor + GetCursor
	cur := physical.Cursor{Timestamp: 12345, Sequence: 99}
	if err := be.PutCursor(ctx, "sub-1", cur); err != nil {
		t.Fatalf("PutCursor: %v", err)
	}

	got, err := be.GetCursor(ctx, "sub-1")
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if got.Timestamp != 12345 || got.Sequence != 99 {
		t.Errorf("cursor = %+v, want {12345 99}", got)
	}

	// PutCursor replace
	cur2 := physical.Cursor{Timestamp: 99999, Sequence: 200}
	if err := be.PutCursor(ctx, "sub-1", cur2); err != nil {
		t.Fatalf("PutCursor replace: %v", err)
	}
	got, _ = be.GetCursor(ctx, "sub-1")
	if got.Timestamp != 99999 {
		t.Errorf("cursor timestamp = %d, want 99999", got.Timestamp)
	}

	// DeleteCursor
	if err := be.DeleteCursor(ctx, "sub-1"); err != nil {
		t.Fatalf("DeleteCursor: %v", err)
	}
	_, err = be.GetCursor(ctx, "sub-1")
	if !errors.Is(err, physical.ErrCursorNotFound) {
		t.Errorf("GetCursor after delete = %v, want ErrCursorNotFound", err)
	}

	// DeleteCursor idempotent
	if err := be.DeleteCursor(ctx, "sub-1"); err != nil {
		t.Fatalf("DeleteCursor idempotent: %v", err)
	}
}

func TestCursorsClosed(t *testing.T) {
	be := newTestBackend(t)
	be.Close()
	ctx := context.Background()

	if err := be.PutCursor(ctx, "k", physical.Cursor{}); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("PutCursor on closed = %v, want ErrClosed", err)
	}
	if _, err := be.GetCursor(ctx, "k"); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("GetCursor on closed = %v, want ErrClosed", err)
	}
	if err := be.DeleteCursor(ctx, "k"); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("DeleteCursor on closed = %v, want ErrClosed", err)
	}
}

func TestQueryNilOpts(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	res, err := be.Query(ctx, nil)
	if err != nil {
		t.Fatalf("Query(nil): %v", err)
	}
	if len(res.Entries) != 0 {
		t.Errorf("got %d entries, want 0", len(res.Entries))
	}
}

func TestQueryIncludeExpired(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()
	now := time.Now()

	for i := range 4 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(700+i))
		exp := now.Add(time.Hour).UnixMilli()
		if i < 2 {
			exp = now.Add(-time.Hour).UnixMilli()
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"q": "q"},
			Timestamp: int64(8000 + i),
			ExpiresAt: exp,
		})
	}

	// Without IncludeExpired: only 2
	res, err := be.Query(ctx, &physical.QueryOptions{Limit: 100})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 2 {
		t.Errorf("got %d, want 2", len(res.Entries))
	}

	// With IncludeExpired: all 4
	res, err = be.Query(ctx, &physical.QueryOptions{Limit: 100, IncludeExpired: true})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 4 {
		t.Errorf("got %d, want 4", len(res.Entries))
	}
}

func TestQueryLabelAndTimeDescending(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(800+i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"k": "v"},
			Timestamp: int64(9000 + i),
		})
	}

	// Label + time + descending (exercises the hasLabels && hasTime + descending branch)
	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels:     map[string]string{"k": "v"},
		After:      9002,
		Before:     9008,
		Descending: true,
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Errorf("got %d, want 5 (9003-9007)", len(res.Entries))
	}
	for i := 1; i < len(res.Entries); i++ {
		if res.Entries[i].Timestamp > res.Entries[i-1].Timestamp {
			t.Error("not in descending order")
		}
	}
}

func TestQueryLabelAndTimePagination(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(900+i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"p": "q"},
			Timestamp: int64(10000 + i),
		})
	}

	// Paginate with label + time (cursor in the hasLabels && hasTime path)
	var total int
	cursor := ""
	for {
		res, err := be.Query(ctx, &physical.QueryOptions{
			Labels: map[string]string{"p": "q"},
			After:  10001,
			Before: 10009,
			Limit:  2,
			Cursor: cursor,
		})
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		total += len(res.Entries)
		if !res.HasMore {
			break
		}
		cursor = res.NextCursor
	}
	if total != 7 {
		t.Errorf("paginated total = %d, want 7 (10002-10008)", total)
	}
}

func TestQueryLabelAndTimeDescendingPagination(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(1000+i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"d": "e"},
			Timestamp: int64(11000 + i),
		})
	}

	var total int
	cursor := ""
	for {
		res, err := be.Query(ctx, &physical.QueryOptions{
			Labels:     map[string]string{"d": "e"},
			After:      11001,
			Before:     11009,
			Limit:      2,
			Cursor:     cursor,
			Descending: true,
		})
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		total += len(res.Entries)
		if !res.HasMore {
			break
		}
		cursor = res.NextCursor
	}
	if total != 7 {
		t.Errorf("paginated total = %d, want 7", total)
	}
}

func TestQueryMultiLabelAndTime(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		env := "dev"
		if i%2 == 0 {
			env = "prod"
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(1100+i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"app": "test", "env": env},
			Timestamp: int64(12000 + i),
		})
	}

	// Multi-label + time range (exercises the hasLabels && hasTime path with extra JOIN)
	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"app": "test", "env": "prod"},
		After:  12001,
		Before: 12009,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// prod = even indices: 0,2,4,6,8. After 12001 & before 12009: 2,4,6,8
	if len(res.Entries) != 4 {
		t.Errorf("got %d entries, want 4", len(res.Entries))
	}
}

func TestQueryDescendingPagination(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(1200+i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"x": "y"},
			Timestamp: int64(13000 + i),
		})
	}

	var total int
	cursor := ""
	for {
		res, err := be.Query(ctx, &physical.QueryOptions{
			Limit:      3,
			Cursor:     cursor,
			Descending: true,
		})
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		total += len(res.Entries)
		if !res.HasMore {
			break
		}
		cursor = res.NextCursor
	}
	if total != 10 {
		t.Errorf("paginated total = %d, want 10", total)
	}
}

func TestDeleteExpiredNone(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	// Entry with no expiry
	be.Put(ctx, &physical.Entry{
		Ref:       reference.Compute([]byte("no-expire")),
		Timestamp: 1000,
		ExpiresAt: 0,
	})

	n, err := be.DeleteExpired(ctx, time.Now())
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}
	if n != 0 {
		t.Errorf("deleted = %d, want 0", n)
	}
}

func TestStatsHasSizeBytes(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	// Put some data
	for i := range 5 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(1300+i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"s": "t"},
			Timestamp: int64(14000 + i),
		})
	}

	stats, err := be.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.SizeBytes <= 0 {
		t.Errorf("SizeBytes = %d, want > 0", stats.SizeBytes)
	}
	if stats.BackendType != "sqlite" {
		t.Errorf("BackendType = %s, want sqlite", stats.BackendType)
	}
}

func TestCloseIdempotent(t *testing.T) {
	cfg := map[string]string{"path": filepath.Join(t.TempDir(), "close.db")}
	be, err := NewFactory(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := be.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second close should be no-op
	if err := be.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

var (
	benchSizes = []int{100, 1_000, 10_000}

	typeValues = []string{"text", "image", "video", "audio", "document", "spreadsheet", "presentation", "archive", "code", "binary"}
	envValues  = []string{"prod", "staging", "dev", "test", "ci"}
	userPool   = func() []string {
		u := make([]string, 100)
		for i := range u {
			u[i] = fmt.Sprintf("user-%03d", i)
		}
		return u
	}()
)

const (
	baseTimestamp = int64(1735689600000)
	farFuture     = int64(2051222400000)
)

func benchRef(seed int) reference.Reference {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(seed))
	return reference.Reference(sha256.Sum256(buf[:]))
}

func benchEntry(rng *rand.Rand, i int) *physical.Entry {
	return &physical.Entry{
		Ref: benchRef(i),
		Labels: map[string]string{
			"type": typeValues[rng.Intn(len(typeValues))],
			"env":  envValues[rng.Intn(len(envValues))],
			"user": userPool[rng.Intn(len(userPool))],
		},
		Timestamp: baseTimestamp + int64(i),
		ExpiresAt: farFuture + int64(i),
	}
}

func benchEntryWithExpiry(rng *rand.Rand, i int, n int) *physical.Entry {
	var expiresAt int64
	if rng.Intn(2) == 0 {
		expiresAt = baseTimestamp + int64(rng.Intn(n/2))
	} else {
		expiresAt = farFuture + int64(rng.Intn(n))
	}
	return &physical.Entry{
		Ref: benchRef(i),
		Labels: map[string]string{
			"type": typeValues[rng.Intn(len(typeValues))],
			"env":  envValues[rng.Intn(len(envValues))],
			"user": userPool[rng.Intn(len(userPool))],
		},
		Timestamp: baseTimestamp + int64(i),
		ExpiresAt: expiresAt,
	}
}

func seedBench(b *testing.B, be physical.Backend, n int) {
	b.Helper()
	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()
	const batchSize = 1000
	batch := make([]*physical.Entry, 0, batchSize)
	for i := range n {
		batch = append(batch, benchEntry(rng, i))
		if len(batch) >= batchSize {
			if err := be.PutBatch(ctx, batch); err != nil {
				b.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := be.PutBatch(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func seedBenchWithExpiry(b *testing.B, be physical.Backend, n int) {
	b.Helper()
	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()
	const batchSize = 1000
	batch := make([]*physical.Entry, 0, batchSize)
	for i := range n {
		batch = append(batch, benchEntryWithExpiry(rng, i, n))
		if len(batch) >= batchSize {
			if err := be.PutBatch(ctx, batch); err != nil {
				b.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := be.PutBatch(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func newBenchBackend(b *testing.B) physical.Backend {
	b.Helper()
	cfg := map[string]string{"path": filepath.Join(b.TempDir(), "bench.db")}
	be, err := NewFactory(context.Background(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { be.Close() })
	return be
}

func queryBench(b *testing.B, sizes []int, opts func(n int) *physical.QueryOptions) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBench(b, be, n)
			ctx := context.Background()
			qo := opts(n)
			b.ResetTimer()
			for range b.N {
				if _, err := be.Query(ctx, qo); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func countBench(b *testing.B, sizes []int, opts func(n int) *physical.QueryOptions) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBench(b, be, n)
			ctx := context.Background()
			qo := opts(n)
			b.ResetTimer()
			for range b.N {
				if _, err := be.Count(ctx, qo); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Put benchmarks
// ---------------------------------------------------------------------------

func BenchmarkPut(b *testing.B) {
	be := newBenchBackend(b)
	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()

	b.ResetTimer()
	for i := range b.N {
		entry := &physical.Entry{
			Ref: benchRef(i),
			Labels: map[string]string{
				"type": typeValues[rng.Intn(len(typeValues))],
				"env":  envValues[rng.Intn(len(envValues))],
				"user": userPool[rng.Intn(len(userPool))],
			},
			Timestamp: baseTimestamp + int64(i),
			ExpiresAt: baseTimestamp + int64(i) + 1000,
		}
		if err := be.Put(ctx, entry); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutBatch(b *testing.B) {
	for _, batchSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			be := newBenchBackend(b)
			rng := rand.New(rand.NewSource(42))
			ctx := context.Background()

			b.ResetTimer()
			for i := range b.N {
				batch := make([]*physical.Entry, batchSize)
				for j := range batchSize {
					idx := i*batchSize + j
					batch[j] = &physical.Entry{
						Ref: benchRef(idx),
						Labels: map[string]string{
							"type": typeValues[rng.Intn(len(typeValues))],
							"env":  envValues[rng.Intn(len(envValues))],
							"user": userPool[rng.Intn(len(userPool))],
						},
						Timestamp: baseTimestamp + int64(idx),
						ExpiresAt: baseTimestamp + int64(idx) + 1000,
					}
				}
				if err := be.PutBatch(ctx, batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Get benchmarks
// ---------------------------------------------------------------------------

func BenchmarkGet(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBench(b, be, n)
			rng := rand.New(rand.NewSource(99))
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				ref := benchRef(rng.Intn(n))
				if _, err := be.Get(ctx, ref); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Query benchmarks
// ---------------------------------------------------------------------------

func BenchmarkQueryNoFilter(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Limit: 100}
	})
}

func BenchmarkQuerySingleLabel(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text"}, Limit: 100}
	})
}

func BenchmarkQueryMultiLabel(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}, Limit: 100}
	})
}

func BenchmarkQueryTimeRange(b *testing.B) {
	queryBench(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			After: baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100), Limit: 100,
		}
	})
}

func BenchmarkQueryLabelAndTime(b *testing.B) {
	queryBench(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text"},
			After:  baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100), Limit: 100,
		}
	})
}

func BenchmarkQueryMultiLabelAndTime(b *testing.B) {
	queryBench(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text", "env": "prod"},
			After:  baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100), Limit: 100,
		}
	})
}

func BenchmarkQueryDescending(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text"}, Limit: 100, Descending: true}
	})
}

func BenchmarkQueryMultiLabelDescending(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}, Limit: 100, Descending: true}
	})
}

func BenchmarkQueryThreeLabel(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod", "user": "user-001"}, Limit: 100}
	})
}

func BenchmarkQueryPaginationBench(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBench(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				cursor := ""
				for {
					res, err := be.Query(ctx, &physical.QueryOptions{Limit: 100, Cursor: cursor})
					if err != nil {
						b.Fatal(err)
					}
					if !res.HasMore {
						break
					}
					cursor = res.NextCursor
				}
			}
		})
	}
}

func BenchmarkQueryPaginationWithLabel(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBench(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				cursor := ""
				for {
					res, err := be.Query(ctx, &physical.QueryOptions{
						Labels: map[string]string{"type": "text"}, Limit: 100, Cursor: cursor,
					})
					if err != nil {
						b.Fatal(err)
					}
					if !res.HasMore {
						break
					}
					cursor = res.NextCursor
				}
			}
		})
	}
}

func BenchmarkQueryLabelFilter(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{
			LabelFilter: &physical.LabelFilter{
				OR: []physical.LabelFilterGroup{
					{Predicates: []physical.LabelPredicate{{Key: "type", Value: "text"}}},
					{Predicates: []physical.LabelPredicate{{Key: "type", Value: "image"}}},
				},
			},
			Limit: 100,
		}
	})
}

func BenchmarkQueryLabelFilterMultiLabel(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{
			LabelFilter: &physical.LabelFilter{
				OR: []physical.LabelFilterGroup{
					{Predicates: []physical.LabelPredicate{{Key: "type", Value: "text"}, {Key: "env", Value: "prod"}}},
					{Predicates: []physical.LabelPredicate{{Key: "type", Value: "image"}, {Key: "env", Value: "staging"}}},
				},
			},
			Limit: 100,
		}
	})
}

// ---------------------------------------------------------------------------
// Count benchmarks
// ---------------------------------------------------------------------------

func BenchmarkCountNoFilter(b *testing.B) {
	countBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{}
	})
}

func BenchmarkCount(b *testing.B) {
	countBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text"}}
	})
}

func BenchmarkCountMultiLabel(b *testing.B) {
	countBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}}
	})
}

func BenchmarkCountThreeLabel(b *testing.B) {
	countBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod", "user": "user-001"}}
	})
}

func BenchmarkCountTimeRange(b *testing.B) {
	countBench(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			After: baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100),
		}
	})
}

func BenchmarkCountLabelAndTime(b *testing.B) {
	countBench(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text"},
			After:  baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100),
		}
	})
}

func BenchmarkCountMultiLabelAndTime(b *testing.B) {
	countBench(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text", "env": "prod"},
			After:  baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100),
		}
	})
}

// ---------------------------------------------------------------------------
// DeleteExpired benchmarks
// ---------------------------------------------------------------------------

func BenchmarkDeleteExpiredBench(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ctx := context.Background()
			midpoint := time.UnixMilli(baseTimestamp + int64(n/2))

			for range b.N {
				b.StopTimer()
				be := newBenchBackend(b)
				seedBenchWithExpiry(b, be, n)
				b.StartTimer()

				if _, err := be.DeleteExpired(ctx, midpoint); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				be.Close()
				b.StartTimer()
			}
		})
	}
}
