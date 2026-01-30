package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/gezibash/arc/pkg/reference"

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
		exp := now.Add(time.Hour).UnixNano()
		if i < 3 {
			exp = now.Add(-time.Hour).UnixNano()
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
