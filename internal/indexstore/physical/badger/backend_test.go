package badger

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/gezibash/arc/v2/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

func newTestBackend(t *testing.T) physical.Backend {
	t.Helper()
	cfg := map[string]string{"path": t.TempDir()}
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
}

func TestGetNotFound(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	_, err := be.Get(ctx, testRef(0xFF))
	if !errors.Is(err, physical.ErrNotFound) {
		t.Errorf("Get = %v, want ErrNotFound", err)
	}
}

func TestDelete(t *testing.T) {
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
}

func TestQueryWithLabels(t *testing.T) {
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

	// Insert 6 entries: 3 already expired (TTL ~1ms), 3 expiring in 1h.
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

	// Wait for the 1ms TTL to fire.
	time.Sleep(5 * time.Millisecond)

	// Badger handles expiration natively via WithTTL — DeleteExpired is a no-op.
	n, err := be.DeleteExpired(ctx, now)
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}
	if n != 0 {
		t.Errorf("deleted = %d, want 0 (badger TTL handles expiration)", n)
	}

	// Expired entries should be invisible to Get.
	for i := range 3 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		ref := reference.Reference(sha256.Sum256(buf[:]))
		_, err := be.Get(ctx, ref)
		if !errors.Is(err, physical.ErrNotFound) {
			t.Errorf("Get(expired ref %d) = %v, want ErrNotFound", i, err)
		}
	}

	// Non-expired entries should still be visible via Get.
	for i := 3; i < 6; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		ref := reference.Reference(sha256.Sum256(buf[:]))
		got, err := be.Get(ctx, ref)
		if err != nil {
			t.Fatalf("Get(valid ref %d): %v", i, err)
		}
		if got.Labels["i"] != "v" {
			t.Errorf("Get(valid ref %d) labels = %v, want i=v", i, got.Labels)
		}
	}

	// Expired entries should be invisible to queries.
	result, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"i": "v"},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Entries) != 3 {
		t.Errorf("visible entries = %d, want 3 (expired entries should be hidden)", len(result.Entries))
	}
}

func TestStats(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	stats, err := be.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.BackendType == "" {
		t.Error("BackendType is empty")
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

func newTestBackendInMemory(t *testing.T) *Backend {
	t.Helper()
	be, err := newInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { be.Close() })
	return be
}

func TestDefaults(t *testing.T) {
	d := Defaults()
	if d[KeyPath] != "~/.arc/index" {
		t.Errorf("KeyPath = %q, want ~/.arc/index", d[KeyPath])
	}
	if d[KeyInMemory] != "false" {
		t.Errorf("KeyInMemory = %q, want false", d[KeyInMemory])
	}
	if d[KeySyncWrites] != "false" {
		t.Errorf("KeySyncWrites = %q, want false", d[KeySyncWrites])
	}
	if d[KeyValueLogFileSize] == "" {
		t.Error("KeyValueLogFileSize is empty")
	}
	if d[KeyMemTableSize] == "" {
		t.Error("KeyMemTableSize is empty")
	}
}

func TestNewInMemory(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("in-memory-test"))
	err := be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"k": "v"},
		Timestamp: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := be.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Labels["k"] != "v" {
		t.Errorf("labels = %v, want k=v", got.Labels)
	}
}

func TestNewFactoryInMemoryConfig(t *testing.T) {
	cfg := map[string]string{KeyInMemory: "true"}
	be, err := NewFactory(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewFactory in-memory: %v", err)
	}
	t.Cleanup(func() { be.Close() })

	ref := reference.Compute([]byte("factory-inmem"))
	if err := be.Put(context.Background(), &physical.Entry{
		Ref: ref, Labels: map[string]string{"a": "b"}, Timestamp: 1000,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
}

func TestNewFactoryInvalidInMemory(t *testing.T) {
	cfg := map[string]string{KeyInMemory: "notabool"}
	_, err := NewFactory(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for invalid in_memory value")
	}
}

func TestNewFactoryEmptyPath(t *testing.T) {
	cfg := map[string]string{KeyPath: "", KeyInMemory: "false"}
	_, err := NewFactory(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestNewFactoryInvalidSyncWrites(t *testing.T) {
	cfg := map[string]string{KeyPath: t.TempDir(), KeySyncWrites: "notbool"}
	_, err := NewFactory(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for invalid sync_writes")
	}
}

func TestNewFactoryInvalidValueLogFileSize(t *testing.T) {
	cfg := map[string]string{KeyPath: t.TempDir(), KeyValueLogFileSize: "notanumber"}
	_, err := NewFactory(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for invalid value_log_file_size")
	}
}

func TestNewFactoryInvalidMemTableSize(t *testing.T) {
	cfg := map[string]string{KeyPath: t.TempDir(), KeyMemTableSize: "notanumber"}
	_, err := NewFactory(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for invalid mem_table_size")
	}
}

func TestRegisterCompositeIndex(t *testing.T) {
	be := newTestBackendInMemory(t)

	def := physical.CompositeIndexDef{Name: "test_idx", Keys: []string{"a", "b"}}
	if err := be.RegisterCompositeIndex(def); err != nil {
		t.Fatalf("RegisterCompositeIndex: %v", err)
	}

	// Duplicate registration should fail.
	if err := be.RegisterCompositeIndex(def); err == nil {
		t.Fatal("expected error for duplicate composite index registration")
	}
}

func TestCompositeIndexes(t *testing.T) {
	be := newTestBackendInMemory(t)

	if got := be.CompositeIndexes(); len(got) != 0 {
		t.Errorf("expected 0 composite indexes, got %d", len(got))
	}

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "idx1", Keys: []string{"a", "b"}})
	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "idx2", Keys: []string{"c"}})

	got := be.CompositeIndexes()
	if len(got) != 2 {
		t.Fatalf("expected 2 composite indexes, got %d", len(got))
	}
	if got[0].Name != "idx1" || got[1].Name != "idx2" {
		t.Errorf("unexpected composite index names: %v, %v", got[0].Name, got[1].Name)
	}
}

func TestPutBatch(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	entries := make([]*physical.Entry, 5)
	for i := range entries {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		entries[i] = &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"batch": "yes"},
			Timestamp: int64(1000 + i),
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
			t.Errorf("labels = %v, want batch=yes", got.Labels)
		}
	}
}

func TestPutBatchClosed(t *testing.T) {
	be := newTestBackendInMemory(t)
	be.Close()

	err := be.PutBatch(context.Background(), []*physical.Entry{
		{Ref: testRef(1), Labels: map[string]string{"a": "b"}, Timestamp: 1000},
	})
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("PutBatch on closed = %v, want ErrClosed", err)
	}
}

func TestPutClosed(t *testing.T) {
	be := newTestBackendInMemory(t)
	be.Close()

	err := be.Put(context.Background(), &physical.Entry{
		Ref: testRef(1), Labels: map[string]string{"a": "b"}, Timestamp: 1000,
	})
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Put on closed = %v, want ErrClosed", err)
	}
}

func TestGetClosed(t *testing.T) {
	be := newTestBackendInMemory(t)
	be.Close()

	_, err := be.Get(context.Background(), testRef(1))
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Get on closed = %v, want ErrClosed", err)
	}
}

func TestDeleteClosed(t *testing.T) {
	be := newTestBackendInMemory(t)
	be.Close()

	err := be.Delete(context.Background(), testRef(1))
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Delete on closed = %v, want ErrClosed", err)
	}
}

func TestQueryClosed(t *testing.T) {
	be := newTestBackendInMemory(t)
	be.Close()

	_, err := be.Query(context.Background(), nil)
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Query on closed = %v, want ErrClosed", err)
	}
}

func TestCountClosed(t *testing.T) {
	be := newTestBackendInMemory(t)
	be.Close()

	_, err := be.Count(context.Background(), nil)
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Count on closed = %v, want ErrClosed", err)
	}
}

func TestDeleteExpiredClosed(t *testing.T) {
	be := newTestBackendInMemory(t)
	be.Close()

	_, err := be.DeleteExpired(context.Background(), time.Now())
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("DeleteExpired on closed = %v, want ErrClosed", err)
	}
}

func TestStatsClosed(t *testing.T) {
	be := newTestBackendInMemory(t)
	be.Close()

	_, err := be.Stats(context.Background())
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Stats on closed = %v, want ErrClosed", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	be := newTestBackendInMemory(t)
	if err := be.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second close should be a no-op, not an error.
	if err := be.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestBuildCompositeKey(t *testing.T) {
	def := physical.CompositeIndexDef{Name: "test", Keys: []string{"a", "b"}}
	labels := map[string]string{"a": "x", "b": "y"}

	key, ok := buildCompositeKey(def, labels, "0000000000001000", "abcd")
	if !ok {
		t.Fatal("expected ok=true")
	}
	expected := "cidx/test/x/y/0000000000001000/abcd"
	if string(key) != expected {
		t.Errorf("key = %q, want %q", string(key), expected)
	}

	// Missing label should return ok=false.
	_, ok = buildCompositeKey(def, map[string]string{"a": "x"}, "0000000000001000", "abcd")
	if ok {
		t.Fatal("expected ok=false when label missing")
	}
}

func TestPutWithCompositeIndex(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		env := "prod"
		if i%2 == 0 {
			env = "dev"
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": env},
			Timestamp: int64(1000 + i),
		})
	}

	// Query should use composite index.
	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Errorf("got %d entries, want 5", len(res.Entries))
	}
}

func TestQueryByCompositeDescending(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	for i := range 5 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": "prod"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels:     map[string]string{"type": "text", "env": "prod"},
		Descending: true,
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Fatalf("got %d entries, want 5", len(res.Entries))
	}
	// Should be in descending timestamp order.
	for i := 1; i < len(res.Entries); i++ {
		if res.Entries[i].Timestamp > res.Entries[i-1].Timestamp {
			t.Errorf("entry %d ts=%d > entry %d ts=%d, expected descending", i, res.Entries[i].Timestamp, i-1, res.Entries[i-1].Timestamp)
		}
	}
}

func TestQueryByCompositeWithTimeRange(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": "prod"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
		After:  1002,
		Before: 1007,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// After=1002 is inclusive (seek start), Before=1007 is exclusive.
	// Entries: ts 1002,1003,1004,1005,1006 = 5.
	if len(res.Entries) != 5 {
		t.Errorf("got %d entries, want 5", len(res.Entries))
	}
}

func TestQueryByCompositePagination(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": "prod"},
			Timestamp: int64(1000 + i),
		})
	}

	var total int
	cursor := ""
	for {
		res, err := be.Query(ctx, &physical.QueryOptions{
			Labels: map[string]string{"type": "text", "env": "prod"},
			Limit:  3,
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
	if total != 10 {
		t.Errorf("paginated total = %d, want 10", total)
	}
}

func TestBackfillCompositeIndex(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	// Insert entries first (before registering composite).
	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": "prod"},
			Timestamp: int64(1000 + i),
		})
	}

	def := physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}}
	be.RegisterCompositeIndex(def)

	n, err := be.BackfillCompositeIndex(ctx, def)
	if err != nil {
		t.Fatalf("BackfillCompositeIndex: %v", err)
	}
	if n != 10 {
		t.Errorf("backfilled = %d, want 10", n)
	}

	// Now queries using the composite should work.
	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query after backfill: %v", err)
	}
	if len(res.Entries) != 10 {
		t.Errorf("got %d entries after backfill, want 10", len(res.Entries))
	}
}

func TestBackfillCompositeIndexClosed(t *testing.T) {
	be := newTestBackendInMemory(t)
	be.Close()

	_, err := be.BackfillCompositeIndex(context.Background(), physical.CompositeIndexDef{
		Name: "x", Keys: []string{"a"},
	})
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("BackfillCompositeIndex on closed = %v, want ErrClosed", err)
	}
}

func TestBackfillCompositeIndexCancelled(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx, cancel := context.WithCancel(context.Background())

	// Insert some entries.
	for i := range 5 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text"},
			Timestamp: int64(1000 + i),
		})
	}

	cancel()
	def := physical.CompositeIndexDef{Name: "type_only", Keys: []string{"type"}}
	be.RegisterCompositeIndex(def)

	_, err := be.BackfillCompositeIndex(ctx, def)
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestBackfillPartialMatch(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	// Insert entries, some with both labels, some with only one.
	for i := range 6 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		labels := map[string]string{"type": "text"}
		if i%2 == 0 {
			labels["env"] = "prod"
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    labels,
			Timestamp: int64(1000 + i),
		})
	}

	def := physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}}
	be.RegisterCompositeIndex(def)

	n, err := be.BackfillCompositeIndex(ctx, def)
	if err != nil {
		t.Fatalf("BackfillCompositeIndex: %v", err)
	}
	// Only 3 entries have both "type" and "env".
	if n != 3 {
		t.Errorf("backfilled = %d, want 3", n)
	}
}

func TestPickDrivingLabel(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	// Insert 10 entries with type=text, 2 with type=text + env=rare.
	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		labels := map[string]string{"type": "text"}
		if i < 2 {
			labels["env"] = "rare"
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    labels,
			Timestamp: int64(1000 + i),
		})
	}

	k, v := pickDrivingLabel(be.db, map[string]string{"type": "text", "env": "rare"})
	// "env=rare" has fewer entries (2) than "type=text" (10), so it should be picked.
	if k != "env" || v != "rare" {
		t.Errorf("pickDrivingLabel = (%q, %q), want (env, rare)", k, v)
	}
}

func TestPickDrivingLabelSingle(t *testing.T) {
	be := newTestBackendInMemory(t)

	k, v := pickDrivingLabel(be.db, map[string]string{"x": "y"})
	if k != "x" || v != "y" {
		t.Errorf("pickDrivingLabel = (%q, %q), want (x, y)", k, v)
	}
}

func TestPickDrivingLabelEmpty(t *testing.T) {
	be := newTestBackendInMemory(t)

	k, v := pickDrivingLabel(be.db, map[string]string{})
	if k != "" || v != "" {
		t.Errorf("pickDrivingLabel empty = (%q, %q), want empty", k, v)
	}
}

func TestCount(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		label := "a"
		if i%2 == 0 {
			label = "b"
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"group": label},
			Timestamp: int64(1000 + i),
		})
	}

	// Count all.
	n, err := be.Count(ctx, nil)
	if err != nil {
		t.Fatalf("Count all: %v", err)
	}
	if n != 10 {
		t.Errorf("Count all = %d, want 10", n)
	}

	// Count with label.
	n, err = be.Count(ctx, &physical.QueryOptions{Labels: map[string]string{"group": "b"}})
	if err != nil {
		t.Fatalf("Count label: %v", err)
	}
	if n != 5 {
		t.Errorf("Count label = %d, want 5", n)
	}

	// Count with time range (After inclusive, Before exclusive).
	n, err = be.Count(ctx, &physical.QueryOptions{After: 1002, Before: 1007})
	if err != nil {
		t.Fatalf("Count time range: %v", err)
	}
	if n != 5 {
		t.Errorf("Count time range = %d, want 5", n)
	}
}

func TestCountMultiLabel(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		env := "prod"
		if i%2 == 0 {
			env = "dev"
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": env},
			Timestamp: int64(1000 + i),
		})
	}

	n, err := be.Count(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
	})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 5 {
		t.Errorf("Count = %d, want 5", n)
	}
}

func TestCountByComposite(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		env := "prod"
		if i%2 == 0 {
			env = "dev"
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": env},
			Timestamp: int64(1000 + i),
		})
	}

	n, err := be.Count(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
	})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 5 {
		t.Errorf("Count = %d, want 5", n)
	}

	// With time range.
	n, err = be.Count(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
		After:  1002,
		Before: 1008,
	})
	if err != nil {
		t.Fatalf("Count with time: %v", err)
	}
	if n != 3 {
		t.Errorf("Count with time = %d, want 3", n)
	}
}

func TestStatsValues(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.Put(ctx, &physical.Entry{
		Ref: reference.Compute([]byte("stats-test")), Labels: map[string]string{"a": "b"}, Timestamp: 1000,
	})

	stats, err := be.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.BackendType != "badger" {
		t.Errorf("BackendType = %q, want badger", stats.BackendType)
	}
}

func TestDeleteExpiredNoOp(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	n, err := be.DeleteExpired(ctx, time.Now())
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}
	if n != 0 {
		t.Errorf("DeleteExpired = %d, want 0", n)
	}
}

func TestPutCursorGetCursorDeleteCursor(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	cur := physical.Cursor{Timestamp: 12345, Sequence: 67890}
	if err := be.PutCursor(ctx, "test-cursor", cur); err != nil {
		t.Fatalf("PutCursor: %v", err)
	}

	got, err := be.GetCursor(ctx, "test-cursor")
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if got.Timestamp != 12345 || got.Sequence != 67890 {
		t.Errorf("GetCursor = %+v, want {12345, 67890}", got)
	}

	// Delete cursor.
	if err := be.DeleteCursor(ctx, "test-cursor"); err != nil {
		t.Fatalf("DeleteCursor: %v", err)
	}

	_, err = be.GetCursor(ctx, "test-cursor")
	if !errors.Is(err, physical.ErrCursorNotFound) {
		t.Errorf("GetCursor after delete = %v, want ErrCursorNotFound", err)
	}

	// Delete non-existent cursor should not error.
	if err := be.DeleteCursor(ctx, "nonexistent"); err != nil {
		t.Fatalf("DeleteCursor nonexistent: %v", err)
	}
}

func TestGetCursorNotFound(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	_, err := be.GetCursor(ctx, "does-not-exist")
	if !errors.Is(err, physical.ErrCursorNotFound) {
		t.Errorf("GetCursor = %v, want ErrCursorNotFound", err)
	}
}

func TestPutCursorClosed(t *testing.T) {
	be := newTestBackendInMemory(t)
	be.Close()

	err := be.PutCursor(context.Background(), "k", physical.Cursor{})
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("PutCursor on closed = %v, want ErrClosed", err)
	}
}

func TestGetCursorClosed(t *testing.T) {
	be := newTestBackendInMemory(t)
	be.Close()

	_, err := be.GetCursor(context.Background(), "k")
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("GetCursor on closed = %v, want ErrClosed", err)
	}
}

func TestDeleteCursorClosed(t *testing.T) {
	be := newTestBackendInMemory(t)
	be.Close()

	err := be.DeleteCursor(context.Background(), "k")
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("DeleteCursor on closed = %v, want ErrClosed", err)
	}
}

func TestRunGC(t *testing.T) {
	cfg := map[string]string{"path": t.TempDir()}
	be, err := NewFactory(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { be.Close() })
	// On-disk DB with no data: GC should return nil (ErrNoRewrite is swallowed).
	if err := be.(*Backend).RunGC(0.5); err != nil {
		t.Fatalf("RunGC: %v", err)
	}
}

func TestRunGCClosed(t *testing.T) {
	be := newTestBackendInMemory(t)
	be.Close()

	err := be.RunGC(0.5)
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("RunGC on closed = %v, want ErrClosed", err)
	}
}

func TestScanPrefix(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("scan-test"))
	be.Put(ctx, &physical.Entry{
		Ref: ref, Labels: map[string]string{"a": "b"}, Timestamp: 1000,
	})

	refHex := reference.Hex(ref)
	prefix := refHex[:4]
	refs, err := be.ScanPrefix(ctx, prefix, 10)
	if err != nil {
		t.Fatalf("ScanPrefix: %v", err)
	}
	if len(refs) == 0 {
		t.Fatal("ScanPrefix returned no results")
	}
	found := false
	for _, r := range refs {
		if reference.Equal(r, ref) {
			found = true
		}
	}
	if !found {
		t.Errorf("ScanPrefix did not find expected ref")
	}
}

func TestScanPrefixClosed(t *testing.T) {
	be := newTestBackendInMemory(t)
	be.Close()

	_, err := be.ScanPrefix(context.Background(), "ab", 10)
	if !errors.Is(err, physical.ErrClosed) {
		t.Errorf("ScanPrefix on closed = %v, want ErrClosed", err)
	}
}

func TestPutReplacesEntry(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("replace-me"))
	be.Put(ctx, &physical.Entry{
		Ref: ref, Labels: map[string]string{"v": "1"}, Timestamp: 1000,
	})

	// Replace with different labels and timestamp.
	be.Put(ctx, &physical.Entry{
		Ref: ref, Labels: map[string]string{"v": "2"}, Timestamp: 2000,
	})

	got, err := be.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Labels["v"] != "2" {
		t.Errorf("labels = %v, want v=2", got.Labels)
	}
	if got.Timestamp != 2000 {
		t.Errorf("timestamp = %d, want 2000", got.Timestamp)
	}

	// Old label index should be gone.
	res, err := be.Query(ctx, &physical.QueryOptions{Labels: map[string]string{"v": "1"}, Limit: 10})
	if err != nil {
		t.Fatalf("Query old label: %v", err)
	}
	if len(res.Entries) != 0 {
		t.Errorf("old label still has %d entries, want 0", len(res.Entries))
	}
}

func TestPutReplacesWithComposite(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "v_k", Keys: []string{"v", "k"}})

	ref := reference.Compute([]byte("replace-composite"))
	be.Put(ctx, &physical.Entry{
		Ref: ref, Labels: map[string]string{"v": "1", "k": "a"}, Timestamp: 1000,
	})
	be.Put(ctx, &physical.Entry{
		Ref: ref, Labels: map[string]string{"v": "2", "k": "a"}, Timestamp: 2000,
	})

	// Old composite should be gone.
	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"v": "1", "k": "a"}, Limit: 10,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 0 {
		t.Errorf("old composite has %d entries, want 0", len(res.Entries))
	}

	// New composite should work.
	res, err = be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"v": "2", "k": "a"}, Limit: 10,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 1 {
		t.Errorf("new composite has %d entries, want 1", len(res.Entries))
	}
}

func TestDeleteWithComposite(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	ref := reference.Compute([]byte("delete-composite"))
	be.Put(ctx, &physical.Entry{
		Ref: ref, Labels: map[string]string{"type": "text", "env": "prod"}, Timestamp: 1000,
	})

	be.Delete(ctx, ref)

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"}, Limit: 10,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 0 {
		t.Errorf("entries after delete = %d, want 0", len(res.Entries))
	}
}

func TestDeleteNonExistent(t *testing.T) {
	be := newTestBackendInMemory(t)
	// Deleting a ref that doesn't exist should not error.
	err := be.Delete(context.Background(), testRef(0xAA))
	if err != nil {
		t.Errorf("Delete non-existent = %v, want nil", err)
	}
}

func TestQueryNilOpts(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.Put(ctx, &physical.Entry{
		Ref: reference.Compute([]byte("nil-opts")), Labels: map[string]string{"a": "b"}, Timestamp: 1000,
	})

	res, err := be.Query(ctx, nil)
	if err != nil {
		t.Fatalf("Query nil opts: %v", err)
	}
	if len(res.Entries) != 1 {
		t.Errorf("entries = %d, want 1", len(res.Entries))
	}
}

func TestQueryDescendingNoFilter(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 5 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"a": "b"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{Descending: true, Limit: 100})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Fatalf("got %d entries, want 5", len(res.Entries))
	}
	for i := 1; i < len(res.Entries); i++ {
		if res.Entries[i].Timestamp > res.Entries[i-1].Timestamp {
			t.Errorf("not descending at position %d", i)
		}
	}
}

func TestQueryWithBeforeNoFilter(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"a": "b"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{Before: 1005, Limit: 100})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Errorf("got %d entries, want 5", len(res.Entries))
	}
}

func TestQueryDescendingWithBeforeNoFilter(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"a": "b"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{Before: 1005, Descending: true, Limit: 100})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// In descending mode, Before controls the seek start; entries at ts=1005 are included.
	if len(res.Entries) != 6 {
		t.Errorf("got %d entries, want 6", len(res.Entries))
	}
}

func TestQueryDescendingWithAfterLabel(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels:     map[string]string{"type": "text"},
		After:      1005,
		Descending: true,
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// Entries with ts 1006,1007,1008,1009
	if len(res.Entries) != 4 {
		t.Errorf("got %d entries, want 4", len(res.Entries))
	}
}

func TestQueryLabelFilter(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		label := "a"
		if i%2 == 0 {
			label = "b"
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"group": label},
			Timestamp: int64(1000 + i),
		})
	}

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
}

func TestQueryLabelFilterDescending(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 6 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		label := "a"
		if i%2 == 0 {
			label = "b"
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"group": label},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		LabelFilter: &physical.LabelFilter{
			OR: []physical.LabelFilterGroup{
				{Predicates: []physical.LabelPredicate{{Key: "group", Value: "a"}}},
				{Predicates: []physical.LabelPredicate{{Key: "group", Value: "b"}}},
			},
		},
		Descending: true,
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 6 {
		t.Fatalf("got %d entries, want 6", len(res.Entries))
	}
	for i := 1; i < len(res.Entries); i++ {
		if res.Entries[i].Timestamp > res.Entries[i-1].Timestamp {
			t.Errorf("not descending at position %d", i)
		}
	}
}

func TestQueryLabelFilterPagination(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"group": "a"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		LabelFilter: &physical.LabelFilter{
			OR: []physical.LabelFilterGroup{
				{Predicates: []physical.LabelPredicate{{Key: "group", Value: "a"}}},
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

func TestQueryWithExpiredExcluded(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	now := time.Now().UnixMilli()
	// Entry that expires in the past (already expired by application logic).
	be.Put(ctx, &physical.Entry{
		Ref:       reference.Compute([]byte("expired")),
		Labels:    map[string]string{"a": "b"},
		Timestamp: now - 2000,
		ExpiresAt: now - 1000,
	})
	// Entry that's still valid.
	be.Put(ctx, &physical.Entry{
		Ref:       reference.Compute([]byte("valid")),
		Labels:    map[string]string{"a": "b"},
		Timestamp: now,
		ExpiresAt: now + 3600000,
	})

	// Wait for TTL to take effect.
	time.Sleep(10 * time.Millisecond)

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"a": "b"},
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// Expired entry should be hidden.
	if len(res.Entries) != 1 {
		t.Errorf("got %d entries, want 1", len(res.Entries))
	}
}

func TestPutWithTTL(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("ttl-test"))
	be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"a": "b"},
		Timestamp: time.Now().UnixMilli(),
		ExpiresAt: time.Now().Add(time.Hour).UnixMilli(),
	})

	got, err := be.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ExpiresAt == 0 {
		t.Error("ExpiresAt should be non-zero")
	}
}

func TestBackfillWithTTL(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("backfill-ttl"))
	be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"type": "text", "env": "prod"},
		Timestamp: time.Now().UnixMilli(),
		ExpiresAt: time.Now().Add(time.Hour).UnixMilli(),
	})

	def := physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}}
	be.RegisterCompositeIndex(def)

	n, err := be.BackfillCompositeIndex(ctx, def)
	if err != nil {
		t.Fatalf("BackfillCompositeIndex: %v", err)
	}
	if n != 1 {
		t.Errorf("backfilled = %d, want 1", n)
	}
}

func TestQueryByLabelDescendingWithBefore(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels:     map[string]string{"type": "text"},
		Before:     1005,
		Descending: true,
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// In descending mode, Before controls the seek start; entries at ts=1005 are included.
	if len(res.Entries) != 6 {
		t.Errorf("got %d entries, want 6", len(res.Entries))
	}
}

func TestCountByLabelWithTimeRange(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text"},
			Timestamp: int64(1000 + i),
		})
	}

	n, err := be.Count(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text"},
		After:  1002,
		Before: 1007,
	})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 5 {
		t.Errorf("Count = %d, want 5", n)
	}
}

func TestFindCompositeIndexPicksBest(t *testing.T) {
	be := newTestBackendInMemory(t)

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "single", Keys: []string{"a"}})
	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "double", Keys: []string{"a", "b"}})

	def, vals, ok := be.findCompositeIndex(map[string]string{"a": "x", "b": "y"})
	if !ok {
		t.Fatal("expected to find composite index")
	}
	if def.Name != "double" {
		t.Errorf("expected 'double' index, got %q", def.Name)
	}
	if len(vals) != 2 || vals[0] != "x" || vals[1] != "y" {
		t.Errorf("unexpected vals: %v", vals)
	}
}

func TestFindCompositeIndexNoMatch(t *testing.T) {
	be := newTestBackendInMemory(t)

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "ab", Keys: []string{"a", "b"}})

	_, _, ok := be.findCompositeIndex(map[string]string{"c": "x"})
	if ok {
		t.Fatal("expected no match")
	}
}

func TestTimestampHexRoundTrip(t *testing.T) {
	for _, ts := range []int64{0, 1, 1000, 1735689600000, 9999999999999} {
		h := timestampToHex(ts)
		got, err := hexToTimestamp(h)
		if err != nil {
			t.Fatalf("hexToTimestamp(%q): %v", h, err)
		}
		if got != ts {
			t.Errorf("roundtrip %d -> %q -> %d", ts, h, got)
		}
	}
}

func TestHexToTimestampInvalid(t *testing.T) {
	_, err := hexToTimestamp("not-hex")
	if err == nil {
		t.Fatal("expected error")
	}
	_, err = hexToTimestamp("abcd")
	if err == nil {
		t.Fatal("expected error for short hex")
	}
}

func TestCountByCompositeWithExtraLabels(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		user := "alice"
		if i%2 == 0 {
			user = "bob"
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": "prod", "user": user},
			Timestamp: int64(1000 + i),
		})
	}

	// Query with extra label beyond the composite index keys.
	n, err := be.Count(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod", "user": "alice"},
	})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 5 {
		t.Errorf("Count = %d, want 5", n)
	}
}

func TestQueryByCompositeWithExpiryExcluded(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	now := time.Now().UnixMilli()
	// Expired entry.
	be.Put(ctx, &physical.Entry{
		Ref:       reference.Compute([]byte("expired-composite")),
		Labels:    map[string]string{"type": "text", "env": "prod"},
		Timestamp: now - 2000,
		ExpiresAt: now - 1000,
	})
	// Valid entry.
	be.Put(ctx, &physical.Entry{
		Ref:       reference.Compute([]byte("valid-composite")),
		Labels:    map[string]string{"type": "text", "env": "prod"},
		Timestamp: now,
		ExpiresAt: now + 3600000,
	})

	time.Sleep(10 * time.Millisecond)

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 1 {
		t.Errorf("got %d entries, want 1", len(res.Entries))
	}
}

func TestScanPrefixLimit(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"a": "b"},
			Timestamp: int64(1000 + i),
		})
	}

	refs, err := be.ScanPrefix(ctx, "", 3)
	if err != nil {
		t.Fatalf("ScanPrefix: %v", err)
	}
	if len(refs) != 3 {
		t.Errorf("got %d refs, want 3", len(refs))
	}
}

func TestQueryByEntryCursor(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 5 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"a": "b"},
			Timestamp: int64(1000 + i),
		})
	}

	// First page.
	res, err := be.Query(ctx, &physical.QueryOptions{Limit: 2})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 2 || !res.HasMore {
		t.Fatalf("first page: entries=%d hasMore=%v", len(res.Entries), res.HasMore)
	}

	// Second page via cursor.
	res2, err := be.Query(ctx, &physical.QueryOptions{Limit: 2, Cursor: res.NextCursor})
	if err != nil {
		t.Fatalf("Query page 2: %v", err)
	}
	if len(res2.Entries) != 2 {
		t.Errorf("second page: entries=%d, want 2", len(res2.Entries))
	}
}

func TestQueryByEntryDescendingCursor(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 5 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"a": "b"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{Limit: 2, Descending: true})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !res.HasMore {
		t.Fatal("expected HasMore")
	}

	res2, err := be.Query(ctx, &physical.QueryOptions{Limit: 10, Descending: true, Cursor: res.NextCursor})
	if err != nil {
		t.Fatalf("Query page 2: %v", err)
	}
	if len(res2.Entries) != 3 {
		t.Errorf("second page: entries=%d, want 3", len(res2.Entries))
	}
}

func TestQueryByEntryDescendingWithAfter(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"a": "b"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{After: 1005, Descending: true, Limit: 100})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// Descending from end, stopping when ts <= afterHex (1005).
	// So entries 1009,1008,1007,1006 = 4.
	if len(res.Entries) != 4 {
		t.Errorf("got %d entries, want 4", len(res.Entries))
	}
}

func TestQueryByLabelCursor(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 5 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text"}, Limit: 2,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !res.HasMore {
		t.Fatal("expected HasMore")
	}

	res2, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text"}, Limit: 10, Cursor: res.NextCursor,
	})
	if err != nil {
		t.Fatalf("Query page 2: %v", err)
	}
	if len(res2.Entries) != 3 {
		t.Errorf("page 2: entries=%d, want 3", len(res2.Entries))
	}
}

func TestQueryByLabelDescendingCursor(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 5 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text"}, Limit: 2, Descending: true,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !res.HasMore {
		t.Fatal("expected HasMore")
	}

	res2, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text"}, Limit: 10, Descending: true, Cursor: res.NextCursor,
	})
	if err != nil {
		t.Fatalf("Query page 2: %v", err)
	}
	if len(res2.Entries) != 3 {
		t.Errorf("page 2: entries=%d, want 3", len(res2.Entries))
	}
}

func TestQueryByCompositeDescendingCursor(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	for i := range 5 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": "prod"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"}, Limit: 2, Descending: true,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !res.HasMore {
		t.Fatal("expected HasMore")
	}

	res2, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"}, Limit: 10, Descending: true, Cursor: res.NextCursor,
	})
	if err != nil {
		t.Fatalf("Query page 2: %v", err)
	}
	if len(res2.Entries) != 3 {
		t.Errorf("page 2: entries=%d, want 3", len(res2.Entries))
	}
}

func TestQueryByCompositeDescendingWithAfter(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": "prod"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels:     map[string]string{"type": "text", "env": "prod"},
		After:      1005,
		Descending: true,
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 4 {
		t.Errorf("got %d entries, want 4", len(res.Entries))
	}
}

func TestQueryByLabelAfterAscending(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text"},
		After:  1005,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Errorf("got %d entries, want 5", len(res.Entries))
	}
}

func TestQueryByLabelBeforeAscending(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text"},
		Before: 1005,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Errorf("got %d entries, want 5", len(res.Entries))
	}
}

func TestQueryByCompositeBeforeAscending(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": "prod"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
		Before: 1005,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Errorf("got %d entries, want 5", len(res.Entries))
	}
}

func TestQueryByCompositeAfterAscending(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": "prod"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
		After:  1005,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Errorf("got %d entries, want 5", len(res.Entries))
	}
}

func TestQueryByCompositeDescendingWithBefore(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": "prod"},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels:     map[string]string{"type": "text", "env": "prod"},
		Before:     1005,
		Descending: true,
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// Descending seek from Before, includes ts=1005.
	if len(res.Entries) < 5 {
		t.Errorf("got %d entries, want >= 5", len(res.Entries))
	}
}

func TestQueryIncludeExpired(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	now := time.Now().UnixMilli()
	be.Put(ctx, &physical.Entry{
		Ref:       reference.Compute([]byte("soon-expired")),
		Labels:    map[string]string{"a": "b"},
		Timestamp: now,
		ExpiresAt: now + 3600000,
	})

	res, err := be.Query(ctx, &physical.QueryOptions{
		IncludeExpired: true,
		Limit:          100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 1 {
		t.Errorf("got %d entries, want 1", len(res.Entries))
	}
}

func TestCountByEntryWithAfter(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"a": "b"},
			Timestamp: int64(1000 + i),
		})
	}

	n, err := be.Count(ctx, &physical.QueryOptions{After: 1005})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 5 {
		t.Errorf("Count = %d, want 5", n)
	}
}

func TestCountByEntryWithBefore(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"a": "b"},
			Timestamp: int64(1000 + i),
		})
	}

	n, err := be.Count(ctx, &physical.QueryOptions{Before: 1005})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 5 {
		t.Errorf("Count = %d, want 5", n)
	}
}

func TestCountByLabelWithAfterOnly(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text"},
			Timestamp: int64(1000 + i),
		})
	}

	n, err := be.Count(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text"},
		After:  1005,
	})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 5 {
		t.Errorf("Count = %d, want 5", n)
	}
}

func TestCountByCompositeWithAfterOnly(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": "prod"},
			Timestamp: int64(1000 + i),
		})
	}

	n, err := be.Count(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
		After:  1005,
	})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 5 {
		t.Errorf("Count = %d, want 5", n)
	}
}

func TestMatchesMetaFilterExpired(t *testing.T) {
	now := int64(5000)
	// Expired entry.
	if matchesMetaFilter(4000, map[string]string{"a": "b"}, &physical.QueryOptions{
		Labels: map[string]string{"a": "b"},
	}, now) {
		t.Error("expected expired entry to be filtered out")
	}
	// Not expired.
	if !matchesMetaFilter(6000, map[string]string{"a": "b"}, &physical.QueryOptions{
		Labels: map[string]string{"a": "b"},
	}, now) {
		t.Error("expected valid entry to pass filter")
	}
	// Label mismatch.
	if matchesMetaFilter(0, map[string]string{"a": "b"}, &physical.QueryOptions{
		Labels: map[string]string{"a": "c"},
	}, now) {
		t.Error("expected label mismatch to be filtered out")
	}
	// IncludeExpired.
	if !matchesMetaFilter(4000, map[string]string{"a": "b"}, &physical.QueryOptions{
		Labels:         map[string]string{"a": "b"},
		IncludeExpired: true,
	}, now) {
		t.Error("expected IncludeExpired to pass expired entry")
	}
}

func TestCountByMultiLabelWithBefore(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": "prod"},
			Timestamp: int64(1000 + i),
		})
	}

	n, err := be.Count(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
		Before: 1005,
	})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 5 {
		t.Errorf("Count = %d, want 5", n)
	}
}

func TestCountByMultiLabelWithAfterAndBefore(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"type": "text", "env": "prod"},
			Timestamp: int64(1000 + i),
		})
	}

	n, err := be.Count(ctx, &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
		After:  1003,
		Before: 1007,
	})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 4 {
		t.Errorf("Count = %d, want 4", n)
	}
}

func TestPutReplacesEntryWithExpiry(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("replace-ttl"))
	now := time.Now().UnixMilli()

	// First put with TTL.
	be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"v": "1"},
		Timestamp: 1000,
		ExpiresAt: now + 3600000,
	})

	// Replace with different data and TTL.
	be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"v": "2"},
		Timestamp: 2000,
		ExpiresAt: now + 7200000,
	})

	got, err := be.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Labels["v"] != "2" {
		t.Errorf("labels = %v, want v=2", got.Labels)
	}
}

func TestPutWithCompositeAndTTL(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	be.RegisterCompositeIndex(physical.CompositeIndexDef{Name: "type_env", Keys: []string{"type", "env"}})

	now := time.Now().UnixMilli()
	ref := reference.Compute([]byte("composite-ttl"))
	be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"type": "text", "env": "prod"},
		Timestamp: now,
		ExpiresAt: now + 3600000,
	})

	got, err := be.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Labels["type"] != "text" {
		t.Errorf("labels = %v", got.Labels)
	}
}

func TestPutNoLabels(t *testing.T) {
	be := newTestBackendInMemory(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("no-labels"))
	be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{},
		Timestamp: 1000,
	})

	got, err := be.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Labels) != 0 {
		t.Errorf("labels = %v, want empty", got.Labels)
	}
}

func TestDecodeMetaErrors(t *testing.T) {
	// Too short.
	_, _, err := decodeMeta([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for short data")
	}

	// Truncated label.
	data := make([]byte, 10)
	binary.BigEndian.PutUint16(data[8:10], 1) // 1 label
	_, _, err = decodeMeta(data)
	if err == nil {
		t.Error("expected error for truncated label")
	}
}

func TestDecodeMetaExpiresAtShort(t *testing.T) {
	_, err := decodeMetaExpiresAt([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for short data")
	}
}

func TestEntryFromSuffixInvalid(t *testing.T) {
	// Invalid timestamp hex.
	_, err := entryFromSuffix("zzzzzzzzzzzzzzzz/"+fmt.Sprintf("%064x", 1), 0, nil)
	if err == nil {
		t.Error("expected error for invalid timestamp")
	}

	// Invalid ref hex.
	_, err = entryFromSuffix(fmt.Sprintf("%016x", 1000)+"/zzzz", 0, nil)
	if err == nil {
		t.Error("expected error for invalid ref")
	}
}

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

var (
	benchSizes = []int{100, 1_000, 10_000, 100_000}

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
	baseTimestamp = int64(1735689600000) // 2025-01-01T00:00:00Z in ms
	farFuture     = int64(2051222400000) // ~2035-01-01
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
	cfg := map[string]string{"path": b.TempDir()}
	be, err := NewFactory(context.Background(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { be.Close() })
	return be
}

func registerComposite2(b *testing.B, be physical.Backend) bool {
	b.Helper()
	ci, ok := be.(physical.CompositeIndexer)
	if !ok {
		return false
	}
	if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
		Name: "type_env",
		Keys: []string{"type", "env"},
	}); err != nil {
		b.Fatal(err)
	}
	return true
}

func registerComposite3(b *testing.B, be physical.Backend) bool {
	b.Helper()
	ci, ok := be.(physical.CompositeIndexer)
	if !ok {
		return false
	}
	if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
		Name: "type_env_user",
		Keys: []string{"type", "env", "user"},
	}); err != nil {
		b.Fatal(err)
	}
	return true
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

func queryBenchComposite(b *testing.B, sizes []int, opts func(n int) *physical.QueryOptions) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			if !registerComposite2(b, be) {
				b.Skip("backend does not support composite indexes")
			}
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

func countBenchComposite(b *testing.B, sizes []int, opts func(n int) *physical.QueryOptions) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			if !registerComposite2(b, be) {
				b.Skip("backend does not support composite indexes")
			}
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

func BenchmarkPutWithComposite(b *testing.B) {
	for _, name := range []string{"NoComposite", "Composite2", "Composite3"} {
		b.Run(name, func(b *testing.B) {
			be := newBenchBackend(b)
			switch name {
			case "Composite2":
				if !registerComposite2(b, be) {
					b.Skip("no composite support")
				}
			case "Composite3":
				if !registerComposite3(b, be) {
					b.Skip("no composite support")
				}
			}

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
		})
	}
}

func BenchmarkPutCompositeScaling(b *testing.B) {
	labelKeys := make([]string, 10)
	for i := range labelKeys {
		labelKeys[i] = fmt.Sprintf("lbl%02d", i)
	}
	labelVals := []string{"alpha", "bravo", "charlie", "delta", "echo"}

	makeEntry := func(rng *rand.Rand, i int) *physical.Entry {
		labels := make(map[string]string, len(labelKeys))
		for _, k := range labelKeys {
			labels[k] = labelVals[rng.Intn(len(labelVals))]
		}
		return &physical.Entry{
			Ref:       benchRef(i),
			Labels:    labels,
			Timestamp: baseTimestamp + int64(i),
			ExpiresAt: baseTimestamp + int64(i) + 1000,
		}
	}

	type pair struct{ a, b int }
	var allPairs []pair
	for i := 0; i < len(labelKeys); i++ {
		for j := i + 1; j < len(labelKeys); j++ {
			allPairs = append(allPairs, pair{i, j})
		}
	}

	for _, count := range []int{0, 10, 45, 100, 500} {
		b.Run(fmt.Sprintf("composites=%d", count), func(b *testing.B) {
			be := newBenchBackend(b)
			ci, ok := be.(physical.CompositeIndexer)
			if !ok && count > 0 {
				b.Skip("backend does not support composite indexes")
			}

			registered := 0
			for registered < count && registered < len(allPairs) {
				p := allPairs[registered]
				if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
					Name: fmt.Sprintf("c2_%d_%d", p.a, p.b),
					Keys: []string{labelKeys[p.a], labelKeys[p.b]},
				}); err != nil {
					b.Fatal(err)
				}
				registered++
			}
			for i := 0; registered < count && i < len(labelKeys); i++ {
				for j := i + 1; registered < count && j < len(labelKeys); j++ {
					for k := j + 1; registered < count && k < len(labelKeys); k++ {
						if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
							Name: fmt.Sprintf("c3_%d_%d_%d", i, j, k),
							Keys: []string{labelKeys[i], labelKeys[j], labelKeys[k]},
						}); err != nil {
							b.Fatal(err)
						}
						registered++
					}
				}
			}
			for i := 0; registered < count && i < len(labelKeys); i++ {
				for j := i + 1; registered < count && j < len(labelKeys); j++ {
					for k := j + 1; registered < count && k < len(labelKeys); k++ {
						for l := k + 1; registered < count && l < len(labelKeys); l++ {
							if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
								Name: fmt.Sprintf("c4_%d_%d_%d_%d", i, j, k, l),
								Keys: []string{labelKeys[i], labelKeys[j], labelKeys[k], labelKeys[l]},
							}); err != nil {
								b.Fatal(err)
							}
							registered++
						}
					}
				}
			}

			rng := rand.New(rand.NewSource(42))
			ctx := context.Background()
			b.ResetTimer()
			for i := range b.N {
				if err := be.Put(ctx, makeEntry(rng, i)); err != nil {
					b.Fatal(err)
				}
			}
		})
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
// Query — no composite
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

// ---------------------------------------------------------------------------
// Query — composite
// ---------------------------------------------------------------------------

func BenchmarkQueryMultiLabelComposite(b *testing.B) {
	queryBenchComposite(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}, Limit: 100}
	})
}

func BenchmarkQueryMultiLabelAndTimeComposite(b *testing.B) {
	queryBenchComposite(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text", "env": "prod"},
			After:  baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100), Limit: 100,
		}
	})
}

func BenchmarkQueryMultiLabelDescendingComposite(b *testing.B) {
	queryBenchComposite(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}, Limit: 100, Descending: true}
	})
}

func BenchmarkQueryLabelFilterMultiLabelComposite(b *testing.B) {
	queryBenchComposite(b, benchSizes, func(_ int) *physical.QueryOptions {
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

func BenchmarkQueryThreeLabelComposite(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			if !registerComposite3(b, be) {
				b.Skip("backend does not support composite indexes")
			}
			seedBench(b, be, n)
			ctx := context.Background()

			qo := &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod", "user": "user-001"}, Limit: 100}
			b.ResetTimer()
			for range b.N {
				if _, err := be.Query(ctx, qo); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Count — no composite
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
// Count — composite
// ---------------------------------------------------------------------------

func BenchmarkCountMultiLabelComposite(b *testing.B) {
	countBenchComposite(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}}
	})
}

func BenchmarkCountThreeLabelComposite(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			if !registerComposite3(b, be) {
				b.Skip("backend does not support composite indexes")
			}
			seedBench(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				if _, err := be.Count(ctx, &physical.QueryOptions{
					Labels: map[string]string{"type": "text", "env": "prod", "user": "user-001"},
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCountMultiLabelAndTimeComposite(b *testing.B) {
	countBenchComposite(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text", "env": "prod"},
			After:  baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100),
		}
	})
}

// ---------------------------------------------------------------------------
// Count vs Query
// ---------------------------------------------------------------------------

func BenchmarkCountVsQuery(b *testing.B) {
	opts := &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}}
	n := 10_000

	for _, variant := range []string{"NoComposite", "Composite"} {
		b.Run(variant, func(b *testing.B) {
			runCountVsQueryVariant(b, opts, n, variant == "Composite")
		})
	}
}

func BenchmarkCountVsQueryScaling(b *testing.B) {
	opts := &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}}

	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d/NoComposite", n), func(b *testing.B) {
			runCountVsQueryVariant(b, opts, n, false)
		})
		b.Run(fmt.Sprintf("n=%d/Composite", n), func(b *testing.B) {
			runCountVsQueryVariant(b, opts, n, true)
		})
	}
}

func runCountVsQueryVariant(b *testing.B, opts *physical.QueryOptions, n int, composite bool) {
	b.Helper()

	setup := func(b *testing.B) physical.Backend {
		b.Helper()
		be := newBenchBackend(b)
		if composite {
			if ci, ok := be.(physical.CompositeIndexer); ok {
				if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
					Name: "type_env", Keys: []string{"type", "env"},
				}); err != nil {
					b.Fatal(err)
				}
			} else {
				b.Skip("backend does not support composite indexes")
			}
		}
		seedBench(b, be, n)
		return be
	}

	b.Run("Count", func(b *testing.B) {
		be := setup(b)
		ctx := context.Background()
		b.ResetTimer()
		for range b.N {
			if _, err := be.Count(ctx, opts); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Query", func(b *testing.B) {
		be := setup(b)
		ctx := context.Background()
		b.ResetTimer()
		for range b.N {
			if _, err := be.Query(ctx, &physical.QueryOptions{Labels: opts.Labels, Limit: 1_000_000}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Backfill
// ---------------------------------------------------------------------------

func BenchmarkBackfill(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ctx := context.Background()

			for range b.N {
				b.StopTimer()
				be := newBenchBackend(b)
				seedBench(b, be, n)

				ci, ok := be.(physical.CompositeIndexer)
				if !ok {
					b.Skip("backend does not support composite indexes")
				}
				if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
					Name: "type_env", Keys: []string{"type", "env"},
				}); err != nil {
					b.Fatal(err)
				}

				bf, ok := be.(physical.Backfiller)
				if !ok {
					b.Skip("backend does not support backfill")
				}
				b.StartTimer()

				if _, err := bf.BackfillCompositeIndex(ctx, physical.CompositeIndexDef{
					Name: "type_env", Keys: []string{"type", "env"},
				}); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				be.Close()
				b.StartTimer()
			}
		})
	}
}

// ---------------------------------------------------------------------------
// DeleteExpired
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
