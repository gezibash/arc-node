package indexstore

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc/v2/pkg/reference"
)

// PartitionConfig configures time-based partitioning.
type PartitionConfig struct {
	// Window is the duration of each partition (e.g., 1 * time.Hour).
	Window time.Duration

	// Factory creates a new backend for a partition.
	// The partitionID is a string like "20240101T150000" (time-truncated).
	Factory func(ctx context.Context, partitionID string) (physical.Backend, error)

	// MaxOpenPartitions limits how many partitions can be open simultaneously.
	// Older partitions are closed (but not deleted) when this limit is exceeded.
	// 0 = unlimited.
	MaxOpenPartitions int
}

// PartitionedBackend wraps multiple time-partitioned backends behind
// the physical.Backend interface.
type PartitionedBackend struct {
	config     PartitionConfig
	mu         sync.RWMutex
	partitions map[string]*partitionEntry
	sorted     []string // sorted partition IDs for query planning
}

type partitionEntry struct {
	backend  physical.Backend
	start    int64 // partition start timestamp (ms)
	end      int64 // partition end timestamp (ms)
	lastUsed time.Time
}

// NewPartitionedBackend creates a partitioned backend.
func NewPartitionedBackend(config PartitionConfig) *PartitionedBackend {
	return &PartitionedBackend{
		config:     config,
		partitions: make(map[string]*partitionEntry),
	}
}

func (pb *PartitionedBackend) partitionID(tsMillis int64) string {
	t := time.UnixMilli(tsMillis)
	truncated := t.Truncate(pb.config.Window)
	return truncated.Format("20060102T150405")
}

func (pb *PartitionedBackend) getOrCreatePartition(ctx context.Context, tsMillis int64) (physical.Backend, error) {
	id := pb.partitionID(tsMillis)

	pb.mu.RLock()
	_, ok := pb.partitions[id]
	pb.mu.RUnlock()
	if ok {
		pb.mu.Lock()
		if p, ok := pb.partitions[id]; ok {
			p.lastUsed = time.Now()
			be := p.backend
			pb.mu.Unlock()
			return be, nil
		}
		pb.mu.Unlock()
	}

	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Double-check
	if p, ok := pb.partitions[id]; ok {
		p.lastUsed = time.Now()
		return p.backend, nil
	}

	backend, err := pb.config.Factory(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("create partition %s: %w", id, err)
	}

	t := time.UnixMilli(tsMillis).Truncate(pb.config.Window)
	pb.partitions[id] = &partitionEntry{
		backend:  backend,
		start:    t.UnixMilli(),
		end:      t.Add(pb.config.Window).UnixMilli(),
		lastUsed: time.Now(),
	}

	pb.sorted = append(pb.sorted, id)
	sort.Strings(pb.sorted)

	pb.evictOldestLocked()

	return backend, nil
}

func (pb *PartitionedBackend) Put(ctx context.Context, entry *physical.Entry) error {
	backend, err := pb.getOrCreatePartition(ctx, entry.Timestamp)
	if err != nil {
		return err
	}
	return backend.Put(ctx, entry)
}

func (pb *PartitionedBackend) PutBatch(ctx context.Context, entries []*physical.Entry) error {
	// Group entries by partition
	groups := make(map[string][]*physical.Entry)
	for _, entry := range entries {
		id := pb.partitionID(entry.Timestamp)
		groups[id] = append(groups[id], entry)
	}

	for _, group := range groups {
		backend, err := pb.getOrCreatePartition(ctx, group[0].Timestamp)
		if err != nil {
			return err
		}
		if err := backend.PutBatch(ctx, group); err != nil {
			return err
		}
	}
	return nil
}

func (pb *PartitionedBackend) Get(ctx context.Context, r reference.Reference) (*physical.Entry, error) {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	// Search all partitions (newest first for likely hits)
	for i := len(pb.sorted) - 1; i >= 0; i-- {
		p := pb.partitions[pb.sorted[i]]
		entry, err := p.backend.Get(ctx, r)
		if err == nil {
			return entry, nil
		}
		if err != physical.ErrNotFound {
			return nil, err
		}
	}
	return nil, physical.ErrNotFound
}

func (pb *PartitionedBackend) Delete(ctx context.Context, r reference.Reference) error {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	for _, id := range pb.sorted {
		p := pb.partitions[id]
		if err := p.backend.Delete(ctx, r); err != nil && err != physical.ErrNotFound {
			return err
		}
	}
	return nil
}

func (pb *PartitionedBackend) Query(ctx context.Context, opts *physical.QueryOptions) (*physical.QueryResult, error) {
	partitions := pb.partitionsForRange(opts.After, opts.Before)

	if len(partitions) == 0 {
		return &physical.QueryResult{}, nil
	}

	if len(partitions) == 1 {
		return partitions[0].backend.Query(ctx, opts)
	}

	// Order partitions based on Descending flag
	if opts.Descending {
		for i, j := 0, len(partitions)-1; i < j; i, j = i+1, j-1 {
			partitions[i], partitions[j] = partitions[j], partitions[i]
		}
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	var allEntries []*physical.Entry
	for _, p := range partitions {
		partOpts := *opts
		partOpts.Limit = limit - len(allEntries) + 1 // +1 to detect hasMore
		result, err := p.backend.Query(ctx, &partOpts)
		if err != nil {
			return nil, err
		}
		allEntries = append(allEntries, result.Entries...)
		if len(allEntries) > limit {
			break
		}
	}

	hasMore := len(allEntries) > limit
	if hasMore {
		allEntries = allEntries[:limit]
	}

	var nextCursor string
	if hasMore && len(allEntries) > 0 {
		last := allEntries[len(allEntries)-1]
		nextCursor = fmt.Sprintf("%016x/%s", last.Timestamp, reference.Hex(last.Ref))
	}

	return &physical.QueryResult{
		Entries:    allEntries,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func (pb *PartitionedBackend) Count(ctx context.Context, opts *physical.QueryOptions) (int64, error) {
	partitions := pb.partitionsForRange(opts.After, opts.Before)

	var total int64
	for _, p := range partitions {
		n, err := p.backend.Count(ctx, opts)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func (pb *PartitionedBackend) DeleteExpired(ctx context.Context, now time.Time) (int, error) {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	total := 0
	for _, id := range pb.sorted {
		p := pb.partitions[id]
		n, err := p.backend.DeleteExpired(ctx, now)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (pb *PartitionedBackend) Stats(ctx context.Context) (*physical.Stats, error) {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	var totalSize int64
	for _, id := range pb.sorted {
		p := pb.partitions[id]
		s, err := p.backend.Stats(ctx)
		if err != nil {
			return nil, err
		}
		if s.SizeBytes > 0 {
			totalSize += s.SizeBytes
		}
	}

	return &physical.Stats{
		SizeBytes:   totalSize,
		BackendType: "partitioned",
	}, nil
}

func (pb *PartitionedBackend) Close() error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	var firstErr error
	for _, p := range pb.partitions {
		if err := p.backend.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	pb.partitions = nil
	pb.sorted = nil
	return firstErr
}

func (pb *PartitionedBackend) partitionsForRange(after, before int64) []*partitionEntry {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	if after == 0 && before == 0 {
		// No time bounds: return all partitions
		result := make([]*partitionEntry, 0, len(pb.sorted))
		for _, id := range pb.sorted {
			result = append(result, pb.partitions[id])
		}
		return result
	}

	var result []*partitionEntry
	for _, id := range pb.sorted {
		p := pb.partitions[id]
		if before > 0 && p.start >= before {
			continue
		}
		if after > 0 && p.end <= after {
			continue
		}
		result = append(result, p)
	}
	return result
}

func (pb *PartitionedBackend) evictOldestLocked() {
	if pb.config.MaxOpenPartitions <= 0 || len(pb.partitions) <= pb.config.MaxOpenPartitions {
		return
	}

	var oldestID string
	var oldestTime time.Time
	for id, p := range pb.partitions {
		if oldestID == "" || p.lastUsed.Before(oldestTime) {
			oldestID = id
			oldestTime = p.lastUsed
		}
	}

	if oldestID != "" {
		pb.partitions[oldestID].backend.Close()
		delete(pb.partitions, oldestID)
		newSorted := make([]string, 0, len(pb.partitions))
		for id := range pb.partitions {
			newSorted = append(newSorted, id)
		}
		sort.Strings(newSorted)
		pb.sorted = newSorted
	}
}
