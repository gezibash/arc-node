// Package redis provides a Redis-backed index storage backend.
package redis

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/gezibash/arc/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/internal/storage"
)

const (
	KeyAddr         = "addr"
	KeyPassword     = "password"
	KeyDB           = "db"
	KeyMaxRetries   = "max_retries"
	KeyDialTimeout  = "dial_timeout"
	KeyReadTimeout  = "read_timeout"
	KeyWriteTimeout = "write_timeout"
	KeyPoolSize     = "pool_size"
	KeyKeyPrefix    = "key_prefix"

	pipelineBatchSize      = 1000
	deleteExpiredBatchSize = 500
)

func init() {
	physical.Register("redis", NewFactory, Defaults)
}

// Defaults returns the default configuration for the Redis backend.
func Defaults() map[string]string {
	return map[string]string{
		KeyAddr:         "localhost:6379",
		KeyPassword:     "",
		KeyDB:           "1",
		KeyMaxRetries:   "3",
		KeyDialTimeout:  "5s",
		KeyReadTimeout:  "3s",
		KeyWriteTimeout: "3s",
		KeyPoolSize:     "0",
		KeyKeyPrefix:    "arc:",
	}
}

// NewFactory creates a new Redis backend from a configuration map.
func NewFactory(_ context.Context, config map[string]string) (physical.Backend, error) {
	addr := storage.GetString(config, KeyAddr, "")
	if addr == "" {
		return nil, storage.NewConfigError("redis", KeyAddr, "cannot be empty")
	}

	db, err := storage.GetInt(config, KeyDB, 1)
	if err != nil {
		return nil, storage.NewConfigErrorWithValue("redis", KeyDB, config[KeyDB], err.Error())
	}
	if db < 0 {
		return nil, storage.NewConfigErrorWithValue("redis", KeyDB, config[KeyDB], "must be non-negative")
	}

	maxRetries, err := storage.GetInt(config, KeyMaxRetries, 3)
	if err != nil {
		return nil, storage.NewConfigErrorWithValue("redis", KeyMaxRetries, config[KeyMaxRetries], err.Error())
	}

	dialTimeout, err := storage.GetDuration(config, KeyDialTimeout, 5*time.Second)
	if err != nil {
		return nil, storage.NewConfigErrorWithValue("redis", KeyDialTimeout, config[KeyDialTimeout], err.Error())
	}

	readTimeout, err := storage.GetDuration(config, KeyReadTimeout, 3*time.Second)
	if err != nil {
		return nil, storage.NewConfigErrorWithValue("redis", KeyReadTimeout, config[KeyReadTimeout], err.Error())
	}

	writeTimeout, err := storage.GetDuration(config, KeyWriteTimeout, 3*time.Second)
	if err != nil {
		return nil, storage.NewConfigErrorWithValue("redis", KeyWriteTimeout, config[KeyWriteTimeout], err.Error())
	}

	poolSize, err := storage.GetInt(config, KeyPoolSize, 0)
	if err != nil {
		return nil, storage.NewConfigErrorWithValue("redis", KeyPoolSize, config[KeyPoolSize], err.Error())
	}

	password := storage.GetString(config, KeyPassword, "")
	keyPrefix := storage.GetString(config, KeyKeyPrefix, "arc:")

	opts := &redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		MaxRetries:   maxRetries,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}
	if poolSize > 0 {
		opts.PoolSize = poolSize
	}

	client := redis.NewClient(opts)

	pingCtx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, storage.NewConfigErrorWithCause("redis", KeyAddr, "failed to connect", err)
	}

	slog.Info("redis indexstore initialized", "addr", addr, "db", db, "key_prefix", keyPrefix)

	return &Backend{
		client: client,
		prefix: keyPrefix,
	}, nil
}

// Backend is a Redis implementation of physical.Backend.
type Backend struct {
	client *redis.Client
	prefix string
	closed atomic.Bool
}

// NewWithClient creates a new backend with an existing Redis client.
func NewWithClient(client *redis.Client, prefix string) *Backend {
	if prefix == "" {
		prefix = "arc:"
	}
	return &Backend{
		client: client,
		prefix: prefix,
	}
}

func (b *Backend) Put(ctx context.Context, entry *physical.Entry) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	pipe := b.client.TxPipeline()

	if err := b.putInPipe(ctx, pipe, entry); err != nil {
		return err
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis put: %w", err)
	}
	return nil
}

// PutBatch stores multiple entries in a single transaction pipeline.
func (b *Backend) PutBatch(ctx context.Context, entries []*physical.Entry) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	// Phase 1: batch GET old entries for cleanup via a read pipeline.
	refHexes := make([]string, len(entries))
	getPipe := b.client.Pipeline()
	getCmds := make([]*redis.StringCmd, len(entries))
	for i, entry := range entries {
		refHexes[i] = reference.Hex(entry.Ref)
		getCmds[i] = getPipe.Get(ctx, b.entryKey(refHexes[i]))
	}
	_, _ = getPipe.Exec(ctx)

	// Phase 2: single TxPipeline for all writes.
	pipe := b.client.TxPipeline()

	for i, entry := range entries {
		refHex := refHexes[i]

		// Clean up old indexes if entry existed.
		oldData, err := getCmds[i].Bytes()
		if err == nil {
			var oldEntry physical.Entry
			if json.Unmarshal(oldData, &oldEntry) == nil {
				b.cleanupOldEntry(ctx, pipe, refHex, &oldEntry)
			}
		}

		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("encode entry: %w", err)
		}

		var ttl time.Duration
		if entry.ExpiresAt > 0 {
			ttl = time.Until(time.UnixMilli(entry.ExpiresAt))
			if ttl <= 0 {
				ttl = time.Millisecond
			}
		}

		pipe.Set(ctx, b.entryKey(refHex), data, ttl)
		pipe.ZAdd(ctx, b.entriesByTimeKey(), redis.Z{
			Score:  float64(entry.Timestamp),
			Member: refHex,
		})

		for k, v := range entry.Labels {
			pipe.SAdd(ctx, b.labelKey(k, v), refHex)
			pipe.ZAdd(ctx, b.labelTsKey(k, v), redis.Z{
				Score:  float64(entry.Timestamp),
				Member: refHex,
			})
		}

		if entry.ExpiresAt > 0 {
			pipe.ZAdd(ctx, b.expiresKey(), redis.Z{
				Score:  float64(entry.ExpiresAt),
				Member: refHex,
			})
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis put batch: %w", err)
	}
	return nil
}

func (b *Backend) putInPipe(ctx context.Context, pipe redis.Pipeliner, entry *physical.Entry) error {
	refHex := reference.Hex(entry.Ref)

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encode entry: %w", err)
	}

	// Check if entry exists and clean up old indexes
	oldData, err := b.client.Get(ctx, b.entryKey(refHex)).Bytes()
	if err == nil {
		var oldEntry physical.Entry
		if json.Unmarshal(oldData, &oldEntry) == nil {
			b.cleanupOldEntry(ctx, pipe, refHex, &oldEntry)
		}
	} else if err != redis.Nil {
		return fmt.Errorf("check existing entry: %w", err)
	}

	var ttl time.Duration
	if entry.ExpiresAt > 0 {
		ttl = time.Until(time.UnixMilli(entry.ExpiresAt))
		if ttl <= 0 {
			ttl = time.Millisecond
		}
	}

	pipe.Set(ctx, b.entryKey(refHex), data, ttl)
	pipe.ZAdd(ctx, b.entriesByTimeKey(), redis.Z{
		Score:  float64(entry.Timestamp),
		Member: refHex,
	})

	for k, v := range entry.Labels {
		pipe.SAdd(ctx, b.labelKey(k, v), refHex)
		pipe.ZAdd(ctx, b.labelTsKey(k, v), redis.Z{
			Score:  float64(entry.Timestamp),
			Member: refHex,
		})
	}

	if entry.ExpiresAt > 0 {
		pipe.ZAdd(ctx, b.expiresKey(), redis.Z{
			Score:  float64(entry.ExpiresAt),
			Member: refHex,
		})
	}

	return nil
}

func (b *Backend) cleanupOldEntry(ctx context.Context, pipe redis.Pipeliner, refHex string, oldEntry *physical.Entry) {
	pipe.ZRem(ctx, b.entriesByTimeKey(), refHex)
	for k, v := range oldEntry.Labels {
		pipe.SRem(ctx, b.labelKey(k, v), refHex)
		pipe.ZRem(ctx, b.labelTsKey(k, v), refHex)
	}
	if oldEntry.ExpiresAt > 0 {
		pipe.ZRem(ctx, b.expiresKey(), refHex)
	}
}

func (b *Backend) Get(ctx context.Context, r reference.Reference) (*physical.Entry, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	refHex := reference.Hex(r)
	data, err := b.client.Get(ctx, b.entryKey(refHex)).Bytes()
	if err == redis.Nil {
		return nil, physical.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("redis get: %w", err)
	}

	var entry physical.Entry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("decode entry: %w", err)
	}
	return &entry, nil
}

func (b *Backend) Delete(ctx context.Context, r reference.Reference) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	refHex := reference.Hex(r)

	data, err := b.client.Get(ctx, b.entryKey(refHex)).Bytes()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("redis get for delete: %w", err)
	}

	var entry physical.Entry
	if err = json.Unmarshal(data, &entry); err != nil {
		b.client.Del(ctx, b.entryKey(refHex))
		return nil
	}

	pipe := b.client.TxPipeline()
	pipe.Del(ctx, b.entryKey(refHex))
	pipe.ZRem(ctx, b.entriesByTimeKey(), refHex)
	for k, v := range entry.Labels {
		pipe.SRem(ctx, b.labelKey(k, v), refHex)
		pipe.ZRem(ctx, b.labelTsKey(k, v), refHex)
	}
	if entry.ExpiresAt > 0 {
		pipe.ZRem(ctx, b.expiresKey(), refHex)
	}

	if _, err = pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis delete: %w", err)
	}
	return nil
}

func (b *Backend) Query(ctx context.Context, opts *physical.QueryOptions) (*physical.QueryResult, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	if opts == nil {
		opts = &physical.QueryOptions{}
	}

	// Handle OR label filter via union of sub-queries
	if opts.LabelFilter != nil && len(opts.LabelFilter.OR) > 0 {
		return b.queryByLabelFilter(ctx, opts)
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	now := time.Now().UnixMilli()

	// Parse cursor upfront so we can use it for server-side filtering.
	var cursorTs int64
	var cursorRef string
	var hasCursor bool
	if opts.Cursor != "" {
		cursorTs, cursorRef, hasCursor = parseCursor(opts.Cursor)
	}

	minScore := "-inf"
	maxScore := "+inf"
	if opts.After > 0 {
		minScore = "(" + strconv.FormatInt(opts.After, 10)
	}
	if opts.Before > 0 {
		maxScore = "(" + strconv.FormatInt(opts.Before, 10)
	}

	// Apply cursor to narrow range server-side.
	if hasCursor {
		cursorScoreStr := strconv.FormatInt(cursorTs, 10)
		if opts.Descending {
			if maxScore == "+inf" || cursorTs < scoreToInt(maxScore) {
				maxScore = cursorScoreStr
			}
		} else {
			if minScore == "-inf" || cursorTs > scoreToInt(minScore) {
				minScore = cursorScoreStr
			}
		}
	}

	var candidateRefs []string

	if len(opts.Labels) > 0 {
		candidateRefs = b.queryLabels(ctx, opts, minScore, maxScore, limit, hasCursor)
	} else {
		rangeOpts := &redis.ZRangeBy{
			Min:   minScore,
			Max:   maxScore,
			Count: int64(limit + 1),
		}
		// Fetch extra to account for cursor tie-breaking and expired filtering.
		if hasCursor {
			rangeOpts.Count = int64(limit + 1 + 100)
		}

		var zRange *redis.ZSliceCmd
		if opts.Descending {
			zRange = b.client.ZRevRangeByScoreWithScores(ctx, b.entriesByTimeKey(), rangeOpts)
		} else {
			zRange = b.client.ZRangeByScoreWithScores(ctx, b.entriesByTimeKey(), rangeOpts)
		}

		results, err := zRange.Result()
		if err != nil {
			return nil, fmt.Errorf("redis zrange: %w", err)
		}

		candidateRefs = make([]string, 0, len(results))
		for _, z := range results {
			if s, ok := z.Member.(string); ok {
				candidateRefs = append(candidateRefs, s)
			}
		}
	}

	if len(candidateRefs) == 0 {
		return &physical.QueryResult{}, nil
	}

	// Fix 1: pipelined GETs in batches of pipelineBatchSize.
	candidates := make([]entryWithRef, 0, min(len(candidateRefs), limit+1))
	for batchStart := 0; batchStart < len(candidateRefs); batchStart += pipelineBatchSize {
		batchEnd := min(batchStart+pipelineBatchSize, len(candidateRefs))
		batch := candidateRefs[batchStart:batchEnd]

		pipe := b.client.Pipeline()
		cmds := make([]*redis.StringCmd, len(batch))
		for i, refHex := range batch {
			cmds[i] = pipe.Get(ctx, b.entryKey(refHex))
		}
		_, _ = pipe.Exec(ctx)

		for i, cmd := range cmds {
			data, err := cmd.Bytes()
			if err != nil {
				continue
			}

			var entry physical.Entry
			if err := json.Unmarshal(data, &entry); err != nil {
				continue
			}

			if !matchesFilter(&entry, opts, now) {
				continue
			}

			// Apply cursor filter.
			if hasCursor {
				cmp := compareCursor(entry.Timestamp, batch[i], cursorTs, cursorRef)
				if opts.Descending {
					if cmp >= 0 {
						continue
					}
				} else if cmp <= 0 {
					continue
				}
			}

			candidates = append(candidates, entryWithRef{entry: &entry, refHex: batch[i]})
		}
	}

	if len(candidates) == 0 {
		return &physical.QueryResult{}, nil
	}

	// Tie-breaking sort on the small result set.
	sortEntries(candidates, opts.Descending)

	hasMore := len(candidates) > limit
	if hasMore {
		candidates = candidates[:limit]
	}

	entries := make([]*physical.Entry, len(candidates))
	for i, item := range candidates {
		entries[i] = item.entry
	}

	nextCursor := ""
	if hasMore && len(entries) > 0 {
		last := entries[len(entries)-1]
		nextCursor = timestampToHex(last.Timestamp) + "/" + reference.Hex(last.Ref)
	}

	return &physical.QueryResult{
		Entries:    entries,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// queryLabels uses label sorted sets to get time-ordered, range-limited candidate refs.
func (b *Backend) queryLabels(ctx context.Context, opts *physical.QueryOptions, minScore, maxScore string, limit int, hasCursor bool) []string {
	fetchCount := int64(limit + 1)
	if hasCursor {
		fetchCount = int64(limit + 1 + 100)
	}

	rangeOpts := &redis.ZRangeBy{
		Min:   minScore,
		Max:   maxScore,
		Count: fetchCount,
	}

	if len(opts.Labels) == 1 {
		// Single label: query directly from label_ts sorted set.
		var key string
		for k, v := range opts.Labels {
			key = b.labelTsKey(k, v)
		}

		var zRange *redis.ZSliceCmd
		if opts.Descending {
			zRange = b.client.ZRevRangeByScoreWithScores(ctx, key, rangeOpts)
		} else {
			zRange = b.client.ZRangeByScoreWithScores(ctx, key, rangeOpts)
		}

		results, err := zRange.Result()
		if err != nil {
			return nil
		}

		refs := make([]string, 0, len(results))
		for _, z := range results {
			if s, ok := z.Member.(string); ok {
				refs = append(refs, s)
			}
		}
		return refs
	}

	// Multi-label: ZInterStore into temp key, then ZRangeByScore.
	labelTsKeys := make([]string, 0, len(opts.Labels))
	for k, v := range opts.Labels {
		labelTsKeys = append(labelTsKeys, b.labelTsKey(k, v))
	}

	tmpKey := b.prefix + "tmp:" + uniqueID()

	pipe := b.client.Pipeline()
	pipe.ZInterStore(ctx, tmpKey, &redis.ZStore{
		Keys:      labelTsKeys,
		Aggregate: "MIN",
	})

	var rangeCmd *redis.ZSliceCmd
	if opts.Descending {
		rangeCmd = pipe.ZRevRangeByScoreWithScores(ctx, tmpKey, rangeOpts)
	} else {
		rangeCmd = pipe.ZRangeByScoreWithScores(ctx, tmpKey, rangeOpts)
	}
	pipe.Del(ctx, tmpKey)
	_, _ = pipe.Exec(ctx)

	results, err := rangeCmd.Result()
	if err != nil {
		return nil
	}

	refs := make([]string, 0, len(results))
	for _, z := range results {
		if s, ok := z.Member.(string); ok {
			refs = append(refs, s)
		}
	}
	return refs
}

// queryByLabelFilter handles OR label filter queries by running sub-queries per group and merging.
func (b *Backend) queryByLabelFilter(ctx context.Context, opts *physical.QueryOptions) (*physical.QueryResult, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	seen := make(map[string]bool)
	var allCandidates []entryWithRef
	now := time.Now().UnixMilli()

	for _, group := range opts.LabelFilter.OR {
		groupLabels := make(map[string]string, len(group.Predicates))
		for _, p := range group.Predicates {
			groupLabels[p.Key] = p.Value
		}
		groupOpts := *opts
		groupOpts.Labels = groupLabels
		groupOpts.LabelFilter = nil
		groupOpts.Limit = limit + 1

		result, err := b.Query(ctx, &groupOpts)
		if err != nil {
			return nil, err
		}
		for _, e := range result.Entries {
			rh := reference.Hex(e.Ref)
			if !seen[rh] {
				seen[rh] = true
				if !opts.IncludeExpired && e.ExpiresAt > 0 && e.ExpiresAt <= now {
					continue
				}
				allCandidates = append(allCandidates, entryWithRef{entry: e, refHex: rh})
			}
		}
	}

	sortEntries(allCandidates, opts.Descending)

	hasMore := len(allCandidates) > limit
	if hasMore {
		allCandidates = allCandidates[:limit]
	}

	entries := make([]*physical.Entry, len(allCandidates))
	for i, item := range allCandidates {
		entries[i] = item.entry
	}

	nextCursor := ""
	if hasMore && len(entries) > 0 {
		last := entries[len(entries)-1]
		nextCursor = timestampToHex(last.Timestamp) + "/" + reference.Hex(last.Ref)
	}

	return &physical.QueryResult{
		Entries:    entries,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// Count returns the number of entries matching the given options.
// For no-label queries it uses ZCOUNT on entries_by_time. For single-label
// it uses ZCOUNT on the label sorted set. For multi-label it falls back
// to ZInterStore + ZCARD.
func (b *Backend) Count(ctx context.Context, opts *physical.QueryOptions) (int64, error) {
	if b.closed.Load() {
		return 0, physical.ErrClosed
	}
	if opts == nil {
		opts = &physical.QueryOptions{}
	}

	minScore := "-inf"
	maxScore := "+inf"
	if opts.After > 0 {
		minScore = "(" + strconv.FormatInt(opts.After, 10)
	}
	if opts.Before > 0 {
		maxScore = "(" + strconv.FormatInt(opts.Before, 10)
	}

	if len(opts.Labels) == 0 {
		n, err := b.client.ZCount(ctx, b.entriesByTimeKey(), minScore, maxScore).Result()
		if err != nil {
			return 0, fmt.Errorf("redis count: %w", err)
		}
		return n, nil
	}

	if len(opts.Labels) == 1 {
		var key string
		for k, v := range opts.Labels {
			key = b.labelTsKey(k, v)
		}
		n, err := b.client.ZCount(ctx, key, minScore, maxScore).Result()
		if err != nil {
			return 0, fmt.Errorf("redis count: %w", err)
		}
		return n, nil
	}

	// Multi-label: intersect into temp key, count, delete.
	labelTsKeys := make([]string, 0, len(opts.Labels))
	for k, v := range opts.Labels {
		labelTsKeys = append(labelTsKeys, b.labelTsKey(k, v))
	}

	tmpKey := b.prefix + "tmp:" + uniqueID()

	pipe := b.client.Pipeline()
	pipe.ZInterStore(ctx, tmpKey, &redis.ZStore{
		Keys:      labelTsKeys,
		Aggregate: "MIN",
	})
	countCmd := pipe.ZCount(ctx, tmpKey, minScore, maxScore)
	pipe.Del(ctx, tmpKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("redis count: %w", err)
	}

	n, err := countCmd.Result()
	if err != nil {
		return 0, fmt.Errorf("redis count: %w", err)
	}
	return n, nil
}

func (b *Backend) DeleteExpired(ctx context.Context, now time.Time) (int, error) {
	if b.closed.Load() {
		return 0, physical.ErrClosed
	}

	nowMillis := now.UnixMilli()

	expired, err := b.client.ZRangeByScore(ctx, b.expiresKey(), &redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatInt(nowMillis, 10),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("redis zrangebyscore: %w", err)
	}

	if len(expired) == 0 {
		return 0, nil
	}

	deleted := 0

	for batchStart := 0; batchStart < len(expired); batchStart += deleteExpiredBatchSize {
		batchEnd := min(batchStart+deleteExpiredBatchSize, len(expired))
		batch := expired[batchStart:batchEnd]

		// Entry data keys are already gone via Redis TTL.
		// We only need to GET entries that still exist to discover their labels
		// for index cleanup. Entries already evicted skip the label cleanup.
		getPipe := b.client.Pipeline()
		getCmds := make([]*redis.StringCmd, len(batch))
		for i, refHex := range batch {
			getCmds[i] = getPipe.Get(ctx, b.entryKey(refHex))
		}
		_, _ = getPipe.Exec(ctx)

		delPipe := b.client.TxPipeline()
		batchDeleted := 0
		for i, cmd := range getCmds {
			refHex := batch[i]

			// Always clean up sorted-set index references.
			delPipe.ZRem(ctx, b.expiresKey(), refHex)
			delPipe.ZRem(ctx, b.entriesByTimeKey(), refHex)

			// If the entry data is still around (TTL hasn't fired yet), delete it
			// and clean up label indexes.
			data, err := cmd.Bytes()
			if err == nil {
				delPipe.Del(ctx, b.entryKey(refHex))
				var entry physical.Entry
				if json.Unmarshal(data, &entry) == nil {
					for k, v := range entry.Labels {
						delPipe.SRem(ctx, b.labelKey(k, v), refHex)
						delPipe.ZRem(ctx, b.labelTsKey(k, v), refHex)
					}
				}
			}

			batchDeleted++
		}

		if _, err := delPipe.Exec(ctx); err != nil {
			return deleted, fmt.Errorf("delete expired batch: %w", err)
		}
		deleted += batchDeleted
	}

	return deleted, nil
}

func (b *Backend) Stats(ctx context.Context) (*physical.Stats, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	return &physical.Stats{
		SizeBytes:   -1,
		BackendType: "redis",
	}, nil
}

func (b *Backend) Close() error {
	if b.closed.Swap(true) {
		return nil
	}
	return b.client.Close()
}

type entryWithRef struct {
	entry  *physical.Entry
	refHex string
}

func (b *Backend) entryKey(refHex string) string     { return b.prefix + "entry:" + refHex }
func (b *Backend) entriesByTimeKey() string          { return b.prefix + "entries_by_time" }
func (b *Backend) labelKey(key, value string) string { return b.prefix + "label:" + key + ":" + value }
func (b *Backend) labelTsKey(key, value string) string {
	return b.prefix + "label_ts:" + key + ":" + value
}
func (b *Backend) expiresKey() string { return b.prefix + "expires" }

func matchesFilter(entry *physical.Entry, opts *physical.QueryOptions, now int64) bool {
	if !opts.IncludeExpired && entry.ExpiresAt > 0 && entry.ExpiresAt <= now {
		return false
	}
	if opts.After > 0 && entry.Timestamp <= opts.After {
		return false
	}
	if opts.Before > 0 && entry.Timestamp >= opts.Before {
		return false
	}
	for k, v := range opts.Labels {
		if entry.Labels[k] != v {
			return false
		}
	}
	return true
}

func sortEntries(entries []entryWithRef, descending bool) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].entry.Timestamp == entries[j].entry.Timestamp {
			if descending {
				return entries[i].refHex > entries[j].refHex
			}
			return entries[i].refHex < entries[j].refHex
		}
		if descending {
			return entries[i].entry.Timestamp > entries[j].entry.Timestamp
		}
		return entries[i].entry.Timestamp < entries[j].entry.Timestamp
	})
}

func compareCursor(ts int64, refHex string, cursorTs int64, cursorRef string) int {
	if ts < cursorTs {
		return -1
	}
	if ts > cursorTs {
		return 1
	}
	if refHex < cursorRef {
		return -1
	}
	if refHex > cursorRef {
		return 1
	}
	return 0
}

func parseCursor(cursor string) (int64, string, bool) {
	parts := strings.SplitN(cursor, "/", 2)
	if len(parts) != 2 {
		return 0, "", false
	}
	tsHex := parts[0]
	refHex := strings.ToLower(parts[1])
	if len(tsHex) != 16 || len(refHex) != 64 {
		return 0, "", false
	}
	tsBytes, err := hex.DecodeString(tsHex)
	if err != nil || len(tsBytes) != 8 {
		return 0, "", false
	}
	if _, err := hex.DecodeString(refHex); err != nil {
		return 0, "", false
	}
	ts := int64(binary.BigEndian.Uint64(tsBytes)) //nolint:gosec // timestamp fits in int64
	return ts, refHex, true
}

func timestampToHex(ts int64) string {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(ts)) //nolint:gosec // timestamp is always positive
	return fmt.Sprintf("%016x", buf)
}

// scoreToInt parses a Redis score string (possibly with "(" exclusive prefix) to int64.
func scoreToInt(s string) int64 {
	s = strings.TrimPrefix(s, "(")
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

// uniqueID returns a short random hex string for temp key naming.
func uniqueID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
