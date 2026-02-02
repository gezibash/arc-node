package journal

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
)

const searchIndexContentType = "application/x-arc-journal-search-index+encrypted"

// RemoteSearchInfo holds metadata about the latest remote search index.
type RemoteSearchInfo struct {
	LastUpdate int64
	EntryCount int
	DBHash     string // hex-encoded content hash
}

// FetchRemoteSearchInfo queries the node for the latest search-index message
// metadata without downloading the blob. Returns nil if no remote index exists.
func (j *Journal) FetchRemoteSearchInfo(ctx context.Context) (*RemoteSearchInfo, error) {
	pub := j.kp.PublicKey()
	result, err := j.client.QueryMessages(ctx, &client.QueryOptions{
		Expression: "true",
		Labels: map[string]string{
			"app":   "journal",
			"type":  "search-index",
			"owner": hex.EncodeToString(pub[:]),
		},
		Limit:      1,
		Descending: true,
	})
	if err != nil {
		return nil, fmt.Errorf("query search index: %w", err)
	}
	if len(result.Entries) == 0 {
		return nil, nil
	}
	e := result.Entries[0]
	return &RemoteSearchInfo{
		LastUpdate: parseLastUpdate(e.Labels),
		EntryCount: parseEntryCount(e.Labels),
		DBHash:     e.Labels["db-hash"],
	}, nil
}

// PullSearchIndex downloads the remote search index blob, decrypts it, and
// writes it to destPath. Returns the opened SearchIndex.
func (j *Journal) PullSearchIndex(ctx context.Context, destPath string) (*SearchIndex, error) {
	pub := j.kp.PublicKey()
	result, err := j.client.QueryMessages(ctx, &client.QueryOptions{
		Expression: "true",
		Labels: map[string]string{
			"app":   "journal",
			"type":  "search-index",
			"owner": hex.EncodeToString(pub[:]),
		},
		Limit:      1,
		Descending: true,
	})
	if err != nil {
		return nil, fmt.Errorf("query search index: %w", err)
	}
	if len(result.Entries) == 0 {
		return nil, fmt.Errorf("no remote search index found")
	}

	entry := result.Entries[0]
	contentHex := entry.Labels["content"]
	if contentHex == "" {
		return nil, fmt.Errorf("search index message has no content label")
	}
	contentRef, err := reference.FromHex(contentHex)
	if err != nil {
		return nil, fmt.Errorf("parse content ref: %w", err)
	}

	ciphertext, err := j.client.GetContent(ctx, contentRef)
	if err != nil {
		return nil, fmt.Errorf("get search index content: %w", err)
	}

	data, err := decrypt(ciphertext, &j.symKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt search index: %w", err)
	}

	if err := os.WriteFile(destPath, data, 0600); err != nil {
		return nil, fmt.Errorf("write search index: %w", err)
	}

	return OpenSearchIndex(ctx, destPath)
}

// PushSearchIndex encrypts and uploads the local search DB to the node.
func (j *Journal) PushSearchIndex(ctx context.Context, idx *SearchIndex) (reference.Reference, error) {
	ts, err := idx.LastIndexedTimestamp(ctx)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("last indexed timestamp: %w", err)
	}

	var count int
	_ = idx.Count(ctx, &count)

	dbHash, err := idx.ContentHash(ctx)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("content hash: %w", err)
	}

	dbPath, err := idx.DBPath()
	if err != nil {
		return reference.Reference{}, fmt.Errorf("get db path: %w", err)
	}

	data, err := os.ReadFile(filepath.Clean(dbPath))
	if err != nil {
		return reference.Reference{}, fmt.Errorf("read search db: %w", err)
	}

	ciphertext, err := encrypt(data, &j.symKey)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("encrypt search index: %w", err)
	}

	contentRef, err := j.client.PutContent(ctx, ciphertext)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("put search index content: %w", err)
	}

	pub := j.kp.PublicKey()
	toPub := j.recipientKey()

	msg := message.New(pub, toPub, contentRef, searchIndexContentType)
	if err := message.Sign(&msg, j.kp); err != nil {
		return reference.Reference{}, fmt.Errorf("sign search index message: %w", err)
	}

	labels := map[string]string{
		"app":         "journal",
		"type":        "search-index",
		"owner":       hex.EncodeToString(pub[:]),
		"last-update": strconv.FormatInt(ts, 16),
		"entry-count": strconv.Itoa(count),
		"db-hash":     reference.Hex(dbHash),
	}

	ref, err := j.client.SendMessage(ctx, msg, labels, &nodev1.Dimensions{
		Persistence: nodev1.Persistence_PERSISTENCE_DURABLE,
		Visibility:  nodev1.Visibility_VISIBILITY_PRIVATE,
	})
	if err != nil {
		return reference.Reference{}, fmt.Errorf("send search index message: %w", err)
	}

	return ref, nil
}

func parseEntryCount(labels map[string]string) int {
	s := labels["entry-count"]
	if s == "" {
		return 0
	}
	v, _ := strconv.Atoi(s)
	return v
}

func parseLastUpdate(labels map[string]string) int64 {
	s := labels["last-update"]
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseInt(s, 16, 64)
	return v
}
