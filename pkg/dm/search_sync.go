package dm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
)

const searchIndexContentType = "application/x-arc-dm-search-index+encrypted"

// RemoteSearchInfo holds metadata about the latest remote search index.
type RemoteSearchInfo struct {
	LastUpdate int64
	EntryCount int
	DBHash     string
}

// deriveSymKey derives a symmetric key from the keypair seed for self-encryption
// (used for search index sync, not for DM message encryption).
func deriveSymKey(seed []byte) [32]byte {
	return sha256.Sum256(seed)
}

// FetchRemoteSearchInfo queries the node for the latest search-index message
// metadata without downloading the blob. Returns nil if no remote index exists.
func (t *Threads) FetchRemoteSearchInfo(ctx context.Context) (*RemoteSearchInfo, error) {
	pub := t.kp.PublicKey()
	result, err := t.client.QueryMessages(ctx, &client.QueryOptions{
		Expression: "true",
		Labels: map[string]string{
			"app":   "dm",
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
func (t *Threads) PullSearchIndex(ctx context.Context, destPath string) (*SearchIndex, error) {
	pub := t.kp.PublicKey()
	result, err := t.client.QueryMessages(ctx, &client.QueryOptions{
		Expression: "true",
		Labels: map[string]string{
			"app":   "dm",
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

	ciphertext, err := t.client.GetContent(ctx, contentRef)
	if err != nil {
		return nil, fmt.Errorf("get search index content: %w", err)
	}

	symKey := deriveSymKey(t.kp.Seed())
	data, err := decrypt(ciphertext, &symKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt search index: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0700); err != nil {
		return nil, fmt.Errorf("create search dir: %w", err)
	}
	if err := os.WriteFile(destPath, data, 0600); err != nil {
		return nil, fmt.Errorf("write search index: %w", err)
	}

	return OpenSearchIndex(ctx, destPath)
}

// PushSearchIndex encrypts and uploads the local search DB to the node.
func (t *Threads) PushSearchIndex(ctx context.Context, idx *SearchIndex) (reference.Reference, error) {
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

	symKey := deriveSymKey(t.kp.Seed())
	ciphertext, err := encrypt(data, &symKey)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("encrypt search index: %w", err)
	}

	contentRef, err := t.client.PutContent(ctx, ciphertext)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("put search index content: %w", err)
	}

	pub := t.kp.PublicKey()
	toPub := t.recipientKey()

	msg := message.New(pub, toPub, contentRef, searchIndexContentType)
	if err := message.Sign(&msg, t.kp); err != nil {
		return reference.Reference{}, fmt.Errorf("sign search index message: %w", err)
	}

	labels := map[string]string{
		"app":         "dm",
		"type":        "search-index",
		"owner":       hex.EncodeToString(pub[:]),
		"last-update": strconv.FormatInt(ts, 16),
		"entry-count": strconv.Itoa(count),
		"db-hash":     reference.Hex(dbHash),
	}

	ref, err := t.client.SendMessage(ctx, msg, labels)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("send search index message: %w", err)
	}

	return ref, nil
}

func (t *Threads) recipientKey() identity.PublicKey {
	if t.client != nil {
		if key, ok := t.client.NodeKey(); ok {
			return key
		}
	}
	if t.nodeKey != nil {
		return *t.nodeKey
	}
	return t.kp.PublicKey()
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
