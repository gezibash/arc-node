package dm

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/reference"
)

// Threads provides cross-conversation DM operations without requiring a peer pubkey.
type Threads struct {
	client  *client.Client
	kp      *identity.Keypair
	nodeKey *identity.PublicKey
	search  *SearchIndex
}

// Thread represents a conversation with a peer, summarized by latest activity.
type Thread struct {
	ConvID  string
	PeerPub identity.PublicKey
	LastMsg Message
	Preview string // lazily populated
}

// ThreadsOption configures a Threads instance.
type ThreadsOption func(*Threads)

// WithThreadsSearchIndex attaches a search index to a Threads instance.
func WithThreadsSearchIndex(idx *SearchIndex) ThreadsOption {
	return func(t *Threads) { t.search = idx }
}

// NewThreads creates a Threads instance for listing and subscribing across all conversations.
func NewThreads(c *client.Client, kp *identity.Keypair, opts ...Option) *Threads {
	t := &Threads{client: c, kp: kp}
	// Reuse Option type — only WithNodeKey applies.
	d := &DM{}
	for _, o := range opts {
		o(d)
	}
	t.nodeKey = d.nodeKey
	return t
}

// ListThreads queries all DM messages involving self and groups them by
// conversation, returning one Thread per peer sorted by most recent activity.
//
// Instead of using a CEL expression (which is applied post-query and causes
// full index scans), we issue two label-indexed queries — one for messages
// sent by self (dm_from=self) and one for messages received (dm_to=self) —
// then merge the results client-side.
func (t *Threads) ListThreads(ctx context.Context) ([]Thread, error) {
	self := t.kp.PublicKey()
	selfHex := hex.EncodeToString(self[:])

	baseLabels := map[string]string{
		"app":  "dm",
		"type": "message",
	}

	// Query sent messages (dm_from=self).
	sentLabels := make(map[string]string, len(baseLabels)+1)
	for k, v := range baseLabels {
		sentLabels[k] = v
	}
	sentLabels["dm_from"] = selfHex

	// Query received messages (dm_to=self).
	rcvdLabels := make(map[string]string, len(baseLabels)+1)
	for k, v := range baseLabels {
		rcvdLabels[k] = v
	}
	rcvdLabels["dm_to"] = selfHex

	sentResult, err := t.client.QueryMessages(ctx, &client.QueryOptions{
		Labels:     sentLabels,
		Limit:      200,
		Descending: true,
	})
	if err != nil {
		return nil, fmt.Errorf("query sent threads: %w", err)
	}

	rcvdResult, err := t.client.QueryMessages(ctx, &client.QueryOptions{
		Labels:     rcvdLabels,
		Limit:      200,
		Descending: true,
	})
	if err != nil {
		return nil, fmt.Errorf("query received threads: %w", err)
	}

	// Merge and deduplicate by conversation, keeping the newest message per conversation.
	convMap := make(map[string]*Thread)

	addEntries := func(entries []*client.Entry) {
		for _, e := range entries {
			convID := e.Labels["conversation"]
			if convID == "" {
				continue
			}

			// Keep only the newest message per conversation.
			if existing, ok := convMap[convID]; ok && existing.LastMsg.Timestamp >= e.Timestamp {
				continue
			}

			var from, to [32]byte
			if f, err := hex.DecodeString(e.Labels["dm_from"]); err == nil {
				copy(from[:], f)
			}
			if tt, err := hex.DecodeString(e.Labels["dm_to"]); err == nil {
				copy(to[:], tt)
			}

			peer := from
			if from == self {
				peer = to
			}

			convMap[convID] = &Thread{
				ConvID:  convID,
				PeerPub: peer,
				LastMsg: Message{
					Ref:       e.Ref,
					Labels:    e.Labels,
					Timestamp: e.Timestamp,
					From:      from,
					To:        to,
				},
			}
		}
	}

	addEntries(sentResult.Entries)
	addEntries(rcvdResult.Entries)

	threads := make([]Thread, 0, len(convMap))
	for _, th := range convMap {
		threads = append(threads, *th)
	}
	sort.Slice(threads, func(i, j int) bool {
		return threads[i].LastMsg.Timestamp > threads[j].LastMsg.Timestamp
	})
	return threads, nil
}

// SubscribeAll opens a real-time subscription for all DM messages involving self.
func (t *Threads) SubscribeAll(ctx context.Context) (<-chan *Message, <-chan error, error) {
	self := t.kp.PublicKey()
	selfHex := hex.EncodeToString(self[:])
	expr := `labels["dm_from"] == "` + selfHex + `" || labels["dm_to"] == "` + selfHex + `" || labels["from"] == "` + selfHex + `"`

	labels := map[string]string{
		"app":  "dm",
		"type": "message",
	}

	clientEntries, clientErrs, err := t.client.SubscribeMessages(ctx, expr, labels)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe all: %w", err)
	}

	msgs := make(chan *Message)
	errs := make(chan error, 1)

	go func() {
		defer close(msgs)
		defer close(errs)
		for {
			select {
			case ce, ok := <-clientEntries:
				if !ok {
					return
				}
				var from, to [32]byte
				if f, err := hex.DecodeString(ce.Labels["dm_from"]); err == nil {
					copy(from[:], f)
				}
				if tt, err := hex.DecodeString(ce.Labels["dm_to"]); err == nil {
					copy(to[:], tt)
				}
				msgs <- &Message{
					Ref:       ce.Ref,
					Labels:    ce.Labels,
					Timestamp: ce.Timestamp,
					From:      from,
					To:        to,
				}
			case err, ok := <-clientErrs:
				if !ok {
					return
				}
				errs <- err
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return msgs, errs, nil
}

// PreviewThread decrypts a short preview of the latest message in a thread.
func (t *Threads) PreviewThread(ctx context.Context, th Thread) (string, error) {
	d, err := t.OpenConversation(th.PeerPub)
	if err != nil {
		return "", err
	}
	return d.Preview(ctx, th.LastMsg)
}

// OpenConversation creates a DM instance for the given peer.
func (t *Threads) OpenConversation(peerPub identity.PublicKey) (*DM, error) {
	var opts []Option
	if t.nodeKey != nil {
		opts = append(opts, WithNodeKey(*t.nodeKey))
	}
	if t.search != nil {
		opts = append(opts, WithSearchIndex(t.search))
	}
	return New(t.client, t.kp, peerPub, opts...)
}

// SearchIndex returns the attached search index, or nil.
func (t *Threads) SearchIndex() *SearchIndex { return t.search }

// SetSearchIndex replaces the attached search index.
func (t *Threads) SetSearchIndex(idx *SearchIndex) { t.search = idx }

// Search queries the local full-text search index.
func (t *Threads) Search(ctx context.Context, query string, opts SearchOptions) (*SearchResponse, error) {
	if t.search == nil {
		return nil, fmt.Errorf("no search index configured")
	}
	return t.search.Search(ctx, query, opts)
}

// IndexMessage indexes a single decrypted message in the search index.
func (t *Threads) IndexMessage(ctx context.Context, contentRef, msgRef reference.Reference, plaintext string, timestamp int64) {
	if t.search != nil {
		_ = t.search.Index(ctx, contentRef, msgRef, plaintext, timestamp)
	}
}

// ReindexResult holds the outcome of a reindex operation.
type ReindexResult struct {
	Indexed int
	Errors  int
}

// Reindex rebuilds the search index from all existing DM conversations.
func (t *Threads) Reindex(ctx context.Context) (*ReindexResult, error) {
	if t.search == nil {
		return nil, fmt.Errorf("no search index configured")
	}

	if err := t.search.Clear(ctx); err != nil {
		return nil, fmt.Errorf("clear search index: %w", err)
	}

	threads, err := t.ListThreads(ctx)
	if err != nil {
		return nil, fmt.Errorf("list threads: %w", err)
	}

	var result ReindexResult
	for _, th := range threads {
		sdk, err := t.OpenConversation(th.PeerPub)
		if err != nil {
			result.Errors++
			continue
		}

		var cursor string
		for {
			lr, err := sdk.List(ctx, ListOptions{
				Limit:      50,
				Cursor:     cursor,
				Descending: false,
			})
			if err != nil {
				result.Errors++
				break
			}

			for _, m := range lr.Messages {
				contentHex := m.Labels["content"]
				if contentHex == "" {
					result.Errors++
					continue
				}
				contentRef, err := reference.FromHex(contentHex)
				if err != nil {
					result.Errors++
					continue
				}
				msg, err := sdk.Read(ctx, contentRef)
				if err != nil {
					result.Errors++
					continue
				}
				_ = t.search.Index(ctx, contentRef, m.Ref, string(msg.Content), m.Timestamp)
				result.Indexed++
			}

			if !lr.HasMore {
				break
			}
			cursor = lr.NextCursor
		}
	}
	return &result, nil
}
