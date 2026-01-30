package dm

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/gezibash/arc/pkg/identity"
	"github.com/gezibash/arc-node/pkg/client"
)

// Threads provides cross-conversation DM operations without requiring a peer pubkey.
type Threads struct {
	client  *client.Client
	kp      *identity.Keypair
	nodeKey *identity.PublicKey
}

// Thread represents a conversation with a peer, summarized by latest activity.
type Thread struct {
	ConvID  string
	PeerPub identity.PublicKey
	LastMsg Message
	Preview string // lazily populated
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

// ListThreads queries all DM messages and groups them by conversation, returning
// one Thread per peer sorted by most recent activity (descending).
func (t *Threads) ListThreads(ctx context.Context) ([]Thread, error) {
	self := t.kp.PublicKey()
	selfHex := hex.EncodeToString(self[:])
	expr := `labels["dm_from"] == "` + selfHex + `" || labels["dm_to"] == "` + selfHex + `" || labels["from"] == "` + selfHex + `"`

	result, err := t.client.QueryMessages(ctx, &client.QueryOptions{
		Expression: expr,
		Labels: map[string]string{
			"app":  "dm",
			"type": "message",
		},
		Limit:      200,
		Descending: true,
	})
	if err != nil {
		return nil, fmt.Errorf("query threads: %w", err)
	}

	convMap := make(map[string]*Thread)

	for _, e := range result.Entries {
		convID := e.Labels["conversation"]
		if convID == "" {
			continue
		}

		// Already have a newer message for this conversation.
		if _, exists := convMap[convID]; exists {
			continue
		}

		var from, to [32]byte
		if f, err := hex.DecodeString(e.Labels["dm_from"]); err == nil {
			copy(from[:], f)
		}
		if tt, err := hex.DecodeString(e.Labels["dm_to"]); err == nil {
			copy(to[:], tt)
		}

		// Determine peer: whichever of from/to is not self.
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
	return New(t.client, t.kp, peerPub, opts...)
}
