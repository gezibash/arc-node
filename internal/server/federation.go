package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gezibash/arc-node/internal/blobstore"
	"github.com/gezibash/arc-node/internal/indexstore"
	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/pkg/identity"
	"github.com/gezibash/arc/pkg/reference"
)

type federationManager struct {
	mu         sync.Mutex
	active     map[string]*federation
	blobs      *blobstore.BlobStore
	index      *indexstore.IndexStore
	kp         *identity.Keypair
	baseCtx    context.Context
	baseCancel context.CancelFunc
}

type federation struct {
	key               string
	peerAddr          string
	labels            map[string]string
	cancel            context.CancelFunc
	startedAt         time.Time
	bytesReceived     atomic.Int64
	entriesReplicated atomic.Int64
}

// FederationInfo is a snapshot of an active outbound federation.
type FederationInfo struct {
	Address           string
	Labels            map[string]string
	BytesReceived     int64
	EntriesReplicated int64
	StartedAt         time.Time
}

// SubscriberInfo is a snapshot of an active inbound subscriber.
type SubscriberInfo struct {
	PublicKey   identity.PublicKey
	Labels      map[string]string
	Expression  string
	EntriesSent int64
	StartedAt   time.Time
}

type subscriber struct {
	pubKey      identity.PublicKey
	labels      map[string]string
	expression  string
	startedAt   time.Time
	entriesSent atomic.Int64
}

type subscriberTracker struct {
	mu   sync.Mutex
	subs map[*subscriber]struct{}
}

func newSubscriberTracker() *subscriberTracker {
	return &subscriberTracker{subs: make(map[*subscriber]struct{})}
}

func (t *subscriberTracker) Add(pubKey identity.PublicKey, labels map[string]string, expression string) *subscriber {
	s := &subscriber{
		pubKey:     pubKey,
		labels:     copyLabels(labels),
		expression: expression,
		startedAt:  time.Now(),
	}
	t.mu.Lock()
	t.subs[s] = struct{}{}
	t.mu.Unlock()
	return s
}

func (t *subscriberTracker) Remove(s *subscriber) {
	t.mu.Lock()
	delete(t.subs, s)
	t.mu.Unlock()
}

func (t *subscriberTracker) List() []SubscriberInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]SubscriberInfo, 0, len(t.subs))
	for s := range t.subs {
		out = append(out, SubscriberInfo{
			PublicKey:   s.pubKey,
			Labels:      copyLabels(s.labels),
			Expression:  s.expression,
			EntriesSent: s.entriesSent.Load(),
			StartedAt:   s.startedAt,
		})
	}
	return out
}

func newFederationManager(blobs *blobstore.BlobStore, index *indexstore.IndexStore, kp *identity.Keypair) *federationManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &federationManager{
		active:     make(map[string]*federation),
		blobs:      blobs,
		index:      index,
		kp:         kp,
		baseCtx:    ctx,
		baseCancel: cancel,
	}
}

func (f *federationManager) Start(peerAddr string, labels map[string]string) (bool, error) {
	if f == nil {
		return false, fmt.Errorf("federation manager unavailable")
	}
	if peerAddr == "" {
		return false, fmt.Errorf("peer required")
	}

	key := federationKey(peerAddr, labels)

	f.mu.Lock()
	if _, ok := f.active[key]; ok {
		f.mu.Unlock()
		return false, nil
	}

	ctx, cancel := context.WithCancel(f.baseCtx)
	fed := &federation{
		key:       key,
		peerAddr:  peerAddr,
		labels:    copyLabels(labels),
		cancel:    cancel,
		startedAt: time.Now(),
	}
	f.active[key] = fed
	f.mu.Unlock()

	go f.run(ctx, fed)
	return true, nil
}

func (f *federationManager) List() []FederationInfo {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]FederationInfo, 0, len(f.active))
	for _, fed := range f.active {
		out = append(out, FederationInfo{
			Address:           fed.peerAddr,
			Labels:            copyLabels(fed.labels),
			BytesReceived:     fed.bytesReceived.Load(),
			EntriesReplicated: fed.entriesReplicated.Load(),
			StartedAt:         fed.startedAt,
		})
	}
	return out
}

func (f *federationManager) StopAll() {
	if f == nil {
		return
	}
	f.baseCancel()
	f.mu.Lock()
	for _, fed := range f.active {
		fed.cancel()
	}
	f.active = make(map[string]*federation)
	f.mu.Unlock()
}

func (f *federationManager) run(ctx context.Context, fed *federation) {
	defer func() {
		f.remove(fed.key, fed)
		slog.Info("federation stopped", "peer", fed.peerAddr)
	}()

	slog.Info("federation started", "peer", fed.peerAddr, "labels", fed.labels)

	peerClient, err := client.Dial(fed.peerAddr, client.WithIdentity(f.kp))
	if err != nil {
		slog.Error("federation dial failed", "peer", fed.peerAddr, "error", err)
		return
	}
	defer peerClient.Close()

	if _, err := peerClient.Ping(ctx); err != nil {
		slog.Error("federation ping failed", "peer", fed.peerAddr, "error", err)
		return
	}

	entries, errs, err := peerClient.SubscribeMessages(ctx, "true", fed.labels)
	if err != nil {
		slog.Error("federation subscribe failed", "peer", fed.peerAddr, "error", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-entries:
			if !ok {
				return
			}
			if err := f.replicateEntry(ctx, peerClient, fed, entry); err != nil {
				slog.Error("federation entry error", "peer", fed.peerAddr, "error", err)
			}
		case err := <-errs:
			if err != nil {
				slog.Error("federation stream error", "peer", fed.peerAddr, "error", err)
			}
			return
		}
	}
}

func (f *federationManager) replicateEntry(ctx context.Context, peer *client.Client, fed *federation, entry *client.Entry) error {
	if entry == nil {
		return fmt.Errorf("entry required")
	}

	if _, err := f.index.Get(ctx, entry.Ref); err == nil {
		return nil
	} else if !errors.Is(err, indexstore.ErrNotFound) {
		return fmt.Errorf("check entry: %w", err)
	}

	labels := copyLabels(entry.Labels)
	contentHex := labels["content"]
	if contentHex == "" {
		return fmt.Errorf("entry %s missing content label", reference.Hex(entry.Ref))
	}
	contentRef, err := reference.FromHex(contentHex)
	if err != nil {
		return fmt.Errorf("parse content label: %w", err)
	}

	exists, err := f.blobs.Exists(ctx, contentRef)
	if err != nil {
		return fmt.Errorf("check content: %w", err)
	}
	if !exists {
		data, err := peer.GetContent(ctx, contentRef)
		if err != nil {
			return fmt.Errorf("get content: %w", err)
		}
		storedRef, err := f.blobs.Store(ctx, data)
		if err != nil {
			return fmt.Errorf("store content: %w", err)
		}
		if !reference.Equal(storedRef, contentRef) {
			return fmt.Errorf("content ref mismatch: got %s want %s", reference.Hex(storedRef), reference.Hex(contentRef))
		}
		fed.bytesReceived.Add(int64(len(data)))
	}

	fedEntry := &physical.Entry{
		Ref:       entry.Ref,
		Labels:    labels,
		Timestamp: entry.Timestamp,
	}
	if err := f.index.Index(ctx, fedEntry); err != nil {
		return fmt.Errorf("index entry: %w", err)
	}

	fed.entriesReplicated.Add(1)
	return nil
}

func (f *federationManager) remove(key string, fed *federation) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := f.active[key]; ok && cur == fed {
		delete(f.active, key)
	}
}

func federationKey(peerAddr string, labels map[string]string) string {
	if len(labels) == 0 {
		return peerAddr
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(peerAddr)
	for _, k := range keys {
		b.WriteString("|")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(labels[k])
	}
	return b.String()
}

func copyLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}

func normalizePeerAddr(raw string) string {
	if strings.Contains(raw, "://") {
		if parsed, err := url.Parse(raw); err == nil && parsed.Host != "" {
			return parsed.Host
		}
	}
	return raw
}
