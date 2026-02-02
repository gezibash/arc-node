package server

import (
	"sync"
	"time"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
)

type presenceEntry struct {
	pubKey    identity.PublicKey
	status    string
	typing    bool
	metadata  map[string]string
	updatedAt time.Time
	expiresAt time.Time // zero = connection lifetime
}

func (e *presenceEntry) toProto(removed bool) *nodev1.PresenceEventFrame {
	return &nodev1.PresenceEventFrame{
		PublicKey: e.pubKey[:],
		Status:    e.status,
		Typing:    e.typing,
		Metadata:  e.metadata,
		UpdatedAt: e.updatedAt.UnixMilli(),
		Removed:   removed,
	}
}

type presenceSub struct {
	keys   map[identity.PublicKey]struct{} // empty = all
	writer *streamWriter
}

func (ps *presenceSub) matches(pk identity.PublicKey) bool {
	if len(ps.keys) == 0 {
		return true
	}
	_, ok := ps.keys[pk]
	return ok
}

type presenceManager struct {
	mu     sync.RWMutex
	states map[identity.PublicKey]*presenceEntry
	conns  map[string]map[identity.PublicKey]struct{} // connID → pubkeys
	subs   map[string]*presenceSub                    // connID → subscriber
	stopCh chan struct{}
}

func newPresenceManager() *presenceManager {
	pm := &presenceManager{
		states: make(map[identity.PublicKey]*presenceEntry),
		conns:  make(map[string]map[identity.PublicKey]struct{}),
		subs:   make(map[string]*presenceSub),
		stopCh: make(chan struct{}),
	}
	go pm.expiryLoop()
	return pm
}

func (pm *presenceManager) Stop() {
	close(pm.stopCh)
}

func (pm *presenceManager) Set(connID string, pubKey identity.PublicKey, status string, typing bool, metadata map[string]string, ttlSeconds int64) {
	pm.mu.Lock()

	now := time.Now()
	entry := &presenceEntry{
		pubKey:    pubKey,
		status:    status,
		typing:    typing,
		metadata:  metadata,
		updatedAt: now,
	}
	if ttlSeconds > 0 {
		entry.expiresAt = now.Add(time.Duration(ttlSeconds) * time.Second)
	}

	pm.states[pubKey] = entry

	if pm.conns[connID] == nil {
		pm.conns[connID] = make(map[identity.PublicKey]struct{})
	}
	pm.conns[connID][pubKey] = struct{}{}

	// Collect matching subscribers.
	var notify []*presenceSub
	for _, sub := range pm.subs {
		if sub.matches(pubKey) {
			notify = append(notify, sub)
		}
	}
	pm.mu.Unlock()

	// Notify outside lock.
	frame := &nodev1.ServerFrame{
		RequestId: 0,
		Frame:     &nodev1.ServerFrame_PresenceEvent{PresenceEvent: entry.toProto(false)},
	}
	for _, sub := range notify {
		_ = sub.writer.send(frame)
	}
}

func (pm *presenceManager) Query(keys []identity.PublicKey) []*presenceEntry {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if len(keys) == 0 {
		out := make([]*presenceEntry, 0, len(pm.states))
		for _, e := range pm.states {
			out = append(out, e)
		}
		return out
	}

	out := make([]*presenceEntry, 0, len(keys))
	for _, k := range keys {
		if e, ok := pm.states[k]; ok {
			out = append(out, e)
		}
	}
	return out
}

func (pm *presenceManager) Subscribe(connID string, keys []identity.PublicKey, writer *streamWriter) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	keySet := make(map[identity.PublicKey]struct{}, len(keys))
	for _, k := range keys {
		keySet[k] = struct{}{}
	}
	pm.subs[connID] = &presenceSub{keys: keySet, writer: writer}
}

func (pm *presenceManager) Unsubscribe(connID string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.subs, connID)
}

func (pm *presenceManager) CleanupConnection(connID string) {
	pm.mu.Lock()

	// Remove presence entries for this connection.
	keys, ok := pm.conns[connID]
	if !ok {
		delete(pm.subs, connID)
		pm.mu.Unlock()
		return
	}
	delete(pm.conns, connID)
	delete(pm.subs, connID)

	var removed []*presenceEntry
	var notify []*presenceSub
	for pk := range keys {
		if e, exists := pm.states[pk]; exists {
			removed = append(removed, e)
			delete(pm.states, pk)
		}
	}

	if len(removed) > 0 {
		for _, sub := range pm.subs {
			for _, e := range removed {
				if sub.matches(e.pubKey) {
					notify = append(notify, sub)
					break
				}
			}
		}
	}
	pm.mu.Unlock()

	// Notify outside lock.
	for _, e := range removed {
		frame := &nodev1.ServerFrame{
			RequestId: 0,
			Frame:     &nodev1.ServerFrame_PresenceEvent{PresenceEvent: e.toProto(true)},
		}
		for _, sub := range notify {
			if sub.matches(e.pubKey) {
				_ = sub.writer.send(frame)
			}
		}
	}
}

func (pm *presenceManager) expiryLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-pm.stopCh:
			return
		case now := <-ticker.C:
			pm.expire(now)
		}
	}
}

func (pm *presenceManager) expire(now time.Time) {
	pm.mu.Lock()

	var expired []*presenceEntry
	for pk, e := range pm.states {
		if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
			expired = append(expired, e)
			delete(pm.states, pk)
			// Remove from conn tracking too.
			for _, keys := range pm.conns {
				delete(keys, pk)
			}
		}
	}

	var notify []*presenceSub
	if len(expired) > 0 {
		for _, sub := range pm.subs {
			for _, e := range expired {
				if sub.matches(e.pubKey) {
					notify = append(notify, sub)
					break
				}
			}
		}
	}
	pm.mu.Unlock()

	for _, e := range expired {
		frame := &nodev1.ServerFrame{
			RequestId: 0,
			Frame:     &nodev1.ServerFrame_PresenceEvent{PresenceEvent: e.toProto(true)},
		}
		for _, sub := range notify {
			if sub.matches(e.pubKey) {
				_ = sub.writer.send(frame)
			}
		}
	}
}
