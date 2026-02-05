package relay

import (
	"sync"
	"time"
)

// ReturnPathCache caches return paths for cross-relay response routing.
// When a forwarded envelope arrives, we cache the source relay so responses
// can be routed back: pubkey → relay_name.
type ReturnPathCache struct {
	mu      sync.RWMutex
	entries map[string]returnPathEntry
	ttl     time.Duration
}

type returnPathEntry struct {
	relayName string
	expiresAt time.Time
}

// NewReturnPathCache creates a new return path cache with the given TTL.
func NewReturnPathCache(ttl time.Duration) *ReturnPathCache {
	if ttl <= 0 {
		ttl = 5 * time.Minute // default TTL
	}
	return &ReturnPathCache{
		entries: make(map[string]returnPathEntry),
		ttl:     ttl,
	}
}

// Set caches a return path: pubkey → relay_name.
func (c *ReturnPathCache) Set(pubkey, relayName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[pubkey] = returnPathEntry{
		relayName: relayName,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// Get retrieves the cached relay name for a pubkey.
// Returns ("", false) if not found or expired.
func (c *ReturnPathCache) Get(pubkey string) (string, bool) {
	c.mu.RLock()
	entry, ok := c.entries[pubkey]
	c.mu.RUnlock()

	if !ok {
		return "", false
	}
	if time.Now().After(entry.expiresAt) {
		// Expired — clean up lazily
		c.mu.Lock()
		delete(c.entries, pubkey)
		c.mu.Unlock()
		return "", false
	}
	return entry.relayName, true
}

// Cleanup removes expired entries. Call periodically if needed.
func (c *ReturnPathCache) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for pubkey, entry := range c.entries {
		if now.After(entry.expiresAt) {
			delete(c.entries, pubkey)
		}
	}
}
