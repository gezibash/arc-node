package server

import (
	"sync"

	"github.com/gezibash/arc-node/pkg/group"
	"github.com/gezibash/arc/v2/pkg/identity"
)

// groupCache maintains an in-memory mapping of group membership for
// fast visibility checks.
type groupCache struct {
	mu      sync.RWMutex
	members map[identity.PublicKey]map[identity.PublicKey]group.Role // groupPK → memberPK → role
}

func newGroupCache() *groupCache {
	return &groupCache{
		members: make(map[identity.PublicKey]map[identity.PublicKey]group.Role),
	}
}

// HasGroup returns true if the group cache has any membership data for groupPK.
func (gc *groupCache) HasGroup(groupPK identity.PublicKey) bool {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	_, ok := gc.members[groupPK]
	return ok
}

// IsMember returns true if callerPK is a member (any role) of groupPK.
func (gc *groupCache) IsMember(groupPK, callerPK identity.PublicKey) bool {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	mems, ok := gc.members[groupPK]
	if !ok {
		return false
	}
	_, found := mems[callerPK]
	return found
}

// LoadManifest replaces the membership set for the manifest's group ID.
func (gc *groupCache) LoadManifest(m *group.Manifest) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	mems := make(map[identity.PublicKey]group.Role, len(m.Members))
	for _, mem := range m.Members {
		mems[mem.PublicKey] = mem.Role
	}
	gc.members[m.ID] = mems
}

// Invalidate removes all cached membership data for a group.
func (gc *groupCache) Invalidate(groupPK identity.PublicKey) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	delete(gc.members, groupPK)
}
