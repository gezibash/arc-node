package server

import (
	"encoding/hex"
	"fmt"

	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

// visibilityChecker evaluates entry visibility using group membership
// for VISIBILITY_LABEL_SCOPED entries.
type visibilityChecker struct {
	groups *groupCache
}

func newVisibilityChecker(groups *groupCache) *visibilityChecker {
	return &visibilityChecker{groups: groups}
}

// Check returns true if the caller identified by callerKey is allowed
// to see the entry based on its Visibility dimension.
func (vc *visibilityChecker) Check(entry *physical.Entry, callerKey [32]byte) bool {
	switch entry.Visibility {
	case 1: // VISIBILITY_PRIVATE
		callerHex := fmt.Sprintf("%x", callerKey)
		from := entry.Labels["from"]
		to := entry.Labels["to"]
		return callerHex == from || callerHex == to
	case 2: // VISIBILITY_LABEL_SCOPED
		scope := entry.Labels["scope"]
		if scope == "" {
			return true
		}
		scopeBytes, err := hex.DecodeString(scope)
		if err != nil || len(scopeBytes) != identity.PublicKeySize {
			return true // not a valid group scope, allow
		}
		var scopeKey identity.PublicKey
		copy(scopeKey[:], scopeBytes)
		return vc.groups.IsMember(scopeKey, identity.PublicKey(callerKey))
	case 3: // VISIBILITY_FEDERATED — stub, allow all
		return true
	default: // 0 = VISIBILITY_PUBLIC
		return true
	}
}
