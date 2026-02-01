// Package group implements sovereign group keypairs with encrypted seed
// distribution and chained manifests. Groups are first-class Arc identities.
package group

import (
	"errors"

	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/reference"
)

// Role indicates a member's privilege level within a group.
type Role int

const (
	RoleMember Role = iota
	RoleAdmin
)

// Member represents a group member with their public key, role, and
// optionally an encrypted copy of the group seed (admin only).
type Member struct {
	PublicKey  identity.PublicKey
	Role      Role
	SealedSeed []byte // encrypted group seed, admin-only
}

// Policy describes group governance rules.
type Policy struct {
	AdminThreshold int // future: multi-sig quorum
}

// Manifest describes the current state of a group: its identity,
// members, policy, and chain link to the previous manifest.
type Manifest struct {
	ID      identity.PublicKey   // group public key
	Name    string
	Members []Member
	Policy  Policy
	Parent  *reference.Reference // chain to previous manifest
}

var (
	ErrNotAdmin        = errors.New("caller is not a group admin")
	ErrMemberExists    = errors.New("member already exists in group")
	ErrMemberNotFound  = errors.New("member not found in group")
	ErrInvalidManifest = errors.New("invalid manifest")
	ErrDecryptFailed   = errors.New("seed decryption failed")
)
