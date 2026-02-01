package group

import (
	"fmt"

	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/reference"
)

// Create generates a new group keypair and initial manifest with the given
// admins. The group seed is encrypted to each admin.
func Create(name string, adminKPs ...*identity.Keypair) (*identity.Keypair, *Manifest, error) {
	if len(adminKPs) == 0 {
		return nil, nil, fmt.Errorf("%w: at least one admin required", ErrInvalidManifest)
	}

	groupKP, err := identity.Generate()
	if err != nil {
		return nil, nil, fmt.Errorf("generate group keypair: %w", err)
	}

	members := make([]Member, len(adminKPs))
	for i, kp := range adminKPs {
		sealed, err := SealSeed(groupKP.Seed(), kp.PublicKey())
		if err != nil {
			return nil, nil, fmt.Errorf("seal seed for admin %d: %w", i, err)
		}
		members[i] = Member{
			PublicKey:  kp.PublicKey(),
			Role:       RoleAdmin,
			SealedSeed: sealed,
		}
	}

	m := &Manifest{
		ID:      groupKP.PublicKey(),
		Name:    name,
		Members: members,
		Policy:  Policy{AdminThreshold: 1},
	}

	return groupKP, m, nil
}

// AddMember adds a member to the group, producing a new manifest chained to
// the current one. If role is RoleAdmin, the group seed is encrypted to them.
func AddMember(current *Manifest, groupKP *identity.Keypair, newMember identity.PublicKey, role Role) (*Manifest, error) {
	for _, m := range current.Members {
		if m.PublicKey == newMember {
			return nil, ErrMemberExists
		}
	}

	currentData, err := MarshalManifest(current)
	if err != nil {
		return nil, err
	}
	parentRef := reference.Compute(currentData)

	mem := Member{
		PublicKey: newMember,
		Role:      role,
	}
	if role == RoleAdmin {
		sealed, err := SealSeed(groupKP.Seed(), newMember)
		if err != nil {
			return nil, fmt.Errorf("seal seed for new admin: %w", err)
		}
		mem.SealedSeed = sealed
	}

	next := &Manifest{
		ID:      current.ID,
		Name:    current.Name,
		Members: append(append([]Member{}, current.Members...), mem),
		Policy:  current.Policy,
		Parent:  &parentRef,
	}
	return next, nil
}

// RemoveMember removes a member from the group, producing a new manifest.
func RemoveMember(current *Manifest, groupKP *identity.Keypair, member identity.PublicKey) (*Manifest, error) {
	found := false
	var remaining []Member
	for _, m := range current.Members {
		if m.PublicKey == member {
			found = true
			continue
		}
		remaining = append(remaining, m)
	}
	if !found {
		return nil, ErrMemberNotFound
	}

	currentData, err := MarshalManifest(current)
	if err != nil {
		return nil, err
	}
	parentRef := reference.Compute(currentData)

	next := &Manifest{
		ID:      current.ID,
		Name:    current.Name,
		Members: remaining,
		Policy:  current.Policy,
		Parent:  &parentRef,
	}
	return next, nil
}

// RotateKey generates a new group keypair and creates a new manifest chain.
// The old group identity is abandoned; the new manifest starts a fresh chain.
func RotateKey(current *Manifest, newAdminKPs ...*identity.Keypair) (*identity.Keypair, *Manifest, error) {
	if len(newAdminKPs) == 0 {
		return nil, nil, fmt.Errorf("%w: at least one admin required", ErrInvalidManifest)
	}

	newGroupKP, err := identity.Generate()
	if err != nil {
		return nil, nil, fmt.Errorf("generate new group keypair: %w", err)
	}

	members := make([]Member, len(newAdminKPs))
	for i, kp := range newAdminKPs {
		sealed, err := SealSeed(newGroupKP.Seed(), kp.PublicKey())
		if err != nil {
			return nil, nil, fmt.Errorf("seal seed for admin %d: %w", i, err)
		}
		members[i] = Member{
			PublicKey:  kp.PublicKey(),
			Role:       RoleAdmin,
			SealedSeed: sealed,
		}
	}

	m := &Manifest{
		ID:      newGroupKP.PublicKey(),
		Name:    current.Name,
		Members: members,
		Policy:  current.Policy,
	}

	return newGroupKP, m, nil
}

// RecoverGroupKey decrypts the sealed seed and reconstructs the group keypair.
func RecoverGroupKey(sealedSeed []byte, adminKP *identity.Keypair) (*identity.Keypair, error) {
	seed, err := OpenSeed(sealedSeed, adminKP)
	if err != nil {
		return nil, fmt.Errorf("decrypt group seed: %w", err)
	}
	kp, err := identity.FromSeed(seed)
	if err != nil {
		return nil, fmt.Errorf("reconstruct group keypair: %w", err)
	}
	return kp, nil
}
