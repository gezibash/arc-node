package server

import (
	"testing"

	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc-node/pkg/group"
)

func TestGroupCacheIsMember(t *testing.T) {
	gc := newGroupCache()

	groupKP, _ := identity.Generate()
	admin, _ := identity.Generate()
	member, _ := identity.Generate()
	stranger, _ := identity.Generate()

	m := &group.Manifest{
		ID: groupKP.PublicKey(),
		Members: []group.Member{
			{PublicKey: admin.PublicKey(), Role: group.RoleAdmin},
			{PublicKey: member.PublicKey(), Role: group.RoleMember},
		},
	}
	gc.LoadManifest(m)

	if !gc.IsMember(groupKP.PublicKey(), admin.PublicKey()) {
		t.Error("admin should be a member")
	}
	if !gc.IsMember(groupKP.PublicKey(), member.PublicKey()) {
		t.Error("member should be a member")
	}
	if gc.IsMember(groupKP.PublicKey(), stranger.PublicKey()) {
		t.Error("stranger should not be a member")
	}
}

func TestGroupCacheInvalidate(t *testing.T) {
	gc := newGroupCache()

	groupKP, _ := identity.Generate()
	admin, _ := identity.Generate()

	m := &group.Manifest{
		ID: groupKP.PublicKey(),
		Members: []group.Member{
			{PublicKey: admin.PublicKey(), Role: group.RoleAdmin},
		},
	}
	gc.LoadManifest(m)

	if !gc.IsMember(groupKP.PublicKey(), admin.PublicKey()) {
		t.Error("should be member before invalidate")
	}

	gc.Invalidate(groupKP.PublicKey())

	if gc.IsMember(groupKP.PublicKey(), admin.PublicKey()) {
		t.Error("should not be member after invalidate")
	}
}

func TestGroupCacheLoadManifestReplaces(t *testing.T) {
	gc := newGroupCache()

	groupKP, _ := identity.Generate()
	old, _ := identity.Generate()
	new_, _ := identity.Generate()

	m1 := &group.Manifest{
		ID:      groupKP.PublicKey(),
		Members: []group.Member{{PublicKey: old.PublicKey(), Role: group.RoleMember}},
	}
	gc.LoadManifest(m1)

	m2 := &group.Manifest{
		ID:      groupKP.PublicKey(),
		Members: []group.Member{{PublicKey: new_.PublicKey(), Role: group.RoleMember}},
	}
	gc.LoadManifest(m2)

	if gc.IsMember(groupKP.PublicKey(), old.PublicKey()) {
		t.Error("old member should be gone after manifest replacement")
	}
	if !gc.IsMember(groupKP.PublicKey(), new_.PublicKey()) {
		t.Error("new member should exist after manifest replacement")
	}
}

func TestGroupCacheUnknownGroup(t *testing.T) {
	gc := newGroupCache()
	kp, _ := identity.Generate()
	unknown, _ := identity.Generate()

	if gc.IsMember(unknown.PublicKey(), kp.PublicKey()) {
		t.Error("unknown group should have no members")
	}
}
