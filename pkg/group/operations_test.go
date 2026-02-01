package group

import (
	"bytes"
	"testing"

	"github.com/gezibash/arc/v2/pkg/identity"
)

func TestCreateGroup(t *testing.T) {
	admin1, _ := identity.Generate()
	admin2, _ := identity.Generate()

	groupKP, m, err := Create("my-group", admin1, admin2)
	if err != nil {
		t.Fatal(err)
	}

	if m.ID != groupKP.PublicKey() {
		t.Error("manifest ID should match group public key")
	}
	if m.Name != "my-group" {
		t.Error("name mismatch")
	}
	if len(m.Members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(m.Members))
	}
	for _, mem := range m.Members {
		if mem.Role != RoleAdmin {
			t.Error("all initial members should be admins")
		}
		if len(mem.SealedSeed) == 0 {
			t.Error("admin should have sealed seed")
		}
	}
}

func TestCreateGroupNoAdmins(t *testing.T) {
	_, _, err := Create("empty")
	if err == nil {
		t.Error("expected error creating group with no admins")
	}
}

func TestAddMember(t *testing.T) {
	admin, _ := identity.Generate()
	groupKP, m, _ := Create("test", admin)

	newMember, _ := identity.Generate()
	m2, err := AddMember(m, groupKP, newMember.PublicKey(), RoleMember)
	if err != nil {
		t.Fatal(err)
	}

	if len(m2.Members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(m2.Members))
	}
	if m2.Parent == nil {
		t.Error("new manifest should have parent")
	}

	// Verify the new member has no sealed seed.
	for _, mem := range m2.Members {
		if mem.PublicKey == newMember.PublicKey() {
			if len(mem.SealedSeed) != 0 {
				t.Error("regular member should not have sealed seed")
			}
		}
	}
}

func TestAddMemberAdmin(t *testing.T) {
	admin, _ := identity.Generate()
	groupKP, m, _ := Create("test", admin)

	newAdmin, _ := identity.Generate()
	m2, err := AddMember(m, groupKP, newAdmin.PublicKey(), RoleAdmin)
	if err != nil {
		t.Fatal(err)
	}

	for _, mem := range m2.Members {
		if mem.PublicKey == newAdmin.PublicKey() {
			if len(mem.SealedSeed) == 0 {
				t.Error("new admin should have sealed seed")
			}
		}
	}
}

func TestAddMemberDuplicate(t *testing.T) {
	admin, _ := identity.Generate()
	groupKP, m, _ := Create("test", admin)

	_, err := AddMember(m, groupKP, admin.PublicKey(), RoleMember)
	if err != ErrMemberExists {
		t.Errorf("expected ErrMemberExists, got %v", err)
	}
}

func TestRemoveMember(t *testing.T) {
	admin, _ := identity.Generate()
	groupKP, m, _ := Create("test", admin)

	newMember, _ := identity.Generate()
	m2, _ := AddMember(m, groupKP, newMember.PublicKey(), RoleMember)

	m3, err := RemoveMember(m2, groupKP, newMember.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	if len(m3.Members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(m3.Members))
	}
	if m3.Parent == nil {
		t.Error("new manifest should have parent")
	}
}

func TestRemoveMemberNotFound(t *testing.T) {
	admin, _ := identity.Generate()
	groupKP, m, _ := Create("test", admin)

	stranger, _ := identity.Generate()
	_, err := RemoveMember(m, groupKP, stranger.PublicKey())
	if err != ErrMemberNotFound {
		t.Errorf("expected ErrMemberNotFound, got %v", err)
	}
}

func TestRotateKey(t *testing.T) {
	admin, _ := identity.Generate()
	_, m, _ := Create("test", admin)

	newGroupKP, m2, err := RotateKey(m, admin)
	if err != nil {
		t.Fatal(err)
	}

	if m2.ID == m.ID {
		t.Error("rotated group should have new ID")
	}
	if m2.ID != newGroupKP.PublicKey() {
		t.Error("manifest ID should match new group key")
	}
	if m2.Name != m.Name {
		t.Error("name should be preserved")
	}
	if m2.Parent != nil {
		t.Error("rotated manifest starts new chain, should have no parent")
	}
}

func TestRecoverGroupKey(t *testing.T) {
	admin, _ := identity.Generate()
	groupKP, m, _ := Create("test", admin)

	// Find admin's sealed seed.
	var sealed []byte
	for _, mem := range m.Members {
		if mem.PublicKey == admin.PublicKey() {
			sealed = mem.SealedSeed
		}
	}
	if sealed == nil {
		t.Fatal("no sealed seed found")
	}

	recovered, err := RecoverGroupKey(sealed, admin)
	if err != nil {
		t.Fatal(err)
	}

	if recovered.PublicKey() != groupKP.PublicKey() {
		t.Error("recovered key does not match original group key")
	}
	if !bytes.Equal(recovered.Seed(), groupKP.Seed()) {
		t.Error("recovered seed does not match")
	}
}

func TestRecoverGroupKeyWrongAdmin(t *testing.T) {
	admin, _ := identity.Generate()
	_, m, _ := Create("test", admin)

	wrongAdmin, _ := identity.Generate()
	var sealed []byte
	for _, mem := range m.Members {
		if mem.PublicKey == admin.PublicKey() {
			sealed = mem.SealedSeed
		}
	}

	_, err := RecoverGroupKey(sealed, wrongAdmin)
	if err == nil {
		t.Error("expected error recovering with wrong admin key")
	}
}
