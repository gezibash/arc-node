package server

import (
	"fmt"
	"testing"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/pkg/group"
	"github.com/gezibash/arc/v2/pkg/identity"
)

func newTestVisibilityChecker() *visibilityChecker {
	return newVisibilityChecker(newGroupCache())
}

func TestVisibilityPublic(t *testing.T) {
	vc := newTestVisibilityChecker()
	entry := &physical.Entry{
		Visibility: 0, // PUBLIC
		Labels:     map[string]string{"from": "aaa", "to": "bbb"},
	}
	if !vc.Check(entry, [32]byte{0xCC}) {
		t.Error("public entry should be visible to anyone")
	}
}

func TestVisibilityPrivate(t *testing.T) {
	vc := newTestVisibilityChecker()
	senderKey := [32]byte{0x01}
	recipientKey := [32]byte{0x02}

	entry := &physical.Entry{
		Visibility: 1, // PRIVATE
		Labels: map[string]string{
			"from": fmt.Sprintf("%x", senderKey),
			"to":   fmt.Sprintf("%x", recipientKey),
		},
	}

	if !vc.Check(entry, senderKey) {
		t.Error("sender should see private entry")
	}
	if !vc.Check(entry, recipientKey) {
		t.Error("recipient should see private entry")
	}
}

func TestVisibilityPrivateReject(t *testing.T) {
	vc := newTestVisibilityChecker()
	senderKey := [32]byte{0x01}
	recipientKey := [32]byte{0x02}
	thirdParty := [32]byte{0x03}

	entry := &physical.Entry{
		Visibility: 1, // PRIVATE
		Labels: map[string]string{
			"from": fmt.Sprintf("%x", senderKey),
			"to":   fmt.Sprintf("%x", recipientKey),
		},
	}

	if vc.Check(entry, thirdParty) {
		t.Error("third party should NOT see private entry")
	}
}

func TestVisibilityGroupMember(t *testing.T) {
	gc := newGroupCache()
	vc := newVisibilityChecker(gc)

	groupKP, _ := identity.Generate()
	member, _ := identity.Generate()

	gc.LoadManifest(&group.Manifest{
		ID: groupKP.PublicKey(),
		Members: []group.Member{
			{PublicKey: member.PublicKey(), Role: group.RoleMember},
		},
	})

	entry := &physical.Entry{
		Visibility: 2, // VISIBILITY_LABEL_SCOPED
		Labels: map[string]string{
			"scope": fmt.Sprintf("%x", groupKP.PublicKey()),
		},
	}

	if !vc.Check(entry, member.PublicKey()) {
		t.Error("group member should see scoped entry")
	}
}

func TestVisibilityGroupReject(t *testing.T) {
	gc := newGroupCache()
	vc := newVisibilityChecker(gc)

	groupKP, _ := identity.Generate()
	member, _ := identity.Generate()
	stranger, _ := identity.Generate()

	gc.LoadManifest(&group.Manifest{
		ID: groupKP.PublicKey(),
		Members: []group.Member{
			{PublicKey: member.PublicKey(), Role: group.RoleMember},
		},
	})

	entry := &physical.Entry{
		Visibility: 2, // VISIBILITY_LABEL_SCOPED
		Labels: map[string]string{
			"scope": fmt.Sprintf("%x", groupKP.PublicKey()),
		},
	}

	if vc.Check(entry, stranger.PublicKey()) {
		t.Error("non-member should NOT see scoped entry")
	}
}

func TestVisibilityLabelScopedNoScope(t *testing.T) {
	vc := newTestVisibilityChecker()
	entry := &physical.Entry{
		Visibility: 2, // VISIBILITY_LABEL_SCOPED
		Labels:     map[string]string{},
	}
	if !vc.Check(entry, [32]byte{0x01}) {
		t.Error("label-scoped entry with no scope label should be visible")
	}
}
