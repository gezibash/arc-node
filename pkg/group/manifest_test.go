package group

import (
	"testing"

	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/reference"
)

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	kp1, _ := identity.Generate()
	kp2, _ := identity.Generate()
	groupKP, _ := identity.Generate()

	m := &Manifest{
		ID:   groupKP.PublicKey(),
		Name: "test-group",
		Members: []Member{
			{PublicKey: kp1.PublicKey(), Role: RoleAdmin, SealedSeed: []byte("sealed1")},
			{PublicKey: kp2.PublicKey(), Role: RoleMember},
		},
		Policy: Policy{AdminThreshold: 1},
	}

	data, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}

	got, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}

	if got.ID != m.ID {
		t.Error("ID mismatch")
	}
	if got.Name != m.Name {
		t.Error("name mismatch")
	}
	if len(got.Members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(got.Members))
	}
	if got.Policy.AdminThreshold != 1 {
		t.Error("policy mismatch")
	}
}

func TestMarshalDeterministic(t *testing.T) {
	kp1, _ := identity.Generate()
	kp2, _ := identity.Generate()
	groupKP, _ := identity.Generate()

	m1 := &Manifest{
		ID:   groupKP.PublicKey(),
		Name: "test",
		Members: []Member{
			{PublicKey: kp1.PublicKey(), Role: RoleMember},
			{PublicKey: kp2.PublicKey(), Role: RoleAdmin},
		},
	}
	// Reverse member order.
	m2 := &Manifest{
		ID:   groupKP.PublicKey(),
		Name: "test",
		Members: []Member{
			{PublicKey: kp2.PublicKey(), Role: RoleAdmin},
			{PublicKey: kp1.PublicKey(), Role: RoleMember},
		},
	}

	d1, _ := MarshalManifest(m1)
	d2, _ := MarshalManifest(m2)

	r1 := reference.Compute(d1)
	r2 := reference.Compute(d2)
	if r1 != r2 {
		t.Error("manifests with same members in different order should produce same hash")
	}
}

func TestSignVerifyManifest(t *testing.T) {
	groupKP, _ := identity.Generate()
	admin, _ := identity.Generate()

	m := &Manifest{
		ID:   groupKP.PublicKey(),
		Name: "signed-group",
		Members: []Member{
			{PublicKey: admin.PublicKey(), Role: RoleAdmin},
		},
		Policy: Policy{AdminThreshold: 1},
	}

	msg, data, err := SignManifest(m, groupKP, nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := VerifyManifest(msg, data)
	if err != nil {
		t.Fatal(err)
	}

	if got.Name != "signed-group" {
		t.Error("unexpected name after verify")
	}
}

func TestManifestChaining(t *testing.T) {
	groupKP, _ := identity.Generate()
	admin, _ := identity.Generate()

	m1 := &Manifest{
		ID:   groupKP.PublicKey(),
		Name: "chained",
		Members: []Member{
			{PublicKey: admin.PublicKey(), Role: RoleAdmin},
		},
	}

	msg1, data1, err := SignManifest(m1, groupKP, nil)
	if err != nil {
		t.Fatal(err)
	}
	if msg1.Parent != nil {
		t.Error("first manifest should have no parent")
	}

	parentRef := reference.Compute(data1)

	m2 := &Manifest{
		ID:   groupKP.PublicKey(),
		Name: "chained",
		Members: []Member{
			{PublicKey: admin.PublicKey(), Role: RoleAdmin},
		},
	}

	msg2, data2, err := SignManifest(m2, groupKP, &parentRef)
	if err != nil {
		t.Fatal(err)
	}

	got, err := VerifyManifest(msg2, data2)
	if err != nil {
		t.Fatal(err)
	}
	if got.Parent == nil {
		t.Fatal("second manifest should have parent")
	}
	if *got.Parent != parentRef {
		t.Error("parent reference mismatch")
	}
}

func TestVerifyManifestWrongSigner(t *testing.T) {
	groupKP, _ := identity.Generate()
	otherKP, _ := identity.Generate()

	m := &Manifest{
		ID:   groupKP.PublicKey(),
		Name: "test",
		Members: []Member{
			{PublicKey: groupKP.PublicKey(), Role: RoleAdmin},
		},
	}

	// Sign with wrong key.
	msg, data, err := SignManifest(m, otherKP, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = VerifyManifest(msg, data)
	if err == nil {
		t.Error("expected error: signer does not match manifest ID")
	}
}

func TestUnmarshalInvalid(t *testing.T) {
	_, err := UnmarshalManifest(nil)
	if err == nil {
		t.Error("expected error for nil data")
	}

	_, err = UnmarshalManifest([]byte{2}) // bad version
	if err == nil {
		t.Error("expected error for bad version")
	}
}
