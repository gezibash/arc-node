package group

import (
	"errors"
	"testing"

	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
)

func TestRotateKeyNoAdmins(t *testing.T) {
	admin, _ := identity.Generate()
	_, m, _ := Create("test", admin)
	_, _, err := RotateKey(m)
	if err == nil {
		t.Error("expected error rotating with no admins")
	}
}

func TestRecoverGroupKeyBadSeed(t *testing.T) {
	admin, _ := identity.Generate()
	_, err := RecoverGroupKey([]byte("short"), admin)
	if err == nil {
		t.Error("expected error for short sealed data")
	}
}

func TestAddMemberPreservesExisting(t *testing.T) {
	admin, _ := identity.Generate()
	groupKP, m, _ := Create("test", admin)

	m1, _ := identity.Generate()
	m2, _ := identity.Generate()

	m2m, _ := AddMember(m, groupKP, m1.PublicKey(), RoleMember)
	m3m, _ := AddMember(m2m, groupKP, m2.PublicKey(), RoleAdmin)

	if len(m3m.Members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(m3m.Members))
	}
}

func TestRemoveMemberAdmin(t *testing.T) {
	a1, _ := identity.Generate()
	a2, _ := identity.Generate()
	groupKP, m, _ := Create("test", a1, a2)

	m2, err := RemoveMember(m, groupKP, a2.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	if len(m2.Members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(m2.Members))
	}
}

func TestVerifyManifestWrongContentType(t *testing.T) {
	groupKP, _ := identity.Generate()
	msg := message.New(groupKP.PublicKey(), identity.PublicKey{}, reference.Reference{}, "wrong/type")
	_ = message.Sign(&msg, groupKP)
	_, err := VerifyManifest(msg, []byte("data"))
	if err == nil {
		t.Error("expected error for wrong content type")
	}
}

func TestVerifyManifestBadSignature(t *testing.T) {
	groupKP, _ := identity.Generate()
	admin, _ := identity.Generate()

	m := &Manifest{
		ID:   groupKP.PublicKey(),
		Name: "test",
		Members: []Member{
			{PublicKey: admin.PublicKey(), Role: RoleAdmin},
		},
	}

	data, _ := MarshalManifest(m)
	contentRef := reference.Compute(data)
	msg := message.New(groupKP.PublicKey(), identity.PublicKey{}, contentRef, contentTypeManifest)
	// Don't sign - signature is zero bytes, should fail verification
	_, err := VerifyManifest(msg, data)
	if err == nil {
		t.Error("expected error for bad signature")
	}
}

func TestUnmarshalManifestVersionOnly(t *testing.T) {
	// Version byte + 31 bytes (not enough for 32 byte group ID)
	data := make([]byte, 32)
	data[0] = 1
	_, err := UnmarshalManifest(data)
	if err == nil {
		t.Error("expected error for truncated group ID")
	}
}

func TestUnmarshalManifestTruncatedName(t *testing.T) {
	// Version + group ID (32) + name length (2 bytes saying 100) but no name data
	data := make([]byte, 35)
	data[0] = 1
	data[33] = 0
	data[34] = 100 // name length = 100 but no data follows
	_, err := UnmarshalManifest(data)
	if err == nil {
		t.Error("expected error for truncated name")
	}
}

func TestUnmarshalManifestTruncatedParent(t *testing.T) {
	// Version(1) + ID(32) + nameLen(2) + name(0) + parentFlag(1=has parent) + only 10 bytes
	data := make([]byte, 46)
	data[0] = 1
	// name len = 0
	data[33] = 0
	data[34] = 0
	// parent flag = 1
	data[35] = 1
	// Only 10 bytes of parent ref instead of 32
	_, err := UnmarshalManifest(data)
	if err == nil {
		t.Error("expected error for truncated parent ref")
	}
}

func TestSealSeedEdToX25519(t *testing.T) {
	// Test that SealSeed works with multiple recipients
	kp1, _ := identity.Generate()
	kp2, _ := identity.Generate()

	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i + 10)
	}

	s1, err := SealSeed(seed, kp1.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	s2, err := SealSeed(seed, kp2.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	// Each sealed output should be different (different ephemeral keys)
	if string(s1) == string(s2) {
		t.Error("sealed outputs should differ for different recipients")
	}

	// But both should decrypt to same seed
	got1, _ := OpenSeed(s1, kp1)
	got2, _ := OpenSeed(s2, kp2)
	if string(got1) != string(got2) {
		t.Error("decrypted seeds should match")
	}
}

func TestCreateMultipleAdmins(t *testing.T) {
	a1, _ := identity.Generate()
	a2, _ := identity.Generate()
	a3, _ := identity.Generate()

	groupKP, m, err := Create("multi", a1, a2, a3)
	if err != nil {
		t.Fatal(err)
	}

	// All admins should be able to recover the group key
	for i, admin := range []*identity.Keypair{a1, a2, a3} {
		var sealed []byte
		for _, mem := range m.Members {
			if mem.PublicKey == admin.PublicKey() {
				sealed = mem.SealedSeed
			}
		}
		recovered, err := RecoverGroupKey(sealed, admin)
		if err != nil {
			t.Fatalf("admin %d: %v", i, err)
		}
		if recovered.PublicKey() != groupKP.PublicKey() {
			t.Fatalf("admin %d: recovered key mismatch", i)
		}
	}
}

func TestOpenSeedCorruptedData(t *testing.T) {
	kp, _ := identity.Generate()
	seed := make([]byte, 32)
	sealed, _ := SealSeed(seed, kp.PublicKey())

	// Corrupt the ciphertext portion
	sealed[len(sealed)-1] ^= 0xFF
	_, err := OpenSeed(sealed, kp)
	if !errors.Is(err, ErrDecryptFailed) {
		t.Errorf("expected ErrDecryptFailed, got %v", err)
	}
}

func TestMarshalUnmarshalWithParent(t *testing.T) {
	groupKP, _ := identity.Generate()
	admin, _ := identity.Generate()
	parentRef := reference.Compute([]byte("parent"))

	m := &Manifest{
		ID:   groupKP.PublicKey(),
		Name: "with-parent",
		Members: []Member{
			{PublicKey: admin.PublicKey(), Role: RoleAdmin, SealedSeed: []byte("sealed")},
		},
		Policy: Policy{AdminThreshold: 2},
		Parent: &parentRef,
	}

	data, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}

	got, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}

	if got.Parent == nil {
		t.Fatal("expected parent ref")
	}
	if *got.Parent != parentRef {
		t.Error("parent ref mismatch")
	}
	if got.Policy.AdminThreshold != 2 {
		t.Error("policy mismatch")
	}
}

func TestUnmarshalManifestTruncatedPolicy(t *testing.T) {
	groupKP, _ := identity.Generate()
	// Version(1) + ID(32) + nameLen(2,0) + parentFlag(0) = 36 bytes, missing policy
	data := make([]byte, 36)
	data[0] = 1
	gpub := groupKP.PublicKey()
	copy(data[1:33], gpub[:])
	// nameLen = 0
	data[33] = 0
	data[34] = 0
	// parentFlag = 0
	data[35] = 0
	// Missing policy bytes
	_, err := UnmarshalManifest(data)
	if err == nil {
		t.Error("expected error for truncated policy")
	}
}

func TestUnmarshalManifestTruncatedMemberCount(t *testing.T) {
	groupKP, _ := identity.Generate()
	// Version(1) + ID(32) + nameLen(2,0) + parentFlag(0) + policy(4) = 40 bytes, missing member count
	data := make([]byte, 40)
	data[0] = 1
	gpub := groupKP.PublicKey()
	copy(data[1:33], gpub[:])
	data[33] = 0
	data[34] = 0
	data[35] = 0
	// policy = 1 (big endian int32)
	data[36] = 0
	data[37] = 0
	data[38] = 0
	data[39] = 1
	// Missing member count
	_, err := UnmarshalManifest(data)
	if err == nil {
		t.Error("expected error for truncated member count")
	}
}

func TestUnmarshalManifestTruncatedMemberKey(t *testing.T) {
	groupKP, _ := identity.Generate()
	// Full header + member count = 1, but no member key data
	data := make([]byte, 44)
	data[0] = 1
	gpub := groupKP.PublicKey()
	copy(data[1:33], gpub[:])
	data[33] = 0
	data[34] = 0
	data[35] = 0
	data[36] = 0
	data[37] = 0
	data[38] = 0
	data[39] = 1
	// member count = 1
	data[40] = 0
	data[41] = 0
	data[42] = 0
	data[43] = 1
	_, err := UnmarshalManifest(data)
	if err == nil {
		t.Error("expected error for truncated member key")
	}
}

func TestUnmarshalManifestTruncatedMemberRole(t *testing.T) {
	groupKP, _ := identity.Generate()
	admin, _ := identity.Generate()
	// Full header + member count=1 + pubkey(32) but no role byte
	data := make([]byte, 76)
	data[0] = 1
	gpub := groupKP.PublicKey()
	copy(data[1:33], gpub[:])
	data[33] = 0
	data[34] = 0
	data[35] = 0
	data[36] = 0
	data[37] = 0
	data[38] = 0
	data[39] = 1
	data[40] = 0
	data[41] = 0
	data[42] = 0
	data[43] = 1
	adminPub := admin.PublicKey()
	copy(data[44:76], adminPub[:])
	// Missing role byte
	_, err := UnmarshalManifest(data)
	if err == nil {
		t.Error("expected error for truncated member role")
	}
}

func TestUnmarshalManifestTruncatedSealedLen(t *testing.T) {
	groupKP, _ := identity.Generate()
	admin, _ := identity.Generate()
	// Full header + member count=1 + pubkey(32) + role(1) but no sealed len
	data := make([]byte, 77)
	data[0] = 1
	gpub := groupKP.PublicKey()
	copy(data[1:33], gpub[:])
	data[33] = 0
	data[34] = 0
	data[35] = 0
	data[36] = 0
	data[37] = 0
	data[38] = 0
	data[39] = 1
	data[40] = 0
	data[41] = 0
	data[42] = 0
	data[43] = 1
	adminPub := admin.PublicKey()
	copy(data[44:76], adminPub[:])
	data[76] = 1 // role = admin
	// Missing sealed length bytes
	_, err := UnmarshalManifest(data)
	if err == nil {
		t.Error("expected error for truncated sealed len")
	}
}

func TestUnmarshalManifestTruncatedSealedSeed(t *testing.T) {
	groupKP, _ := identity.Generate()
	admin, _ := identity.Generate()
	// Full header + member count=1 + pubkey(32) + role(1) + sealedLen(4, value=100) but no seed data
	data := make([]byte, 81)
	data[0] = 1
	gpub := groupKP.PublicKey()
	copy(data[1:33], gpub[:])
	data[33] = 0
	data[34] = 0
	data[35] = 0
	data[36] = 0
	data[37] = 0
	data[38] = 0
	data[39] = 1
	data[40] = 0
	data[41] = 0
	data[42] = 0
	data[43] = 1
	adminPub := admin.PublicKey()
	copy(data[44:76], adminPub[:])
	data[76] = 1 // role
	data[77] = 0
	data[78] = 0
	data[79] = 0
	data[80] = 100 // sealed len = 100
	_, err := UnmarshalManifest(data)
	if err == nil {
		t.Error("expected error for truncated sealed seed")
	}
}
