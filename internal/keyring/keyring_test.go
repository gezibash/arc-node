package keyring

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc-node/pkg/group"
)

func newTestKeyring(t *testing.T) *Keyring {
	t.Helper()
	return New(t.TempDir())
}

func generateKey(t *testing.T, kr *Keyring, alias string) *Key {
	t.Helper()
	key, err := kr.Generate(context.Background(), alias)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return key
}

func TestGenerateCreatesKeyFiles(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "")

	keyPath := filepath.Join(kr.dir, "keys", key.PublicKey+".key")
	metaPath := filepath.Join(kr.dir, "keys", key.PublicKey+".json")

	if _, err := os.Stat(keyPath); err != nil {
		t.Fatalf("key file not found: %v", err)
	}
	if _, err := os.Stat(metaPath); err != nil {
		t.Fatalf("metadata file not found: %v", err)
	}
}

func TestGenerateWithAlias(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "myalias")

	loaded, err := kr.Load(context.Background(), "myalias")
	if err != nil {
		t.Fatalf("load by alias: %v", err)
	}
	if loaded.PublicKey != key.PublicKey {
		t.Fatalf("public key mismatch: got %s, want %s", loaded.PublicKey, key.PublicKey)
	}
}

func TestImportFromSeed(t *testing.T) {
	// Generate a keypair to get a valid seed.
	orig, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	kr := newTestKeyring(t)
	key, err := kr.Import(context.Background(), orig.Seed(), "")
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	loaded, err := kr.Load(context.Background(), key.PublicKey)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.PublicKey != key.PublicKey {
		t.Fatalf("round-trip mismatch: got %s, want %s", loaded.PublicKey, key.PublicKey)
	}
}

func TestLoadByPublicKeyHex(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "")

	loaded, err := kr.Load(context.Background(), key.PublicKey)
	if err != nil {
		t.Fatalf("load by hex: %v", err)
	}
	if loaded.PublicKey != key.PublicKey {
		t.Fatal("public key mismatch")
	}
}

func TestLoadByAlias(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "testalias")

	loaded, err := kr.Load(context.Background(), "testalias")
	if err != nil {
		t.Fatalf("load by alias: %v", err)
	}
	if loaded.PublicKey != key.PublicKey {
		t.Fatal("public key mismatch")
	}
}

func TestLoadNotFound(t *testing.T) {
	kr := newTestKeyring(t)

	_, err := kr.Load(context.Background(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrAliasNotFound) {
		t.Fatalf("expected ErrNotFound or ErrAliasNotFound, got %v", err)
	}
}

func TestLoadDefaultNoDefault(t *testing.T) {
	kr := newTestKeyring(t)

	_, err := kr.LoadDefault(context.Background())
	// When no keyring file exists yet, LoadDefault returns ErrNotFound
	// (from loadKeyringFile). When the file exists but Default is empty,
	// it returns ErrNoDefault. Both are acceptable "no default" states.
	if !errors.Is(err, ErrNoDefault) && !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNoDefault or ErrNotFound, got %v", err)
	}
}

func TestSetDefaultAndLoadDefault(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "primary")

	if err := kr.SetDefault("primary"); err != nil {
		t.Fatalf("set default: %v", err)
	}

	loaded, err := kr.LoadDefault(context.Background())
	if err != nil {
		t.Fatalf("load default: %v", err)
	}
	if loaded.PublicKey != key.PublicKey {
		t.Fatal("default key mismatch")
	}
}

func TestSetAliasForMissingKey(t *testing.T) {
	kr := newTestKeyring(t)

	err := kr.SetAlias("ghost", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestSetDefaultUnknownAlias(t *testing.T) {
	kr := newTestKeyring(t)

	err := kr.SetDefault("nonexistent")
	if !errors.Is(err, ErrAliasNotFound) {
		t.Fatalf("expected ErrAliasNotFound, got %v", err)
	}
}

func TestDeleteRemovesKeyAndAliases(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "todelete")

	if err := kr.Delete(context.Background(), "todelete"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err := kr.Load(context.Background(), key.PublicKey)
	if !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrAliasNotFound) {
		t.Fatalf("expected key to be gone, got %v", err)
	}

	// Alias should also be removed.
	_, err = kr.Load(context.Background(), "todelete")
	if err == nil {
		t.Fatal("expected error loading deleted alias")
	}
}

func TestDeleteClearsDefault(t *testing.T) {
	kr := newTestKeyring(t)
	generateKey(t, kr, "def")

	if err := kr.SetDefault("def"); err != nil {
		t.Fatalf("set default: %v", err)
	}

	if err := kr.Delete(context.Background(), "def"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err := kr.LoadDefault(context.Background())
	// After deleting the default key, the default field is cleared.
	// LoadDefault may return ErrNoDefault or ErrAliasNotFound depending
	// on internal state. Either way, loading should fail.
	if err == nil {
		t.Fatal("expected error after deleting default key")
	}
}

func TestListEmpty(t *testing.T) {
	kr := newTestKeyring(t)

	infos, err := kr.List(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(infos) != 0 {
		t.Fatalf("expected 0 keys, got %d", len(infos))
	}
}

func TestListMultiple(t *testing.T) {
	kr := newTestKeyring(t)

	for i := 0; i < 3; i++ {
		generateKey(t, kr, "")
	}

	infos, err := kr.List(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(infos) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(infos))
	}
}

func TestLoadOrGenerateExisting(t *testing.T) {
	kr := newTestKeyring(t)
	orig := generateKey(t, kr, "existing")

	key, err := kr.LoadOrGenerate(context.Background(), "existing")
	if err != nil {
		t.Fatalf("load or generate: %v", err)
	}
	if key.PublicKey != orig.PublicKey {
		t.Fatal("expected existing key to be returned")
	}
}

func TestLoadOrGenerateNew(t *testing.T) {
	kr := newTestKeyring(t)

	key, err := kr.LoadOrGenerate(context.Background(), "fresh")
	if err != nil {
		t.Fatalf("load or generate: %v", err)
	}
	if key.PublicKey == "" {
		t.Fatal("expected a new key to be generated")
	}

	// Should be loadable by alias now.
	loaded, err := kr.Load(context.Background(), "fresh")
	if err != nil {
		t.Fatalf("load after generate: %v", err)
	}
	if loaded.PublicKey != key.PublicKey {
		t.Fatal("mismatch")
	}
}

func TestImportGroupKey(t *testing.T) {
	kr := newTestKeyring(t)

	adminKP, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	// Create a group to get a sealed seed.
	groupKP, _, err := group.Create("testgroup", adminKP)
	if err != nil {
		t.Fatalf("create group: %v", err)
	}

	sealedSeed, err := group.SealSeed(groupKP.Seed(), adminKP.PublicKey())
	if err != nil {
		t.Fatalf("seal seed: %v", err)
	}

	key, err := kr.ImportGroupKey(context.Background(), sealedSeed, adminKP, "mygroup")
	if err != nil {
		t.Fatalf("import group key: %v", err)
	}

	if key.Metadata.Type != "group" {
		t.Fatalf("expected type 'group', got %q", key.Metadata.Type)
	}
	if key.PublicKey != pubKeyHex(groupKP) {
		t.Fatal("group public key mismatch")
	}
}

func TestGenerateSaveKeyFailure(t *testing.T) {
	// Point keys dir at a file to make MkdirAll fail.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "keys"), []byte("blocker"), 0o600); err != nil {
		t.Fatal(err)
	}
	kr := New(dir)

	_, err := kr.Generate(context.Background(), "")
	if err == nil {
		t.Fatal("expected error when keys dir is a file")
	}
}

func TestImportInvalidSeed(t *testing.T) {
	kr := newTestKeyring(t)

	_, err := kr.Import(context.Background(), []byte("short"), "")
	if err == nil {
		t.Fatal("expected error for invalid seed length")
	}
}

func TestImportWithAlias(t *testing.T) {
	orig, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	kr := newTestKeyring(t)
	key, err := kr.Import(context.Background(), orig.Seed(), "imported")
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	loaded, err := kr.Load(context.Background(), "imported")
	if err != nil {
		t.Fatalf("load by alias: %v", err)
	}
	if loaded.PublicKey != key.PublicKey {
		t.Fatal("public key mismatch")
	}
}

func TestImportSaveKeyFailure(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "keys"), []byte("blocker"), 0o600); err != nil {
		t.Fatal(err)
	}
	kr := New(dir)

	orig, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	_, err = kr.Import(context.Background(), orig.Seed(), "")
	if err == nil {
		t.Fatal("expected error when keys dir is a file")
	}
}

func TestLoadMissingKeyFile(t *testing.T) {
	kr := newTestKeyring(t)

	// Load with a valid hex that doesn't exist.
	_, err := kr.Load(context.Background(), "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrAliasNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestLoadDefaultWithEmptyDefault(t *testing.T) {
	kr := newTestKeyring(t)
	// Create a keyring file with empty default.
	generateKey(t, kr, "somekey")
	// The keyring file exists now (alias was set), but Default is "".
	_, err := kr.LoadDefault(context.Background())
	if !errors.Is(err, ErrNoDefault) {
		t.Fatalf("expected ErrNoDefault, got %v", err)
	}
}

func TestLoadOrGenerateNonRetryableError(t *testing.T) {
	// Make the keyring dir unreadable to cause a non-retryable error.
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		t.Fatal(err)
	}
	// Create a corrupt keyring.json to cause a parse error.
	if err := os.WriteFile(filepath.Join(dir, "keyring.json"), []byte("{corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	kr := New(dir)

	_, err := kr.LoadOrGenerate(context.Background(), "anything")
	if err == nil {
		t.Fatal("expected error from corrupt keyring file")
	}
}

func TestListWithMalformedKeyFile(t *testing.T) {
	kr := newTestKeyring(t)

	// Generate a good key.
	generateKey(t, kr, "")

	// Write a malformed .key file.
	keysDir := filepath.Join(kr.dir, "keys")
	badKeyPath := filepath.Join(keysDir, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc.key")
	if err := os.WriteFile(badKeyPath, []byte("bad seed"), 0o600); err != nil {
		t.Fatal(err)
	}

	infos, err := kr.List(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	// The malformed key should be skipped; only the good key remains.
	if len(infos) != 1 {
		t.Fatalf("expected 1 key, got %d", len(infos))
	}
}

func TestListWithDefaultMarked(t *testing.T) {
	kr := newTestKeyring(t)
	generateKey(t, kr, "first")
	if err := kr.SetDefault("first"); err != nil {
		t.Fatalf("set default: %v", err)
	}

	infos, err := kr.List(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	found := false
	for _, info := range infos {
		if info.IsDefault {
			found = true
		}
	}
	if !found {
		t.Fatal("expected one key to be marked as default")
	}
}

func TestDeleteByPublicKeyHex(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "")

	if err := kr.Delete(context.Background(), key.PublicKey); err != nil {
		t.Fatalf("delete by hex: %v", err)
	}

	_, err := kr.Load(context.Background(), key.PublicKey)
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	kr := newTestKeyring(t)
	err := kr.Delete(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error deleting nonexistent key")
	}
}

func TestDeleteAliasedKeyRemovesAlias(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "aliased")

	// Add a second alias.
	if err := kr.SetAlias("second", key.PublicKey); err != nil {
		t.Fatalf("set alias: %v", err)
	}

	if err := kr.Delete(context.Background(), key.PublicKey); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Both aliases should be gone.
	_, err := kr.Load(context.Background(), "aliased")
	if err == nil {
		t.Fatal("expected error loading deleted alias 'aliased'")
	}
	_, err = kr.Load(context.Background(), "second")
	if err == nil {
		t.Fatal("expected error loading deleted alias 'second'")
	}
}

func TestSetAliasCreatesKeyringFile(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "")

	// No keyring.json exists yet; SetAlias should create it.
	if err := kr.SetAlias("newalias", key.PublicKey); err != nil {
		t.Fatalf("set alias: %v", err)
	}

	loaded, err := kr.Load(context.Background(), "newalias")
	if err != nil {
		t.Fatalf("load by new alias: %v", err)
	}
	if loaded.PublicKey != key.PublicKey {
		t.Fatal("public key mismatch")
	}
}

func TestSetDefaultCreatesKeyringFile(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "first")

	if err := kr.SetDefault("first"); err != nil {
		t.Fatalf("set default: %v", err)
	}

	loaded, err := kr.LoadDefault(context.Background())
	if err != nil {
		t.Fatalf("load default: %v", err)
	}
	if loaded.PublicKey != key.PublicKey {
		t.Fatal("mismatch")
	}
}

func TestResolveToPublicKeyHexDirectly(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "")

	// Resolve by hex (no alias set, no keyring.json).
	loaded, err := kr.Load(context.Background(), key.PublicKey)
	if err != nil {
		t.Fatalf("load by hex: %v", err)
	}
	if loaded.PublicKey != key.PublicKey {
		t.Fatal("mismatch")
	}
}

func TestResolveAliasNotFound(t *testing.T) {
	kr := newTestKeyring(t)
	generateKey(t, kr, "exists")

	_, err := kr.Load(context.Background(), "doesnotexist")
	if !errors.Is(err, ErrAliasNotFound) {
		t.Fatalf("expected ErrAliasNotFound, got %v", err)
	}
}

func TestResolveAliasPointsToDeletedKey(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "stale")

	// Delete the key files directly (leaving the alias in keyring.json).
	if err := os.Remove(kr.keyPath(key.PublicKey)); err != nil {
		t.Fatal(err)
	}

	_, err := kr.Load(context.Background(), "stale")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for stale alias, got %v", err)
	}
}

func TestIsHex(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"abcdef0123456789", true},
		{"ABCDEF", true},
		{"", true},
		{"xyz", false},
		{"0g", false},
		{"abcde", true}, // odd length but valid hex chars
	}

	for _, tt := range tests {
		got := isHex(tt.input)
		if got != tt.want {
			t.Errorf("isHex(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestNormalize(t *testing.T) {
	if got := normalize("  ABCDEF  "); got != "abcdef" {
		t.Fatalf("normalize: got %q, want %q", got, "abcdef")
	}
}

func TestImportGroupKeyInvalidSealedSeed(t *testing.T) {
	kr := newTestKeyring(t)

	adminKP, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	_, err = kr.ImportGroupKey(context.Background(), []byte("garbage"), adminKP, "")
	if err == nil {
		t.Fatal("expected error for invalid sealed seed")
	}
}

func TestImportGroupKeyWithSaveFailure(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "keys"), []byte("blocker"), 0o600); err != nil {
		t.Fatal(err)
	}
	kr := New(dir)

	adminKP, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	groupKP, _, err := group.Create("testgroup", adminKP)
	if err != nil {
		t.Fatalf("create group: %v", err)
	}

	sealedSeed, err := group.SealSeed(groupKP.Seed(), adminKP.PublicKey())
	if err != nil {
		t.Fatalf("seal seed: %v", err)
	}

	_, err = kr.ImportGroupKey(context.Background(), sealedSeed, adminKP, "")
	if err == nil {
		t.Fatal("expected error when keys dir is a file")
	}
}

func TestListGroupsEmpty(t *testing.T) {
	kr := newTestKeyring(t)

	groups, err := kr.ListGroups(context.Background())
	if err != nil {
		t.Fatalf("list groups: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("expected 0 groups, got %d", len(groups))
	}
}

func TestListGroupsOnlyPersonalKeys(t *testing.T) {
	kr := newTestKeyring(t)
	generateKey(t, kr, "personal1")
	generateKey(t, kr, "personal2")

	groups, err := kr.ListGroups(context.Background())
	if err != nil {
		t.Fatalf("list groups: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("expected 0 groups, got %d", len(groups))
	}
}

func TestDeleteKeyThenListClean(t *testing.T) {
	kr := newTestKeyring(t)
	key1 := generateKey(t, kr, "one")
	generateKey(t, kr, "two")

	if err := kr.Delete(context.Background(), key1.PublicKey); err != nil {
		t.Fatalf("delete: %v", err)
	}

	infos, err := kr.List(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(infos) != 1 {
		t.Fatalf("expected 1 key, got %d", len(infos))
	}
}

func TestLoadKeyMissingMetadata(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "")

	// Remove the metadata file but keep the key file.
	os.Remove(kr.metaPath(key.PublicKey))

	loaded, err := kr.Load(context.Background(), key.PublicKey)
	if err != nil {
		t.Fatalf("load without metadata: %v", err)
	}
	if loaded.PublicKey != key.PublicKey {
		t.Fatal("public key mismatch")
	}
}

func TestLoadKeyCorruptMetadata(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "")

	// Corrupt the metadata file.
	if err := os.WriteFile(kr.metaPath(key.PublicKey), []byte("{corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := kr.Load(context.Background(), key.PublicKey)
	if err == nil {
		t.Fatal("expected error for corrupt metadata")
	}
}

func TestListWithCorruptKeyringFile(t *testing.T) {
	kr := newTestKeyring(t)
	generateKey(t, kr, "")

	// Write corrupt keyring.json.
	if err := os.WriteFile(kr.keyringFilePath(), []byte("{corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := kr.List(context.Background())
	if err == nil {
		t.Fatal("expected error from corrupt keyring file")
	}
}

func TestSetAliasWithCorruptKeyringFile(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "")

	// Write corrupt keyring.json.
	if err := os.WriteFile(kr.keyringFilePath(), []byte("{corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := kr.SetAlias("myalias", key.PublicKey)
	if err == nil {
		t.Fatal("expected error from corrupt keyring file")
	}
}

func TestSetDefaultWithCorruptKeyringFile(t *testing.T) {
	kr := newTestKeyring(t)

	// Write corrupt keyring.json.
	if err := os.WriteFile(kr.keyringFilePath(), []byte("{corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := kr.SetDefault("anything")
	if err == nil {
		t.Fatal("expected error from corrupt keyring file")
	}
}

func TestDeleteWithCorruptKeyringFile(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "")

	// Write corrupt keyring.json.
	if err := os.WriteFile(kr.keyringFilePath(), []byte("{corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := kr.Delete(context.Background(), key.PublicKey)
	if err == nil {
		t.Fatal("expected error from corrupt keyring file")
	}
}

func TestResolveAliasToPublicKeyHexFallback(t *testing.T) {
	// When the alias is not in the alias map but is a valid 64-char hex
	// that corresponds to an existing key, resolveAliasToPublicKey should
	// resolve it via the hex fallback path.
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "somealias")

	// Load by hex through alias resolution (keyring.json exists but hex
	// is not in aliases map). This exercises resolveAliasToPublicKey's
	// hex fallback.
	loaded, err := kr.Load(context.Background(), key.PublicKey)
	if err != nil {
		t.Fatalf("load by hex fallback: %v", err)
	}
	if loaded.PublicKey != key.PublicKey {
		t.Fatal("mismatch")
	}
}

func TestListGroupsWithCorruptKeyringFile(t *testing.T) {
	kr := newTestKeyring(t)

	// Write corrupt keyring.json.
	if err := os.WriteFile(kr.keyringFilePath(), []byte("{corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := kr.ListGroups(context.Background())
	if err == nil {
		t.Fatal("expected error from corrupt keyring file")
	}
}

func TestLoadDefaultWithCorruptKeyringFile(t *testing.T) {
	kr := newTestKeyring(t)

	if err := os.WriteFile(kr.keyringFilePath(), []byte("{corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := kr.LoadDefault(context.Background())
	if err == nil {
		t.Fatal("expected error from corrupt keyring file")
	}
}

func TestResolveToPublicKeyWithCorruptKeyringFile(t *testing.T) {
	kr := newTestKeyring(t)

	// Write corrupt keyring.json so loadKeyringFile returns a non-ErrNotFound error.
	if err := os.WriteFile(kr.keyringFilePath(), []byte("{corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := kr.Load(context.Background(), "somealias")
	if err == nil {
		t.Fatal("expected error from corrupt keyring file")
	}
}

func TestDeleteKeyWithDefaultByHex(t *testing.T) {
	// Delete a key that is the default, resolving by hex rather than alias.
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "mykey")

	if err := kr.SetDefault("mykey"); err != nil {
		t.Fatalf("set default: %v", err)
	}

	// Delete using the raw public key hex.
	if err := kr.Delete(context.Background(), key.PublicKey); err != nil {
		t.Fatalf("delete by hex: %v", err)
	}

	// Default should be cleared.
	_, err := kr.LoadDefault(context.Background())
	if err == nil {
		t.Fatal("expected error after deleting default key")
	}
}

func TestImportGroupKeyNoAlias(t *testing.T) {
	kr := newTestKeyring(t)

	adminKP, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	groupKP, _, err := group.Create("testgroup", adminKP)
	if err != nil {
		t.Fatalf("create group: %v", err)
	}

	sealedSeed, err := group.SealSeed(groupKP.Seed(), adminKP.PublicKey())
	if err != nil {
		t.Fatalf("seal seed: %v", err)
	}

	key, err := kr.ImportGroupKey(context.Background(), sealedSeed, adminKP, "")
	if err != nil {
		t.Fatalf("import group key: %v", err)
	}
	if key.Metadata.Type != "group" {
		t.Fatalf("expected type 'group', got %q", key.Metadata.Type)
	}
}

func TestListKeyFilesNonExistentKeysDir(t *testing.T) {
	// When the keys directory doesn't exist, listKeyFiles returns nil, nil.
	kr := New(t.TempDir())
	hexes, err := kr.listKeyFiles()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hexes != nil {
		t.Fatalf("expected nil, got %v", hexes)
	}
}

func TestDeleteKeyFilesNotFound(t *testing.T) {
	kr := newTestKeyring(t)
	err := kr.deleteKeyFiles("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestLoadKeyringFileNotFound(t *testing.T) {
	kr := New(t.TempDir())
	_, err := kr.loadKeyringFile()
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestLoadKeyringFileNilAliases(t *testing.T) {
	// A keyring.json with no aliases field should get an initialized map.
	kr := New(t.TempDir())
	if err := os.MkdirAll(kr.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(kr.keyringFilePath(), []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}

	kf, err := kr.loadKeyringFile()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if kf.Aliases == nil {
		t.Fatal("expected aliases to be initialized")
	}
}

func TestSaveKeyWriteKeyFileFailure(t *testing.T) {
	kr := newTestKeyring(t)

	// Create keys dir as read-only so WriteFile fails.
	keysDir := kr.keysDir()
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(keysDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(keysDir, 0o700) })

	_, err := kr.Generate(context.Background(), "")
	if err == nil {
		t.Fatal("expected error writing to read-only keys dir")
	}
}

func TestSaveKeyWriteMetaFileFailure(t *testing.T) {
	// We need MkdirAll and WriteFile for key to succeed, but WriteFile for
	// meta to fail. We can achieve this by creating the meta path as a
	// directory before saveKey runs.
	kr := newTestKeyring(t)
	keysDir := kr.keysDir()
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		t.Fatal(err)
	}

	// Generate to get a key, then manually test saveKey with a blocked meta path.
	kp, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	pkHex := pubKeyHex(kp)

	// Create meta path as a directory to block WriteFile.
	if err := os.MkdirAll(kr.metaPath(pkHex), 0o700); err != nil {
		t.Fatal(err)
	}

	meta := &Metadata{PublicKey: pkHex}
	err = kr.saveKey(kp, pkHex, meta)
	if err == nil {
		t.Fatal("expected error writing metadata file")
	}

	// The key file should have been cleaned up.
	if _, statErr := os.Stat(kr.keyPath(pkHex)); statErr == nil {
		t.Fatal("expected key file to be cleaned up after meta write failure")
	}
}

func TestListKeyFilesReadDirError(t *testing.T) {
	kr := newTestKeyring(t)

	// Create keys dir as a file to make ReadDir fail with a non-ErrNotExist error.
	if err := os.MkdirAll(kr.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(kr.keysDir(), []byte("blocker"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := kr.listKeyFiles()
	if err == nil {
		t.Fatal("expected error from ReadDir on a file")
	}
}

func TestListKeyFilesReadDirErrorPropagatesInList(t *testing.T) {
	kr := newTestKeyring(t)

	// Create keys as a file so listKeyFiles returns error.
	if err := os.MkdirAll(kr.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(kr.keysDir(), []byte("blocker"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := kr.List(context.Background())
	if err == nil {
		t.Fatal("expected error from List when listKeyFiles fails")
	}
}

func TestDeleteKeyFilesNonExistError(t *testing.T) {
	kr := newTestKeyring(t)

	// Create a key file inside a read-only directory so Remove returns
	// a permission error (not ErrNotExist).
	keysDir := kr.keysDir()
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		t.Fatal(err)
	}
	fakeHex := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	keyPath := kr.keyPath(fakeHex)
	if err := os.WriteFile(keyPath, []byte("seed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(keysDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(keysDir, 0o700) })

	err := kr.deleteKeyFiles(fakeHex)
	if err == nil {
		t.Fatal("expected error from permission denied")
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatal("expected permission error, not ErrNotFound")
	}
}

func TestSaveKeyringFileMkdirAllFailure(t *testing.T) {
	// Point kr.dir at a file to make MkdirAll fail.
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocked")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	kr := New(blocker) // kr.dir is now a file

	kf := &keyringFile{Version: 1, Aliases: map[string]string{"a": "b"}}
	err := kr.saveKeyringFile(kf)
	if err == nil {
		t.Fatal("expected error from MkdirAll on a file path")
	}
}

func TestSaveKeyringFileWriteFailure(t *testing.T) {
	dir := t.TempDir()
	kr := New(dir)

	// Create keyring.json as a directory to make WriteFile fail.
	if err := os.MkdirAll(kr.keyringFilePath(), 0o700); err != nil {
		t.Fatal(err)
	}

	kf := &keyringFile{Version: 1, Aliases: map[string]string{"a": "b"}}
	err := kr.saveKeyringFile(kf)
	if err == nil {
		t.Fatal("expected error writing keyring file")
	}
}

func TestGenerateSetAliasFailureCleanup(t *testing.T) {
	kr := newTestKeyring(t)

	// Create a corrupt keyring.json so SetAlias will fail during Generate.
	if err := os.MkdirAll(kr.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// Make keyring.json a directory so saveKeyringFile's WriteFile fails.
	if err := os.MkdirAll(kr.keyringFilePath(), 0o700); err != nil {
		t.Fatal(err)
	}

	_, err := kr.Generate(context.Background(), "willbreak")
	if err == nil {
		t.Fatal("expected error when SetAlias fails")
	}

	// Key files should have been cleaned up.
	hexes, _ := kr.listKeyFiles()
	if len(hexes) != 0 {
		t.Fatalf("expected key files to be cleaned up, found %d", len(hexes))
	}
}

func TestImportSetAliasFailureCleanup(t *testing.T) {
	kr := newTestKeyring(t)

	orig, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	// Make keyring.json a directory so saveKeyringFile's WriteFile fails.
	if err := os.MkdirAll(kr.keyringFilePath(), 0o700); err != nil {
		t.Fatal(err)
	}

	_, err = kr.Import(context.Background(), orig.Seed(), "willbreak")
	if err == nil {
		t.Fatal("expected error when SetAlias fails")
	}

	// Key files should have been cleaned up.
	hexes, _ := kr.listKeyFiles()
	if len(hexes) != 0 {
		t.Fatalf("expected key files to be cleaned up, found %d", len(hexes))
	}
}

func TestImportGroupKeySetAliasFailureCleanup(t *testing.T) {
	kr := newTestKeyring(t)

	adminKP, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	groupKP, _, err := group.Create("testgroup", adminKP)
	if err != nil {
		t.Fatalf("create group: %v", err)
	}

	sealedSeed, err := group.SealSeed(groupKP.Seed(), adminKP.PublicKey())
	if err != nil {
		t.Fatalf("seal seed: %v", err)
	}

	// Make keyring.json a directory so saveKeyringFile's WriteFile fails.
	if err := os.MkdirAll(kr.keyringFilePath(), 0o700); err != nil {
		t.Fatal(err)
	}

	_, err = kr.ImportGroupKey(context.Background(), sealedSeed, adminKP, "willbreak")
	if err == nil {
		t.Fatal("expected error when SetAlias fails")
	}
}

func TestDeleteSaveKeyringFileFailure(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "todel")

	// Make keyring.json a directory so saveKeyringFile fails when trying
	// to persist alias removal.
	os.Remove(kr.keyringFilePath())
	if err := os.MkdirAll(kr.keyringFilePath(), 0o700); err != nil {
		t.Fatal(err)
	}

	err := kr.Delete(context.Background(), key.PublicKey)
	if err == nil {
		t.Fatal("expected error when saveKeyringFile fails during delete")
	}
}

func TestLoadKeyReadErrorNotErrNotExist(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "")

	// Replace the key file with a directory to cause a non-ErrNotExist read error.
	os.Remove(kr.keyPath(key.PublicKey))
	if err := os.MkdirAll(kr.keyPath(key.PublicKey), 0o700); err != nil {
		t.Fatal(err)
	}

	_, _, err := kr.loadKey(key.PublicKey)
	if err == nil {
		t.Fatal("expected error reading key as directory")
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatal("expected non-ErrNotFound error")
	}
}

func TestLoadKeyMetadataReadErrorNotErrNotExist(t *testing.T) {
	kr := newTestKeyring(t)
	key := generateKey(t, kr, "")

	// Replace metadata file with a directory.
	os.Remove(kr.metaPath(key.PublicKey))
	if err := os.MkdirAll(kr.metaPath(key.PublicKey), 0o700); err != nil {
		t.Fatal(err)
	}

	_, _, err := kr.loadKey(key.PublicKey)
	if err == nil {
		t.Fatal("expected error reading metadata as directory")
	}
}

func TestListGroups(t *testing.T) {
	kr := newTestKeyring(t)

	// Generate a personal key.
	generateKey(t, kr, "personal")

	// Import a group key.
	adminKP, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	groupKP, _, err := group.Create("testgroup", adminKP)
	if err != nil {
		t.Fatalf("create group: %v", err)
	}

	sealedSeed, err := group.SealSeed(groupKP.Seed(), adminKP.PublicKey())
	if err != nil {
		t.Fatalf("seal seed: %v", err)
	}

	_, err = kr.ImportGroupKey(context.Background(), sealedSeed, adminKP, "grp")
	if err != nil {
		t.Fatalf("import group key: %v", err)
	}

	groups, err := kr.ListGroups(context.Background())
	if err != nil {
		t.Fatalf("list groups: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if groups[0].Type != "group" {
		t.Fatalf("expected type 'group', got %q", groups[0].Type)
	}
}
