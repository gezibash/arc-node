package keyring

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/gezibash/arc/v2/pkg/identity/ed25519"
)

type keyringFile struct {
	Version int               `json:"version"`
	Default string            `json:"default,omitempty"`
	Aliases map[string]string `json:"aliases"`
}

func (kr *Keyring) keysDir() string {
	return filepath.Join(kr.dir, "keys")
}

func (kr *Keyring) keyringFilePath() string {
	return filepath.Join(kr.dir, "keyring.json")
}

func (kr *Keyring) keyPath(pkHex string) string {
	return filepath.Join(kr.keysDir(), normalize(pkHex)+".key")
}

func (kr *Keyring) metaPath(pkHex string) string {
	return filepath.Join(kr.keysDir(), normalize(pkHex)+".json")
}

func (kr *Keyring) keyExists(pkHex string) bool {
	_, err := os.Stat(kr.keyPath(pkHex))
	return err == nil
}

func (kr *Keyring) saveKey(kp *ed25519.Keypair, pkHex string, meta *Metadata) error {
	pkHex = normalize(pkHex)

	if err := os.MkdirAll(kr.keysDir(), 0o700); err != nil {
		return fmt.Errorf("create keys directory: %w", err)
	}

	keyPath := kr.keyPath(pkHex)
	metaPath := kr.metaPath(pkHex)

	if err := os.WriteFile(keyPath, kp.Seed(), 0o600); err != nil {
		return fmt.Errorf("write key file: %w", err)
	}

	metaJSON, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		_ = os.Remove(keyPath)
		return fmt.Errorf("marshal metadata: %w", err)
	}

	if err := os.WriteFile(metaPath, metaJSON, 0o600); err != nil {
		_ = os.Remove(keyPath)
		return fmt.Errorf("write metadata file: %w", err)
	}

	return nil
}

func (kr *Keyring) loadKey(pkHex string) (*ed25519.Keypair, *Metadata, error) {
	pkHex = normalize(pkHex)

	seed, err := os.ReadFile(kr.keyPath(pkHex))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, fmt.Errorf("read key file: %w", err)
	}

	kp, err := ed25519.FromSeed(seed)
	if err != nil {
		return nil, nil, fmt.Errorf("create keypair from seed: %w", err)
	}

	var meta *Metadata
	metaJSON, err := os.ReadFile(kr.metaPath(pkHex))
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, nil, fmt.Errorf("read metadata file: %w", err)
		}
		pk := kp.PublicKey()
		meta = &Metadata{PublicKey: hex.EncodeToString(pk.Bytes)}
	} else {
		meta = &Metadata{}
		if err := json.Unmarshal(metaJSON, meta); err != nil {
			return nil, nil, fmt.Errorf("parse metadata: %w", err)
		}
	}

	return kp, meta, nil
}

func (kr *Keyring) deleteKeyFiles(pkHex string) error {
	pkHex = normalize(pkHex)
	if err := os.Remove(kr.keyPath(pkHex)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ErrNotFound
		}
		return fmt.Errorf("delete key file: %w", err)
	}
	_ = os.Remove(kr.metaPath(pkHex))
	return nil
}

func (kr *Keyring) listKeyFiles() ([]string, error) {
	entries, err := os.ReadDir(kr.keysDir())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read keys directory: %w", err)
	}

	var hexes []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".key") {
			continue
		}
		hexes = append(hexes, strings.TrimSuffix(entry.Name(), ".key"))
	}
	return hexes, nil
}

func (kr *Keyring) loadKeyringFile() (*keyringFile, error) {
	data, err := os.ReadFile(kr.keyringFilePath())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("read keyring file: %w", err)
	}

	kf := &keyringFile{}
	if err := json.Unmarshal(data, kf); err != nil {
		return nil, fmt.Errorf("parse keyring file: %w", err)
	}

	if kf.Aliases == nil {
		kf.Aliases = make(map[string]string)
	}

	for alias, pkHex := range kf.Aliases {
		kf.Aliases[alias] = normalize(pkHex)
	}

	return kf, nil
}

func (kr *Keyring) saveKeyringFile(kf *keyringFile) error {
	if err := os.MkdirAll(kr.dir, 0o700); err != nil {
		return fmt.Errorf("create keyring directory: %w", err)
	}

	data, err := json.MarshalIndent(kf, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal keyring file: %w", err)
	}

	if err := os.WriteFile(kr.keyringFilePath(), data, 0o600); err != nil {
		return fmt.Errorf("write keyring file: %w", err)
	}

	return nil
}
