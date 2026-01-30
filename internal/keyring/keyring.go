package keyring

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/gezibash/arc/pkg/identity"
)

const (
	DefaultAlias       = "default"
	PublicKeyHexLength = 64
)

var (
	ErrNotFound      = errors.New("key not found")
	ErrAliasNotFound = errors.New("alias not found")
	ErrAlreadyExists = errors.New("key already exists")
	ErrNoDefault     = errors.New("no default key set")
)

type Keyring struct {
	dir string
}

type Key struct {
	Keypair   *identity.Keypair
	PublicKey string // hex-encoded
	Metadata  *Metadata
}

type Metadata struct {
	PublicKey string    `json:"public_key"`
	CreatedAt time.Time `json:"created_at"`
}

type KeyInfo struct {
	PublicKey string    `json:"public_key"`
	Aliases   []string  `json:"aliases,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	IsDefault bool      `json:"is_default"`
}

func New(dir string) *Keyring {
	return &Keyring{dir: dir}
}

func pubKeyHex(kp *identity.Keypair) string {
	pk := kp.PublicKey()
	return hex.EncodeToString(pk[:])
}

func (kr *Keyring) Generate(_ context.Context, alias string) (*Key, error) {
	kp, err := identity.Generate()
	if err != nil {
		return nil, err
	}

	pkHex := pubKeyHex(kp)

	if kr.keyExists(pkHex) {
		return nil, ErrAlreadyExists
	}

	meta := &Metadata{
		PublicKey: pkHex,
		CreatedAt: time.Now(),
	}

	if err := kr.saveKey(kp, pkHex, meta); err != nil {
		return nil, err
	}

	if alias != "" {
		if err := kr.SetAlias(alias, pkHex); err != nil {
			_ = kr.deleteKeyFiles(pkHex)
			return nil, err
		}
	}

	return &Key{Keypair: kp, PublicKey: pkHex, Metadata: meta}, nil
}

func (kr *Keyring) Import(_ context.Context, seed []byte, alias string) (*Key, error) {
	kp, err := identity.FromSeed(seed)
	if err != nil {
		return nil, err
	}

	pkHex := pubKeyHex(kp)

	meta := &Metadata{
		PublicKey: pkHex,
		CreatedAt: time.Now(),
	}

	if err := kr.saveKey(kp, pkHex, meta); err != nil {
		return nil, err
	}

	if alias != "" {
		if err := kr.SetAlias(alias, pkHex); err != nil {
			_ = kr.deleteKeyFiles(pkHex)
			return nil, err
		}
	}

	return &Key{Keypair: kp, PublicKey: pkHex, Metadata: meta}, nil
}

func (kr *Keyring) Load(_ context.Context, nameOrID string) (*Key, error) {
	pkHex, err := kr.resolveToPublicKey(nameOrID)
	if err != nil {
		return nil, err
	}

	kp, meta, err := kr.loadKey(pkHex)
	if err != nil {
		return nil, err
	}

	return &Key{Keypair: kp, PublicKey: pkHex, Metadata: meta}, nil
}

func (kr *Keyring) LoadDefault(_ context.Context) (*Key, error) {
	kf, err := kr.loadKeyringFile()
	if err != nil {
		return nil, err
	}

	if kf.Default == "" {
		return nil, ErrNoDefault
	}

	return kr.Load(context.Background(), kf.Default)
}

func (kr *Keyring) LoadOrGenerate(ctx context.Context, alias string) (*Key, error) {
	key, err := kr.Load(ctx, alias)
	if err == nil {
		return key, nil
	}
	if !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrAliasNotFound) {
		return nil, err
	}
	return kr.Generate(ctx, alias)
}

func (kr *Keyring) List(_ context.Context) ([]*KeyInfo, error) {
	kf, err := kr.loadKeyringFile()
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	aliasMap := make(map[string][]string)
	if kf != nil {
		for alias, pkHex := range kf.Aliases {
			aliasMap[pkHex] = append(aliasMap[pkHex], alias)
		}
	}

	hexes, err := kr.listKeyFiles()
	if err != nil {
		return nil, err
	}

	infos := make([]*KeyInfo, 0, len(hexes))
	for _, pkHex := range hexes {
		_, meta, err := kr.loadKey(pkHex)
		if err != nil {
			continue
		}

		info := &KeyInfo{
			PublicKey: meta.PublicKey,
			Aliases:   aliasMap[pkHex],
			CreatedAt: meta.CreatedAt,
		}

		if kf != nil && kf.Default != "" {
			defaultHex, _ := kr.resolveAliasToPublicKey(kf.Default, kf)
			if defaultHex == pkHex {
				info.IsDefault = true
			}
		}

		infos = append(infos, info)
	}

	return infos, nil
}

func (kr *Keyring) Delete(_ context.Context, nameOrID string) error {
	pkHex, err := kr.resolveToPublicKey(nameOrID)
	if err != nil {
		return err
	}

	kf, err := kr.loadKeyringFile()
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}

	if kf != nil {
		changed := false
		for alias, id := range kf.Aliases {
			if id == pkHex {
				delete(kf.Aliases, alias)
				changed = true
			}
		}
		if kf.Default != "" {
			defaultHex, _ := kr.resolveAliasToPublicKey(kf.Default, kf)
			if defaultHex == pkHex {
				kf.Default = ""
				changed = true
			}
		}
		if changed {
			if err := kr.saveKeyringFile(kf); err != nil {
				return err
			}
		}
	}

	return kr.deleteKeyFiles(pkHex)
}

func (kr *Keyring) SetAlias(alias, pkHex string) error {
	pkHex = normalize(pkHex)

	if !kr.keyExists(pkHex) {
		return ErrNotFound
	}

	kf, err := kr.loadKeyringFile()
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			kf = &keyringFile{Version: 1, Aliases: make(map[string]string)}
		} else {
			return err
		}
	}

	kf.Aliases[alias] = pkHex
	return kr.saveKeyringFile(kf)
}

func (kr *Keyring) SetDefault(alias string) error {
	kf, err := kr.loadKeyringFile()
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			kf = &keyringFile{Version: 1, Aliases: make(map[string]string)}
		} else {
			return err
		}
	}

	if _, ok := kf.Aliases[alias]; !ok {
		return ErrAliasNotFound
	}

	kf.Default = alias
	return kr.saveKeyringFile(kf)
}

func (kr *Keyring) resolveToPublicKey(nameOrID string) (string, error) {
	pkHex := normalize(nameOrID)

	if len(pkHex) == PublicKeyHexLength && isHex(pkHex) && kr.keyExists(pkHex) {
		return pkHex, nil
	}

	kf, err := kr.loadKeyringFile()
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			if len(pkHex) == PublicKeyHexLength && isHex(pkHex) && kr.keyExists(pkHex) {
				return pkHex, nil
			}
			return "", ErrAliasNotFound
		}
		return "", err
	}

	return kr.resolveAliasToPublicKey(nameOrID, kf)
}

func (kr *Keyring) resolveAliasToPublicKey(nameOrID string, kf *keyringFile) (string, error) {
	if pkHex, ok := kf.Aliases[nameOrID]; ok {
		pkHex = normalize(pkHex)
		if kr.keyExists(pkHex) {
			return pkHex, nil
		}
		return "", ErrNotFound
	}

	pkHex := normalize(nameOrID)
	if len(pkHex) == PublicKeyHexLength && isHex(pkHex) && kr.keyExists(pkHex) {
		return pkHex, nil
	}

	return "", ErrAliasNotFound
}

func isHex(s string) bool {
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

func normalize(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}
