package group

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
)

const contentTypeManifest = "arc/group.manifest"

// MarshalManifest produces a deterministic binary encoding of the manifest.
// Members are sorted by public key to ensure consistent hashing.
func MarshalManifest(m *Manifest) ([]byte, error) {
	if m == nil {
		return nil, ErrInvalidManifest
	}

	// Sort members by public key for determinism.
	sorted := make([]Member, len(m.Members))
	copy(sorted, m.Members)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i].PublicKey[:], sorted[j].PublicKey[:]) < 0
	})

	var buf bytes.Buffer

	// Version byte.
	buf.WriteByte(1)

	// Group ID (32 bytes).
	buf.Write(m.ID[:])

	// Name (length-prefixed).
	nameBytes := []byte(m.Name)
	binary.Write(&buf, binary.BigEndian, uint16(len(nameBytes)))
	buf.Write(nameBytes)

	// Parent (1 byte flag + optional 32 bytes).
	if m.Parent != nil {
		buf.WriteByte(1)
		buf.Write(m.Parent[:])
	} else {
		buf.WriteByte(0)
	}

	// Policy.
	binary.Write(&buf, binary.BigEndian, int32(m.Policy.AdminThreshold))

	// Members.
	binary.Write(&buf, binary.BigEndian, uint32(len(sorted)))
	for _, mem := range sorted {
		buf.Write(mem.PublicKey[:])
		buf.WriteByte(byte(mem.Role))
		binary.Write(&buf, binary.BigEndian, uint32(len(mem.SealedSeed)))
		buf.Write(mem.SealedSeed)
	}

	return buf.Bytes(), nil
}

// UnmarshalManifest decodes a deterministic binary-encoded manifest.
func UnmarshalManifest(data []byte) (*Manifest, error) {
	if len(data) < 1 {
		return nil, ErrInvalidManifest
	}
	r := bytes.NewReader(data)

	// Version.
	ver, _ := r.ReadByte()
	if ver != 1 {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrInvalidManifest, ver)
	}

	m := &Manifest{}

	// Group ID.
	if _, err := r.Read(m.ID[:]); err != nil {
		return nil, fmt.Errorf("%w: read group ID: %v", ErrInvalidManifest, err)
	}

	// Name.
	var nameLen uint16
	if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
		return nil, fmt.Errorf("%w: read name length: %v", ErrInvalidManifest, err)
	}
	nameBytes := make([]byte, nameLen)
	if _, err := r.Read(nameBytes); err != nil {
		return nil, fmt.Errorf("%w: read name: %v", ErrInvalidManifest, err)
	}
	m.Name = string(nameBytes)

	// Parent.
	parentFlag, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("%w: read parent flag: %v", ErrInvalidManifest, err)
	}
	if parentFlag == 1 {
		var ref reference.Reference
		if _, err := r.Read(ref[:]); err != nil {
			return nil, fmt.Errorf("%w: read parent ref: %v", ErrInvalidManifest, err)
		}
		m.Parent = &ref
	}

	// Policy.
	var threshold int32
	if err := binary.Read(r, binary.BigEndian, &threshold); err != nil {
		return nil, fmt.Errorf("%w: read policy: %v", ErrInvalidManifest, err)
	}
	m.Policy.AdminThreshold = int(threshold)

	// Members.
	var count uint32
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return nil, fmt.Errorf("%w: read member count: %v", ErrInvalidManifest, err)
	}
	m.Members = make([]Member, count)
	for i := range m.Members {
		if _, err := r.Read(m.Members[i].PublicKey[:]); err != nil {
			return nil, fmt.Errorf("%w: read member %d key: %v", ErrInvalidManifest, i, err)
		}
		roleByte, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("%w: read member %d role: %v", ErrInvalidManifest, i, err)
		}
		m.Members[i].Role = Role(roleByte)

		var sealedLen uint32
		if err := binary.Read(r, binary.BigEndian, &sealedLen); err != nil {
			return nil, fmt.Errorf("%w: read member %d sealed len: %v", ErrInvalidManifest, i, err)
		}
		if sealedLen > 0 {
			m.Members[i].SealedSeed = make([]byte, sealedLen)
			if _, err := r.Read(m.Members[i].SealedSeed); err != nil {
				return nil, fmt.Errorf("%w: read member %d sealed seed: %v", ErrInvalidManifest, i, err)
			}
		}
	}

	return m, nil
}

// SignManifest serializes and signs a manifest as an Arc message.
// Returns the signed message (From=group pubkey, ContentType=arc/group.manifest)
// and the serialized manifest bytes.
func SignManifest(m *Manifest, groupKP *identity.Keypair, parent *reference.Reference) (message.Message, []byte, error) {
	m.Parent = parent
	data, err := MarshalManifest(m)
	if err != nil {
		return message.Message{}, nil, err
	}

	contentRef := reference.Compute(data)
	msg := message.New(groupKP.PublicKey(), identity.PublicKey{}, contentRef, contentTypeManifest)
	msg.Parent = parent
	if err := message.Sign(&msg, groupKP); err != nil {
		return message.Message{}, nil, fmt.Errorf("sign manifest: %w", err)
	}
	return msg, data, nil
}

// VerifyManifest verifies the message signature and decodes the manifest.
func VerifyManifest(msg message.Message, data []byte) (*Manifest, error) {
	if msg.ContentType != contentTypeManifest {
		return nil, fmt.Errorf("%w: wrong content type %q", ErrInvalidManifest, msg.ContentType)
	}

	ok, err := message.Verify(msg)
	if err != nil {
		return nil, fmt.Errorf("verify signature: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("%w: signature verification failed", ErrInvalidManifest)
	}

	// Verify content hash matches.
	computed := reference.Compute(data)
	if computed != msg.Content {
		return nil, fmt.Errorf("%w: content hash mismatch", ErrInvalidManifest)
	}

	m, err := UnmarshalManifest(data)
	if err != nil {
		return nil, err
	}

	// Group ID must match signer.
	if m.ID != msg.From {
		return nil, fmt.Errorf("%w: manifest ID does not match signer", ErrInvalidManifest)
	}

	return m, nil
}
