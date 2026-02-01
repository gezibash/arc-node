package dm

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/gezibash/arc/v2/pkg/reference"
)

// Read fetches and decrypts a DM by its content reference.
func (d *DM) Read(ctx context.Context, contentRef reference.Reference) (*Message, error) {
	ciphertext, err := d.client.GetContent(ctx, contentRef)
	if err != nil {
		return nil, fmt.Errorf("get content: %w", err)
	}

	plaintext, err := decrypt(ciphertext, &d.sharedKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	return &Message{
		Ref:     contentRef,
		Content: plaintext,
	}, nil
}

// ParseFrom extracts the sender public key from a message's labels.
func ParseFrom(m Message) ([32]byte, error) {
	fromHex := m.Labels["dm_from"]
	if fromHex == "" {
		return [32]byte{}, fmt.Errorf("no from label")
	}
	b, err := hex.DecodeString(fromHex)
	if err != nil {
		return [32]byte{}, err
	}
	var pub [32]byte
	copy(pub[:], b)
	return pub, nil
}
