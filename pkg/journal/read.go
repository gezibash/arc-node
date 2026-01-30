package journal

import (
	"context"
	"fmt"

	"github.com/gezibash/arc/pkg/reference"
)

// Read fetches and decrypts a journal entry by its content reference.
func (j *Journal) Read(ctx context.Context, contentRef reference.Reference) (*Entry, error) {
	ciphertext, err := j.client.GetContent(ctx, contentRef)
	if err != nil {
		return nil, fmt.Errorf("get content: %w", err)
	}

	plaintext, err := decrypt(ciphertext, &j.symKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	return &Entry{
		Ref:     contentRef,
		Content: plaintext,
	}, nil
}
