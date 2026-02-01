package journal

import (
	"context"
	"fmt"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
)

// ReadForEdit resolves a message reference (hex string), fetches and decrypts
// its content, and returns the plaintext along with the original labels.
func (j *Journal) ReadForEdit(ctx context.Context, msgRefHex string) (plaintext []byte, labels map[string]string, err error) {
	result, err := j.client.ResolveGet(ctx, msgRefHex)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve message: %w", err)
	}
	if result.Kind != client.GetKindMessage {
		return nil, nil, fmt.Errorf("reference is not a message")
	}

	contentHex := result.Labels["content"]
	if contentHex == "" {
		return nil, nil, fmt.Errorf("message has no content label")
	}
	contentRef, err := reference.FromHex(contentHex)
	if err != nil {
		return nil, nil, fmt.Errorf("parse content ref: %w", err)
	}

	ciphertext, err := j.client.GetContent(ctx, contentRef)
	if err != nil {
		return nil, nil, fmt.Errorf("get content: %w", err)
	}

	plaintext, err = decrypt(ciphertext, &j.symKey)
	if err != nil {
		return nil, nil, fmt.Errorf("decrypt: %w", err)
	}

	return plaintext, result.Labels, nil
}

// Edit encrypts new plaintext and publishes it as a replacement for the
// entry identified by oldEntryRef, carrying forward the original labels.
func (j *Journal) Edit(ctx context.Context, oldEntryRef reference.Reference, newPlaintext []byte, origLabels map[string]string) (*EditResult, error) {
	ciphertext, err := encrypt(newPlaintext, &j.symKey)
	if err != nil {
		return nil, fmt.Errorf("encrypt: %w", err)
	}

	newContentRef, err := j.client.PutContent(ctx, ciphertext)
	if err != nil {
		return nil, fmt.Errorf("put content: %w", err)
	}

	pub := j.kp.PublicKey()
	toPub := j.recipientKey()

	msg := message.New(pub, toPub, newContentRef, contentType)
	if err := message.Sign(&msg, j.kp); err != nil {
		return nil, fmt.Errorf("sign message: %w", err)
	}

	labelMap := make(map[string]string, len(origLabels)+1)
	for k, v := range origLabels {
		if k == "content" {
			continue
		}
		labelMap[k] = v
	}
	labelMap["app"] = "journal"
	labelMap["type"] = "entry"
	labelMap["replaces"] = reference.Hex(oldEntryRef)

	newEntryRef := ComputeEntryRef(newContentRef, msg.Timestamp, pub)
	labelMap["entry"] = reference.Hex(newEntryRef)

	ref, err := j.client.SendMessage(ctx, msg, labelMap)
	if err != nil {
		return nil, fmt.Errorf("send message: %w", err)
	}

	if j.search != nil {
		_ = j.search.DeleteByEntryRef(ctx, oldEntryRef)
		_ = j.search.Index(ctx, newContentRef, newEntryRef, string(newPlaintext), msg.Timestamp)
	}

	return &EditResult{Ref: ref, EntryRef: newEntryRef}, nil
}
