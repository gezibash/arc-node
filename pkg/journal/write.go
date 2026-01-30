package journal

import (
	"context"
	"fmt"

	"encoding/hex"

	"github.com/gezibash/arc/pkg/identity"
	"github.com/gezibash/arc/pkg/message"
)

// Write encrypts plaintext and publishes it as a journal entry with the given labels.
func (j *Journal) Write(ctx context.Context, plaintext []byte, labels map[string]string) (*WriteResult, error) {
	ciphertext, err := encrypt(plaintext, &j.symKey)
	if err != nil {
		return nil, fmt.Errorf("encrypt: %w", err)
	}

	contentRef, err := j.client.PutContent(ctx, ciphertext)
	if err != nil {
		return nil, fmt.Errorf("put content: %w", err)
	}

	pub := j.kp.PublicKey()
	toPub := j.recipientKey()

	msg := message.New(pub, toPub, contentRef, contentType)
	if err := message.Sign(&msg, j.kp); err != nil {
		return nil, fmt.Errorf("sign message: %w", err)
	}

	labelMap := j.ownerLabels(labels)

	ref, err := j.client.SendMessage(ctx, msg, labelMap)
	if err != nil {
		return nil, fmt.Errorf("send message: %w", err)
	}

	return &WriteResult{Ref: ref}, nil
}

func (j *Journal) recipientKey() identity.PublicKey {
	if j.nodeKey != nil {
		return *j.nodeKey
	}
	return j.kp.PublicKey()
}

func mergeLabels(extra map[string]string) map[string]string {
	m := make(map[string]string, len(extra)+2)
	for k, v := range extra {
		m[k] = v
	}
	m["app"] = "journal"
	m["type"] = "entry"
	return m
}

func (j *Journal) ownerLabels(extra map[string]string) map[string]string {
	m := mergeLabels(extra)
	pub := j.kp.PublicKey()
	m["owner"] = hex.EncodeToString(pub[:])
	return m
}
