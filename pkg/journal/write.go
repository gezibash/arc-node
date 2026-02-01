package journal

import (
	"context"
	"fmt"

	"encoding/hex"

	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
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

	entryRef := ComputeEntryRef(contentRef, msg.Timestamp, pub)

	labelMap := j.ownerLabels(labels)
	labelMap["entry"] = reference.Hex(entryRef)

	ref, err := j.client.SendMessage(ctx, msg, labelMap)
	if err != nil {
		return nil, fmt.Errorf("send message: %w", err)
	}

	j.IndexEntry(contentRef, entryRef, string(plaintext), msg.Timestamp)

	return &WriteResult{Ref: ref, EntryRef: entryRef}, nil
}

func (j *Journal) recipientKey() identity.PublicKey {
	if j.client != nil {
		if key, ok := j.client.NodeKey(); ok {
			return key
		}
	}
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
