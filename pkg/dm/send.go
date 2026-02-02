package dm

import (
	"context"
	"encoding/hex"
	"fmt"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc/v2/pkg/message"
)

// Send encrypts plaintext and publishes it as a DM with the given extra labels.
func (d *DM) Send(ctx context.Context, plaintext []byte, labels map[string]string) (*SendResult, error) {
	ciphertext, err := encrypt(plaintext, &d.sharedKey)
	if err != nil {
		return nil, fmt.Errorf("encrypt: %w", err)
	}

	contentRef, err := d.client.PutContent(ctx, ciphertext)
	if err != nil {
		return nil, fmt.Errorf("put content: %w", err)
	}

	pub := d.kp.PublicKey()
	toPub := d.recipientKey()

	msg := message.New(pub, toPub, contentRef, contentType)
	if err := message.Sign(&msg, d.kp); err != nil {
		return nil, fmt.Errorf("sign message: %w", err)
	}

	labelMap := d.conversationLabels(labels)

	ref, err := d.client.SendMessage(ctx, msg, labelMap, &nodev1.Dimensions{
		Persistence: nodev1.Persistence_PERSISTENCE_DURABLE,
		Visibility:  nodev1.Visibility_VISIBILITY_PRIVATE,
	})
	if err != nil {
		return nil, fmt.Errorf("send message: %w", err)
	}

	if d.search != nil {
		_ = d.search.Index(ctx, contentRef, ref, string(plaintext), msg.Timestamp)
	}

	return &SendResult{Ref: ref}, nil
}

func (d *DM) conversationLabels(extra map[string]string) map[string]string {
	pub := d.kp.PublicKey()
	m := make(map[string]string, len(extra)+5)
	for k, v := range extra {
		m[k] = v
	}
	m["app"] = "dm"
	m["type"] = "message"
	m["conversation"] = d.convID
	m["dm_from"] = hex.EncodeToString(pub[:])
	m["dm_to"] = hex.EncodeToString(d.peerPub[:])
	return m
}

func (d *DM) queryLabels(extra map[string]string) map[string]string {
	m := make(map[string]string, len(extra)+3)
	for k, v := range extra {
		m[k] = v
	}
	m["app"] = "dm"
	m["type"] = "message"
	m["conversation"] = d.convID
	return m
}
