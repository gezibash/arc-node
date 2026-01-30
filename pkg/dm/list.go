package dm

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/gezibash/arc/pkg/reference"
	"github.com/gezibash/arc-node/pkg/client"
)

// List queries DMs in the conversation matching the given options.
func (d *DM) List(ctx context.Context, opts ListOptions) (*ListResult, error) {
	labelMap := d.queryLabels(opts.Labels)

	expr := opts.Expression
	if expr == "" {
		expr = "true"
	}

	result, err := d.client.QueryMessages(ctx, &client.QueryOptions{
		Expression: expr,
		Labels:     labelMap,
		Limit:      opts.Limit,
		Cursor:     opts.Cursor,
		Descending: opts.Descending,
	})
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	msgs := make([]Message, len(result.Entries))
	for i, e := range result.Entries {
		var from, to [32]byte
		if f, err := hex.DecodeString(e.Labels["dm_from"]); err == nil {
			copy(from[:], f)
		}
		if t, err := hex.DecodeString(e.Labels["dm_to"]); err == nil {
			copy(to[:], t)
		}
		msgs[i] = Message{
			Ref:       e.Ref,
			Labels:    e.Labels,
			Timestamp: e.Timestamp,
			From:      from,
			To:        to,
		}
	}

	return &ListResult{
		Messages:   msgs,
		NextCursor: result.NextCursor,
		HasMore:    result.HasMore,
	}, nil
}

const (
	previewMaxBytes = 256
	previewMaxLines = 4
)

// Preview fetches and decrypts a short preview of the given message's content.
func (d *DM) Preview(ctx context.Context, m Message) (string, error) {
	hexStr := m.Labels["content"]
	if hexStr == "" {
		return "", fmt.Errorf("no content label")
	}
	ref, err := reference.FromHex(hexStr)
	if err != nil {
		return "", err
	}
	ciphertext, err := d.client.GetContent(ctx, ref)
	if err != nil {
		return "", err
	}
	plaintext, err := decrypt(ciphertext, &d.sharedKey)
	if err != nil {
		return "", err
	}
	return truncatePreview(plaintext), nil
}

func truncatePreview(data []byte) string {
	truncated := false
	if len(data) > previewMaxBytes {
		data = data[:previewMaxBytes]
		truncated = true
	}
	text := string(data)
	lines := strings.SplitN(text, "\n", previewMaxLines+1)
	if len(lines) > previewMaxLines {
		lines = lines[:previewMaxLines]
		truncated = true
	}
	result := strings.Join(lines, "\n")
	if truncated {
		result += "..."
	}
	return result
}
