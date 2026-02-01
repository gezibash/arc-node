package journal

import (
	"context"
	"fmt"
	"strings"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/reference"
)

// List queries journal entries matching the given options.
func (j *Journal) List(ctx context.Context, opts ListOptions) (*ListResult, error) {
	labelMap := j.ownerLabels(opts.Labels)

	expr := opts.Expression
	if expr == "" {
		expr = "true"
	}

	result, err := j.client.QueryMessages(ctx, &client.QueryOptions{
		Expression: expr,
		Labels:     labelMap,
		Limit:      opts.Limit,
		Cursor:     opts.Cursor,
		Descending: opts.Descending,
	})
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	entries := make([]Entry, len(result.Entries))
	for i, e := range result.Entries {
		var entryRef reference.Reference
		if h := e.Labels["entry"]; h != "" {
			entryRef, _ = reference.FromHex(h)
		}
		entries[i] = Entry{
			Ref:       e.Ref,
			EntryRef:  entryRef,
			Labels:    e.Labels,
			Timestamp: e.Timestamp,
		}
	}

	return &ListResult{
		Entries:    entries,
		NextCursor: result.NextCursor,
		HasMore:    result.HasMore,
	}, nil
}

const (
	previewMaxBytes = 256
	previewMaxLines = 4
)

// Preview fetches and decrypts a short preview of the given entry's content.
func (j *Journal) Preview(ctx context.Context, e Entry) (string, error) {
	hexStr := e.Labels["content"]
	if hexStr == "" {
		return "", fmt.Errorf("no content label")
	}
	ref, err := reference.FromHex(hexStr)
	if err != nil {
		return "", err
	}
	ciphertext, err := j.client.GetContent(ctx, ref)
	if err != nil {
		return "", err
	}
	plaintext, err := decrypt(ciphertext, &j.symKey)
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
