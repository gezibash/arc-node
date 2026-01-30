package dm

import (
	"context"
	"encoding/hex"
	"fmt"
)

// Subscribe opens a real-time subscription for new DMs in the conversation.
func (d *DM) Subscribe(ctx context.Context, opts ListOptions) (<-chan *Message, <-chan error, error) {
	labelMap := d.queryLabels(opts.Labels)

	expr := opts.Expression
	if expr == "" {
		expr = "true"
	}

	clientEntries, clientErrs, err := d.client.SubscribeMessages(ctx, expr, labelMap)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe: %w", err)
	}

	msgs := make(chan *Message)
	errs := make(chan error, 1)

	go func() {
		defer close(msgs)
		defer close(errs)
		for {
			select {
			case ce, ok := <-clientEntries:
				if !ok {
					return
				}
				var from, to [32]byte
				if f, err := hex.DecodeString(ce.Labels["dm_from"]); err == nil {
					copy(from[:], f)
				}
				if t, err := hex.DecodeString(ce.Labels["dm_to"]); err == nil {
					copy(to[:], t)
				}
				msgs <- &Message{
					Ref:       ce.Ref,
					Labels:    ce.Labels,
					Timestamp: ce.Timestamp,
					From:      from,
					To:        to,
				}
			case err, ok := <-clientErrs:
				if !ok {
					return
				}
				errs <- err
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return msgs, errs, nil
}
