package journal

import (
	"context"
	"fmt"
)

// Subscribe opens a real-time subscription for new journal entries matching
// the given options. It returns channels for entries and errors. The entry
// channel is closed when the subscription ends.
func (j *Journal) Subscribe(ctx context.Context, opts ListOptions) (<-chan *Entry, <-chan error, error) {
	labelMap := mergeLabels(opts.Labels)

	expr := opts.Expression
	if expr == "" {
		expr = "true"
	}

	clientEntries, clientErrs, err := j.client.SubscribeMessages(ctx, expr, labelMap)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe: %w", err)
	}

	entries := make(chan *Entry)
	errs := make(chan error, 1)

	go func() {
		defer close(entries)
		defer close(errs)
		for {
			select {
			case ce, ok := <-clientEntries:
				if !ok {
					return
				}
				entries <- &Entry{
					Ref:       ce.Ref,
					Labels:    ce.Labels,
					Timestamp: ce.Timestamp,
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

	return entries, errs, nil
}
