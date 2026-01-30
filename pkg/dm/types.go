package dm

import "github.com/gezibash/arc/pkg/reference"

// Message represents a decrypted DM.
type Message struct {
	Ref       reference.Reference
	Labels    map[string]string
	Timestamp int64
	Content   []byte // populated by Read; nil in List results
	From      [32]byte
	To        [32]byte
}

// SendResult is returned by Send.
type SendResult struct{ Ref reference.Reference }

// ListResult is returned by List.
type ListResult struct {
	Messages   []Message
	NextCursor string
	HasMore    bool
}

// ListOptions controls list queries.
type ListOptions struct {
	Expression string
	Labels     map[string]string
	Limit      int
	Cursor     string
	Descending bool
}
