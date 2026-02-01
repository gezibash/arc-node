package envelope

import (
	"time"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
)

// Envelope wraps a signed arc message with transport-level metadata.
type Envelope struct {
	Message    message.Message
	Origin     identity.PublicKey
	HopCount   int
	Metadata   map[string]string
	Dimensions *nodev1.Dimensions
}

// Seal constructs an envelope, signs the inner message, and returns it.
// The payload is the serialized proto request/response body; its SHA-256
// becomes Message.Content. Dimensions are NOT part of the inner message
// signature — they are attested by the node envelope.
func Seal(kp *identity.Keypair, to identity.PublicKey, payload []byte, contentType string, origin identity.PublicKey, hopCount int, meta map[string]string, dims *nodev1.Dimensions) (*Envelope, error) {
	contentRef := reference.Compute(payload)

	msg := message.Message{
		From:        kp.PublicKey(),
		To:          to,
		Content:     contentRef,
		ContentType: contentType,
		Timestamp:   time.Now().UnixMilli(),
	}

	if err := message.Sign(&msg, kp); err != nil {
		return nil, err
	}

	return &Envelope{
		Message:    msg,
		Origin:     origin,
		HopCount:   hopCount,
		Metadata:   meta,
		Dimensions: dims,
	}, nil
}

// Open reconstructs an envelope from its constituent parts and verifies the
// inner message signature. Returns an error if the signature is invalid.
func Open(from, to identity.PublicKey, payload []byte, contentType string, timestamp int64, sig identity.Signature, origin identity.PublicKey, hopCount int, meta map[string]string, dims *nodev1.Dimensions) (*Envelope, error) {
	contentRef := reference.Compute(payload)

	msg := message.Message{
		From:        from,
		To:          to,
		Content:     contentRef,
		ContentType: contentType,
		Timestamp:   timestamp,
		Signature:   &sig,
	}

	ok, err := message.Verify(msg)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrInvalidSignature
	}

	return &Envelope{
		Message:    msg,
		Origin:     origin,
		HopCount:   hopCount,
		Metadata:   meta,
		Dimensions: dims,
	}, nil
}
