package blob

import (
	"encoding/hex"

	"github.com/gezibash/arc/v2/pkg/capability"
)

// Request types for blob operations.
// Each type has only the valid fields for that operation.
// The config() method is internal - callers construct the typed struct,
// the framework handles conversion to wire format.

// PutReq represents a blob put operation.
type PutReq struct{}

func (r PutReq) config(payload []byte) capability.RequestConfig {
	return capability.RequestConfig{
		Capability: CapabilityName,
		Labels: map[string]string{
			LabelOp: string(OpPut),
		},
		Payload: payload,
	}
}

// GetReq represents a blob get operation.
type GetReq struct {
	CID [32]byte
}

func (r GetReq) config() capability.RequestConfig {
	return capability.RequestConfig{
		Capability: CapabilityName,
		Labels: map[string]string{
			LabelOp:  string(OpGet),
			LabelCID: hex.EncodeToString(r.CID[:]),
		},
	}
}

// WantReq represents a blob want (discovery) operation.
type WantReq struct {
	CID [32]byte
}

func (r WantReq) labels() map[string]string {
	return map[string]string{
		LabelCapability: CapabilityName,
		LabelOp:         string(OpWant),
		LabelCID:        hex.EncodeToString(r.CID[:]),
	}
}
