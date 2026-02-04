package blobstore

import (
	"context"
	"encoding/hex"
	"fmt"

	blobv1 "github.com/gezibash/arc/v2/api/arc/blob/v1"
	"github.com/gezibash/arc/v2/pkg/blob"
	"github.com/gezibash/arc/v2/pkg/capability"
	"github.com/gezibash/arc/v2/pkg/logging"
	"google.golang.org/protobuf/proto"
)

// BlobHandler implements capability.Handler for blob storage.
type BlobHandler struct {
	store      BlobStore
	listenAddr string
	log        *logging.Logger
}

// NewBlobHandler creates a new blob handler.
func NewBlobHandler(store BlobStore, listenAddr string, log *logging.Logger) *BlobHandler {
	if log == nil {
		log = logging.New(nil)
	}
	return &BlobHandler{
		store:      store,
		listenAddr: listenAddr,
		log:        log,
	}
}

// Handle implements capability.Handler.
func (h *BlobHandler) Handle(ctx context.Context, env *capability.Envelope) (*capability.Response, error) {
	op := blob.Op(env.Labels[blob.LabelOp])
	cidHex := env.Labels[blob.LabelCID]

	log := h.log.WithPubkeyBytes("sender", env.Sender.Bytes)

	switch op {
	case blob.OpPut:
		log.Debug("handling put request")
		return h.handlePut(env)
	case blob.OpWant:
		log.Debug("handling want request", "cid", cidHex)
		return h.handleWant(cidHex)
	case blob.OpGet:
		log.Debug("handling get request", "cid", cidHex)
		return h.handleGet(cidHex)
	default:
		log.Warn("unknown op", "op", op)
		return capability.NoReply(), nil
	}
}

// handlePut stores a blob (inline or prepares for redirect).
func (h *BlobHandler) handlePut(env *capability.Envelope) (*capability.Response, error) {
	var req blobv1.BlobRequest
	if err := proto.Unmarshal(env.Payload, &req); err != nil {
		return h.errorResponse(400, "invalid request payload"), nil
	}

	put := req.GetPut()
	if put == nil {
		return h.errorResponse(400, "expected put request"), nil
	}

	// Small blob: store inline
	if len(put.Data) > 0 && len(put.Data) <= blob.InlineThreshold {
		cid, err := h.store.Put(put.Data)
		if err != nil {
			return h.errorResponse(500, fmt.Sprintf("store failed: %v", err)), nil
		}
		resp := &blobv1.BlobResponse{
			Response: &blobv1.BlobResponse_Stored{
				Stored: &blobv1.StoredResponse{
					Cid:  cid[:],
					Size: int64(len(put.Data)),
				},
			},
		}
		return h.protoResponse(resp)
	}

	// Large blob: redirect to direct connection
	resp := &blobv1.BlobResponse{
		Response: &blobv1.BlobResponse_Redirect{
			Redirect: &blobv1.RedirectResponse{
				Address: h.listenAddr,
			},
		},
	}
	return h.protoResponse(resp)
}

// handleWant checks if we have a blob and responds if we do.
func (h *BlobHandler) handleWant(cidHex string) (*capability.Response, error) {
	cid, err := cidFromHex(cidHex)
	if err != nil {
		return capability.NoReply(), nil // Invalid CID, ignore
	}

	if !h.store.Has(cid) {
		return capability.NoReply(), nil // We don't have it, stay silent
	}

	size := h.store.Size(cid)

	resp := &blobv1.BlobResponse{
		Response: &blobv1.BlobResponse_Have{
			Have: &blobv1.HaveResponse{
				Cid:     cid,
				Size:    size,
				Mode:    blobv1.ConnectionMode_CONNECTION_MODE_DIRECT,
				Address: h.listenAddr,
			},
		},
	}
	return h.protoResponse(resp)
}

// handleGet retrieves a blob (inline for small, redirect for large).
func (h *BlobHandler) handleGet(cidHex string) (*capability.Response, error) {
	cid, err := cidFromHex(cidHex)
	if err != nil {
		return h.errorResponse(400, "invalid CID"), nil
	}

	if !h.store.Has(cid) {
		return h.errorResponse(404, "blob not found"), nil
	}

	size := h.store.Size(cid)

	// Small blob: return inline
	if size <= blob.InlineThreshold {
		data, err := h.store.Get(cid)
		if err != nil {
			return h.errorResponse(500, fmt.Sprintf("read failed: %v", err)), nil
		}
		resp := &blobv1.BlobResponse{
			Response: &blobv1.BlobResponse_Data{
				Data: &blobv1.DataResponse{
					Cid:  cid,
					Data: data,
				},
			},
		}
		return h.protoResponse(resp)
	}

	// Large blob: redirect
	resp := &blobv1.BlobResponse{
		Response: &blobv1.BlobResponse_Redirect{
			Redirect: &blobv1.RedirectResponse{
				Address: h.listenAddr,
				Cid:     cid,
			},
		},
	}
	return h.protoResponse(resp)
}

// errorResponse creates an error response.
func (h *BlobHandler) errorResponse(code int32, message string) *capability.Response {
	resp := &blobv1.BlobResponse{
		Response: &blobv1.BlobResponse_Error{
			Error: &blobv1.ErrorResponse{
				Code:    code,
				Message: message,
			},
		},
	}
	payload, _ := proto.Marshal(resp)
	return &capability.Response{
		Payload: payload,
		Labels:  map[string]string{"error": message},
	}
}

// protoResponse marshals a protobuf response.
func (h *BlobHandler) protoResponse(resp *blobv1.BlobResponse) (*capability.Response, error) {
	payload, err := proto.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("marshal response: %w", err)
	}
	return capability.OK(payload), nil
}

// cidFromHex parses a hex-encoded CID.
func cidFromHex(s string) ([]byte, error) {
	if len(s) != 64 {
		return nil, fmt.Errorf("invalid CID length: %d", len(s))
	}
	return hex.DecodeString(s)
}
