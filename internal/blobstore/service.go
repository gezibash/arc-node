package blobstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	blobv1 "github.com/gezibash/arc/v2/api/arc/blob/v1"
	"github.com/gezibash/arc/v2/pkg/blob"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service implements the BlobService gRPC interface for direct connections.
type Service struct {
	blobv1.UnimplementedBlobServiceServer
	store BlobStore
}

// NewService creates a new blob service.
func NewService(store BlobStore) *Service {
	return &Service{store: store}
}

// Put receives a stream of chunks and stores the blob.
func (s *Service) Put(stream blobv1.BlobService_PutServer) error {
	// Collect chunks into a buffer while computing hash
	h := sha256.New()
	var buf bytes.Buffer
	w := io.MultiWriter(&buf, h)

	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recv chunk: %v", err)
		}

		if _, err := w.Write(chunk.Data); err != nil {
			return status.Errorf(codes.Internal, "write chunk: %v", err)
		}
	}

	// Store the blob
	cid, err := s.store.Put(stream.Context(), buf.Bytes())
	if err != nil {
		return status.Errorf(codes.Internal, "store blob: %v", err)
	}

	return stream.SendAndClose(&blobv1.PutResponse{
		Cid:  cid[:],
		Size: int64(buf.Len()),
	})
}

// Get streams a blob by CID.
func (s *Service) Get(req *blobv1.GetRequest, stream blobv1.BlobService_GetServer) error {
	if len(req.Cid) != 32 {
		return status.Error(codes.InvalidArgument, "invalid CID length")
	}

	r, size, err := s.store.GetReader(stream.Context(), req.Cid)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return status.Error(codes.NotFound, "blob not found")
		}
		return status.Errorf(codes.Internal, "get blob: %v", err)
	}
	defer func() { _ = r.Close() }()

	// Handle range request
	if req.Offset > 0 {
		if seeker, ok := r.(io.Seeker); ok {
			if _, err := seeker.Seek(req.Offset, io.SeekStart); err != nil {
				return status.Errorf(codes.Internal, "seek: %v", err)
			}
			size -= req.Offset
		}
	}

	// Apply limit
	var reader io.Reader = r
	if req.Limit > 0 && req.Limit < size {
		reader = io.LimitReader(r, req.Limit)
	}

	// Stream chunks
	buf := make([]byte, blob.DefaultChunkSize)
	offset := req.Offset

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if err := stream.Send(&blobv1.Chunk{
				Data:   buf[:n],
				Offset: offset,
			}); err != nil {
				return status.Errorf(codes.Internal, "send chunk: %v", err)
			}
			offset += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "read blob: %v", err)
		}
	}

	return nil
}

// VerifySignature checks if an envelope signature is valid.
// This is called before processing requests to authenticate the sender.
func VerifySignature(ctx context.Context, sender, signature, data []byte) error {
	// TODO: Implement Ed25519 signature verification
	// For now, accept all (development mode)
	_ = ctx
	_ = sender
	_ = signature
	_ = data
	return nil
}

// CIDFromHex parses a hex-encoded CID.
func CIDFromHex(s string) ([]byte, error) {
	if len(s) != 64 {
		return nil, fmt.Errorf("invalid CID length: %d", len(s))
	}
	cid := make([]byte, 32)
	for i := 0; i < 32; i++ {
		_, err := fmt.Sscanf(s[i*2:i*2+2], "%02x", &cid[i])
		if err != nil {
			return nil, fmt.Errorf("invalid hex at position %d", i*2)
		}
	}
	return cid, nil
}

// CIDToHex encodes a CID as hex string.
func CIDToHex(cid []byte) string {
	return fmt.Sprintf("%x", cid)
}
