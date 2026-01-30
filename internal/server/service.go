package server

import (
	"context"
	"errors"
	"fmt"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/internal/blobstore"
	"github.com/gezibash/arc-node/internal/envelope"
	"github.com/gezibash/arc-node/internal/indexstore"
	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/internal/observability"
	"github.com/gezibash/arc/pkg/message"
	"github.com/gezibash/arc/pkg/reference"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nodeService struct {
	nodev1.UnimplementedNodeServiceServer
	blobs   *blobstore.BlobStore
	index   *indexstore.IndexStore
	metrics *observability.Metrics
}

func (s *nodeService) PutContent(ctx context.Context, req *nodev1.PutContentRequest) (*nodev1.PutContentResponse, error) {
	ref, err := s.blobs.Store(ctx, req.Data)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "store content: %v", err)
	}
	return &nodev1.PutContentResponse{Reference: ref[:]}, nil
}

func (s *nodeService) GetContent(ctx context.Context, req *nodev1.GetContentRequest) (*nodev1.GetContentResponse, error) {
	ref, err := referenceFromBytes(req.Reference)
	if err != nil {
		return nil, err
	}
	data, err := s.blobs.Fetch(ctx, ref)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "fetch content: %v", err)
	}
	return &nodev1.GetContentResponse{Data: data}, nil
}

func (s *nodeService) SendMessage(ctx context.Context, req *nodev1.SendMessageRequest) (*nodev1.SendMessageResponse, error) {
	if len(req.Message) == 0 {
		return nil, status.Error(codes.InvalidArgument, "message required")
	}

	// Deserialize canonical bytes
	msg, err := message.FromCanonicalBytes(req.Message)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse message: %v", err)
	}

	// Verify signature using arc protocol
	ok, err := message.Verify(msg)
	if err != nil || !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid signature")
	}

	// Verify the envelope signer matches the message author.
	if caller, hasCaller := envelope.GetCaller(ctx); hasCaller {
		if caller.PublicKey != msg.From {
			return nil, status.Error(codes.PermissionDenied, "envelope signer does not match message author")
		}
	}

	// Compute message reference
	msgRef, err := message.Ref(msg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "compute reference: %v", err)
	}

	// Build labels
	labels := make(map[string]string)
	for k, v := range req.Labels {
		labels[k] = v
	}
	labels["from"] = reference.Hex(reference.Reference(msg.From))
	labels["to"] = reference.Hex(reference.Reference(msg.To))
	labels["content"] = reference.Hex(msg.Content)
	if msg.ContentType != "" {
		labels["contentType"] = msg.ContentType
	}

	entry := &physical.Entry{
		Ref:       msgRef,
		Labels:    labels,
		Timestamp: msg.Timestamp,
	}

	if err := s.index.Index(ctx, entry); err != nil {
		return nil, status.Errorf(codes.Internal, "index message: %v", err)
	}

	return &nodev1.SendMessageResponse{Reference: msgRef[:]}, nil
}

func (s *nodeService) QueryMessages(ctx context.Context, req *nodev1.QueryMessagesRequest) (*nodev1.QueryMessagesResponse, error) {
	opts := &indexstore.QueryOptions{
		Expression: req.Expression,
		Labels:     req.Labels,
		Limit:      int(req.Limit),
		Cursor:     req.Cursor,
		Descending: req.Order == nodev1.Order_ORDER_DESCENDING,
	}

	result, err := s.index.Query(ctx, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query: %v", err)
	}

	entries := make([]*nodev1.IndexEntry, len(result.Entries))
	for i, e := range result.Entries {
		entries[i] = &nodev1.IndexEntry{
			Reference: e.Ref[:],
			Labels:    e.Labels,
			Timestamp: e.Timestamp,
		}
	}

	return &nodev1.QueryMessagesResponse{
		Entries:    entries,
		NextCursor: result.NextCursor,
		HasMore:    result.HasMore,
	}, nil
}

func (s *nodeService) SubscribeMessages(req *nodev1.SubscribeMessagesRequest, stream nodev1.NodeService_SubscribeMessagesServer) error {
	// Build combined expression from labels and expression
	expr := req.Expression
	if expr == "" {
		expr = "true"
	}

	// Add label constraints to expression
	for k, v := range req.Labels {
		expr = fmt.Sprintf(`%s && "%s" in labels && labels["%s"] == "%s"`, expr, k, k, v)
	}

	sub, err := s.index.Subscribe(stream.Context(), expr)
	if err != nil {
		return status.Errorf(codes.Internal, "subscribe: %v", err)
	}
	defer sub.Cancel()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case entry, ok := <-sub.Entries():
			if !ok {
				if err := sub.Err(); err != nil {
					return status.Errorf(codes.Internal, "subscription error: %v", err)
				}
				return nil
			}
			resp := &nodev1.SubscribeMessagesResponse{
				Entry: &nodev1.IndexEntry{
					Reference: entry.Ref[:],
					Labels:    entry.Labels,
					Timestamp: entry.Timestamp,
				},
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

func (s *nodeService) ResolveGet(ctx context.Context, req *nodev1.ResolveGetRequest) (*nodev1.ResolveGetResponse, error) {
	if req.Prefix == "" {
		return nil, status.Error(codes.InvalidArgument, "prefix required")
	}

	// Try blob store first.
	ref, err := s.blobs.ResolvePrefix(ctx, req.Prefix)
	if err == nil {
		data, fetchErr := s.blobs.Fetch(ctx, ref)
		if fetchErr != nil {
			return nil, status.Errorf(codes.Internal, "fetch blob: %v", fetchErr)
		}
		return &nodev1.ResolveGetResponse{
			Kind:      nodev1.ResolveGetResponse_KIND_BLOB,
			Reference: ref[:],
			Data:      data,
		}, nil
	}

	// If blob prefix was ambiguous or too short, return immediately.
	if errors.Is(err, blobstore.ErrAmbiguousPrefix) {
		return nil, status.Errorf(codes.FailedPrecondition, "prefix %q matches multiple blobs", req.Prefix)
	}
	if errors.Is(err, blobstore.ErrPrefixTooShort) {
		return nil, status.Errorf(codes.InvalidArgument, "prefix too short (minimum 4 characters)")
	}

	// Blob not found — try index store.
	ref, indexErr := s.index.ResolvePrefix(ctx, req.Prefix)
	if indexErr == nil {
		entry, getErr := s.index.Get(ctx, ref)
		if getErr != nil {
			return nil, status.Errorf(codes.Internal, "get index entry: %v", getErr)
		}
		return &nodev1.ResolveGetResponse{
			Kind:      nodev1.ResolveGetResponse_KIND_MESSAGE,
			Reference: ref[:],
			Labels:    entry.Labels,
			Timestamp: entry.Timestamp,
		}, nil
	}

	if errors.Is(indexErr, indexstore.ErrAmbiguousPrefix) {
		return nil, status.Errorf(codes.FailedPrecondition, "prefix %q matches multiple entries", req.Prefix)
	}

	return nil, status.Errorf(codes.NotFound, "no blob or message matches prefix %q", req.Prefix)
}

func referenceFromBytes(b []byte) (reference.Reference, error) {
	if len(b) != 32 {
		return reference.Reference{}, status.Errorf(codes.InvalidArgument, "reference must be 32 bytes, got %d", len(b))
	}
	var ref reference.Reference
	copy(ref[:], b)
	return ref, nil
}
