package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/internal/blobstore"
	"github.com/gezibash/arc-node/internal/indexstore"
	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/internal/observability"
	"github.com/gezibash/arc-node/pkg/envelope"
	"github.com/gezibash/arc-node/pkg/group"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nodeService struct {
	nodev1.UnimplementedNodeServiceServer
	blobs       *blobstore.BlobStore
	index       *indexstore.IndexStore
	metrics     *observability.Metrics
	federator   *federationManager
	subscribers *subscriberTracker
	visCheck    func(entry *physical.Entry, callerKey [32]byte) bool
	groupCache  *groupCache
	adminKey    identity.PublicKey
}

func (s *nodeService) doPut(ctx context.Context, data []byte) (reference.Reference, error) {
	ref, err := s.blobs.Store(ctx, data)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("store content: %w", err)
	}
	return ref, nil
}

func (s *nodeService) doGet(ctx context.Context, ref reference.Reference) ([]byte, error) {
	data, err := s.blobs.Fetch(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("fetch content: %w", err)
	}
	return data, nil
}

func (s *nodeService) doPublish(ctx context.Context, msgBytes []byte, labels map[string]string, dims *nodev1.Dimensions) (reference.Reference, error) {
	if len(msgBytes) == 0 {
		return reference.Reference{}, fmt.Errorf("message required")
	}

	msg, err := message.FromCanonicalBytes(msgBytes)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("parse message: %w", err)
	}

	ok, err := message.Verify(msg)
	if err != nil || !ok {
		return reference.Reference{}, fmt.Errorf("invalid signature")
	}

	if caller, hasCaller := envelope.GetCaller(ctx); hasCaller {
		if caller.PublicKey != msg.From {
			if !s.canPublishFor(ctx, caller.PublicKey, msg.From, msg.ContentType, msg.Content) {
				return reference.Reference{}, fmt.Errorf("envelope signer does not match message author")
			}
		}
	}

	msgRef, err := message.Ref(msg)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("compute reference: %w", err)
	}

	merged := make(map[string]string)
	for k, v := range labels {
		merged[k] = v
	}
	merged["from"] = reference.Hex(reference.Reference(msg.From))
	merged["to"] = reference.Hex(reference.Reference(msg.To))
	merged["content"] = reference.Hex(msg.Content)
	if msg.ContentType != "" {
		merged["contentType"] = msg.ContentType
	}

	entry := &physical.Entry{
		Ref:       msgRef,
		Labels:    merged,
		Timestamp: msg.Timestamp,
	}

	// Default to durable.
	entry.Persistence = 1
	if dims != nil {
		entry.Persistence = int32(dims.Persistence)
		entry.DeliveryMode = int32(dims.Delivery)
		entry.Visibility = int32(dims.Visibility)
		entry.Pattern = int32(dims.Pattern)
		entry.Affinity = int32(dims.Affinity)
		entry.AffinityKey = dims.AffinityKey
		entry.Ordering = int32(dims.Ordering)
		entry.DedupMode = int32(dims.Dedup)
		entry.IdempotencyKey = dims.IdempotencyKey
		entry.DeliveryComplete = int32(dims.Complete)
		entry.CompleteN = dims.CompleteN
		entry.Priority = dims.Priority
		entry.MaxRedelivery = dims.MaxRedelivery
		entry.AckTimeoutMs = dims.AckTimeoutMs
		entry.Correlation = dims.Correlation
		if dims.TtlMs > 0 {
			entry.ExpiresAt = time.Now().UnixMilli() + dims.TtlMs
		}
	} else if caller, hasDims := envelope.GetCaller(ctx); hasDims && caller.Dimensions != nil {
		d := caller.Dimensions
		entry.Persistence = int32(d.Persistence)
		entry.DeliveryMode = int32(d.Delivery)
		entry.Visibility = int32(d.Visibility)
		entry.Pattern = int32(d.Pattern)
		entry.Affinity = int32(d.Affinity)
		entry.AffinityKey = d.AffinityKey
		entry.Ordering = int32(d.Ordering)
		entry.DedupMode = int32(d.Dedup)
		entry.IdempotencyKey = d.IdempotencyKey
		entry.DeliveryComplete = int32(d.Complete)
		entry.CompleteN = d.CompleteN
		entry.Priority = d.Priority
		entry.MaxRedelivery = d.MaxRedelivery
		entry.AckTimeoutMs = d.AckTimeoutMs
		entry.Correlation = d.Correlation
		if d.TtlMs > 0 {
			entry.ExpiresAt = time.Now().UnixMilli() + d.TtlMs
		}
	}

	// Store correlation as a queryable label.
	if entry.Correlation != "" {
		entry.Labels["_correlation"] = entry.Correlation
	}

	if err := s.index.Index(ctx, entry); err != nil {
		return reference.Reference{}, fmt.Errorf("index message: %w", err)
	}

	// Auto-update group cache when a manifest is published.
	if msg.ContentType == "arc/group.manifest" {
		if data, err := s.blobs.Fetch(ctx, msg.Content); err == nil {
			if m, err := group.UnmarshalManifest(data); err == nil {
				s.groupCache.LoadManifest(m)
			}
		}
	}

	return msgRef, nil
}

func publishErrToStatus(err error) error {
	msg := err.Error()
	switch {
	case msg == "message required", strings.Contains(msg, "parse message"), msg == "invalid signature":
		return status.Error(codes.InvalidArgument, msg)
	case strings.Contains(msg, "envelope signer"):
		return status.Error(codes.PermissionDenied, msg)
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}

func (s *nodeService) doQuery(ctx context.Context, opts *indexstore.QueryOptions) (*indexstore.QueryResult, error) {
	result, err := s.index.Query(ctx, opts)
	if err != nil {
		return nil, err
	}

	// Post-filter by visibility using caller's public key.
	var callerKey [32]byte
	if caller, ok := envelope.GetCaller(ctx); ok {
		callerKey = caller.PublicKey
	}
	filtered := result.Entries[:0]
	for _, e := range result.Entries {
		if s.visCheck(e, callerKey) {
			filtered = append(filtered, e)
		}
	}
	result.Entries = filtered
	return result, nil
}

func (s *nodeService) doResolveGet(ctx context.Context, prefix string) (*nodev1.ResolveGetResponseFrame, error) {
	if prefix == "" {
		return nil, status.Error(codes.InvalidArgument, "prefix required")
	}

	// Try blob store first.
	ref, err := s.blobs.ResolvePrefix(ctx, prefix)
	if err == nil {
		data, fetchErr := s.blobs.Fetch(ctx, ref)
		if fetchErr != nil {
			return nil, status.Errorf(codes.Internal, "fetch blob: %v", fetchErr)
		}
		return &nodev1.ResolveGetResponseFrame{
			Kind:      nodev1.ResolveGetResponseFrame_KIND_BLOB,
			Reference: ref[:],
			Data:      data,
		}, nil
	}

	if errors.Is(err, blobstore.ErrAmbiguousPrefix) {
		return nil, status.Errorf(codes.FailedPrecondition, "prefix %q matches multiple blobs", prefix)
	}
	if errors.Is(err, blobstore.ErrPrefixTooShort) {
		return nil, status.Errorf(codes.InvalidArgument, "prefix too short (minimum 4 characters)")
	}

	// Blob not found — try index store.
	ref, indexErr := s.index.ResolvePrefix(ctx, prefix)
	if indexErr == nil {
		entry, getErr := s.index.Get(ctx, ref)
		if getErr != nil {
			return nil, status.Errorf(codes.Internal, "get index entry: %v", getErr)
		}
		return &nodev1.ResolveGetResponseFrame{
			Kind:      nodev1.ResolveGetResponseFrame_KIND_MESSAGE,
			Reference: ref[:],
			Labels:    entry.Labels,
			Timestamp: entry.Timestamp,
		}, nil
	}

	if errors.Is(indexErr, indexstore.ErrAmbiguousPrefix) {
		return nil, status.Errorf(codes.FailedPrecondition, "prefix %q matches multiple entries", prefix)
	}

	return nil, status.Errorf(codes.NotFound, "no blob or message matches prefix %q", prefix)
}

func entryToProtoDims(e *physical.Entry) *nodev1.Dimensions {
	dims := &nodev1.Dimensions{
		Visibility:     nodev1.Visibility(e.Visibility),
		Persistence:    nodev1.Persistence(e.Persistence),
		Delivery:       nodev1.Delivery(e.DeliveryMode),
		Pattern:        nodev1.Pattern(e.Pattern),
		Affinity:       nodev1.Affinity(e.Affinity),
		AffinityKey:    e.AffinityKey,
		Ordering:       nodev1.Ordering(e.Ordering),
		Dedup:          nodev1.Dedup(e.DedupMode),
		IdempotencyKey: e.IdempotencyKey,
		Complete:       nodev1.DeliveryComplete(e.DeliveryComplete),
		CompleteN:      e.CompleteN,
		Priority:       e.Priority,
		MaxRedelivery:  e.MaxRedelivery,
		AckTimeoutMs:   e.AckTimeoutMs,
		Correlation:    e.Correlation,
	}
	if e.ExpiresAt > 0 && e.Timestamp > 0 {
		dims.TtlMs = e.ExpiresAt - e.Timestamp
	}
	return dims
}

func entryToProto(e *physical.Entry) *nodev1.IndexEntry {
	return &nodev1.IndexEntry{
		Reference:  e.Ref[:],
		Labels:     e.Labels,
		Timestamp:  e.Timestamp,
		Dimensions: entryToProtoDims(e),
	}
}

func entriesToProto(entries []*physical.Entry) []*nodev1.IndexEntry {
	out := make([]*nodev1.IndexEntry, len(entries))
	for i, e := range entries {
		out[i] = entryToProto(e)
	}
	return out
}

func referenceFromBytes(b []byte) (reference.Reference, error) {
	if len(b) != 32 {
		return reference.Reference{}, status.Errorf(codes.InvalidArgument, "reference must be 32 bytes, got %d", len(b))
	}
	var ref reference.Reference
	copy(ref[:], b)
	return ref, nil
}

// canPublishFor checks whether callerPK is allowed to publish a message
// on behalf of authorPK (a group identity). It first checks the group cache,
// then falls back to bootstrapping from the manifest blob for the first publish.
func (s *nodeService) canPublishFor(ctx context.Context, callerPK, authorPK identity.PublicKey, contentType string, contentRef reference.Reference) bool {
	// Fast path: cache already populated from a prior manifest.
	if s.groupCache.IsMember(authorPK, callerPK) {
		return true
	}

	// Bootstrap path: only for manifest publishes where the cache is empty.
	if contentType != "arc/group.manifest" {
		return false
	}

	data, err := s.blobs.Fetch(ctx, contentRef)
	if err != nil {
		return false
	}
	m, err := group.UnmarshalManifest(data)
	if err != nil {
		return false
	}
	if m.ID != authorPK {
		return false
	}

	// Check if caller is an admin in this manifest.
	for _, mem := range m.Members {
		if mem.PublicKey == callerPK && mem.Role == group.RoleAdmin {
			s.groupCache.LoadManifest(m)
			return true
		}
	}
	return false
}

// isAdmin checks whether the caller in context matches the admin key.
func (s *nodeService) isAdmin(ctx context.Context) bool {
	caller, ok := envelope.GetCaller(ctx)
	if !ok {
		return false
	}
	return caller.PublicKey == s.adminKey
}
