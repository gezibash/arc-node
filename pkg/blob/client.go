// Package blob provides a client for the blob storage capability.
package blob

import (
	"context"
	"errors"
	"fmt"
	"io"

	blobv1 "github.com/gezibash/arc/v2/api/arc/blob/v1"
	"github.com/gezibash/arc/v2/pkg/capability"
	"github.com/gezibash/arc/v2/pkg/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// Client is a blob storage client.
type Client struct {
	cap *capability.Client
}

// Discover finds blob storage targets matching the filter.
// Returns a TargetSet that can be further filtered, picked from, or iterated.
func (c *Client) Discover(ctx context.Context, filter DiscoverFilter) (*capability.TargetSet, error) {
	cfg := capability.DiscoverConfig{
		Capability: CapabilityName,
		Labels:     filter.toLabels(),
	}

	// Use CEL expression when numeric filtering is needed
	if filter.MinCapacity > 0 {
		cfg.Expression = filter.toExpression()
	}

	ts, err := c.cap.Discover(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if filter.Direct {
		return ts.WithDirect(), nil
	}
	return ts, nil
}

// NewClient creates a blob client from a runtime.
// The runtime must have the relay capability configured.
func NewClient(rt *runtime.Runtime) (*Client, error) {
	cap, err := capability.NewClient(rt)
	if err != nil {
		return nil, fmt.Errorf("create capability client: %w", err)
	}

	return &Client{cap: cap}, nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	return c.cap.Close()
}

// CapabilityClient returns the underlying capability client.
// Use for discovery operations (finding blob targets by name, listing available stores, etc.).
func (c *Client) CapabilityClient() *capability.Client {
	return c.cap
}

// PutResult is the result of a Put operation.
type PutResult struct {
	CID    [32]byte
	Size   int64
	Direct bool   // true if uploaded via direct connection
	Target string // display name of target that stored the blob
}

// PutOptions configures a Put operation.
type PutOptions struct {
	// Target is the explicit target to send to (from Discover or --to flag).
	// If nil, the client will discover and auto-select a target.
	Target *capability.Target

	// Direct forces direct gRPC upload, bypassing relay for data transfer.
	// Requires the target to support direct connections.
	Direct bool
}

// Put stores a blob using auto-discovery and selection.
// For explicit targeting, use PutTo or PutWithOptions with a Target.
func (c *Client) Put(ctx context.Context, data []byte) (*PutResult, error) {
	return c.PutWithOptions(ctx, data, PutOptions{})
}

// PutTo stores a blob to a specific target.
// The target can be from Discover() or created explicitly via capability.TargetFromPubkey.
func (c *Client) PutTo(ctx context.Context, target *capability.Target, data []byte) (*PutResult, error) {
	return c.PutWithOptions(ctx, data, PutOptions{Target: target})
}

// PutWithOptions stores a blob with configurable options.
func (c *Client) PutWithOptions(ctx context.Context, data []byte, opts PutOptions) (*PutResult, error) {
	target := opts.Target

	// If no target provided, discover one
	if target == nil {
		ts, err := c.Discover(ctx, DiscoverFilter{Direct: opts.Direct})
		if err != nil {
			return nil, fmt.Errorf("discover blob targets: %w", err)
		}
		if ts.IsEmpty() {
			return nil, fmt.Errorf("no blob targets available")
		}
		// Select best target (most free capacity)
		target = ts.Sort(ByFreeCapacity).First()
	}

	// Direct upload if requested and target supports it
	if opts.Direct {
		if !target.HasDirect() {
			return nil, fmt.Errorf("target %s does not support direct connections", target.DisplayName())
		}
		return c.putDirectTo(ctx, target, data)
	}

	// Auto mode: inline for small, direct for large
	return c.putAutoTo(ctx, target, data)
}

// ReplicatedPutOptions configures a replicated put.
type ReplicatedPutOptions struct {
	// Replicas is the number of targets to send to (N).
	Replicas int
	// MinAcks is the minimum acknowledgments needed for success (M).
	// Must be <= Replicas.
	MinAcks int
	// Direct forces direct gRPC upload for all replicas.
	Direct bool
	// Targets overrides auto-discovery. If provided, up to Replicas targets are used.
	Targets *capability.TargetSet
}

// TargetResult is the outcome for a single target in a replicated put.
type TargetResult struct {
	Target string // display name
	CID    [32]byte
	Size   int64
	Direct bool
	Err    error
}

// ReplicatedPutResult is the result of a replicated put.
type ReplicatedPutResult struct {
	CID     [32]byte // content ID (same for all successful targets)
	Size    int64
	Acked   int            // number of successful stores
	Results []TargetResult // per-target outcomes
}

// PutReplicated stores a blob to N targets, returning after M acknowledge.
func (c *Client) PutReplicated(ctx context.Context, data []byte, opts ReplicatedPutOptions) (*ReplicatedPutResult, error) {
	if opts.MinAcks <= 0 {
		opts.MinAcks = 1
	}
	if opts.Replicas < opts.MinAcks {
		opts.Replicas = opts.MinAcks
	}

	// Discover or use provided targets
	var targets []capability.Target
	if opts.Targets != nil && !opts.Targets.IsEmpty() {
		targets = opts.Targets.Sort(ByFreeCapacity).Take(opts.Replicas).All()
	} else {
		ts, err := c.Discover(ctx, DiscoverFilter{Direct: opts.Direct})
		if err != nil {
			return nil, fmt.Errorf("discover: %w", err)
		}
		targets = ts.Sort(ByFreeCapacity).Take(opts.Replicas).All()
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("no blob targets available")
	}
	if len(targets) < opts.MinAcks {
		return nil, fmt.Errorf("need %d targets but only %d available", opts.MinAcks, len(targets))
	}

	// Fan out to all targets concurrently
	type indexedResult struct {
		idx int
		tr  TargetResult
	}
	ch := make(chan indexedResult, len(targets))

	putCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i, t := range targets {
		go func(idx int, target capability.Target) {
			putOpts := PutOptions{Target: &target, Direct: opts.Direct}
			pr, err := c.PutWithOptions(putCtx, data, putOpts)
			tr := TargetResult{
				Target: target.DisplayName(),
				Err:    err,
			}
			if pr != nil {
				tr.CID = pr.CID
				tr.Size = pr.Size
				tr.Direct = pr.Direct
			}
			ch <- indexedResult{idx: idx, tr: tr}
		}(i, t)
	}

	// Collect results, succeed when MinAcks reached
	results := make([]TargetResult, len(targets))
	var acked int
	var firstCID [32]byte
	var firstSize int64

	for range targets {
		r := <-ch
		results[r.idx] = r.tr
		if r.tr.Err == nil {
			acked++
			if acked == 1 {
				firstCID = r.tr.CID
				firstSize = r.tr.Size
			}
			if acked >= opts.MinAcks {
				cancel() // cancel remaining puts (best effort — they may still complete)
			}
		}
	}

	if acked < opts.MinAcks {
		return &ReplicatedPutResult{
			CID: firstCID, Size: firstSize, Acked: acked, Results: results,
		}, fmt.Errorf("need %d acks but only got %d", opts.MinAcks, acked)
	}

	return &ReplicatedPutResult{
		CID: firstCID, Size: firstSize, Acked: acked, Results: results,
	}, nil
}

// putDirectTo uploads directly to a specific target.
// The target must have a direct address.
func (c *Client) putDirectTo(ctx context.Context, target *capability.Target, data []byte) (*PutResult, error) {
	if target.Address == "" {
		return nil, fmt.Errorf("target has no direct address")
	}

	cid, size, err := uploadDirect(ctx, target.Address, data)
	if err != nil {
		return nil, fmt.Errorf("direct upload to %s: %w", target.DisplayName(), err)
	}

	return &PutResult{
		CID:    cid,
		Size:   size,
		Direct: true,
		Target: target.DisplayName(),
	}, nil
}

// putAutoTo sends to a specific target, choosing inline or direct based on size.
func (c *Client) putAutoTo(ctx context.Context, target *capability.Target, data []byte) (*PutResult, error) {
	// Large blobs: go direct if target supports it
	if len(data) > InlineThreshold && target.HasDirect() {
		return c.putDirectTo(ctx, target, data)
	}

	// Build inline request
	req := &blobv1.BlobRequest{
		Request: &blobv1.BlobRequest_Put{
			Put: &blobv1.PutIntent{
				Data: data,
			},
		},
	}

	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	// Send PUT request to target via relay
	result, err := c.cap.RequestTo(ctx, target, PutReq{}.config(payload))
	if err != nil {
		return nil, fmt.Errorf("send to %s: %w", target.DisplayName(), err)
	}

	// Parse response
	var resp blobv1.BlobResponse
	if err := proto.Unmarshal(result.Payload, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	switch r := resp.Response.(type) {
	case *blobv1.BlobResponse_Stored:
		var cid [32]byte
		copy(cid[:], r.Stored.Cid)
		return &PutResult{
			CID:    cid,
			Size:   r.Stored.Size,
			Target: target.DisplayName(),
		}, nil

	case *blobv1.BlobResponse_Error:
		return nil, fmt.Errorf("blob error [%d]: %s", r.Error.Code, r.Error.Message)

	default:
		return nil, fmt.Errorf("unexpected response type: %T", resp.Response)
	}
}

// GetResult is the result of a Get operation.
type GetResult struct {
	Data   []byte
	Direct bool   // true if downloaded via direct connection
	Target string // display name of target that provided the blob
}

// GetOptions configures a Get operation.
type GetOptions struct {
	// Target is the explicit target to get from (from Want or --from flag).
	// If nil, the client will discover and auto-select a target.
	Target *capability.Target

	// Direct forces direct gRPC download, bypassing relay for data transfer.
	// Requires the target to support direct connections.
	Direct bool
}

// Get retrieves a blob by CID using auto-discovery.
// For explicit targeting, use GetFrom or GetWithOptions with a Target.
func (c *Client) Get(ctx context.Context, cid [32]byte) (*GetResult, error) {
	return c.GetWithOptions(ctx, cid, GetOptions{})
}

// GetFrom retrieves a blob from a specific target.
func (c *Client) GetFrom(ctx context.Context, cid [32]byte, target *capability.Target) (*GetResult, error) {
	return c.GetWithOptions(ctx, cid, GetOptions{Target: target})
}

// GetWithOptions retrieves a blob with configurable options.
func (c *Client) GetWithOptions(ctx context.Context, cid [32]byte, opts GetOptions) (*GetResult, error) {
	target := opts.Target

	// If no target provided, discover one
	if target == nil {
		ts, err := c.Discover(ctx, DiscoverFilter{Direct: opts.Direct})
		if err != nil {
			return nil, fmt.Errorf("discover blob targets: %w", err)
		}
		if ts.IsEmpty() {
			return nil, fmt.Errorf("no blob targets available")
		}
		target = ts.SortByLatency().First()
	}

	// Direct download if requested and target supports it
	if opts.Direct {
		if !target.HasDirect() {
			return nil, fmt.Errorf("target %s does not support direct connections", target.DisplayName())
		}
		data, err := downloadDirect(ctx, target.Address, cid[:])
		if err != nil {
			return nil, fmt.Errorf("direct download from %s: %w", target.DisplayName(), err)
		}
		return &GetResult{
			Data:   data,
			Direct: true,
			Target: target.DisplayName(),
		}, nil
	}

	// Request via relay
	result, err := c.cap.RequestTo(ctx, target, GetReq{CID: cid}.config())
	if err != nil {
		return nil, fmt.Errorf("send to %s: %w", target.DisplayName(), err)
	}

	// Parse response
	var resp blobv1.BlobResponse
	if err := proto.Unmarshal(result.Payload, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	switch r := resp.Response.(type) {
	case *blobv1.BlobResponse_Data:
		return &GetResult{
			Data:   r.Data.Data,
			Target: target.DisplayName(),
		}, nil

	case *blobv1.BlobResponse_Redirect:
		// Large blob: connect directly (using redirect address, not target address)
		data, err := downloadDirect(ctx, r.Redirect.Address, cid[:])
		if err != nil {
			return nil, fmt.Errorf("direct download: %w", err)
		}
		return &GetResult{
			Data:   data,
			Direct: true,
			Target: target.DisplayName(),
		}, nil

	case *blobv1.BlobResponse_Error:
		return nil, fmt.Errorf("blob error [%d]: %s", r.Error.Code, r.Error.Message)

	default:
		return nil, fmt.Errorf("unexpected response type: %T", resp.Response)
	}
}

// WantResult contains results from a Want query.
type WantResult struct {
	// Size is the blob size reported by responding targets.
	Size int64
	// Targets is the set of targets that have this blob.
	Targets *capability.TargetSet
}

// Want discovers which blob targets have a given CID.
// Broadcasts a WANT query and collects HAVE responses until context is cancelled.
// Use with a context with timeout to limit collection time.
func (c *Client) Want(ctx context.Context, cid [32]byte) (*WantResult, error) {
	// Send WANT request (broadcast to all blob targets)
	err := c.cap.SendWithLabels(ctx, WantReq{CID: cid}.labels(), nil)
	if err != nil {
		return nil, fmt.Errorf("send: %w", err)
	}

	// Collect HAVE responses until timeout
	var targets []capability.Target
	var size int64
	for {
		select {
		case <-ctx.Done():
			return &WantResult{
				Size:    size,
				Targets: capability.NewTargetSet(targets),
			}, nil
		default:
		}

		delivery, err := c.cap.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return &WantResult{
					Size:    size,
					Targets: capability.NewTargetSet(targets),
				}, nil
			}
			return nil, fmt.Errorf("receive: %w", err)
		}

		var resp blobv1.BlobResponse
		if err := proto.Unmarshal(delivery.Payload, &resp); err != nil {
			continue // Not a blob response
		}

		if have := resp.GetHave(); have != nil {
			targets = append(targets, capability.Target{
				PublicKey: delivery.Sender,
				Address:   have.Address,
			})
			if size == 0 {
				size = have.Size
			}
		}
	}
}

func uploadDirect(ctx context.Context, addr string, data []byte) ([32]byte, int64, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = conn.Close() }()

	blobClient := blobv1.NewBlobServiceClient(conn)
	stream, err := blobClient.Put(ctx)
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("put stream: %w", err)
	}

	// Send data in chunks
	const chunkSize = 64 * 1024
	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := &blobv1.Chunk{
			Data:   data[offset:end],
			Offset: int64(offset),
		}
		if err := stream.Send(chunk); err != nil {
			return [32]byte{}, 0, fmt.Errorf("send chunk: %w", err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("close stream: %w", err)
	}

	var cid [32]byte
	copy(cid[:], resp.Cid)
	return cid, resp.Size, nil
}

func downloadDirect(ctx context.Context, addr string, cid []byte) ([]byte, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = conn.Close() }()

	blobClient := blobv1.NewBlobServiceClient(conn)
	stream, err := blobClient.Get(ctx, &blobv1.GetRequest{Cid: cid})
	if err != nil {
		return nil, fmt.Errorf("get stream: %w", err)
	}

	var data []byte
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("recv chunk: %w", err)
		}
		data = append(data, chunk.Data...)
	}

	return data, nil
}
