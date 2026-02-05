package blob

import (
	"fmt"
	"strings"

	"github.com/gezibash/arc/v2/pkg/capability"
)

// Capability name for discovery.
const CapabilityName = "blob"

// Label keys used in blob capability discovery and routing.
const (
	LabelCapability  = "capability"
	LabelOp          = "op"
	LabelCID         = "cid"
	LabelBackend     = "backend"
	LabelRegion      = "region"
	LabelTier        = "tier"
	LabelDirect      = "direct"        // direct connection address
	LabelCapacity    = "capacity"      // total capacity in bytes (int64)
	LabelMaxBlobSize = "max_blob_size" // max single blob size in bytes (int64)
)

// State keys reported dynamically by blob servers.
const (
	StateUsedBytes = "used_bytes" // int64: total bytes stored
	StateBlobCount = "blob_count" // int64: number of blobs
)

// Op represents a blob operation.
type Op string

const (
	OpPut  Op = "put"
	OpGet  Op = "get"
	OpWant Op = "want"
)

// Backend represents a storage backend type.
type Backend string

const (
	BackendLocal   Backend = "local"   // local filesystem
	BackendAWS     Backend = "aws"     // AWS S3
	BackendSeaweed Backend = "seaweed" // SeaweedFS
	BackendMinio   Backend = "minio"   // MinIO
	BackendIPFS    Backend = "ipfs"    // IPFS
	BackendMemory  Backend = "memory"  // in-memory (testing)
)

// Tier represents storage tier (performance/cost tradeoff).
type Tier string

const (
	TierHot  Tier = "hot"  // fast access, higher cost
	TierWarm Tier = "warm" // balanced
	TierCold Tier = "cold" // slow access, lower cost
)

// Labels represents the advertised labels for a blob capability.
// Used by servers when subscribing to advertise their capabilities.
type Labels struct {
	Backend     Backend // storage backend type
	Region      string  // geographic region (e.g., "us-east-1", "eu-west")
	Tier        Tier    // storage tier
	Direct      string  // direct connection address (e.g., "localhost:50052")
	Capacity    int64   // total capacity in bytes
	MaxBlobSize int64   // max single blob size in bytes
}

// ToMap converts Labels to a typed map for subscription.
// String fields are stored as string, numeric fields as int64.
func (l Labels) ToMap() map[string]any {
	m := map[string]any{
		LabelCapability: CapabilityName,
	}
	if l.Backend != "" {
		m[LabelBackend] = string(l.Backend)
	}
	if l.Region != "" {
		m[LabelRegion] = l.Region
	}
	if l.Tier != "" {
		m[LabelTier] = string(l.Tier)
	}
	if l.Direct != "" {
		m[LabelDirect] = l.Direct
	}
	if l.Capacity > 0 {
		m[LabelCapacity] = l.Capacity
	}
	if l.MaxBlobSize > 0 {
		m[LabelMaxBlobSize] = l.MaxBlobSize
	}
	return m
}

// DiscoverFilter specifies criteria for finding blob targets.
// All fields are optional - empty fields are not included in the filter.
type DiscoverFilter struct {
	Backend     Backend // filter by storage backend
	Region      string  // filter by region
	Tier        Tier    // filter by storage tier
	Direct      bool    // filter to only targets with direct connection
	MinCapacity int64   // minimum capacity in bytes (0 = no minimum)
}

// toLabels converts the filter to capability discovery labels.
// Used for exact-match discovery (no numeric filtering).
func (f DiscoverFilter) toLabels() map[string]string {
	labels := make(map[string]string)
	if f.Backend != "" {
		labels[LabelBackend] = string(f.Backend)
	}
	if f.Region != "" {
		labels[LabelRegion] = f.Region
	}
	if f.Tier != "" {
		labels[LabelTier] = string(f.Tier)
	}
	// Note: Direct filtering is done client-side via TargetSet.WithDirect()
	return labels
}

// ByFreeCapacity sorts targets by available free capacity (most free first).
// Free = capacity - used_bytes. Targets without capacity info sort last.
func ByFreeCapacity(a, b capability.Target) bool {
	aFree := a.Int64Label(LabelCapacity) - a.Int64State(StateUsedBytes)
	bFree := b.Int64Label(LabelCapacity) - b.Int64State(StateUsedBytes)
	// Targets with no capacity info (0) sort last
	if aFree <= 0 && bFree > 0 {
		return false
	}
	if aFree > 0 && bFree <= 0 {
		return true
	}
	return aFree > bFree
}

// toExpression generates a CEL expression for filters that need numeric comparison.
// Returns empty string if no CEL-specific filtering is needed.
func (f DiscoverFilter) toExpression() string {
	parts := []string{`capability == "blob"`}
	if f.Backend != "" {
		parts = append(parts, `backend == "`+string(f.Backend)+`"`)
	}
	if f.Region != "" {
		parts = append(parts, `region == "`+f.Region+`"`)
	}
	if f.Tier != "" {
		parts = append(parts, `tier == "`+string(f.Tier)+`"`)
	}
	if f.MinCapacity > 0 {
		parts = append(parts, fmt.Sprintf("capacity >= %d", f.MinCapacity))
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return strings.Join(parts, " && ")
}
