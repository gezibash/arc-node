package blob

// Capability name for discovery.
const CapabilityName = "blob"

// Label keys used in blob capability discovery and routing.
const (
	LabelCapability = "capability"
	LabelOp         = "op"
	LabelCID        = "cid"
	LabelBackend    = "backend"
	LabelRegion     = "region"
	LabelTier       = "tier"
	LabelDirect     = "direct" // direct connection address
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
	Backend Backend // storage backend type
	Region  string  // geographic region (e.g., "us-east-1", "eu-west")
	Tier    Tier    // storage tier
	Direct  string  // direct connection address (e.g., "localhost:50052")
}

// ToMap converts Labels to a map for subscription.
func (l Labels) ToMap() map[string]string {
	m := map[string]string{
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
	return m
}

// DiscoverFilter specifies criteria for finding blob targets.
// All fields are optional - empty fields are not included in the filter.
type DiscoverFilter struct {
	Backend Backend // filter by storage backend
	Region  string  // filter by region
	Tier    Tier    // filter by storage tier
	Direct  bool    // filter to only targets with direct connection
}

// toLabels converts the filter to capability discovery labels.
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
