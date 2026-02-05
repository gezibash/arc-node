package blob

const (
	// InlineThreshold is the max size for inline blob responses via relay.
	// Blobs larger than this are transferred via direct gRPC connection.
	InlineThreshold = 1024 * 1024 // 1MB

	// DefaultChunkSize is the recommended chunk size for streaming.
	DefaultChunkSize = 256 * 1024 // 256KB

	// DefaultCapacity is the default total storage capacity (1GB).
	DefaultCapacity = 1 << 30 // 1GB

	// DefaultMaxBlobSize is the default maximum size for a single blob (64MB).
	DefaultMaxBlobSize = 64 * 1024 * 1024 // 64MB
)
