package blob

const (
	// InlineThreshold is the max size for inline blob responses via relay.
	// Blobs larger than this are transferred via direct gRPC connection.
	InlineThreshold = 1024 * 1024 // 1MB

	// DefaultChunkSize is the recommended chunk size for streaming.
	DefaultChunkSize = 256 * 1024 // 256KB
)
