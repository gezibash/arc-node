package gossip

// Config holds gossip cluster configuration.
type Config struct {
	// NodeName is this relay's unique name in the cluster.
	// Defaults to a BIP-39 petname derived from RelayPubkey (e.g., "leader-monkey-parrot").
	NodeName string

	// BindAddr is the address to bind for gossip (default: "0.0.0.0").
	BindAddr string

	// BindPort is the port for gossip (default: 7946).
	BindPort int

	// AdvertiseAddr is the address to advertise to other nodes (for containers/NAT).
	AdvertiseAddr string

	// AdvertisePort is the port to advertise (0 = same as BindPort).
	AdvertisePort int

	// Seeds are the addresses of peers to join on startup.
	Seeds []string

	// GRPCAddr is this relay's gRPC address, advertised to the cluster.
	GRPCAddr string

	// RelayPubkey is this relay's hex-encoded public key.
	RelayPubkey string
}
