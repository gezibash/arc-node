package gossip

import (
	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/internal/names"
)

// DiscoverRemote finds capability providers on remote relays.
// Excludes entries from the local relay (those are handled by local discovery).
// Implements relay.RemoteDiscovery.
func (g *Gossip) DiscoverRemote(filter map[string]string, limit int) []*relayv1.ProviderInfo {
	entries := g.capabilities.Match(filter, limit+100) // over-fetch to account for local filtering

	var providers []*relayv1.ProviderInfo
	for _, entry := range entries {
		// Skip local relay entries — those come from local table discovery
		if entry.RelayName == g.localName {
			continue
		}

		// Look up relay pubkey from relays section
		var relayPubkey string
		if re, ok := g.relays.Get(entry.RelayName); ok {
			relayPubkey = re.Pubkey
		}

		providers = append(providers, &relayv1.ProviderInfo{
			Pubkey:              []byte(entry.ProviderPubkey),
			Name:                entry.ProviderName,
			Labels:              entry.Labels,
			SubscriptionId:      entry.SubID,
			RelayPubkey:         []byte(relayPubkey),
			Petname:             names.PetnameFromEncoded(entry.ProviderPubkey),
			LatencyNs:           entry.LatencyNs,
			InterRelayLatencyNs: g.RelayRTT(entry.RelayName).Nanoseconds(),
		})

		if len(providers) >= limit {
			break
		}
	}
	return providers
}
