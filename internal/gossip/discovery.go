package gossip

import (
	"log/slog"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/internal/names"
)

// DiscoverRemote finds capability providers on remote relays.
// Excludes entries from the local relay (those are handled by local discovery).
// Implements relay.RemoteDiscovery.
func (g *Gossip) DiscoverRemote(filter map[string]string, limit int) []*relayv1.ProviderInfo {
	entries := g.capabilities.Match(filter, limit+100) // over-fetch to account for local filtering
	slog.Debug("gossip remote discovery",
		"component", "gossip",
		"filter", filter,
		"limit", limit,
		"matched_entries", len(entries),
		"local_name", g.localName,
	)
	return g.entriesToProviders(entries, limit)
}

// DiscoverRemoteCEL finds capability providers on remote relays using a CEL expression.
// Implements relay.RemoteDiscovery.
func (g *Gossip) DiscoverRemoteCEL(expr string, limit int) []*relayv1.ProviderInfo {
	entries, err := g.capabilities.MatchCEL(expr, limit+100)
	if err != nil {
		return nil
	}
	return g.entriesToProviders(entries, limit)
}

func (g *Gossip) entriesToProviders(entries []*CapabilityEntry, limit int) []*relayv1.ProviderInfo {
	if limit <= 0 {
		limit = 100 // default limit
	}
	var providers []*relayv1.ProviderInfo
	for _, entry := range entries {
		// Skip local relay entries — those come from local table discovery
		if entry.RelayName == g.localName {
			slog.Debug("skipping local entry",
				"component", "gossip",
				"relay", entry.RelayName,
				"provider", entry.ProviderPubkey[:16],
			)
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
			Labels:              anyToStringMap(entry.Labels),
			TypedLabels:         anyToProtoMap(entry.Labels),
			State:               anyToProtoMap(entry.State),
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

// anyToStringMap extracts only string-valued entries from a typed map.
func anyToStringMap(m map[string]any) map[string]string {
	result := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			result[k] = s
		}
	}
	return result
}

// anyToProtoMap converts a map[string]any to protobuf typed_labels map.
func anyToProtoMap(m map[string]any) map[string]*relayv1.LabelValue {
	if len(m) == 0 {
		return nil
	}
	result := make(map[string]*relayv1.LabelValue, len(m))
	for k, v := range m {
		result[k] = anyToProtoValue(v)
	}
	return result
}

func anyToProtoValue(v any) *relayv1.LabelValue {
	switch val := v.(type) {
	case string:
		return &relayv1.LabelValue{Value: &relayv1.LabelValue_StringValue{StringValue: val}}
	case int64:
		return &relayv1.LabelValue{Value: &relayv1.LabelValue_IntValue{IntValue: val}}
	case float64:
		return &relayv1.LabelValue{Value: &relayv1.LabelValue_DoubleValue{DoubleValue: val}}
	case bool:
		return &relayv1.LabelValue{Value: &relayv1.LabelValue_BoolValue{BoolValue: val}}
	default:
		return &relayv1.LabelValue{Value: &relayv1.LabelValue_StringValue{StringValue: ""}}
	}
}
