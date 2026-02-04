package relay

import (
	"strings"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
)

// Router finds subscribers for an envelope based on labels.
// Routing order: addressed → label-match.
type Router struct {
	table *Table
}

// NewRouter creates a router using the given table.
func NewRouter(table *Table) *Router {
	return &Router{table: table}
}

// Route finds all subscribers that should receive the envelope.
// Returns the matched subscribers and the routing mode used.
func (r *Router) Route(env *relayv1.Envelope) ([]*Subscriber, RouteMode) {
	labels := env.GetLabels()
	if labels == nil {
		return nil, RouteModeNone
	}

	// 1. Addressed routing: "to" label
	if to := labels["to"]; to != "" {
		// Check if it's a pubkey (64 hex chars) or a name
		if looksLikePubkey(to) {
			pubkey, err := identity.DecodePublicKey(to)
			if err == nil {
				canonical := identity.EncodePublicKey(pubkey)
				if s, ok := r.table.LookupPubkey(canonical); ok {
					return []*Subscriber{s}, RouteModeAddressed
				}
			}
		} else {
			if s, ok := r.table.LookupName(to); ok {
				return []*Subscriber{s}, RouteModeAddressed
			}
		}
		return nil, RouteModeAddressed // no match for addressed
	}

	// 2. Label-match routing (includes capabilities)
	subs := r.table.LookupLabels(labels)
	return subs, RouteModeLabelMatch
}

// looksLikePubkey returns true if the string is a 64-char hex string (Ed25519 pubkey).
func looksLikePubkey(s string) bool {
	if strings.Contains(s, ":") {
		_, err := identity.DecodePublicKey(s)
		return err == nil
	}
	if len(s) < 64 {
		return false
	}
	_, err := identity.DecodePublicKey(s)
	return err == nil
}

// RouteMode indicates which routing path was used.
type RouteMode int

const (
	RouteModeNone       RouteMode = iota
	RouteModeAddressed            // "to" label → single recipient
	RouteModeLabelMatch           // subscription filters (includes capabilities)
)

func (m RouteMode) String() string {
	switch m {
	case RouteModeAddressed:
		return "addressed"
	case RouteModeLabelMatch:
		return "label-match"
	default:
		return "none"
	}
}
