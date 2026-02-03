package relay

import (
	relayv1 "github.com/gezibash/arc-node/api/arc/relay/v1"
)

// Router finds subscribers for an envelope based on labels.
// Routing order: addressed → capability → label-match.
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
		if s, ok := r.table.LookupName(to); ok {
			return []*Subscriber{s}, RouteModeAddressed
		}
		return nil, RouteModeAddressed // no match for addressed
	}

	// 2. Capability routing: "capability" label
	if cap := labels["capability"]; cap != "" {
		subs := r.table.LookupCapability(cap)
		return subs, RouteModeCapability
	}

	// 3. Label-match routing: subscription filters
	subs := r.table.LookupLabels(labels)
	return subs, RouteModeLabelMatch
}

// RouteMode indicates which routing path was used.
type RouteMode int

const (
	RouteModeNone       RouteMode = iota
	RouteModeAddressed            // "to" label → single recipient
	RouteModeCapability           // "capability" label → all providers
	RouteModeLabelMatch           // subscription filters
)

func (m RouteMode) String() string {
	switch m {
	case RouteModeAddressed:
		return "addressed"
	case RouteModeCapability:
		return "capability"
	case RouteModeLabelMatch:
		return "label-match"
	default:
		return "none"
	}
}
