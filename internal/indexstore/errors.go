// Package indexstore provides indexed storage with CEL queries.
package indexstore

import "errors"

var (
	// ErrNotFound indicates the requested entry was not found.
	ErrNotFound = errors.New("entry not found")

	// ErrInvalidExpression indicates the CEL expression is invalid.
	ErrInvalidExpression = errors.New("invalid CEL expression")

	// ErrSubscriptionClosed indicates the subscription has been closed.
	ErrSubscriptionClosed = errors.New("subscription closed")

	// ErrAmbiguousPrefix indicates the prefix matches multiple entries.
	ErrAmbiguousPrefix = errors.New("ambiguous prefix")

	// ErrPrefixTooShort indicates the prefix is too short (minimum 4 characters).
	ErrPrefixTooShort = errors.New("prefix too short")
)
