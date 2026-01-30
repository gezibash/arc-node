// Package blobstore provides content-addressed blob storage.
package blobstore

import "errors"

var (
	// ErrNotFound indicates the requested blob was not found.
	ErrNotFound = errors.New("blob not found")

	// ErrIntegrityMismatch indicates stored data doesn't match its reference.
	ErrIntegrityMismatch = errors.New("blob integrity mismatch")

	// ErrAmbiguousPrefix indicates the prefix matches multiple blobs.
	ErrAmbiguousPrefix = errors.New("prefix matches multiple blobs")

	// ErrPrefixTooShort indicates the prefix is shorter than the minimum 4 characters.
	ErrPrefixTooShort = errors.New("prefix too short (minimum 4 characters)")
)
