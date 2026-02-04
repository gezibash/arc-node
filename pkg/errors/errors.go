// Package errors provides shared sentinel errors used throughout the arc ecosystem.
package errors

import stderrors "errors"

var (
	// ErrNotFound indicates the requested resource was not found.
	ErrNotFound = stderrors.New("not found")

	// ErrClosed indicates the resource has been closed.
	ErrClosed = stderrors.New("closed")

	// ErrInvalidInput indicates the input is invalid.
	ErrInvalidInput = stderrors.New("invalid input")

	// ErrAlreadyExists indicates the resource already exists.
	ErrAlreadyExists = stderrors.New("already exists")

	// ErrNotConnected indicates a required connection is not established.
	ErrNotConnected = stderrors.New("not connected")

	// ErrTimeout indicates an operation timed out.
	ErrTimeout = stderrors.New("timeout")

	// ErrBufferFull indicates a buffer is at capacity.
	ErrBufferFull = stderrors.New("buffer full")
)
