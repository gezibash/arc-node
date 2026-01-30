// Package storage provides shared utilities for storage backends.
package storage

import "fmt"

// ConfigError represents a configuration error for a storage backend.
type ConfigError struct {
	Backend string
	Field   string
	Value   string
	Message string
	Cause   error
}

func (e *ConfigError) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("%s: %s", e.Backend, e.Message)
	}
	if e.Value == "" {
		return fmt.Sprintf("%s: %s: %s", e.Backend, e.Field, e.Message)
	}
	return fmt.Sprintf("%s: %s=%q: %s", e.Backend, e.Field, e.Value, e.Message)
}

func (e *ConfigError) Unwrap() error {
	return e.Cause
}

// NewConfigError creates a new ConfigError for a field validation failure.
func NewConfigError(backend, field, message string) *ConfigError {
	return &ConfigError{Backend: backend, Field: field, Message: message}
}

// NewConfigErrorWithValue creates a ConfigError that includes the invalid value.
func NewConfigErrorWithValue(backend, field, value, message string) *ConfigError {
	return &ConfigError{Backend: backend, Field: field, Value: value, Message: message}
}

// NewConfigErrorWithCause creates a ConfigError with an underlying cause.
func NewConfigErrorWithCause(backend, field, message string, cause error) *ConfigError {
	return &ConfigError{Backend: backend, Field: field, Message: message, Cause: cause}
}
