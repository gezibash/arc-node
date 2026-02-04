// Package logging provides shared logging utilities for arc services.
package logging

import (
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/gezibash/arc/v2/pkg/identity"
)

// Logger wraps slog.Logger with arc-specific helpers.
type Logger struct {
	base  *slog.Logger
	attrs []slog.Attr
}

// Setup initializes logging with the given level and format, writing to stdout.
// Valid levels: debug, info, warn, error. Valid formats: json, text.
// Returns the configured Logger and sets it as the slog default.
func Setup(level, format string) *Logger {
	return SetupWriter(level, format, os.Stdout)
}

// SetupWriter initializes logging with the given level, format, and writer.
// Valid levels: debug, info, warn, error. Valid formats: json, text.
// Returns the configured Logger and sets it as the slog default.
func SetupWriter(level, format string, w io.Writer) *Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}

	var handler slog.Handler
	if strings.ToLower(format) == "json" {
		handler = slog.NewJSONHandler(w, opts)
	} else {
		handler = slog.NewTextHandler(w, opts)
	}

	base := slog.New(handler)
	slog.SetDefault(base)
	return &Logger{base: base}
}

// New creates a new Logger wrapping the given slog.Logger.
// If base is nil, uses slog.Default().
func New(base *slog.Logger) *Logger {
	if base == nil {
		base = slog.Default()
	}
	return &Logger{base: base}
}

// With returns a new Logger with the given attributes.
func (l *Logger) With(attrs ...slog.Attr) *Logger {
	newAttrs := make([]slog.Attr, len(l.attrs), len(l.attrs)+len(attrs))
	copy(newAttrs, l.attrs)
	newAttrs = append(newAttrs, attrs...)
	return &Logger{base: l.base, attrs: newAttrs}
}

// WithPubkey adds a formatted public key attribute.
func (l *Logger) WithPubkey(key string, pk identity.PublicKey) *Logger {
	return l.With(slog.String(key, FormatPubkey(pk)))
}

// WithPubkeyBytes adds a formatted public key attribute from bytes.
func (l *Logger) WithPubkeyBytes(key string, pk []byte) *Logger {
	return l.With(slog.String(key, FormatPubkeyBytes(pk)))
}

// WithLabels adds a labels map attribute.
func (l *Logger) WithLabels(key string, labels map[string]string) *Logger {
	return l.With(slog.Any(key, labels))
}

// WithCorrelation adds a correlation ID attribute.
func (l *Logger) WithCorrelation(id string) *Logger {
	return l.With(slog.String("correlation", id))
}

// WithComponent adds a component name attribute.
func (l *Logger) WithComponent(name string) *Logger {
	return l.With(slog.String("component", name))
}

// WithError adds an error attribute.
func (l *Logger) WithError(err error) *Logger {
	return l.With(slog.String("error", err.Error()))
}

// Debug logs at debug level.
func (l *Logger) Debug(msg string, args ...any) {
	l.log(context.Background(), slog.LevelDebug, msg, args...)
}

// Info logs at info level.
func (l *Logger) Info(msg string, args ...any) {
	l.log(context.Background(), slog.LevelInfo, msg, args...)
}

// Warn logs at warn level.
func (l *Logger) Warn(msg string, args ...any) {
	l.log(context.Background(), slog.LevelWarn, msg, args...)
}

// Error logs at error level.
func (l *Logger) Error(msg string, args ...any) {
	l.log(context.Background(), slog.LevelError, msg, args...)
}

// DebugContext logs at debug level with context.
func (l *Logger) DebugContext(ctx context.Context, msg string, args ...any) {
	l.log(ctx, slog.LevelDebug, msg, args...)
}

// InfoContext logs at info level with context.
func (l *Logger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.log(ctx, slog.LevelInfo, msg, args...)
}

// WarnContext logs at warn level with context.
func (l *Logger) WarnContext(ctx context.Context, msg string, args ...any) {
	l.log(ctx, slog.LevelWarn, msg, args...)
}

// ErrorContext logs at error level with context.
func (l *Logger) ErrorContext(ctx context.Context, msg string, args ...any) {
	l.log(ctx, slog.LevelError, msg, args...)
}

func (l *Logger) log(ctx context.Context, level slog.Level, msg string, args ...any) {
	// Convert attrs to args
	allArgs := make([]any, 0, len(l.attrs)*2+len(args))
	for _, attr := range l.attrs {
		allArgs = append(allArgs, attr.Key, attr.Value.Any())
	}
	allArgs = append(allArgs, args...)
	l.base.Log(ctx, level, msg, allArgs...)
}

// Slog returns the underlying slog.Logger for compatibility.
func (l *Logger) Slog() *slog.Logger {
	return l.base
}

// --- Formatting helpers ---

// FormatPubkey returns a shortened hex representation of a public key.
func FormatPubkey(pk identity.PublicKey) string {
	return FormatPubkeyBytes(pk.Bytes)
}

// FormatPubkeyBytes returns a shortened hex representation from bytes.
func FormatPubkeyBytes(pk []byte) string {
	if len(pk) < 8 {
		return hex.EncodeToString(pk)
	}
	return hex.EncodeToString(pk[:8]) + "..."
}

// FormatPubkeyHex shortens an already hex-encoded public key.
func FormatPubkeyHex(hexPk string) string {
	if len(hexPk) <= 16 {
		return hexPk
	}
	return hexPk[:16] + "..."
}
