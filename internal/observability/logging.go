package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// SetupLogger configures the global slog logger.
func SetupLogger(level, format string, w io.Writer) *slog.Logger {
	lvl := parseLevel(level)
	opts := &slog.HandlerOptions{Level: lvl}

	var handler slog.Handler
	switch format {
	case "json":
		handler = slog.NewJSONHandler(w, opts)
	default:
		handler = NewPrettyHandler(w, opts)
	}

	handler = &TraceHandler{Handler: handler}
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// TraceHandler wraps a slog.Handler and injects trace_id/span_id from context.
type TraceHandler struct {
	slog.Handler
}

func (h *TraceHandler) Handle(ctx context.Context, r slog.Record) error {
	sc := trace.SpanContextFromContext(ctx)
	if sc.HasTraceID() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()))
	}
	if sc.HasSpanID() {
		r.AddAttrs(slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h *TraceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TraceHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *TraceHandler) WithGroup(name string) slog.Handler {
	return &TraceHandler{Handler: h.Handler.WithGroup(name)}
}

// PrettyHandler outputs colored, human-readable log lines.
type PrettyHandler struct {
	opts  slog.HandlerOptions
	w     io.Writer
	mu    sync.Mutex
	attrs []slog.Attr
	group string
}

func NewPrettyHandler(w io.Writer, opts *slog.HandlerOptions) *PrettyHandler {
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}
	return &PrettyHandler{opts: *opts, w: w}
}

func (h *PrettyHandler) Enabled(_ context.Context, level slog.Level) bool {
	minLevel := slog.LevelInfo
	if h.opts.Level != nil {
		minLevel = h.opts.Level.Level()
	}
	return level >= minLevel
}

func (h *PrettyHandler) Handle(_ context.Context, r slog.Record) error {
	ts := r.Time.Format(time.TimeOnly)
	lvl := colorLevel(r.Level)

	h.mu.Lock()
	defer h.mu.Unlock()

	_, _ = fmt.Fprintf(h.w, "%s %s %s", ts, lvl, r.Message)
	for _, a := range h.attrs {
		_, _ = fmt.Fprintf(h.w, " %s=%v", a.Key, a.Value)
	}
	r.Attrs(func(a slog.Attr) bool {
		_, _ = fmt.Fprintf(h.w, " %s=%v", a.Key, a.Value)
		return true
	})
	_, _ = fmt.Fprintln(h.w)
	return nil
}

func (h *PrettyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]slog.Attr, len(h.attrs), len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	newAttrs = append(newAttrs, attrs...)
	return &PrettyHandler{opts: h.opts, w: h.w, attrs: newAttrs, group: h.group}
}

func (h *PrettyHandler) WithGroup(name string) slog.Handler {
	return &PrettyHandler{opts: h.opts, w: h.w, attrs: h.attrs, group: name}
}

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorGray   = "\033[90m"
)

func colorLevel(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return colorRed + "ERR" + colorReset
	case l >= slog.LevelWarn:
		return colorYellow + "WRN" + colorReset
	case l >= slog.LevelInfo:
		return colorCyan + "INF" + colorReset
	default:
		return colorGray + "DBG" + colorReset
	}
}
