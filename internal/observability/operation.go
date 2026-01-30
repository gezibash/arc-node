package observability

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Operation tracks a high-level operation with span, metrics, and logging.
type Operation struct {
	ctx     context.Context
	span    trace.Span
	metrics *Metrics
	name    string
	start   time.Time
	logger  *slog.Logger
}

// StartOperation begins tracking an operation with a span, logger context, and timing.
func StartOperation(ctx context.Context, m *Metrics, name string, attrs ...attribute.KeyValue) (*Operation, context.Context) {
	ctx, span := StartSpan(ctx, name, attrs...)
	logger := slog.Default().With("operation", name)
	logger.InfoContext(ctx, "operation started")

	return &Operation{
		ctx:     ctx,
		span:    span,
		metrics: m,
		name:    name,
		start:   time.Now(),
		logger:  logger,
	}, ctx
}

// End finishes the operation, recording duration and status.
func (o *Operation) End(err error) {
	duration := time.Since(o.start).Seconds()
	status := "ok"
	if err != nil {
		status = "error"
		o.logger.ErrorContext(o.ctx, "operation failed", "error", err, "duration", duration)
	} else {
		o.logger.InfoContext(o.ctx, "operation completed", "duration", duration)
	}

	EndSpan(o.span, err)
	o.metrics.OperationDuration.WithLabelValues(o.name, status).Observe(duration)
	o.metrics.OperationTotal.WithLabelValues(o.name, status).Inc()
}
