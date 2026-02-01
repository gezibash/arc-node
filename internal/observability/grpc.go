package observability

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// StreamServerInterceptor returns a gRPC stream interceptor that creates spans and tracks messages.
func StreamServerInterceptor(m *Metrics) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := extractTraceContext(ss.Context())
		ctx, span := otel.Tracer("grpc").Start(ctx, info.FullMethod, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()

		start := time.Now()
		wrapped := &wrappedStream{ServerStream: ss, ctx: ctx}
		err := handler(srv, wrapped)
		duration := time.Since(start).Seconds()

		st, _ := status.FromError(err)
		code := st.Code().String()

		span.SetAttributes(
			attribute.String("rpc.grpc.status_code", code),
			attribute.Int64("rpc.messages_sent", wrapped.sent.Load()),
			attribute.Int64("rpc.messages_received", wrapped.recv.Load()),
		)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}

		m.OperationDuration.WithLabelValues(info.FullMethod, code).Observe(duration)
		m.OperationTotal.WithLabelValues(info.FullMethod, code).Inc()

		return err
	}
}

func extractTraceContext(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	prop := otel.GetTextMapPropagator()
	if prop == nil {
		prop = propagation.TraceContext{}
	}
	return prop.Extract(ctx, propagation.HeaderCarrier(md))
}

type wrappedStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent atomic.Int64
	recv atomic.Int64
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

func (w *wrappedStream) SendMsg(m any) error {
	err := w.ServerStream.SendMsg(m)
	if err == nil {
		w.sent.Add(1)
	}
	return err
}

func (w *wrappedStream) RecvMsg(m any) error {
	err := w.ServerStream.RecvMsg(m)
	if err == nil {
		w.recv.Add(1)
	}
	return err
}
