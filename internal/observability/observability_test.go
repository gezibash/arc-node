package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
)

// --- Shutdown Coordinator ---

func TestShutdownCoordinatorLIFO(t *testing.T) {
	t.Helper()
	var order []int
	sc := &ShutdownCoordinator{}

	for i := 1; i <= 3; i++ {
		i := i
		sc.Register(fmt.Sprintf("h%d", i), func(ctx context.Context) error {
			order = append(order, i)
			return nil
		})
	}

	if err := sc.Shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(order) != 3 || order[0] != 3 || order[1] != 2 || order[2] != 1 {
		t.Fatalf("expected LIFO [3,2,1], got %v", order)
	}
}

func TestShutdownCoordinatorEmpty(t *testing.T) {
	sc := &ShutdownCoordinator{}
	if err := sc.Shutdown(context.Background()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestShutdownCoordinatorError(t *testing.T) {
	var order []int
	sc := &ShutdownCoordinator{}

	sc.Register("first", func(ctx context.Context) error {
		order = append(order, 1)
		return nil
	})
	sc.Register("bad", func(ctx context.Context) error {
		order = append(order, 2)
		return errors.New("fail")
	})
	sc.Register("third", func(ctx context.Context) error {
		order = append(order, 3)
		return nil
	})

	err := sc.Shutdown(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "bad") {
		t.Fatalf("error should mention 'bad': %v", err)
	}
	// All three should have run (LIFO: 3, 2, 1)
	if len(order) != 3 {
		t.Fatalf("expected all 3 handlers to run, got %v", order)
	}
}

// --- Metrics ---

func TestNewMetrics(t *testing.T) {
	m := NewMetrics()
	if m.Registry == nil {
		t.Fatal("registry is nil")
	}

	// Verify metrics are registered by gathering
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("gather error: %v", err)
	}

	names := make(map[string]bool)
	for _, f := range families {
		names[f.GetName()] = true
	}

	// Use a counter to create a metric point, then verify collection works
	m.OperationTotal.WithLabelValues("test_op", "ok").Inc()

	count := testutil.ToFloat64(m.OperationTotal.WithLabelValues("test_op", "ok"))
	if count != 1 {
		t.Fatalf("expected count 1, got %f", count)
	}
}

// --- Logging ---

func TestSetupLoggerJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := SetupLogger("info", "json", &buf)

	logger.Info("hello", "key", "val")

	var entry map[string]interface{}
	if err := json.NewDecoder(&buf).Decode(&entry); err != nil {
		t.Fatalf("output not valid JSON: %v\nraw: %s", err, buf.String())
	}
	if entry["msg"] != "hello" {
		t.Fatalf("expected msg=hello, got %v", entry["msg"])
	}
}

func TestSetupLoggerText(t *testing.T) {
	var buf bytes.Buffer
	SetupLogger("info", "text", &buf)

	slog.Info("testmsg")

	out := buf.String()
	if !strings.Contains(out, "testmsg") {
		t.Fatalf("expected 'testmsg' in output: %s", out)
	}
	// Should NOT be JSON
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &m); err == nil {
		t.Fatal("expected non-JSON output for text format")
	}
}

func TestSetupLoggerLevels(t *testing.T) {
	tests := []struct {
		level      string
		logAt      slog.Level
		shouldShow bool
	}{
		{"debug", slog.LevelDebug, true},
		{"debug", slog.LevelInfo, true},
		{"info", slog.LevelDebug, false},
		{"info", slog.LevelInfo, true},
		{"warn", slog.LevelInfo, false},
		{"warn", slog.LevelWarn, true},
		{"error", slog.LevelWarn, false},
		{"error", slog.LevelError, true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%s", tt.level, tt.logAt), func(t *testing.T) {
			var buf bytes.Buffer
			logger := SetupLogger(tt.level, "json", &buf)

			logger.Log(context.Background(), tt.logAt, "test")

			got := buf.Len() > 0
			if got != tt.shouldShow {
				t.Fatalf("level=%s logAt=%s: expected visible=%v got %v", tt.level, tt.logAt, tt.shouldShow, got)
			}
		})
	}
}

// --- PrettyHandler ---

func TestPrettyHandlerEnabled(t *testing.T) {
	h := NewPrettyHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn})

	if h.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("info should be disabled at warn level")
	}
	if !h.Enabled(context.Background(), slog.LevelWarn) {
		t.Fatal("warn should be enabled at warn level")
	}
	if !h.Enabled(context.Background(), slog.LevelError) {
		t.Fatal("error should be enabled at warn level")
	}
}

func TestPrettyHandlerOutput(t *testing.T) {
	var buf bytes.Buffer
	h := NewPrettyHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(h)

	logger.Info("hello world", "foo", "bar")

	out := buf.String()
	if !strings.Contains(out, "hello world") {
		t.Fatalf("missing message in output: %s", out)
	}
	if !strings.Contains(out, "foo=bar") {
		t.Fatalf("missing attr in output: %s", out)
	}
}

// --- Operation ---

func TestStartOperationEnd(t *testing.T) {
	m := NewMetrics()
	ctx := context.Background()

	op, _ := StartOperation(ctx, m, "test_op")
	op.End(nil)

	count := testutil.ToFloat64(m.OperationTotal.WithLabelValues("test_op", "ok"))
	if count != 1 {
		t.Fatalf("expected 1 ok operation, got %f", count)
	}

	// Verify duration was recorded by gathering from registry
	families, _ := m.Registry.Gather()
	found := false
	for _, f := range families {
		if f.GetName() == "arc_operation_duration_seconds" {
			found = true
		}
	}
	if !found {
		t.Fatal("duration metric not found")
	}
}

func TestStartOperationEndError(t *testing.T) {
	m := NewMetrics()
	ctx := context.Background()

	op, _ := StartOperation(ctx, m, "fail_op")
	op.End(errors.New("boom"))

	count := testutil.ToFloat64(m.OperationTotal.WithLabelValues("fail_op", "error"))
	if count != 1 {
		t.Fatalf("expected 1 error operation, got %f", count)
	}

	okCount := testutil.ToFloat64(m.OperationTotal.WithLabelValues("fail_op", "ok"))
	if okCount != 0 {
		t.Fatalf("expected 0 ok operations, got %f", okCount)
	}
}

// --- Observability ---

func TestNewObservabilityNoOTLP(t *testing.T) {
	obs, err := New(context.Background(), ObsConfig{
		LogLevel:       "info",
		LogFormat:      "json",
		ServiceName:    "test",
		ServiceVersion: "0.0.1",
	}, io.Discard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.Logger == nil {
		t.Fatal("logger is nil")
	}
	if obs.Metrics == nil {
		t.Fatal("metrics is nil")
	}

	// Should be noop tracer provider (value or pointer)
	switch obs.TracerProvider.(type) {
	case *tracenoop.TracerProvider, tracenoop.TracerProvider:
		// ok
	default:
		t.Fatalf("expected noop tracer provider, got %T", obs.TracerProvider)
	}
}

// --- colorLevel ---

// --- gRPC Interceptors ---

type mockServerStream struct {
	grpc.ServerStream
	ctx     context.Context
	sendErr error
	recvErr error
}

func (m *mockServerStream) Context() context.Context { return m.ctx }
func (m *mockServerStream) SendMsg(msg any) error    { return m.sendErr }
func (m *mockServerStream) RecvMsg(msg any) error     { return m.recvErr }

func TestStreamServerInterceptor(t *testing.T) {
	m := NewMetrics()
	interceptor := StreamServerInterceptor(m)

	info := &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}
	ctx := context.Background()
	ss := &mockServerStream{ctx: ctx}

	handler := func(srv any, stream grpc.ServerStream) error {
		return nil
	}

	err := interceptor(nil, ss, info, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count := testutil.ToFloat64(m.OperationTotal.WithLabelValues("/test.Service/Stream", "OK"))
	if count != 1 {
		t.Fatalf("expected 1, got %f", count)
	}
}

func TestStreamServerInterceptorError(t *testing.T) {
	m := NewMetrics()
	interceptor := StreamServerInterceptor(m)

	info := &grpc.StreamServerInfo{FullMethod: "/test.Service/StreamFail"}
	ctx := context.Background()
	ss := &mockServerStream{ctx: ctx}

	handler := func(srv any, stream grpc.ServerStream) error {
		return grpcstatus.Errorf(grpccodes.Internal, "internal")
	}

	err := interceptor(nil, ss, info, handler)
	if err == nil {
		t.Fatal("expected error")
	}

	count := testutil.ToFloat64(m.OperationTotal.WithLabelValues("/test.Service/StreamFail", "Internal"))
	if count != 1 {
		t.Fatalf("expected 1, got %f", count)
	}
}

// --- wrappedStream ---

func TestWrappedStreamContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), struct{}{}, "test")
	ss := &mockServerStream{ctx: context.Background()}
	w := &wrappedStream{ServerStream: ss, ctx: ctx}

	if w.Context() != ctx {
		t.Fatal("Context() should return the wrapped context")
	}
}

func TestWrappedStreamSendMsg(t *testing.T) {
	ss := &mockServerStream{ctx: context.Background()}
	w := &wrappedStream{ServerStream: ss, ctx: context.Background()}

	if err := w.SendMsg("msg"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.sent.Load() != 1 {
		t.Fatalf("expected sent=1, got %d", w.sent.Load())
	}

	// Error case: should not increment
	ss.sendErr = errors.New("send fail")
	if err := w.SendMsg("msg"); err == nil {
		t.Fatal("expected error")
	}
	if w.sent.Load() != 1 {
		t.Fatalf("expected sent=1 (unchanged), got %d", w.sent.Load())
	}
}

func TestWrappedStreamRecvMsg(t *testing.T) {
	ss := &mockServerStream{ctx: context.Background()}
	w := &wrappedStream{ServerStream: ss, ctx: context.Background()}

	if err := w.RecvMsg(nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.recv.Load() != 1 {
		t.Fatalf("expected recv=1, got %d", w.recv.Load())
	}

	ss.recvErr = errors.New("recv fail")
	if err := w.RecvMsg(nil); err == nil {
		t.Fatal("expected error")
	}
	if w.recv.Load() != 1 {
		t.Fatalf("expected recv=1 (unchanged), got %d", w.recv.Load())
	}
}

// --- extractTraceContext ---

func TestExtractTraceContextNoMetadata(t *testing.T) {
	ctx := context.Background()
	got := extractTraceContext(ctx)
	if got != ctx {
		t.Fatal("expected same context when no metadata")
	}
}

func TestExtractTraceContextWithMetadata(t *testing.T) {
	md := metadata.New(map[string]string{"traceparent": "00-00000000000000000000000000000001-0000000000000001-01"})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	got := extractTraceContext(ctx)
	// Should not panic; context should be returned
	if got == nil {
		t.Fatal("expected non-nil context")
	}
}

// --- PrettyHandler WithAttrs / WithGroup ---

func TestPrettyHandlerWithAttrs(t *testing.T) {
	var buf bytes.Buffer
	h := NewPrettyHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})

	h2 := h.WithAttrs([]slog.Attr{slog.String("key1", "val1")})
	logger := slog.New(h2)
	logger.Info("test")

	out := buf.String()
	if !strings.Contains(out, "key1=val1") {
		t.Fatalf("expected pre-set attr in output: %s", out)
	}
}

func TestPrettyHandlerWithGroup(t *testing.T) {
	var buf bytes.Buffer
	h := NewPrettyHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})

	h2 := h.WithGroup("mygroup")
	logger := slog.New(h2)
	logger.Info("test")

	// Should not panic; group is stored
	if buf.Len() == 0 {
		t.Fatal("expected output")
	}
}

// --- TraceHandler WithAttrs / WithGroup ---

func TestTraceHandlerWithAttrs(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := &TraceHandler{Handler: inner}

	h2 := h.WithAttrs([]slog.Attr{slog.String("a", "b")})
	if _, ok := h2.(*TraceHandler); !ok {
		t.Fatalf("expected *TraceHandler, got %T", h2)
	}

	logger := slog.New(h2)
	logger.Info("test")

	if !strings.Contains(buf.String(), `"a":"b"`) {
		t.Fatalf("expected attr in output: %s", buf.String())
	}
}

func TestTraceHandlerWithGroup(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := &TraceHandler{Handler: inner}

	h2 := h.WithGroup("grp")
	if _, ok := h2.(*TraceHandler); !ok {
		t.Fatalf("expected *TraceHandler, got %T", h2)
	}
}

// --- Observability.Close ---

func TestObservabilityClose(t *testing.T) {
	obs, err := New(context.Background(), ObsConfig{
		LogLevel:       "info",
		LogFormat:      "json",
		ServiceName:    "test",
		ServiceVersion: "0.0.1",
	}, io.Discard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Register a handler to verify Close calls Shutdown
	var called bool
	obs.Shutdown.Register("test-close", func(ctx context.Context) error {
		called = true
		return nil
	})

	if err := obs.Close(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected shutdown handler to be called")
	}
}

func TestObservabilityCloseError(t *testing.T) {
	obs, err := New(context.Background(), ObsConfig{
		LogLevel:       "info",
		LogFormat:      "json",
		ServiceName:    "test",
		ServiceVersion: "0.0.1",
	}, io.Discard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	obs.Shutdown.Register("fail-close", func(ctx context.Context) error {
		return errors.New("close fail")
	})

	if err := obs.Close(context.Background()); err == nil {
		t.Fatal("expected error from Close")
	}
}

// --- PrettyHandler nil opts ---

func TestPrettyHandlerNilOpts(t *testing.T) {
	var buf bytes.Buffer
	h := NewPrettyHandler(&buf, nil)
	logger := slog.New(h)
	logger.Info("test nil opts")

	if !strings.Contains(buf.String(), "test nil opts") {
		t.Fatalf("expected output: %s", buf.String())
	}
}

// --- Span ---

func TestStartSpanNoAttrs(t *testing.T) {
	ctx := context.Background()
	ctx2, span := StartSpan(ctx, "test-span")
	defer span.End()
	if ctx2 == nil {
		t.Fatal("expected non-nil context")
	}
}

func TestStartSpanWithAttrs(t *testing.T) {
	ctx := context.Background()
	ctx2, span := StartSpan(ctx, "test-span-attrs",
		attribute.String("k", "v"),
		attribute.Int("n", 42),
	)
	defer span.End()
	if ctx2 == nil {
		t.Fatal("expected non-nil context")
	}
}

func TestEndSpanNoError(t *testing.T) {
	_, span := StartSpan(context.Background(), "end-no-err")
	EndSpan(span, nil) // should not panic
}

func TestEndSpanWithError(t *testing.T) {
	_, span := StartSpan(context.Background(), "end-with-err")
	EndSpan(span, errors.New("boom")) // should not panic
}

// --- InitTracer ---

func TestInitTracerHTTP(t *testing.T) {
	// Uses default (http) protocol. The exporter will be created but won't connect.
	tp, sdkTP, err := InitTracer(context.Background(), TracerConfig{
		Endpoint:       "localhost:4318",
		Protocol:       "http",
		ServiceName:    "test-svc",
		ServiceVersion: "0.0.1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tp == nil || sdkTP == nil {
		t.Fatal("expected non-nil providers")
	}
	// Clean up
	_ = sdkTP.Shutdown(context.Background())
}

func TestInitTracerGRPC(t *testing.T) {
	tp, sdkTP, err := InitTracer(context.Background(), TracerConfig{
		Endpoint:       "localhost:4317",
		Protocol:       "grpc",
		ServiceName:    "test-svc",
		ServiceVersion: "0.0.1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tp == nil || sdkTP == nil {
		t.Fatal("expected non-nil providers")
	}
	_ = sdkTP.Shutdown(context.Background())
}

// --- New with OTLP ---

func TestNewObservabilityWithOTLP(t *testing.T) {
	obs, err := New(context.Background(), ObsConfig{
		LogLevel:       "debug",
		LogFormat:      "json",
		OTLPEndpoint:   "localhost:4318",
		OTLPProtocol:   "http",
		ServiceName:    "test",
		ServiceVersion: "0.0.1",
	}, io.Discard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.sdkTP == nil {
		t.Fatal("expected non-nil sdkTP when OTLP enabled")
	}
	// Close should shut down the tracer without error
	if err := obs.Close(context.Background()); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}

func TestNewObservabilityWithOTLPGRPC(t *testing.T) {
	obs, err := New(context.Background(), ObsConfig{
		LogLevel:       "info",
		LogFormat:      "text",
		OTLPEndpoint:   "localhost:4317",
		OTLPProtocol:   "grpc",
		ServiceName:    "test",
		ServiceVersion: "0.0.1",
	}, io.Discard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := obs.Close(context.Background()); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}

// --- ServeMetrics ---

func TestServeMetrics(t *testing.T) {
	obs, err := New(context.Background(), ObsConfig{
		LogLevel:       "error",
		LogFormat:      "json",
		ServiceName:    "test",
		ServiceVersion: "0.0.1",
	}, io.Discard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	srv := obs.ServeMetrics(context.Background(), "127.0.0.1:0")
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	// Give the server a moment to start
	time.Sleep(50 * time.Millisecond)

	// The server registered a shutdown handler
	// Verify /health endpoint works by shutting down via obs.Close
	if err := obs.Close(context.Background()); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}

func TestServeMetricsEndpoints(t *testing.T) {
	obs, err := New(context.Background(), ObsConfig{
		LogLevel:       "error",
		LogFormat:      "json",
		ServiceName:    "test",
		ServiceVersion: "0.0.1",
	}, io.Discard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Use a listener to get an actual port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	srv := obs.ServeMetrics(context.Background(), addr)
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	time.Sleep(100 * time.Millisecond)

	// Test /health
	resp, err := http.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatalf("health request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "OK" {
		t.Fatalf("expected OK, got %s", string(body))
	}

	// Test /metrics
	resp2, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("metrics request failed: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp2.StatusCode)
	}
}

// --- TraceHandler.Handle with trace context ---

func TestTraceHandlerHandleWithTraceContext(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := &TraceHandler{Handler: inner}

	// Create a span context with valid trace/span IDs
	traceID, _ := trace.TraceIDFromHex("00000000000000000000000000000001")
	spanID, _ := trace.SpanIDFromHex("0000000000000001")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	logger := slog.New(h)
	logger.InfoContext(ctx, "traced message")

	out := buf.String()
	if !strings.Contains(out, "trace_id") {
		t.Fatalf("expected trace_id in output: %s", out)
	}
	if !strings.Contains(out, "span_id") {
		t.Fatalf("expected span_id in output: %s", out)
	}
}

// --- PrettyHandler.Enabled with nil Level ---

func TestPrettyHandlerEnabledNilLevel(t *testing.T) {
	h := NewPrettyHandler(io.Discard, &slog.HandlerOptions{})
	// Level is nil, should default to Info
	if h.Enabled(context.Background(), slog.LevelDebug) {
		t.Fatal("debug should be disabled with nil/default level")
	}
	if !h.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("info should be enabled with nil/default level")
	}
}

// --- Interceptor with metadata trace propagation ---

func TestStreamServerInterceptorWithMetadata(t *testing.T) {
	m := NewMetrics()
	interceptor := StreamServerInterceptor(m)

	md := metadata.New(map[string]string{
		"traceparent": "00-00000000000000000000000000000001-0000000000000001-01",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	ss := &mockServerStream{ctx: ctx}

	info := &grpc.StreamServerInfo{FullMethod: "/test.Service/TracedStream"}
	handler := func(srv any, stream grpc.ServerStream) error {
		// Exercise send/recv on wrapped stream
		_ = stream.SendMsg("msg")
		_ = stream.RecvMsg(nil)
		return nil
	}

	err := interceptor(nil, ss, info, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- StartOperation with attributes ---

func TestStartOperationWithAttrs(t *testing.T) {
	m := NewMetrics()
	op, ctx := StartOperation(context.Background(), m, "attr_op",
		attribute.String("key", "val"),
	)
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	op.End(nil)

	count := testutil.ToFloat64(m.OperationTotal.WithLabelValues("attr_op", "ok"))
	if count != 1 {
		t.Fatalf("expected 1, got %f", count)
	}
}

func TestColorLevel(t *testing.T) {
	tests := []struct {
		level    slog.Level
		contains string
	}{
		{slog.LevelDebug, "DBG"},
		{slog.LevelInfo, "INF"},
		{slog.LevelWarn, "WRN"},
		{slog.LevelError, "ERR"},
	}

	for _, tt := range tests {
		t.Run(tt.contains, func(t *testing.T) {
			got := colorLevel(tt.level)
			if !strings.Contains(got, tt.contains) {
				t.Fatalf("colorLevel(%v) = %q, expected to contain %q", tt.level, got, tt.contains)
			}
			// Should contain color escape codes
			if !strings.Contains(got, "\033[") {
				t.Fatalf("expected ANSI color codes in %q", got)
			}
		})
	}
}
