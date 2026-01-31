package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// Observability holds all observability components.
type Observability struct {
	Logger         *slog.Logger
	Metrics        *Metrics
	TracerProvider trace.TracerProvider
	Shutdown       *ShutdownCoordinator
	ServiceName    string
	ServiceVersion string

	sdkTP *sdktrace.TracerProvider
}

// New initializes logging, tracing, and metrics.
func New(ctx context.Context, cfg ObsConfig, w io.Writer) (*Observability, error) {
	shutdown := &ShutdownCoordinator{}

	logger := SetupLogger(cfg.LogLevel, cfg.LogFormat, w)
	metrics := NewMetrics()

	var tp trace.TracerProvider
	var sdkTP *sdktrace.TracerProvider

	if cfg.OTLPEndpoint != "" {
		var err error
		tp, sdkTP, err = InitTracer(ctx, TracerConfig{
			Endpoint:       cfg.OTLPEndpoint,
			Protocol:       cfg.OTLPProtocol,
			ServiceName:    cfg.ServiceName,
			ServiceVersion: cfg.ServiceVersion,
		})
		if err != nil {
			return nil, fmt.Errorf("init tracer: %w", err)
		}

		shutdown.Register("tracer", func(ctx context.Context) error {
			return sdkTP.Shutdown(ctx)
		})
	} else {
		tp = tracenoop.NewTracerProvider()
		slog.Info("tracing disabled (no otlp_endpoint configured)")
	}

	return &Observability{
		Logger:         logger,
		Metrics:        metrics,
		TracerProvider: tp,
		Shutdown:       shutdown,
		ServiceName:    cfg.ServiceName,
		ServiceVersion: cfg.ServiceVersion,
		sdkTP:          sdkTP,
	}, nil
}

// Close flushes traces and runs shutdown handlers.
func (o *Observability) Close(ctx context.Context) error {
	return o.Shutdown.Shutdown(ctx)
}

// ServeMetrics starts the HTTP server for /metrics and /health.
func (o *Observability) ServeMetrics(ctx context.Context, addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(o.Metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		slog.Info("metrics server starting", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "error", err)
		}
	}()

	o.Shutdown.Register("metrics-server", func(ctx context.Context) error {
		return srv.Shutdown(ctx)
	})

	return srv
}

// ObsConfig is the config subset needed by the observability package.
type ObsConfig struct {
	LogLevel       string
	LogFormat      string
	OTLPEndpoint   string
	OTLPProtocol   string
	ServiceName    string
	ServiceVersion string
}
