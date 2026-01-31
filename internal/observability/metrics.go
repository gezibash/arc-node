package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the Prometheus metrics registry and standard meters.
type Metrics struct {
	Registry          *prometheus.Registry
	OperationDuration *prometheus.HistogramVec
	OperationTotal    *prometheus.CounterVec
	BytesProcessed    *prometheus.CounterVec
	ErrorsTotal          *prometheus.CounterVec
	AutoIndexPatterns    *prometheus.CounterVec
	AutoIndexReindexTotal *prometheus.CounterVec
}

// NewMetrics creates a custom Prometheus registry with standard arc metrics.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	opDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "arc_operation_duration_seconds",
		Help:    "Duration of operations in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation", "status"})

	opTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "arc_operation_total",
		Help: "Total number of operations.",
	}, []string{"operation", "status"})

	bytesProcessed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "arc_bytes_processed_total",
		Help: "Total bytes processed.",
	}, []string{"direction"})

	errorsTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "arc_errors_total",
		Help: "Total number of errors.",
	}, []string{"operation", "type"})

	autoIndexPatterns := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "arc_query_label_patterns_total",
		Help: "Total multi-label query pattern observations.",
	}, []string{"pattern"})

	autoIndexReindex := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "arc_auto_index_reindex_total",
		Help: "Total automatic composite index reindex operations.",
	}, []string{"index_name", "status"})

	reg.MustRegister(opDuration, opTotal, bytesProcessed, errorsTotal, autoIndexPatterns, autoIndexReindex)

	return &Metrics{
		Registry:              reg,
		OperationDuration:     opDuration,
		OperationTotal:        opTotal,
		BytesProcessed:        bytesProcessed,
		ErrorsTotal:           errorsTotal,
		AutoIndexPatterns:     autoIndexPatterns,
		AutoIndexReindexTotal: autoIndexReindex,
	}
}
