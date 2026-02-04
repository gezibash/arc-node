// Package runtime provides the foundation for all arc services.
// Use the builder pattern to compose capabilities.
package runtime

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/gezibash/arc/v2/internal/config"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/logging"
)

// Extension is a function that extends the runtime with a capability.
// Extensions are called in order during Build().
type Extension func(*Runtime) error

// Option configures a runtime builder.
type Option func(*Builder) error

// Builder constructs a Runtime with composed capabilities.
type Builder struct {
	name      string
	dataDir   string
	logLevel  string
	logFormat string
	logWriter io.Writer

	signer   identity.Signer
	provider identity.Provider
	logger   *logging.Logger

	extensions []Extension
}

// New starts building a runtime for the named service.
func New(name string) *Builder {
	return &Builder{
		name:      name,
		logLevel:  "info",
		logFormat: "text",
	}
}

// Compose builds a runtime using functional options.
func Compose(name string, opts ...Option) (*Runtime, error) {
	b := New(name)
	for _, opt := range opts {
		if err := opt(b); err != nil {
			return nil, err
		}
	}
	return b.Build()
}

// WithDataDir sets the data directory.
func WithDataDir(dir string) Option {
	return func(b *Builder) error {
		b.dataDir = dir
		return nil
	}
}

// WithLogger sets a preconfigured logger.
func WithLogger(l *logging.Logger) Option {
	return func(b *Builder) error {
		b.logger = l
		return nil
	}
}

// WithLogConfig sets the logger level and format.
func WithLogConfig(level, format string) Option {
	return func(b *Builder) error {
		if level != "" {
			b.logLevel = level
		}
		if format != "" {
			b.logFormat = format
		}
		return nil
	}
}

// WithSigner sets a signer directly.
func WithSigner(s identity.Signer) Option {
	return func(b *Builder) error {
		b.signer = s
		return nil
	}
}

// WithIdentityProvider sets an identity provider.
func WithIdentityProvider(p identity.Provider) Option {
	return func(b *Builder) error {
		b.provider = p
		return nil
	}
}

// Use adds a capability extension to the runtime.
// Extensions are applied in order during Build().
func (b *Builder) Use(ext Extension) *Builder {
	b.extensions = append(b.extensions, ext)
	return b
}

// DataDir sets the data directory. Defaults to ~/.arc
func (b *Builder) DataDir(dir string) *Builder {
	b.dataDir = dir
	return b
}

// Logging configures log level and format.
// Levels: debug, info, warn, error. Formats: text, json.
func (b *Builder) Logging(level, format string) *Builder {
	if level != "" {
		b.logLevel = level
	}
	if format != "" {
		b.logFormat = format
	}
	return b
}

// LogWriter sets the output destination for logs.
// Defaults to os.Stdout if not set.
func (b *Builder) LogWriter(w io.Writer) *Builder {
	b.logWriter = w
	return b
}

// IdentityProvider configures the identity provider.
func (b *Builder) IdentityProvider(p identity.Provider) *Builder {
	b.provider = p
	return b
}

// Signer sets a signer directly.
func (b *Builder) Signer(s identity.Signer) *Builder {
	b.signer = s
	return b
}

// Build constructs the runtime with all configured capabilities.
func (b *Builder) Build() (*Runtime, error) {
	if b.name == "" {
		return nil, fmt.Errorf("name is required")
	}

	dataDir := b.dataDir
	if dataDir == "" {
		dataDir = config.DefaultDataDir()
	}

	log := b.logger
	if log == nil {
		w := b.logWriter
		if w == nil {
			w = os.Stdout
		}
		log = logging.SetupWriter(b.logLevel, b.logFormat, w)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info("shutting down...")
		cancel()
		<-sigCh
		log.Warn("forced shutdown")
		os.Exit(1)
	}()

	signer := b.signer
	if signer == nil {
		if b.provider == nil {
			cancel()
			return nil, fmt.Errorf("identity provider or signer required")
		}
		var err error
		signer, err = b.provider.Load(ctx)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("load identity: %w", err)
		}
	}

	pk := signer.PublicKey()
	log.Info("loaded identity",
		"algo", pk.Algo,
		"pubkey", logging.FormatPubkeyBytes(pk.Bytes),
	)

	rt := &Runtime{
		name:       b.name,
		signer:     signer,
		log:        log,
		dataDir:    dataDir,
		ctx:        ctx,
		cancel:     cancel,
		components: make(map[string]any),
		closers:    make([]func() error, 0),
	}

	for _, ext := range b.extensions {
		if err := ext(rt); err != nil {
			_ = rt.Close()
			return nil, err
		}
	}

	return rt, nil
}

// Runtime is the foundation for arc services.
type Runtime struct {
	name       string
	signer     identity.Signer
	log        *logging.Logger
	dataDir    string
	ctx        context.Context
	cancel     context.CancelFunc
	components map[string]any
	closers    []func() error
}

// Name returns the service name.
func (r *Runtime) Name() string { return r.name }

// Signer returns the identity signer.
func (r *Runtime) Signer() identity.Signer { return r.signer }

// PublicKey returns the public key.
func (r *Runtime) PublicKey() identity.PublicKey { return r.signer.PublicKey() }

// Log returns the logger.
func (r *Runtime) Log() *logging.Logger { return r.log }

// DataDir returns the data directory.
func (r *Runtime) DataDir() string { return r.dataDir }

// DataPath joins elements to the data directory.
func (r *Runtime) DataPath(elem ...string) string {
	return filepath.Join(append([]string{r.dataDir}, elem...)...)
}

// Context returns the lifecycle context (cancelled on shutdown).
func (r *Runtime) Context() context.Context { return r.ctx }

// Shutdown triggers graceful shutdown.
func (r *Runtime) Shutdown() { r.cancel() }

// Wait blocks until shutdown.
func (r *Runtime) Wait() { <-r.ctx.Done() }

// Set stores a component for later retrieval.
// Used by capability extensions to register themselves.
func (r *Runtime) Set(key string, component any) {
	r.components[key] = component
}

// Get retrieves a component by key.
// Used by capability accessors (e.g., relay.From(rt)).
func (r *Runtime) Get(key string) any {
	return r.components[key]
}

// OnClose registers a cleanup function to be called on Close().
func (r *Runtime) OnClose(fn func() error) {
	r.closers = append(r.closers, fn)
}

// Close cleans up all resources.
func (r *Runtime) Close() error {
	r.cancel()
	var errs []error
	for i := len(r.closers) - 1; i >= 0; i-- {
		if err := r.closers[i](); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}
