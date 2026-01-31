// Package s3 provides an S3-backed blob storage backend.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/gezibash/arc/pkg/reference"

	"github.com/gezibash/arc-node/internal/blobstore/physical"
	"github.com/gezibash/arc-node/internal/storage"
)

const (
	KeyBucket          = "bucket"
	KeyRegion          = "region"
	KeyEndpoint        = "endpoint"
	KeyPrefix          = "prefix"
	KeyAccessKeyID     = "access_key_id"
	KeySecretAccessKey = "secret_access_key"
	KeyForcePathStyle  = "force_path_style"
)

func init() {
	physical.Register("s3", NewFactory, Defaults)
}

// Defaults returns the default configuration for the S3 backend.
func Defaults() map[string]string {
	return map[string]string{
		KeyRegion:          "us-east-1",
		KeyEndpoint:        "",
		KeyPrefix:          "",
		KeyAccessKeyID:     "",
		KeySecretAccessKey: "",
		KeyForcePathStyle:  "false",
	}
}

// NewFactory creates a new S3 backend from a configuration map.
func NewFactory(ctx context.Context, config map[string]string) (physical.Backend, error) {
	bucket := storage.GetString(config, KeyBucket, "")
	if bucket == "" {
		return nil, storage.NewConfigError("s3", KeyBucket, "cannot be empty")
	}

	region := storage.GetString(config, KeyRegion, "us-east-1")
	endpoint := storage.GetString(config, KeyEndpoint, "")
	prefix := storage.GetString(config, KeyPrefix, "")
	accessKeyID := storage.GetString(config, KeyAccessKeyID, "")
	secretAccessKey := storage.GetString(config, KeySecretAccessKey, "")

	forcePathStyle, err := storage.GetBool(config, KeyForcePathStyle, false)
	if err != nil {
		return nil, storage.NewConfigErrorWithValue("s3", KeyForcePathStyle, config[KeyForcePathStyle], err.Error())
	}

	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(region))

	if accessKeyID != "" && secretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, ""),
		))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, storage.NewConfigErrorWithCause("s3", "", "failed to load AWS config", err)
	}

	var s3Opts []func(*s3.Options)
	if endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}
	if forcePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(cfg, s3Opts...)

	// Fail fast: verify bucket access.
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return nil, storage.NewConfigErrorWithCause("s3", KeyBucket, "bucket not accessible", err)
	}

	slog.Info("s3 blobstore initialized", "bucket", bucket, "region", region, "prefix", prefix)

	return &Backend{
		client: client,
		bucket: bucket,
		prefix: prefix,
	}, nil
}

// Backend is an S3 implementation of physical.Backend.
type Backend struct {
	client *s3.Client
	bucket string
	prefix string
	closed atomic.Bool
}

func (b *Backend) key(r reference.Reference) string {
	return b.prefix + reference.Hex(r)
}

// Put stores data at the given reference.
func (b *Backend) Put(ctx context.Context, r reference.Reference, data []byte) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.key(r)),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("s3 put: %w", err)
	}
	return nil
}

// Get retrieves data by reference.
func (b *Backend) Get(ctx context.Context, r reference.Reference) ([]byte, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.key(r)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, physical.ErrNotFound
		}
		return nil, fmt.Errorf("s3 get: %w", err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3 get: %w", err)
	}
	return data, nil
}

// Exists checks if a blob exists.
func (b *Backend) Exists(ctx context.Context, r reference.Reference) (bool, error) {
	if b.closed.Load() {
		return false, physical.ErrClosed
	}

	_, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.key(r)),
	})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("s3 exists: %w", err)
	}
	return true, nil
}

// Delete removes a blob by reference. S3 delete is already idempotent.
func (b *Backend) Delete(ctx context.Context, r reference.Reference) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.key(r)),
	})
	if err != nil {
		return fmt.Errorf("s3 delete: %w", err)
	}
	return nil
}

// Stats returns storage statistics.
func (b *Backend) Stats(_ context.Context) (*physical.Stats, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	return &physical.Stats{
		SizeBytes:   0,
		BackendType: "s3",
	}, nil
}

// Close is a no-op; the S3 SDK client needs no cleanup.
func (b *Backend) Close() error {
	b.closed.Store(true)
	return nil
}

func isNotFound(err error) bool {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	// HeadObject returns a generic error with status 404.
	var respErr interface{ HTTPStatusCode() int }
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == 404 {
		return true
	}
	return false
}
