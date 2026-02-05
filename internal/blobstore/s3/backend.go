// Package s3 provides an S3-based BlobStore backend.
package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/gezibash/arc/v2/internal/blobstore"
	"github.com/gezibash/arc/v2/internal/storage"
)

func init() {
	blobstore.Register(blobstore.BackendS3, factory, defaults)
}

func defaults() map[string]string {
	return map[string]string{
		"region":           "us-east-1",
		"prefix":           "",
		"force_path_style": "false",
	}
}

func factory(ctx context.Context, config map[string]string) (blobstore.BlobStore, error) {
	bucket := storage.GetString(config, "bucket", "")
	if bucket == "" {
		return nil, storage.NewConfigError("s3", "bucket", "required")
	}

	region := storage.GetString(config, "region", "us-east-1")
	endpoint := storage.GetString(config, "endpoint", "")
	prefix := storage.GetString(config, "prefix", "")
	accessKey := storage.GetString(config, "access_key_id", "")
	secretKey := storage.GetString(config, "secret_access_key", "")

	forcePathStyle, err := storage.GetBool(config, "force_path_style", false)
	if err != nil {
		return nil, err
	}

	// Build AWS config
	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(region))

	if accessKey != "" && secretKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("s3: load aws config: %w", err)
	}

	// Build S3 client options
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

	// Validate bucket access
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return nil, fmt.Errorf("s3: bucket %q not accessible: %w", bucket, err)
	}

	return &Store{
		client: client,
		bucket: bucket,
		prefix: prefix,
	}, nil
}

// Store provides content-addressed blob storage backed by S3.
type Store struct {
	client *s3.Client
	bucket string
	prefix string
	closed atomic.Bool
}

func (s *Store) checkClosed() error {
	if s.closed.Load() {
		return blobstore.ErrClosed
	}
	return nil
}

func (s *Store) key(cid []byte) string {
	return s.prefix + fmt.Sprintf("%x", cid)
}

// Put stores a blob and returns its CID (SHA-256 hash).
func (s *Store) Put(ctx context.Context, data []byte) ([32]byte, error) {
	if err := s.checkClosed(); err != nil {
		return [32]byte{}, err
	}

	cid := sha256.Sum256(data)

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.key(cid[:])),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return [32]byte{}, fmt.Errorf("s3: put object: %w", err)
	}

	return cid, nil
}

// PutStream stores a blob from a reader. Buffers into memory to compute CID first.
func (s *Store) PutStream(ctx context.Context, r io.Reader) ([32]byte, int64, error) {
	if err := s.checkClosed(); err != nil {
		return [32]byte{}, 0, err
	}

	// S3 needs the key (CID) before upload, so we must buffer
	data, err := io.ReadAll(r)
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("s3: read stream: %w", err)
	}

	cid, err := s.Put(ctx, data)
	return cid, int64(len(data)), err
}

// Get retrieves a blob by CID.
func (s *Store) Get(ctx context.Context, cid []byte) ([]byte, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if len(cid) != 32 {
		return nil, blobstore.ErrInvalidCID
	}

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(cid)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, blobstore.ErrNotFound
		}
		return nil, fmt.Errorf("s3: get object: %w", err)
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3: read body: %w", err)
	}

	return data, nil
}

// GetReader returns a reader for a blob and its size.
func (s *Store) GetReader(ctx context.Context, cid []byte) (io.ReadCloser, int64, error) {
	if err := s.checkClosed(); err != nil {
		return nil, 0, err
	}
	if len(cid) != 32 {
		return nil, 0, blobstore.ErrInvalidCID
	}

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(cid)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, 0, blobstore.ErrNotFound
		}
		return nil, 0, fmt.Errorf("s3: get object: %w", err)
	}

	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return out.Body, size, nil
}

// Has checks if a blob exists.
func (s *Store) Has(ctx context.Context, cid []byte) (bool, error) {
	if err := s.checkClosed(); err != nil {
		return false, err
	}
	if len(cid) != 32 {
		return false, blobstore.ErrInvalidCID
	}

	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(cid)),
	})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("s3: head object: %w", err)
	}
	return true, nil
}

// Size returns the size of a blob.
func (s *Store) Size(ctx context.Context, cid []byte) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}
	if len(cid) != 32 {
		return 0, blobstore.ErrInvalidCID
	}

	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(cid)),
	})
	if err != nil {
		if isNotFound(err) {
			return 0, blobstore.ErrNotFound
		}
		return 0, fmt.Errorf("s3: head object: %w", err)
	}

	if out.ContentLength != nil {
		return *out.ContentLength, nil
	}
	return 0, nil
}

// Delete removes a blob.
func (s *Store) Delete(ctx context.Context, cid []byte) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if len(cid) != 32 {
		return blobstore.ErrInvalidCID
	}

	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(cid)),
	})
	if err != nil {
		return fmt.Errorf("s3: delete object: %w", err)
	}
	return nil
}

// Close marks the store as closed.
func (s *Store) Close() error {
	s.closed.Swap(true)
	return nil
}

// isNotFound checks if an error indicates a missing object.
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	// Also check for HTTP 404 in the response metadata
	var respErr interface{ HTTPStatusCode() int }
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	return false
}
