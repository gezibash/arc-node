package blobstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"testing"

	blobv1 "github.com/gezibash/arc/v2/api/arc/blob/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

func setupServiceTest(t *testing.T) (blobv1.BlobServiceClient, BlobStore, func()) {
	t.Helper()

	store := NewMemoryStore()
	svc := NewService(store)

	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	blobv1.RegisterBlobServiceServer(s, svc)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("Server exited: %v", err)
		}
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}

	client := blobv1.NewBlobServiceClient(conn)

	cleanup := func() {
		_ = conn.Close()
		s.Stop()
		_ = store.Close()
	}

	return client, store, cleanup
}

func TestServicePut(t *testing.T) {
	client, store, cleanup := setupServiceTest(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("test blob data for service")
	expectedCID := sha256.Sum256(data)

	// Stream chunks
	stream, err := client.Put(ctx)
	if err != nil {
		t.Fatalf("Put stream failed: %v", err)
	}

	// Send in chunks
	chunkSize := 10
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&blobv1.Chunk{
			Data:   data[i:end],
			Offset: int64(i),
		}); err != nil {
			t.Fatalf("Send chunk failed: %v", err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv failed: %v", err)
	}

	if !bytes.Equal(resp.Cid, expectedCID[:]) {
		t.Errorf("CID mismatch: got %x, want %x", resp.Cid, expectedCID)
	}
	if resp.Size != int64(len(data)) {
		t.Errorf("Size mismatch: got %d, want %d", resp.Size, len(data))
	}

	// Verify it's in the store
	exists, err := store.Has(ctx, expectedCID[:])
	if err != nil {
		t.Fatalf("Has failed: %v", err)
	}
	if !exists {
		t.Error("Blob not found in store after Put")
	}
}

func TestServiceGet(t *testing.T) {
	client, store, cleanup := setupServiceTest(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("test blob data for get service")
	cid, _ := store.Put(ctx, data)

	// Get via service
	stream, err := client.Get(ctx, &blobv1.GetRequest{Cid: cid[:]})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	var received bytes.Buffer
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv failed: %v", err)
		}
		received.Write(chunk.Data)
	}

	if !bytes.Equal(received.Bytes(), data) {
		t.Errorf("Data mismatch: got %q, want %q", received.Bytes(), data)
	}
}

func TestServiceGetNotFound(t *testing.T) {
	client, _, cleanup := setupServiceTest(t)
	defer cleanup()

	ctx := context.Background()
	fakeCID := sha256.Sum256([]byte("does not exist"))

	stream, err := client.Get(ctx, &blobv1.GetRequest{Cid: fakeCID[:]})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	_, err = stream.Recv()
	if err == nil {
		t.Error("Expected error for non-existent blob")
	}
	// Should be NotFound status
}

func TestServiceGetInvalidCID(t *testing.T) {
	client, _, cleanup := setupServiceTest(t)
	defer cleanup()

	ctx := context.Background()

	stream, err := client.Get(ctx, &blobv1.GetRequest{Cid: []byte("short")})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	_, err = stream.Recv()
	if err == nil {
		t.Error("Expected error for invalid CID")
	}
}

func TestServiceGetWithOffset(t *testing.T) {
	client, store, cleanup := setupServiceTest(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("0123456789abcdef")
	cid, _ := store.Put(ctx, data)

	// Get with offset
	stream, err := client.Get(ctx, &blobv1.GetRequest{
		Cid:    cid[:],
		Offset: 5,
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	var received bytes.Buffer
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv failed: %v", err)
		}
		received.Write(chunk.Data)
	}

	expected := data[5:]
	if !bytes.Equal(received.Bytes(), expected) {
		t.Errorf("Data mismatch: got %q, want %q", received.Bytes(), expected)
	}
}

func TestServiceGetWithLimit(t *testing.T) {
	client, store, cleanup := setupServiceTest(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("0123456789abcdef")
	cid, _ := store.Put(ctx, data)

	// Get with limit
	stream, err := client.Get(ctx, &blobv1.GetRequest{
		Cid:   cid[:],
		Limit: 5,
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	var received bytes.Buffer
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv failed: %v", err)
		}
		received.Write(chunk.Data)
	}

	expected := data[:5]
	if !bytes.Equal(received.Bytes(), expected) {
		t.Errorf("Data mismatch: got %q, want %q", received.Bytes(), expected)
	}
}
