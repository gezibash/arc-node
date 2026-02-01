package envelope

import (
	"context"
	"errors"
	"strconv"
	"testing"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
	"google.golang.org/grpc/metadata"
)

func generateKeypair(t *testing.T) *identity.Keypair {
	t.Helper()
	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	return kp
}

func TestSealOpenRoundTrip(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("hello world")
	contentType := "text/plain"
	origin := kp.PublicKey()
	meta := map[string]string{"foo": "bar"}

	env, err := Seal(kp, to, payload, contentType, origin, 0, meta, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	opened, err := Open(env.Message.From, env.Message.To, payload, contentType, env.Message.Timestamp, *env.Message.Signature, origin, 0, meta, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if opened.Message.From != kp.PublicKey() {
		t.Error("From mismatch")
	}
	if opened.Message.To != to {
		t.Error("To mismatch")
	}
	if opened.Message.ContentType != contentType {
		t.Error("ContentType mismatch")
	}
	if opened.Message.Content != env.Message.Content {
		t.Error("Content reference mismatch")
	}
	if opened.HopCount != 0 {
		t.Error("HopCount mismatch")
	}
	if opened.Metadata["foo"] != "bar" {
		t.Error("Metadata mismatch")
	}
}

func TestSealOpenWithDimensions(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("dimensions test")

	dims := &nodev1.Dimensions{
		Pattern:        nodev1.Pattern_PATTERN_PUB_SUB,
		Delivery:       nodev1.Delivery_DELIVERY_AT_LEAST_ONCE,
		Persistence:    nodev1.Persistence_PERSISTENCE_DURABLE,
		Visibility:     nodev1.Visibility_VISIBILITY_PRIVATE,
		Ordering:       nodev1.Ordering_ORDERING_FIFO,
		Affinity:       nodev1.Affinity_AFFINITY_KEY,
		Dedup:          nodev1.Dedup_DEDUP_IDEMPOTENCY_KEY,
		Complete:       nodev1.DeliveryComplete_DELIVERY_COMPLETE_ALL,
		TtlMs:          60000,
		CompleteN:      5,
		AffinityKey:    "user-123",
		IdempotencyKey: "idem-456",
		Priority:       7,
		MaxRedelivery:  3,
		AckTimeoutMs:   5000,
		Correlation:    "corr-789",
	}

	env, err := Seal(kp, to, payload, "test/dims", kp.PublicKey(), 2, nil, dims)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	opened, err := Open(env.Message.From, env.Message.To, payload, "test/dims", env.Message.Timestamp, *env.Message.Signature, kp.PublicKey(), 2, nil, dims)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	d := opened.Dimensions
	if d.Pattern != nodev1.Pattern_PATTERN_PUB_SUB {
		t.Error("Pattern mismatch")
	}
	if d.Delivery != nodev1.Delivery_DELIVERY_AT_LEAST_ONCE {
		t.Error("Delivery mismatch")
	}
	if d.Persistence != nodev1.Persistence_PERSISTENCE_DURABLE {
		t.Error("Persistence mismatch")
	}
	if d.Visibility != nodev1.Visibility_VISIBILITY_PRIVATE {
		t.Error("Visibility mismatch")
	}
	if d.Ordering != nodev1.Ordering_ORDERING_FIFO {
		t.Error("Ordering mismatch")
	}
	if d.Affinity != nodev1.Affinity_AFFINITY_KEY {
		t.Error("Affinity mismatch")
	}
	if d.Dedup != nodev1.Dedup_DEDUP_IDEMPOTENCY_KEY {
		t.Error("Dedup mismatch")
	}
	if d.Complete != nodev1.DeliveryComplete_DELIVERY_COMPLETE_ALL {
		t.Error("Complete mismatch")
	}
	if d.TtlMs != 60000 {
		t.Errorf("TtlMs = %d, want 60000", d.TtlMs)
	}
	if d.CompleteN != 5 {
		t.Errorf("CompleteN = %d, want 5", d.CompleteN)
	}
	if d.AffinityKey != "user-123" {
		t.Error("AffinityKey mismatch")
	}
	if d.IdempotencyKey != "idem-456" {
		t.Error("IdempotencyKey mismatch")
	}
	if d.Priority != 7 {
		t.Error("Priority mismatch")
	}
	if d.MaxRedelivery != 3 {
		t.Error("MaxRedelivery mismatch")
	}
	if d.AckTimeoutMs != 5000 {
		t.Error("AckTimeoutMs mismatch")
	}
	if d.Correlation != "corr-789" {
		t.Error("Correlation mismatch")
	}
}

func TestOpenInvalidSignature(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("tamper test")

	env, err := Seal(kp, to, payload, "text/plain", kp.PublicKey(), 0, nil, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Tamper with signature
	badSig := *env.Message.Signature
	badSig[0] ^= 0xFF

	_, err = Open(env.Message.From, env.Message.To, payload, "text/plain", env.Message.Timestamp, badSig, kp.PublicKey(), 0, nil, nil)
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature, got %v", err)
	}
}

func TestOpenWrongPayload(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("original payload")

	env, err := Seal(kp, to, payload, "text/plain", kp.PublicKey(), 0, nil, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	_, err = Open(env.Message.From, env.Message.To, []byte("different payload"), "text/plain", env.Message.Timestamp, *env.Message.Signature, kp.PublicKey(), 0, nil, nil)
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature, got %v", err)
	}
}

func TestSealOpenZeroToKey(t *testing.T) {
	kp := generateKeypair(t)
	var zeroKey identity.PublicKey
	payload := []byte("federation bootstrap")

	env, err := Seal(kp, zeroKey, payload, "federation/init", kp.PublicKey(), 0, nil, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	opened, err := Open(env.Message.From, zeroKey, payload, "federation/init", env.Message.Timestamp, *env.Message.Signature, kp.PublicKey(), 0, nil, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if opened.Message.To != zeroKey {
		t.Error("expected zero To key")
	}
}

func TestExtractInjectRoundTrip(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("metadata test")

	env, err := Seal(kp, to, payload, "test/meta", kp.PublicKey(), 3, map[string]string{"key1": "val1"}, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	sig := *env.Message.Signature

	ctx := InjectOutgoing(context.Background(), env, sig)
	outMD, _ := metadata.FromOutgoingContext(ctx)

	// Extract treats it as incoming metadata
	from, toExt, origin, ts, sigExt, ct, hc, meta, _, err := Extract(outMD)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	if from != kp.PublicKey() {
		t.Error("From mismatch")
	}
	if toExt != to {
		t.Error("To mismatch")
	}
	if origin != kp.PublicKey() {
		t.Error("Origin mismatch")
	}
	if ts != env.Message.Timestamp {
		t.Error("Timestamp mismatch")
	}
	if sigExt != sig {
		t.Error("Signature mismatch")
	}
	if ct != "test/meta" {
		t.Error("ContentType mismatch")
	}
	if hc != 3 {
		t.Error("HopCount mismatch")
	}
	if meta["key1"] != "val1" {
		t.Error("Metadata mismatch")
	}
}

func TestInjectOutgoingRoundTrip(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("outgoing test")

	env, err := Seal(kp, to, payload, "test/outgoing", kp.PublicKey(), 1, map[string]string{"x": "y"}, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	sig := *env.Message.Signature
	ctx := InjectOutgoing(context.Background(), env, sig)

	outMD, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("no outgoing metadata")
	}

	from, toExt, origin, ts, sigExt, ct, hc, meta, _, err := Extract(outMD)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	if from != kp.PublicKey() {
		t.Error("From mismatch")
	}
	if toExt != to {
		t.Error("To mismatch")
	}
	if origin != kp.PublicKey() {
		t.Error("Origin mismatch")
	}
	if ts != env.Message.Timestamp {
		t.Error("Timestamp mismatch")
	}
	if sigExt != sig {
		t.Error("Signature mismatch")
	}
	if ct != "test/outgoing" {
		t.Error("ContentType mismatch")
	}
	if hc != 1 {
		t.Error("HopCount mismatch")
	}
	if meta["x"] != "y" {
		t.Error("Metadata mismatch")
	}
}

func TestExtractDimensionsRoundTrip(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("dims metadata test")

	dims := &nodev1.Dimensions{
		Pattern:        nodev1.Pattern_PATTERN_QUEUE,
		Delivery:       nodev1.Delivery_DELIVERY_EXACTLY_ONCE,
		Persistence:    nodev1.Persistence_PERSISTENCE_DURABLE,
		Visibility:     nodev1.Visibility_VISIBILITY_FEDERATED,
		Ordering:       nodev1.Ordering_ORDERING_CAUSAL,
		Affinity:       nodev1.Affinity_AFFINITY_SENDER,
		Dedup:          nodev1.Dedup_DEDUP_REF,
		Complete:       nodev1.DeliveryComplete_DELIVERY_COMPLETE_N_OF_M,
		TtlMs:          30000,
		CompleteN:      10,
		AffinityKey:    "aff-key",
		IdempotencyKey: "idem-key",
		Priority:       99,
		MaxRedelivery:  5,
		AckTimeoutMs:   10000,
		Correlation:    "corr-id",
	}

	env, err := Seal(kp, to, payload, "test/dims-md", kp.PublicKey(), 0, nil, dims)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	sig := *env.Message.Signature
	ctx := InjectOutgoing(context.Background(), env, sig)
	outMD, _ := metadata.FromOutgoingContext(ctx)

	_, _, _, _, _, _, _, _, extractedDims, err := Extract(outMD)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	if extractedDims.Pattern != nodev1.Pattern_PATTERN_QUEUE {
		t.Error("Pattern mismatch")
	}
	if extractedDims.Delivery != nodev1.Delivery_DELIVERY_EXACTLY_ONCE {
		t.Error("Delivery mismatch")
	}
	if extractedDims.Persistence != nodev1.Persistence_PERSISTENCE_DURABLE {
		t.Error("Persistence mismatch")
	}
	if extractedDims.Visibility != nodev1.Visibility_VISIBILITY_FEDERATED {
		t.Error("Visibility mismatch")
	}
	if extractedDims.Ordering != nodev1.Ordering_ORDERING_CAUSAL {
		t.Error("Ordering mismatch")
	}
	if extractedDims.Affinity != nodev1.Affinity_AFFINITY_SENDER {
		t.Error("Affinity mismatch")
	}
	if extractedDims.Dedup != nodev1.Dedup_DEDUP_REF {
		t.Error("Dedup mismatch")
	}
	if extractedDims.Complete != nodev1.DeliveryComplete_DELIVERY_COMPLETE_N_OF_M {
		t.Error("Complete mismatch")
	}
	if extractedDims.TtlMs != 30000 {
		t.Error("TtlMs mismatch")
	}
	if extractedDims.CompleteN != 10 {
		t.Error("CompleteN mismatch")
	}
	if extractedDims.AffinityKey != "aff-key" {
		t.Error("AffinityKey mismatch")
	}
	if extractedDims.IdempotencyKey != "idem-key" {
		t.Error("IdempotencyKey mismatch")
	}
	if extractedDims.Priority != 99 {
		t.Error("Priority mismatch")
	}
	if extractedDims.MaxRedelivery != 5 {
		t.Error("MaxRedelivery mismatch")
	}
	if extractedDims.AckTimeoutMs != 10000 {
		t.Error("AckTimeoutMs mismatch")
	}
	if extractedDims.Correlation != "corr-id" {
		t.Error("Correlation mismatch")
	}
}

func BenchmarkSealOpen(b *testing.B) {
	b.ReportAllocs()
	kp, err := identity.Generate()
	if err != nil {
		b.Fatal(err)
	}
	toKP, err := identity.Generate()
	if err != nil {
		b.Fatal(err)
	}
	payload := []byte("benchmark payload data")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env, err := Seal(kp, toKP.PublicKey(), payload, "bench", kp.PublicKey(), 0, nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		_, err = Open(env.Message.From, env.Message.To, payload, "bench", env.Message.Timestamp, *env.Message.Signature, kp.PublicKey(), 0, nil, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestExtractMissingMetadata(t *testing.T) {
	md := metadata.MD{}
	_, _, _, _, _, _, _, _, _, err := Extract(md)
	if err == nil {
		t.Fatal("expected error for empty metadata")
	}
}

func TestExtractErrorPaths(t *testing.T) {
	kp := generateKeypair(t)
	from := kp.PublicKey()
	to := generateKeypair(t).PublicKey()
	var sig identity.Signature
	copy(sig[:], make([]byte, 64))

	validFrom := string(from[:])
	validTo := string(to[:])
	validSig := string(sig[:])
	validOrigin := string(from[:])

	tests := []struct {
		name string
		md   metadata.MD
	}{
		{"missing to", metadata.Pairs("arc-from-bin", validFrom)},
		{"missing timestamp", metadata.Pairs("arc-from-bin", validFrom, "arc-to-bin", validTo)},
		{"bad timestamp", metadata.Pairs("arc-from-bin", validFrom, "arc-to-bin", validTo, "arc-timestamp", "notanumber")},
		{"missing signature", metadata.Pairs("arc-from-bin", validFrom, "arc-to-bin", validTo, "arc-timestamp", "123")},
		{"bad signature length", metadata.Pairs("arc-from-bin", validFrom, "arc-to-bin", validTo, "arc-timestamp", "123", "arc-signature-bin", "short")},
		{"missing origin", metadata.Pairs("arc-from-bin", validFrom, "arc-to-bin", validTo, "arc-timestamp", "123", "arc-signature-bin", validSig)},
		{"bad hop count", metadata.Pairs("arc-from-bin", validFrom, "arc-to-bin", validTo, "arc-timestamp", "123", "arc-signature-bin", validSig, "arc-origin-bin", validOrigin, "arc-hop-count", "notanumber")},
		{"bad from length", metadata.Pairs("arc-from-bin", "short")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, _, _, _, _, _, _, err := Extract(tt.md)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestExtractBadDimensions(t *testing.T) {
	kp := generateKeypair(t)
	from := kp.PublicKey()
	to := generateKeypair(t).PublicKey()

	// Build valid metadata but with corrupted dimensions
	env, _ := Seal(kp, to, []byte("test"), "test", from, 0, nil, nil)
	sig := *env.Message.Signature
	md := metadata.Pairs(
		"arc-from-bin", string(from[:]),
		"arc-to-bin", string(to[:]),
		"arc-timestamp", strconv.FormatInt(env.Message.Timestamp, 10),
		"arc-signature-bin", string(sig[:]),
		"arc-content-type", "test",
		"arc-origin-bin", string(from[:]),
		"arc-dimensions-bin", "invalid-proto-bytes",
	)

	_, _, _, _, _, _, _, _, dims, err := Extract(md)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	// Bad proto should result in empty dimensions
	if dims == nil {
		t.Error("expected non-nil dims even on bad proto")
	}
}

func TestCallerContext(t *testing.T) {
	kp := generateKeypair(t)
	dims := &nodev1.Dimensions{Pattern: nodev1.Pattern_PATTERN_PUB_SUB}

	caller := &Caller{
		PublicKey:  kp.PublicKey(),
		Origin:     kp.PublicKey(),
		HopCount:   2,
		Metadata:   map[string]string{"a": "b"},
		Dimensions: dims,
	}

	ctx := WithCaller(context.Background(), caller)
	got, ok := GetCaller(ctx)
	if !ok {
		t.Fatal("GetCaller returned false")
	}
	if got.PublicKey != kp.PublicKey() {
		t.Error("PublicKey mismatch")
	}
	if got.Origin != kp.PublicKey() {
		t.Error("Origin mismatch")
	}
	if got.HopCount != 2 {
		t.Error("HopCount mismatch")
	}
	if got.Metadata["a"] != "b" {
		t.Error("Metadata mismatch")
	}
	if got.Dimensions.Pattern != nodev1.Pattern_PATTERN_PUB_SUB {
		t.Error("Dimensions mismatch")
	}

	// GetCaller from bare context should return false
	_, ok = GetCaller(context.Background())
	if ok {
		t.Error("expected false from bare context")
	}
}
