package relay

import (
	"testing"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity/ed25519"
)

func TestSignEnvelope(t *testing.T) {
	kp, err := ed25519.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}

	env := &relayv1.Envelope{
		Ref:     []byte("test-ref"),
		Labels:  map[string]string{"to": "@alice", "type": "message"},
		Payload: []byte("hello world"),
		Sender:  []byte("sender-pubkey"),
	}

	// Sign envelope
	SignEnvelope(env, kp)

	// Check signature was added
	if len(env.GetRelaySignature()) == 0 {
		t.Errorf("expected signature, got empty")
	}
	if len(env.GetRelayPubkey()) == 0 {
		t.Errorf("expected pubkey, got empty")
	}

	// Verify signature
	if !VerifyEnvelopeRelaySignature(env) {
		t.Error("envelope signature verification failed")
	}

	// Tamper with payload and verify fails
	env.Payload = []byte("tampered")
	if VerifyEnvelopeRelaySignature(env) {
		t.Error("expected verification to fail after tampering")
	}
}

func TestSignEnvelopeNilKeypair(t *testing.T) {
	env := &relayv1.Envelope{
		Ref:     []byte("test-ref"),
		Labels:  map[string]string{"type": "test"},
		Payload: []byte("data"),
	}

	// Should not panic with nil keypair
	SignEnvelope(env, nil)

	// Fields should remain empty
	if len(env.GetRelaySignature()) != 0 {
		t.Error("expected empty signature with nil keypair")
	}
}

func TestSignReceipt(t *testing.T) {
	kp, err := ed25519.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}

	receipt := &relayv1.Receipt{
		Ref:         []byte("test-ref"),
		Correlation: "corr-123",
		Status:      relayv1.ReceiptStatus_RECEIPT_STATUS_ACK,
		Delivered:   3,
		Reason:      "",
	}

	// Sign receipt
	SignReceipt(receipt, kp)

	// Check signature was added
	if len(receipt.GetRelaySignature()) == 0 {
		t.Errorf("expected signature, got empty")
	}
	if len(receipt.GetRelayPubkey()) == 0 {
		t.Errorf("expected pubkey, got empty")
	}

	// Verify signature
	if !VerifyReceiptSignature(receipt) {
		t.Error("receipt signature verification failed")
	}

	// Tamper with delivered count and verify fails
	receipt.Delivered = 99
	if VerifyReceiptSignature(receipt) {
		t.Error("expected verification to fail after tampering")
	}
}

func TestVerifyEnvelopeInvalidSignature(t *testing.T) {
	env := &relayv1.Envelope{
		Ref:            []byte("test-ref"),
		Labels:         map[string]string{},
		Payload:        []byte("data"),
		RelaySignature: []byte("too-short"),
		RelayPubkey:    []byte("also-short"),
	}

	if VerifyEnvelopeRelaySignature(env) {
		t.Error("expected verification to fail with invalid signature length")
	}
}

func TestVerifyReceiptInvalidSignature(t *testing.T) {
	receipt := &relayv1.Receipt{
		Ref:            []byte("test-ref"),
		RelaySignature: []byte("invalid"),
		RelayPubkey:    []byte("invalid"),
	}

	if VerifyReceiptSignature(receipt) {
		t.Error("expected verification to fail with invalid signature length")
	}
}
