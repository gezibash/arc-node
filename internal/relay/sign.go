package relay

import (
	"bytes"
	"encoding/binary"
	"sort"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
)

// SignEnvelope signs an envelope with the relay's signer.
// Sets relay_signature and relay_pubkey fields.
func SignEnvelope(env *relayv1.Envelope, signer identity.Signer) {
	if signer == nil {
		return
	}

	// Build canonical bytes: ref + sorted labels + payload + sender
	data := envelopeSigningBytes(env)

	// Sign
	sig, err := signer.Sign(data)
	if err != nil {
		return
	}
	pk := signer.PublicKey()

	env.RelaySignature = []byte(identity.EncodeSignature(sig))
	env.RelayPubkey = []byte(identity.EncodePublicKey(pk))
}

// VerifyEnvelopeRelaySignature verifies the relay signature on an envelope.
func VerifyEnvelopeRelaySignature(env *relayv1.Envelope) bool {
	sig, err := identity.DecodeSignatureBytes(env.GetRelaySignature())
	if err != nil {
		return false
	}
	pk, err := identity.DecodePublicKeyBytes(env.GetRelayPubkey())
	if err != nil {
		return false
	}

	data := envelopeSigningBytes(env)
	return identity.Verify(pk, data, sig)
}

// envelopeSigningBytes returns canonical bytes for signing: ref + sorted labels + payload + sender
func envelopeSigningBytes(env *relayv1.Envelope) []byte {
	var buf bytes.Buffer

	// ref
	buf.Write(env.GetRef())

	// sorted labels
	labels := env.GetLabels()
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		buf.WriteString(k)
		buf.WriteByte(0)
		buf.WriteString(labels[k])
		buf.WriteByte(0)
	}

	// payload
	buf.Write(env.GetPayload())

	// sender
	buf.Write(env.GetSender())

	return buf.Bytes()
}

// SignReceipt signs a receipt with the relay's keypair.
// Sets relay_signature and relay_pubkey fields.
func SignReceipt(r *relayv1.Receipt, signer identity.Signer) {
	if signer == nil {
		return
	}

	// Build canonical bytes: ref + correlation + status + delivered + reason
	data := receiptSigningBytes(r)

	// Sign
	sig, err := signer.Sign(data)
	if err != nil {
		return
	}
	pk := signer.PublicKey()

	r.RelaySignature = []byte(identity.EncodeSignature(sig))
	r.RelayPubkey = []byte(identity.EncodePublicKey(pk))
}

// VerifyReceiptSignature verifies the relay signature on a receipt.
func VerifyReceiptSignature(r *relayv1.Receipt) bool {
	sig, err := identity.DecodeSignatureBytes(r.GetRelaySignature())
	if err != nil {
		return false
	}
	pk, err := identity.DecodePublicKeyBytes(r.GetRelayPubkey())
	if err != nil {
		return false
	}

	data := receiptSigningBytes(r)
	return identity.Verify(pk, data, sig)
}

// receiptSigningBytes returns canonical bytes for signing: ref + correlation + status + delivered + reason
func receiptSigningBytes(r *relayv1.Receipt) []byte {
	var buf bytes.Buffer

	// ref
	buf.Write(r.GetRef())

	// correlation
	buf.WriteString(r.GetCorrelation())
	buf.WriteByte(0)

	// status (as uint32)
	var statusBuf [4]byte
	binary.BigEndian.PutUint32(statusBuf[:], uint32(r.GetStatus()))
	buf.Write(statusBuf[:])

	// delivered (as int32)
	var deliveredBuf [4]byte
	binary.BigEndian.PutUint32(deliveredBuf[:], uint32(r.GetDelivered()))
	buf.Write(deliveredBuf[:])

	// reason
	buf.WriteString(r.GetReason())

	return buf.Bytes()
}
