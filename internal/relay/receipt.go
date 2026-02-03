package relay

import (
	relayv1 "github.com/gezibash/arc-node/api/arc/relay/v1"
)

// ReceiptReason is a machine-readable reason for NACK.
type ReceiptReason string

const (
	ReasonNoRoute    ReceiptReason = "NO_ROUTE"
	ReasonInvalidSig ReceiptReason = "INVALID_SIGNATURE"
	ReasonRateLimit  ReceiptReason = "RATE_LIMITED"
	ReasonTooLarge   ReceiptReason = "TOO_LARGE"
)

// NewACK creates an ACK receipt.
func NewACK(ref []byte, correlation string, delivered int) *relayv1.Receipt {
	return &relayv1.Receipt{
		Ref:         ref,
		Correlation: correlation,
		Status:      relayv1.ReceiptStatus_RECEIPT_STATUS_ACK,
		Delivered:   int32(delivered),
	}
}

// NewNACK creates a NACK receipt with reason.
func NewNACK(ref []byte, correlation string, reason ReceiptReason) *relayv1.Receipt {
	return &relayv1.Receipt{
		Ref:         ref,
		Correlation: correlation,
		Status:      relayv1.ReceiptStatus_RECEIPT_STATUS_NACK,
		Delivered:   0,
		Reason:      string(reason),
	}
}

// WrapReceipt wraps a receipt in a ServerFrame.
func WrapReceipt(r *relayv1.Receipt) *relayv1.ServerFrame {
	return &relayv1.ServerFrame{
		Frame: &relayv1.ServerFrame_Receipt{
			Receipt: &relayv1.ReceiptFrame{Receipt: r},
		},
	}
}

// WrapDeliver wraps an envelope delivery in a ServerFrame.
func WrapDeliver(env *relayv1.Envelope, subscriptionID string) *relayv1.ServerFrame {
	return &relayv1.ServerFrame{
		Frame: &relayv1.ServerFrame_Deliver{
			Deliver: &relayv1.DeliverFrame{
				Envelope:       env,
				SubscriptionId: subscriptionID,
			},
		},
	}
}

// WrapError wraps an error in a ServerFrame.
func WrapError(code int32, message, detail, correlation string, retryable bool) *relayv1.ServerFrame {
	return &relayv1.ServerFrame{
		Frame: &relayv1.ServerFrame_Error{
			Error: &relayv1.ErrorFrame{
				Code:        code,
				Message:     message,
				Detail:      detail,
				Retryable:   retryable,
				Correlation: correlation,
			},
		},
	}
}

// WrapPong wraps a pong response in a ServerFrame.
func WrapPong(nonce []byte, serverTime int64) *relayv1.ServerFrame {
	return &relayv1.ServerFrame{
		Frame: &relayv1.ServerFrame_Pong{
			Pong: &relayv1.PongFrame{
				Nonce:      nonce,
				ServerTime: serverTime,
			},
		},
	}
}
