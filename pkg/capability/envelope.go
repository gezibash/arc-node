// Package capability provides a framework for building arc capabilities.
//
// Capabilities are services that connect to the relay, subscribe to envelopes
// with specific labels, and handle requests. This package eliminates boilerplate
// by providing:
//
//   - Server: Handles relay connection, subscription, and message dispatch
//   - Handler: Simple interface for processing envelopes
//   - Client: Helper for making capability requests through the relay
//
// Example usage:
//
//	handler := &MyHandler{store: store}
//	server, _ := capability.NewServer(capability.ServerConfig{
//	    Name:    "myservice",
//	    Handler: handler,
//	    Relay:   relayClient,
//	    Log:     log,
//	})
//	server.Run(ctx)
package capability

import "github.com/gezibash/arc/v2/pkg/identity"

// Envelope represents an incoming request envelope.
type Envelope struct {
	// Ref is the unique identifier for this envelope.
	Ref []byte

	// Sender is the public key of the envelope sender.
	Sender identity.PublicKey

	// Labels are the routing labels attached to this envelope.
	Labels map[string]string

	// Action is extracted from labels["action"] for convenience.
	// Empty if no action label present.
	Action string

	// Payload is the raw message payload.
	Payload []byte

	// SubscriptionID identifies which subscription matched this envelope.
	SubscriptionID string
}

// Response represents a response to send back to the requester.
type Response struct {
	// Labels to add to the response envelope.
	Labels map[string]string

	// Payload is the response data.
	Payload []byte

	// Error, if non-empty, indicates an error response.
	// Will be sent as labels["error"] = Error.
	Error string

	// NoReply, if true, suppresses sending a response.
	NoReply bool
}

// OK creates a success response with the given payload.
func OK(payload []byte) *Response {
	return &Response{Payload: payload}
}

// OKWithLabels creates a success response with payload and labels.
func OKWithLabels(payload []byte, labels map[string]string) *Response {
	return &Response{Payload: payload, Labels: labels}
}

// Err creates an error response.
func Err(message string) *Response {
	return &Response{Error: message}
}

// NoReply creates a response that suppresses sending any reply.
func NoReply() *Response {
	return &Response{NoReply: true}
}
