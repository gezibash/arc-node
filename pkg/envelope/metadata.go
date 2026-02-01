package envelope

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
	grpcmd "google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	keyFromBin       = "arc-from-bin"
	keyToBin         = "arc-to-bin"
	keyTimestamp     = "arc-timestamp"
	keySignatureBin  = "arc-signature-bin"
	keyContentType   = "arc-content-type"
	keyOriginBin     = "arc-origin-bin"
	keyHopCount      = "arc-hop-count"
	keyMetaPrefix    = "arc-meta-"
	keyDimensionsBin = "arc-dimensions-bin"
)

// Extract pulls envelope fields from incoming gRPC metadata.
func Extract(md grpcmd.MD) (from, to, origin identity.PublicKey, ts int64, sig identity.Signature, contentType string, hopCount int, meta map[string]string, dims *nodev1.Dimensions, err error) {
	from, err = extractPubKey(md, keyFromBin)
	if err != nil {
		return
	}
	to, err = extractPubKey(md, keyToBin)
	if err != nil {
		return
	}

	tsStr := firstVal(md, keyTimestamp)
	if tsStr == "" {
		err = fmt.Errorf("missing %s", keyTimestamp)
		return
	}
	ts, err = strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		err = fmt.Errorf("parse %s: %w", keyTimestamp, err)
		return
	}

	sig, err = extractSignature(md, keySignatureBin)
	if err != nil {
		return
	}

	contentType = firstVal(md, keyContentType)

	origin, err = extractPubKey(md, keyOriginBin)
	if err != nil {
		return
	}

	hcStr := firstVal(md, keyHopCount)
	if hcStr != "" {
		hopCount, err = strconv.Atoi(hcStr)
		if err != nil {
			err = fmt.Errorf("parse %s: %w", keyHopCount, err)
			return
		}
	}

	meta = make(map[string]string)
	for k, vals := range md {
		if strings.HasPrefix(k, keyMetaPrefix) && len(vals) > 0 {
			meta[strings.TrimPrefix(k, keyMetaPrefix)] = vals[0]
		}
	}

	dims = extractDimensions(md)

	return
}

// InjectOutgoing sets envelope fields as outgoing gRPC metadata (for client requests).
func InjectOutgoing(ctx context.Context, env *Envelope, sig identity.Signature) context.Context {
	md := grpcmd.Pairs(
		keyFromBin, string(env.Message.From[:]),
		keyToBin, string(env.Message.To[:]),
		keyTimestamp, strconv.FormatInt(env.Message.Timestamp, 10),
		keySignatureBin, string(sig[:]),
		keyContentType, env.Message.ContentType,
		keyOriginBin, string(env.Origin[:]),
		keyHopCount, strconv.Itoa(env.HopCount),
	)

	for k, v := range env.Metadata {
		md.Append(keyMetaPrefix+k, v)
	}

	if env.Dimensions != nil {
		if b, err := proto.Marshal(env.Dimensions); err == nil {
			md.Append(keyDimensionsBin, string(b))
		}
	}

	return grpcmd.NewOutgoingContext(ctx, md)
}

func extractDimensions(md grpcmd.MD) *nodev1.Dimensions {
	v := firstVal(md, keyDimensionsBin)
	if v == "" {
		return &nodev1.Dimensions{}
	}
	dims := &nodev1.Dimensions{}
	if err := proto.Unmarshal([]byte(v), dims); err != nil {
		return &nodev1.Dimensions{}
	}
	return dims
}

func extractPubKey(md grpcmd.MD, key string) (identity.PublicKey, error) {
	var pk identity.PublicKey
	v := firstVal(md, key)
	if v == "" {
		return pk, fmt.Errorf("missing %s", key)
	}
	b := []byte(v)
	if len(b) != 32 {
		return pk, fmt.Errorf("%s: expected 32 bytes, got %d", key, len(b))
	}
	copy(pk[:], b)
	return pk, nil
}

func extractSignature(md grpcmd.MD, key string) (identity.Signature, error) {
	var sig identity.Signature
	v := firstVal(md, key)
	if v == "" {
		return sig, fmt.Errorf("missing %s", key)
	}
	b := []byte(v)
	if len(b) != 64 {
		return sig, fmt.Errorf("%s: expected 64 bytes, got %d", key, len(b))
	}
	copy(sig[:], b)
	return sig, nil
}

func firstVal(md grpcmd.MD, key string) string {
	vals := md.Get(key)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

