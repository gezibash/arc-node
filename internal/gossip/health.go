package gossip

import (
	"encoding/binary"
	"fmt"
)

// HealthMeta is encoded into memberlist NodeMeta (must fit in 512 bytes).
// Contains infrastructure metadata about this relay node.
type HealthMeta struct {
	GRPCAddr    string // this relay's gRPC address
	Pubkey      string // hex-encoded relay pubkey
	Connections uint32 // number of connected subscribers
	Uptime      uint64 // nanoseconds since relay started
	Version     string // relay version string
	Load        uint16 // load percentage (0-1000, i.e. 10x for one decimal)
}

// Encode serializes HealthMeta to binary.
// Format: [grpcAddrLen:2][grpcAddr][pubkeyLen:2][pubkey][connections:4][uptime:8][versionLen:1][version][load:2]
func (h *HealthMeta) Encode() []byte {
	grpcBytes := []byte(h.GRPCAddr)
	pubkeyBytes := []byte(h.Pubkey)
	versionBytes := []byte(h.Version)

	size := 2 + len(grpcBytes) + 2 + len(pubkeyBytes) + 4 + 8 + 1 + len(versionBytes) + 2
	buf := make([]byte, 0, size)

	buf = binary.BigEndian.AppendUint16(buf, uint16(len(grpcBytes)))
	buf = append(buf, grpcBytes...)

	buf = binary.BigEndian.AppendUint16(buf, uint16(len(pubkeyBytes)))
	buf = append(buf, pubkeyBytes...)

	buf = binary.BigEndian.AppendUint32(buf, h.Connections)
	buf = binary.BigEndian.AppendUint64(buf, h.Uptime)

	if len(versionBytes) > 255 {
		versionBytes = versionBytes[:255]
	}
	buf = append(buf, byte(len(versionBytes)))
	buf = append(buf, versionBytes...)

	buf = binary.BigEndian.AppendUint16(buf, h.Load)

	return buf
}

// DecodeHealthMeta deserializes HealthMeta from binary.
func DecodeHealthMeta(data []byte) (HealthMeta, error) {
	var h HealthMeta
	pos := 0

	if pos+2 > len(data) {
		return h, fmt.Errorf("truncated grpc addr length")
	}
	grpcLen := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2
	if pos+grpcLen > len(data) {
		return h, fmt.Errorf("truncated grpc addr")
	}
	h.GRPCAddr = string(data[pos : pos+grpcLen])
	pos += grpcLen

	if pos+2 > len(data) {
		return h, fmt.Errorf("truncated pubkey length")
	}
	pubkeyLen := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2
	if pos+pubkeyLen > len(data) {
		return h, fmt.Errorf("truncated pubkey")
	}
	h.Pubkey = string(data[pos : pos+pubkeyLen])
	pos += pubkeyLen

	if pos+4 > len(data) {
		return h, fmt.Errorf("truncated connections")
	}
	h.Connections = binary.BigEndian.Uint32(data[pos:])
	pos += 4

	if pos+8 > len(data) {
		return h, fmt.Errorf("truncated uptime")
	}
	h.Uptime = binary.BigEndian.Uint64(data[pos:])
	pos += 8

	if pos+1 > len(data) {
		return h, fmt.Errorf("truncated version length")
	}
	versionLen := int(data[pos])
	pos++
	if pos+versionLen > len(data) {
		return h, fmt.Errorf("truncated version")
	}
	h.Version = string(data[pos : pos+versionLen])
	pos += versionLen

	if pos+2 > len(data) {
		return h, fmt.Errorf("truncated load")
	}
	h.Load = binary.BigEndian.Uint16(data[pos:])

	return h, nil
}
