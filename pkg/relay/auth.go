package relay

import (
	"fmt"

	"github.com/gezibash/arc/v2/pkg/identity"
)

func authPayload(pub identity.PublicKey, ts int64) []byte {
	algo := pub.Algo
	if algo == "" {
		algo = identity.AlgEd25519
	}
	return []byte(fmt.Sprintf("arc-auth-v1:%s:%s:%d", algo, pubKeyHex(pub), ts))
}

func pubKeyHex(pub identity.PublicKey) string {
	return fmt.Sprintf("%x", pub.Bytes)
}
