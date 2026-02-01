package dm

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/gezibash/arc-node/cmd/arc/render"
	"github.com/gezibash/arc/v2/pkg/identity"
)

// readInput reads data from the first positional arg — treated as a file path
// if it exists on disk, otherwise as literal text. With no args, reads stdin.
func readInput(args []string) ([]byte, error) {
	if len(args) == 1 {
		data, err := os.ReadFile(args[0])
		if err == nil {
			return data, nil
		}
		if errors.Is(err, os.ErrNotExist) {
			return []byte(args[0]), nil
		}
		return nil, fmt.Errorf("read input: %w", err)
	}
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return nil, fmt.Errorf("read input: %w", err)
	}
	return data, nil
}

func parseLabels(labels []string) (map[string]string, error) {
	return render.ParseLabels(labels)
}

func writeJSON(w io.Writer, v any) error {
	return render.WriteJSON(w, v)
}

// senderLabel returns "You" if the from key matches ours, otherwise a truncated hex.
func senderLabel(from [32]byte, self identity.PublicKey) string {
	if from == self {
		return "You"
	}
	return hex.EncodeToString(from[:4])
}
