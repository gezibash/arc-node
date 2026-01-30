package journal

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
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
	m := make(map[string]string, len(labels))
	for _, l := range labels {
		parts := strings.SplitN(l, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid label format %q, expected key=value", l)
		}
		m[parts[0]] = parts[1]
	}
	return m, nil
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
