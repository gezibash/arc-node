package node

import (
	"errors"
	"fmt"
	"io"
	"os"
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
