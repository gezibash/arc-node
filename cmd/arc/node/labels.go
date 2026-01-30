package node

import (
	"fmt"
	"strings"
)

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
