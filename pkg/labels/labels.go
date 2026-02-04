// Package labels provides utilities for parsing and formatting envelope labels.
package labels

import (
	"fmt"
	"sort"
	"strings"
)

// Parse converts a slice of "key=value" strings to a map.
// Returns an error if any label is malformed.
func Parse(labels []string) (map[string]string, error) {
	result := make(map[string]string, len(labels))
	for _, l := range labels {
		parts := strings.SplitN(l, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid label format: %s (expected key=value)", l)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" {
			return nil, fmt.Errorf("invalid label: empty key in %q", l)
		}
		result[key] = value
	}
	return result, nil
}

// Format converts a label map to a display string.
// Labels are sorted alphabetically for consistent output.
// Returns "-" for empty maps.
func Format(labels map[string]string) string {
	if len(labels) == 0 {
		return "-"
	}

	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		parts = append(parts, k+"="+labels[k])
	}
	return strings.Join(parts, ", ")
}

// Merge combines multiple label maps. Later maps override earlier ones.
func Merge(labelMaps ...map[string]string) map[string]string {
	result := make(map[string]string)
	for _, m := range labelMaps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

// Has returns true if the map contains all required key-value pairs.
func Has(labels, required map[string]string) bool {
	for k, v := range required {
		if labels[k] != v {
			return false
		}
	}
	return true
}
