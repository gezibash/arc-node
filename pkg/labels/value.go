package labels

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ValidValue reports whether v is one of the four allowed label types:
// string, int64, float64, or bool.
func ValidValue(v any) bool {
	switch v.(type) {
	case string, int64, float64, bool:
		return true
	default:
		return false
	}
}

// ValidateMap checks that every value in m is a valid label type.
func ValidateMap(m map[string]any) error {
	for k, v := range m {
		if !ValidValue(v) {
			return fmt.Errorf("label %q: unsupported type %T (want string, int64, float64, or bool)", k, v)
		}
	}
	return nil
}

// ParseTyped converts a slice of "key=value" strings to a typed map.
// Type inference order: int64 → float64 → bool → string.
func ParseTyped(labels []string) (map[string]any, error) {
	result := make(map[string]any, len(labels))
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
		result[key] = inferType(value)
	}
	return result, nil
}

// inferType attempts to parse s as int64, float64, bool, falling back to string.
func inferType(s string) any {
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v
	}
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return v
	}
	if v, err := strconv.ParseBool(s); err == nil {
		return v
	}
	return s
}

// FormatAny converts a typed label map to a display string.
// Labels are sorted alphabetically. Returns "-" for empty maps.
func FormatAny(m map[string]any) string {
	if len(m) == 0 {
		return "-"
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+formatValue(m[k]))
	}
	return strings.Join(parts, ", ")
}

// formatValue renders a typed value as a string.
func formatValue(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(val)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// MergeAny combines multiple typed label maps. Later maps override earlier ones.
func MergeAny(maps ...map[string]any) map[string]any {
	result := make(map[string]any)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

// ToStringMap extracts only string-valued entries from a typed map.
func ToStringMap(m map[string]any) map[string]string {
	result := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			result[k] = s
		}
	}
	return result
}

// FromStringMap wraps a string map as a typed map.
func FromStringMap(m map[string]string) map[string]any {
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

// HasAny returns true if labels contains all required string key-value pairs.
// Only string-valued entries in labels are compared. Non-string values are skipped.
func HasAny(labels map[string]any, required map[string]string) bool {
	for k, v := range required {
		lv, ok := labels[k]
		if !ok {
			return false
		}
		s, ok := lv.(string)
		if !ok || s != v {
			return false
		}
	}
	return true
}
