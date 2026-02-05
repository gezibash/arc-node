package storage

import (
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// GetString retrieves a string value from config, returning defaultValue if not present or empty.
func GetString(config map[string]string, key, defaultValue string) string {
	if v, ok := config[key]; ok && v != "" {
		return v
	}
	return defaultValue
}

// GetBool retrieves a boolean value from config.
// Accepts "true", "false", "1", "0", "yes", "no" (case-insensitive).
// Returns defaultValue if key is not present.
func GetBool(config map[string]string, key string, defaultValue bool) (bool, error) {
	v, ok := config[key]
	if !ok || v == "" {
		return defaultValue, nil
	}

	switch strings.ToLower(v) {
	case "true", "1", "yes":
		return true, nil
	case "false", "0", "no":
		return false, nil
	default:
		return false, &ConfigError{
			Field:   key,
			Value:   v,
			Message: "must be a boolean (true/false, 1/0, yes/no)",
		}
	}
}

// GetInt retrieves an integer value from config.
func GetInt(config map[string]string, key string, defaultValue int) (int, error) {
	v, ok := config[key]
	if !ok || v == "" {
		return defaultValue, nil
	}

	i, err := strconv.Atoi(v)
	if err != nil {
		return 0, &ConfigError{
			Field:   key,
			Value:   v,
			Message: "must be an integer",
			Cause:   err,
		}
	}
	return i, nil
}

// GetInt64 retrieves an int64 value from config.
func GetInt64(config map[string]string, key string, defaultValue int64) (int64, error) {
	v, ok := config[key]
	if !ok || v == "" {
		return defaultValue, nil
	}

	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, &ConfigError{
			Field:   key,
			Value:   v,
			Message: "must be an integer",
			Cause:   err,
		}
	}
	return i, nil
}

// GetDuration retrieves a duration value from config.
// Accepts Go duration strings (e.g., "5s", "1m30s") or plain integers as seconds.
func GetDuration(config map[string]string, key string, defaultValue time.Duration) (time.Duration, error) {
	v, ok := config[key]
	if !ok || v == "" {
		return defaultValue, nil
	}

	d, err := time.ParseDuration(v)
	if err == nil {
		return d, nil
	}

	secs, err := strconv.ParseInt(v, 10, 64)
	if err == nil {
		return time.Duration(secs) * time.Second, nil
	}

	return 0, &ConfigError{
		Field:   key,
		Value:   v,
		Message: "must be a duration (e.g., '5s', '1m30s') or integer seconds",
	}
}

// ExpandPath expands ~ to the user's home directory and cleans the path.
func ExpandPath(path string) string {
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return path
		}
		return filepath.Join(home, path[2:])
	}
	return filepath.Clean(path)
}

// MergeConfig merges src into dst, returning a new map.
// Values from src override values from dst.
func MergeConfig(dst, src map[string]string) map[string]string {
	result := make(map[string]string, len(dst)+len(src))
	maps.Copy(result, dst)
	maps.Copy(result, src)
	return result
}
