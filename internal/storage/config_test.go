package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGetString(t *testing.T) {
	config := map[string]string{"key": "value"}

	if got := GetString(config, "key", "default"); got != "value" {
		t.Errorf("GetString = %q, want %q", got, "value")
	}
	if got := GetString(config, "missing", "default"); got != "default" {
		t.Errorf("GetString = %q, want %q", got, "default")
	}
	if got := GetString(map[string]string{"key": ""}, "key", "default"); got != "default" {
		t.Errorf("GetString empty = %q, want %q", got, "default")
	}
}

func TestGetBool(t *testing.T) {
	config := map[string]string{"yes": "true", "no": "false", "bad": "maybe"}

	if v, err := GetBool(config, "yes", false); err != nil || !v {
		t.Errorf("GetBool yes: got %v, %v", v, err)
	}
	if v, err := GetBool(config, "no", true); err != nil || v {
		t.Errorf("GetBool no: got %v, %v", v, err)
	}
	if v, err := GetBool(config, "missing", true); err != nil || !v {
		t.Errorf("GetBool missing: got %v, %v", v, err)
	}
	if _, err := GetBool(config, "bad", false); err == nil {
		t.Error("GetBool bad: expected error")
	}
}

func TestGetInt(t *testing.T) {
	config := map[string]string{"num": "42", "bad": "abc"}

	if v, err := GetInt(config, "num", 0); err != nil || v != 42 {
		t.Errorf("GetInt = %d, %v", v, err)
	}
	if v, err := GetInt(config, "missing", 99); err != nil || v != 99 {
		t.Errorf("GetInt missing = %d, %v", v, err)
	}
	if _, err := GetInt(config, "bad", 0); err == nil {
		t.Error("GetInt bad: expected error")
	}
}

func TestGetDuration(t *testing.T) {
	config := map[string]string{"dur": "5s", "secs": "10", "bad": "abc"}

	if v, err := GetDuration(config, "dur", 0); err != nil || v != 5*time.Second {
		t.Errorf("GetDuration dur = %v, %v", v, err)
	}
	if v, err := GetDuration(config, "secs", 0); err != nil || v != 10*time.Second {
		t.Errorf("GetDuration secs = %v, %v", v, err)
	}
	if _, err := GetDuration(config, "bad", 0); err == nil {
		t.Error("GetDuration bad: expected error")
	}
}

func TestExpandPath(t *testing.T) {
	if got := ExpandPath("/absolute/path"); got != "/absolute/path" {
		t.Errorf("ExpandPath absolute = %q", got)
	}
	if got := ExpandPath("relative/path"); got != "relative/path" {
		t.Errorf("ExpandPath relative = %q", got)
	}
}

func TestMergeConfig(t *testing.T) {
	dst := map[string]string{"a": "1", "b": "2"}
	src := map[string]string{"b": "3", "c": "4"}
	result := MergeConfig(dst, src)

	if result["a"] != "1" || result["b"] != "3" || result["c"] != "4" {
		t.Errorf("MergeConfig = %v", result)
	}
	// Verify original maps unchanged
	if dst["b"] != "2" {
		t.Error("MergeConfig modified dst")
	}
}

func TestGetInt64(t *testing.T) {
	config := map[string]string{"num": "9223372036854775807", "zero": "0", "bad": "abc"}

	if v, err := GetInt64(config, "num", 0); err != nil || v != 9223372036854775807 {
		t.Errorf("GetInt64 = %d, %v", v, err)
	}
	if v, err := GetInt64(config, "zero", 99); err != nil || v != 0 {
		t.Errorf("GetInt64 zero = %d, %v", v, err)
	}
	if v, err := GetInt64(config, "missing", 99); err != nil || v != 99 {
		t.Errorf("GetInt64 missing = %d, %v", v, err)
	}
	if v, err := GetInt64(config, "bad", 0); err == nil {
		t.Errorf("GetInt64 bad: expected error, got %d", v)
	}
	// Empty value returns default.
	if v, err := GetInt64(map[string]string{"k": ""}, "k", 7); err != nil || v != 7 {
		t.Errorf("GetInt64 empty = %d, %v", v, err)
	}
}

func TestExpandPath_Home(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("cannot determine home dir")
	}
	got := ExpandPath("~/subdir/file")
	want := filepath.Join(home, "subdir/file")
	if got != want {
		t.Errorf("ExpandPath ~/subdir/file = %q, want %q", got, want)
	}
}

func TestConfigError_Error(t *testing.T) {
	tests := []struct {
		name string
		err  ConfigError
		want string
	}{
		{
			name: "backend only",
			err:  ConfigError{Backend: "badger", Message: "failed"},
			want: "badger: failed",
		},
		{
			name: "field no value",
			err:  ConfigError{Backend: "badger", Field: "path", Message: "required"},
			want: "badger: path: required",
		},
		{
			name: "field with value",
			err:  ConfigError{Backend: "badger", Field: "path", Value: "/tmp", Message: "invalid"},
			want: `badger: path="/tmp": invalid`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestConfigError_Unwrap(t *testing.T) {
	cause := errors.New("underlying")
	ce := &ConfigError{Backend: "s3", Field: "bucket", Message: "bad", Cause: cause}
	if !errors.Is(ce, cause) {
		t.Error("Unwrap: expected to find cause via errors.Is")
	}

	ce2 := &ConfigError{Backend: "s3", Message: "no cause"}
	if ce2.Unwrap() != nil {
		t.Error("Unwrap: expected nil when no cause")
	}
}

func TestNewConfigError(t *testing.T) {
	ce := NewConfigError("redis", "host", "required")
	if ce.Backend != "redis" || ce.Field != "host" || ce.Message != "required" {
		t.Errorf("NewConfigError = %+v", ce)
	}
	if !strings.Contains(ce.Error(), "redis: host: required") {
		t.Errorf("Error() = %q", ce.Error())
	}
}

func TestNewConfigErrorWithValue(t *testing.T) {
	ce := NewConfigErrorWithValue("redis", "port", "abc", "must be integer")
	if ce.Backend != "redis" || ce.Field != "port" || ce.Value != "abc" || ce.Message != "must be integer" {
		t.Errorf("NewConfigErrorWithValue = %+v", ce)
	}
}

func TestNewConfigErrorWithCause(t *testing.T) {
	cause := errors.New("parse error")
	ce := NewConfigErrorWithCause("sqlite", "path", "open failed", cause)
	if ce.Backend != "sqlite" || ce.Field != "path" || ce.Cause != cause {
		t.Errorf("NewConfigErrorWithCause = %+v", ce)
	}
	if !errors.Is(ce, cause) {
		t.Error("expected cause to be unwrappable")
	}
}
