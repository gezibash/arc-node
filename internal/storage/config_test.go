package storage

import (
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
