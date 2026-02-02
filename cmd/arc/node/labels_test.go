package node

import (
	"testing"
)

func TestParseLabels(t *testing.T) {
	tests := []struct {
		name    string
		input   []string
		want    map[string]string
		wantErr bool
	}{
		{
			name:  "empty",
			input: nil,
			want:  map[string]string{},
		},
		{
			name:  "single",
			input: []string{"app=journal"},
			want:  map[string]string{"app": "journal"},
		},
		{
			name:  "multiple",
			input: []string{"app=journal", "priority=high"},
			want:  map[string]string{"app": "journal", "priority": "high"},
		},
		{
			name:  "value with equals",
			input: []string{"query=a=b"},
			want:  map[string]string{"query": "a=b"},
		},
		{
			name:    "missing equals",
			input:   []string{"invalid"},
			wantErr: true,
		},
		{
			name:  "empty key",
			input: []string{"=value"},
			want:  map[string]string{"": "value"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLabels(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d labels, want %d", len(got), len(tt.want))
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("label %q: got %q, want %q", k, got[k], v)
				}
			}
		})
	}
}
