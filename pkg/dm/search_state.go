package dm

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/gezibash/arc/v2/pkg/reference"
)

// SearchState tracks sync state between local and remote search indexes.
type SearchState struct {
	LocalHash  string `json:"local_hash"`
	RemoteHash string `json:"remote_hash"`

	path string `json:"-"`
}

// LoadSearchState reads state from disk. Returns a zero state if the file
// doesn't exist.
func LoadSearchState(dir string) *SearchState {
	p := filepath.Join(dir, "search.state.json")
	s := &SearchState{path: p}
	data, err := os.ReadFile(filepath.Clean(p))
	if err != nil {
		return s
	}
	_ = json.Unmarshal(data, s)
	return s
}

// Save writes the state to disk.
func (s *SearchState) Save() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0600)
}

// LocalRef parses LocalHash as a reference, or returns zero.
func (s *SearchState) LocalRef() reference.Reference {
	r, _ := reference.FromHex(s.LocalHash)
	return r
}

// RemoteRef parses RemoteHash as a reference, or returns zero.
func (s *SearchState) RemoteRef() reference.Reference {
	r, _ := reference.FromHex(s.RemoteHash)
	return r
}
