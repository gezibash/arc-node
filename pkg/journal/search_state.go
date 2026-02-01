package journal

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/gezibash/arc/v2/pkg/reference"
)

// SearchState tracks sync state between local and remote search indexes.
// Stored as ~/.arc/journal/search.state.json.
type SearchState struct {
	// LocalHash is the ContentHash of the local DB after the last push.
	LocalHash string `json:"local_hash"`
	// RemoteHash is the db-hash label from the last fetched remote index.
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
