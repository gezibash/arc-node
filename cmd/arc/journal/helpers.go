package journal

import (
	"io"

	"github.com/gezibash/arc-node/cmd/arc/render"
)

func readInput(args []string) ([]byte, error) {
	return render.ReadInput(args)
}

func parseLabels(labels []string) (map[string]string, error) {
	return render.ParseLabels(labels)
}

func writeJSON(w io.Writer, v any) error {
	return render.WriteJSON(w, v)
}
