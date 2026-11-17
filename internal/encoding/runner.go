package encoding

import (
	"cadenceforge/internal/blob"
	"cadenceforge/internal/domain"
	"cadenceforge/internal/media"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Runner struct {
	Binary string
	Blobs  *blob.Store
}

func (r Runner) Encode(ctx context.Context, render domain.Render, stage string) (string, error) {
	paths := map[string]string{}
	for id, s := range render.Sources {
		path, err := r.Blobs.Object(s.Digest)
		if err != nil {
			return "", err
		}
		if _, err = os.Stat(path); err != nil {
			return "", fmt.Errorf("source %s unavailable: %w", id, err)
		}
		paths[id] = path
	}
	name := "composition." + render.Recipe.Format
	args, err := Arguments(render, paths, filepath.Join(stage, name))
	if err != nil {
		return "", err
	}
	// Formal composition encoding is the long-lived policy owner: its 3 minute
	// deadline must outlast legitimate full-length films, unlike the short
	// probe/inspection deadlines, while sharing the same process lifecycle.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if _, err = media.Run(ctx, r.Binary, args, 65536); err != nil {
		return "", err
	}
	if err = os.WriteFile(filepath.Join(stage, "captions.vtt"), []byte(render.Plan.VTT), 0600); err != nil {
		return "", err
	}
	return name, nil
}
